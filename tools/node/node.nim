import std/random
import std/times
import std/strformat
import std/sugar

import pkg/chronos
import pkg/libp2p
import pkg/libp2p/protocols/protocol
import pkg/libp2p/crypto/secp
import pkg/libp2p_mix
import pkg/libp2p_mix/curve25519
import pkg/protobuf_serialization
import pkg/protobuf_serialization/std/enums
import pkg/stew/endians2

import ../../libp2p_mix_transport

const TransferCodec = "/test/simple-transfer/1.0.0"
const ChunkSize = 1024 * 1024 * 1024

type
  MixPool* = seq[MixPubInfo]

  Node* = object
    info*: MixNodeInfo
    switch*: Switch
    proto*: MixProtocol
    transport*: MixTransport

  MsgType {.pure.} = enum
    transferRequest = 1.byte
    transferResponse = 2.byte

  ResponseStatus = enum
    Accepted
    Error

  TransferRequest {.proto3.} = object
    size {.fieldNumber: 1, pint.}: int32
    seed {.fieldNumber: 2, pint.}: int64

  TransferResponse {.proto3.} = object
    status {.fieldNumber: 1, ext.}: ResponseStatus
    request {.fieldNumber: 2.}: TransferRequest

proc encodeMsg[T](msg: T, msgType: MsgType): seq[byte] =
  let data = Protobuf.encode(msg)
  # 1 byte for type + 2 for message length
  var buffer = newSeqUninit[byte](data.len + 1 + 2)
  let len = buffer.len.uint16.toLE

  buffer[0] = msgType.byte
  copyMem(addr buffer[1], addr len, 2)
  copyMem(addr buffer[3], addr data[0], data.len)
  buffer

proc decodeMsg[T](
    stream: Stream
): Future[T] {.async: (raises: [CancelledError, SerializationError, LPStreamError]).} =
  var len: uint16
  await stream.readExactly(addr len, 2)
  len = len.fromLE

  let data = newSeq[byte](len.int)
  await stream.readExactly(addr data[0], len.int)
  Protobuf.decode(data, T)

proc send[T](
    stream: Stream, msg: sink T, msgType: MsgType
) {.async: (raises: [CancelledError, LPStreamError]).} =
  let payload = encodeMsg(msg, msgType)
  await stream.write(payload)

proc request(
    stream: Stream, size: int32, seed: Option[int64] = int64.none
) {.async: (raises: [CancelledError, LPStreamError]).} =
  let now = getTime()
  var seedValue =
    if seed.isNone:
      now.toUnix * 1_000_000_000 + now.nanosecond
    else:
      seed.get()

  let payload =
    encodeMsg(TransferRequest(size: size, seed: seedValue), MsgType.transferRequest)
  await stream.write(payload) # sink

proc request*(
    self: Node, target: PeerId, size: int32, seed: Option[int64] = int64.none
): Future[Result[void, string]] {.async: (raises: [CancelledError, LPStreamError]).} =
  let stream = (await self.transport.dial(target, TransferCodec)).valueOr:
    return err(error)
  await request(stream, size, seed)

proc request*(
    self: Node, target: MultiAddress, size: int32, seed: Option[int64] = int64.none
): Future[void] {.async: (raises: [CancelledError, LPStreamError, DialFailedError]).} =
  let
    peerId = await self.switch.connect(target)
    stream = await self.switch.dial(peerId, TransferCodec)

  await request(stream, size, seed)

proc handleTransferRequest(
    stream: Stream, request: TransferRequest
) {.async: (raises: [CancelledError, LPStreamError]).} =
  # Send preamble.
  await send(
    stream,
    TransferResponse(status: ResponseStatus.Accepted, request: request),
    MsgType.transferResponse,
  )

  var
    remaining = request.size.int
    rand = initRand(request.seed)
  while remaining > 0:
    # write eats our buffer with sink, so we might as well
    # just allocate a new one on every pass (hoping the compiler
    # won't copy-on-sink because of the assignment here).
    var batch = newSeq[byte](min(remaining, ChunkSize))
    for i in 0 ..< batch.len:
      batch[i] = rand.rand(byte)
    await stream.write(batch)
    remaining -= batch.len

proc handleTransferResponse(
    stream: Stream, response: TransferResponse
) {.async: (raises: [CancelledError, LPError]).} =
  if response.status != ResponseStatus.Accepted:
    raise newException(LPError, "Transfer rejected")

  var
    received = 0
    buffer = newSeq[byte](ChunkSize)
    rand = initRand(response.request.seed)

  while received < response.request.size:
    let toRead = min(buffer.len, response.request.size - received)
    await stream.readExactly(addr buffer[0], toRead)
    received += toRead
    for i in 0 ..< toRead:
      if buffer[i] != rand.rand(byte):
        raise newException(LPError, "Bytestream mismatch")

proc newTransferProtocol*(): LPProtocol =
  let handler: LPProtoHandler = proc(
      stream: Stream, selectedCodec: string
  ): Future[void] {.async: (raises: [CancelledError]).} =
    try:
      var msgType: MsgType
      await stream.readExactly(addr msgType, 1)

      case msgType
      of MsgType.transferRequest:
        await handleTransferRequest(stream, await decodeMsg[TransferRequest](stream))
      of MsgType.transferResponse:
        await handleTransferResponse(stream, await decodeMsg[TransferResponse](stream))
    except SerializationError as e:
      # TODO proper logging
      echo "Malformed message" & $e.msg
    except LPError as e:
      echo "Error handling message" & $e.msg

  LPProtocol.new(@[TransferCodec], handler)

proc createSwitch(
    multiAddr: MultiAddress,
    rng: Rng,
    libp2pPrivKey: Opt[SkPrivateKey] = Opt.none(SkPrivateKey),
): Switch =
  let
    skkey = libp2pPrivKey.valueOr(SkKeyPair.random(rng).seckey)
    privKey = PrivateKey(scheme: Secp256k1, skkey: skkey)
  SwitchBuilder
    .new()
    .withRng(rng)
    .withPrivateKey(privKey)
    .withAddress(multiAddr)
    .withTcpTransport()
    .withMplex()
    .withNoise()
    .build()

proc createMixNodeInfo(info: PeerInfo, listenAddress: MultiAddress): MixNodeInfo =
  let (mixPrivKey, mixPubKey) = generateKeyPair().expect("Generate key pair error")
  MixNodeInfo(
    peerId: info.peerId,
    multiAddr: listenAddress,
    mixPrivKey: mixPrivKey,
    mixPubKey: mixPubKey,
    libp2pPrivKey: info.privateKey.skkey,
    libp2pPubKey: info.publicKey.skkey,
  )

proc init*(
    T: type Node, mixPool: MixPool, listenIp: string, listenPort: uint
): Result[Node, string] =
  let listenAddress = ?MultiAddress.init(fmt"/ip4/{listenIp}/tcp/{listenPort}")

  let
    rng = newRng()
    switch = createSwitch(listenAddress, rng)
    mixNodeInfo = createMixNodeInfo(switch.peerInfo, listenAddress)
    mixProto = MixProtocol.new(mixNodeInfo, switch)
    transferProto = newTransferProtocol()

  mixProto.nodePool.add(mixPool)
  switch.mount(mixProto)
  switch.mount(transferProto)

  Node(
    info: mixNodeInfo,
    switch: switch,
    proto: mixProto,
    transport: newMixTransport(mixProto),
  ).ok

proc start*(node: Node): Future[void] {.async: (raises: [CancelledError, LPError]).} =
  await node.switch.start()
  await node.proto.start()
  (await node.transport.start()).expect("Failed to start Mix transport")
