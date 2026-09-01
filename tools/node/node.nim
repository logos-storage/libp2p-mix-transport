import std/random
import std/times
import std/strformat

import pkg/chronos
import pkg/chronicles
import pkg/libp2p
import pkg/libp2p/protocols/protocol
import pkg/libp2p/crypto/secp
import pkg/libp2p_mix
import pkg/libp2p_mix/curve25519
import pkg/protobuf_serialization
import pkg/protobuf_serialization/std/enums
import pkg/stew/endians2

import ../../libp2p_mix_transport

logScope:
  topics = "node mix_transport"

const TransferCodec = "/test/simple-transfer/1.0.0"
const ChunkSize = 1024 * 1024 * 1024

type
  MixPool* = seq[MixPubInfo]

  Node* = ref object
    info*: MixNodeInfo
    switch*: Switch
    mixProto*: MixProtocol
    mixTransport*: MixTransport
    # When set to true, means the node is ready to take requests.
    running*: bool

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

proc receive[T](
    stream: Stream, msgType: MsgType
): Future[T] {.async: (raises: [CancelledError, SerializationError, LPError]).} =
  var
    len: uint16
    msgTypeByte: byte

  await stream.readExactly(addr msgTypeByte, 1)
  if msgTypeByte != msgType.byte:
    raise newException(LPError, "Invalid message type")

  await stream.readExactly(addr len, 2)
  len = len.fromLE

  let data = newSeq[byte](len.int)
  await stream.readExactly(addr data[0], len.int)
  Protobuf.decode(data, T)

proc send[T](
    stream: Stream, msg: sink T, msgType: MsgType
) {.async: (raises: [CancelledError, LPStreamError]).} =
  let data = Protobuf.encode(msg)
  # Header: [msgType (1 byte), payload length (2 bytes)]
  var buffer = newSeqUninit[byte](data.len + 1 + 2)
  let len = data.len.uint16.toLE

  buffer[0] = msgType.byte
  copyMem(addr buffer[1], addr len, 2)
  copyMem(addr buffer[3], addr data[0], data.len)

  await stream.write(buffer)

proc handleTransferRequest(
    stream: Stream, request: TransferRequest
) {.async: (raises: [CancelledError, LPStreamError]).} =
  info "handling libp2p transfer request", size = request.size, seed = request.seed
  # Send preamble.
  await stream.send(
    TransferResponse(status: ResponseStatus.Accepted, request: request),
    MsgType.transferResponse,
  )

  var
    remaining = request.size.int
    rand = initRand(request.seed)

  info "sending random stream", remaining = remaining, sessionId = stream.peerId
  while remaining > 0:
    # write eats our buffer with sink, so we might as well
    # just allocate a new one on every pass (hoping the compiler
    # won't copy-on-sink because of the assignment here).
    var batch = newSeq[byte](min(remaining, ChunkSize))
    for i in 0 ..< batch.len:
      batch[i] = rand.rand(byte)
    await stream.write(batch)
    remaining -= batch.len

  info "Bytes sent", size = request.size, sessionId = stream.peerId, seed = request.seed

proc handleTransferResponse(
    stream: Stream, response: TransferResponse
) {.async: (raises: [CancelledError, LPError]).} =
  info "receiving libp2p transfer response",
    status = response.status,
    size = response.request.size,
    sessionId = stream.peerId,
    seed = response.request.seed
  if response.status != ResponseStatus.Accepted:
    raise newException(LPError, "Transfer rejected")

  var
    received = 0
    buffer = newSeq[byte](min(ChunkSize, response.request.size.int))
    rand = initRand(response.request.seed)

  while received < response.request.size:
    let toRead = min(buffer.len, response.request.size - received)
    await stream.readExactly(addr buffer[0], toRead)
    received += toRead
    for i in 0 ..< toRead:
      if buffer[i] != rand.rand(byte):
        raise newException(LPError, "Bytestream mismatch")

  info "Bytestream validated successfully",
    size = response.request.size, seed = response.request.seed

proc request(
    stream: Stream, size: int32, seed: Option[int64] = int64.none, peerId: PeerId
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  defer:
    await stream.close()

  let now = getTime()
  let seedValue =
    if seed.isNone:
      now.toUnix * 1_000_000_000 + now.nanosecond
    else:
      seed.get()

  info "Sending libp2p transfer request", size = size, peerId = peerId, seed = seedValue

  try:
    await stream.send(
      TransferRequest(size: size, seed: seedValue), MsgType.transferRequest
    )
    await handleTransferResponse(
      stream, await receive[TransferResponse](stream, MsgType.transferResponse)
    )
  except CancelledError as e:
    raise e
  except CatchableError as e:
    return err(e.msg)

  ok()

## Asks the remote peer for a random byte stream of size `size` using
## an optionally provided seed, and then waits for the result and
## verifies it. This version uses a Mix transport connection.
proc request*(
    self: Node, target: PeerId, size: int32, seed: Option[int64] = int64.none
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let stream = (await self.mixTransport.dial(target, TransferCodec)).valueOr:
    return err(error)

  await request(stream, size, seed, stream.sessionId)

## Asks the remote peer for a random byte stream of size `size` using
## an optionally provided seed, and then waits for the result and
## verifies it. This version uses a direct libp2p connection.
proc request*(
    self: Node, target: MultiAddress, size: int32, seed: Option[int64] = int64.none
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let stream =
    try:
      let peerId = await self.switch.connect(target, true)
      await self.switch.dial(peerId, TransferCodec)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      return err(e.msg)

  ?await request(stream, size, seed, stream.peerId)

  # Because we're calling `connect` without a peer id, the ConnManager will internally
  # open a connection per dial. If we don't drop the peer, we'll exhaust the maximum
  # of connections simply by running repeated transfers.
  await self.switch.connManager.dropPeer(stream.peerId)
  ok()

proc newTransferProtocol*(): LPProtocol =
  let handler: LPProtoHandler = proc(
      stream: Stream, selectedCodec: string
  ): Future[void] {.async: (raises: [CancelledError]).} =
    defer:
      debug "exiting read loop"
      await stream.close()

    try:
      debug "entering read loop", peer = stream.peerId
      while not stream.atEof:
        await handleTransferRequest(
          stream, await receive[TransferRequest](stream, MsgType.transferRequest)
        )
    except CancelledError as e:
      raise e
    except LPStreamEOFError:
      debug "transfer stream closed by peer"
    except CatchableError as e:
      error "Error handling message", msg = e.msg

  LPProtocol.new(@[TransferCodec], handler)

proc createSwitch(
    multiAddr: MultiAddress,
    rng: Rng,
    maxConnections: int,
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
    .withMaxConnections(maxConnections)
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
    T: type Node,
    mixPool: MixPool,
    listenIp: string,
    listenPort: uint,
    maxConnections: int,
): Result[Node, string] =
  let listenAddress = ?MultiAddress.init(fmt"/ip4/{listenIp}/tcp/{listenPort}")

  let
    rng = newRng()
    switch = createSwitch(listenAddress, rng, maxConnections)
    mixNodeInfo = createMixNodeInfo(switch.peerInfo, listenAddress)
    mixProto = MixProtocol.new(mixNodeInfo, switch)
    transferProto = newTransferProtocol()

  mixProto.nodePool.add(mixPool)
  switch.mount(mixProto)
  switch.mount(transferProto)

  Node(
    info: mixNodeInfo,
    switch: switch,
    mixProto: mixProto,
    mixTransport: MixTransport.newMixTransport(mixProto),
  ).ok

proc start*(
    self: Node
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  try:
    await self.switch.start()
    await self.mixProto.start()
  except CatchableError as e:
    error "Error starting node", msg = e.msg
    return err(e.msg)
  ?await self.mixTransport.start()
  self.running = true
  ok()
