# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/tables

import chronos, results
import libp2p/peerid
import libp2p/stream/bufferstream
import libp2p/utils/opt

from ./wire import AckBitmapBytes, MaxInflightChunks, ReceiveWindowChunks

type
  StreamWriteHandler* = proc(data: sink seq[byte]): Future[void] {.
    async: (raises: [CancelledError, LPStreamError])
  .}

  InboundDataDisposition* {.pure.} = enum
    Accepted
    Duplicate
    OutsideWindow

  AckSnapshot* = object
    receiveBase*: uint64
    acknowledgementBitmap*: seq[byte]

  StreamDirection* {.pure.} = enum
    Outbound
    Inbound

  StreamState* {.pure.} = enum
    Pending
    Established
    Rejected

  TransportStream* = ref object of BufferStream
    sessionId: PeerId
    streamId: uint64
    codec: string
    direction: StreamDirection
    state: StreamState
    rejectionReason: string
    resolved: AsyncEvent
    writeHandler: StreamWriteHandler
    writeLock: AsyncLock
    nextOutboundSequence: uint64
    remoteReceiveBase: uint64
    pendingOutbound: Table[uint64, seq[byte]]
    sendStateChanged: AsyncEvent
    receiveBase: uint64
    acknowledgementBitmap: seq[byte]
    pendingInbound: Table[uint64, seq[byte]]
    dataAvailable: AsyncEvent
    shouldSendAck: AsyncEvent

func sessionId*(stream: TransportStream): PeerId =
  stream.sessionId

func streamId*(stream: TransportStream): uint64 =
  stream.streamId

func codec*(stream: TransportStream): string =
  stream.codec

func direction*(stream: TransportStream): StreamDirection =
  stream.direction

func state*(stream: TransportStream): StreamState =
  stream.state

func rejectionReason*(stream: TransportStream): string =
  stream.rejectionReason

func receiveBase*(stream: TransportStream): uint64 =
  stream.receiveBase

func pendingInboundCount*(stream: TransportStream): int =
  stream.pendingInbound.len

func pendingOutboundCount*(stream: TransportStream): int =
  stream.pendingOutbound.len

func nextOutboundSequence*(stream: TransportStream): uint64 =
  stream.nextOutboundSequence

func remoteReceiveLimit*(stream: TransportStream): uint64 =
  stream.remoteReceiveBase + ReceiveWindowChunks.uint64

func bitmapContains(bitmap: openArray[byte], offset: uint64): bool =
  let
    byteIndex = int(offset div 8)
    bitIndex = int(offset mod 8)
  (bitmap[byteIndex] and (1'u8 shl bitIndex)) != 0

proc setBitmapBit(bitmap: var seq[byte], offset: uint64) =
  let
    byteIndex = int(offset div 8)
    bitIndex = int(offset mod 8)
  bitmap[byteIndex] = bitmap[byteIndex] or (1'u8 shl bitIndex)

proc shiftBitmap(stream: TransportStream) =
  for index in 0 ..< stream.acknowledgementBitmap.len:
    let carry =
      if index + 1 < stream.acknowledgementBitmap.len:
        (stream.acknowledgementBitmap[index + 1] and 1'u8) shl 7
      else:
        0'u8
    stream.acknowledgementBitmap[index] =
      (stream.acknowledgementBitmap[index] shr 1) or carry

proc fireShouldSendAckEvent(stream: TransportStream) =
  stream.shouldSendAck.fire()

proc acknowledgementSnapshot*(stream: TransportStream): AckSnapshot =
  var bitmap = newSeq[byte](AckBitmapBytes)
  for index, value in stream.acknowledgementBitmap:
    bitmap[index] = value
  AckSnapshot(receiveBase: stream.receiveBase, acknowledgementBitmap: move(bitmap))

proc waitForShouldSendAck*(
    stream: TransportStream
): Future[void].Raising([CancelledError]) =
  stream.shouldSendAck.wait().join()

proc clearShouldSendAck*(stream: TransportStream) =
  stream.shouldSendAck.clear()

proc waitForInboundData*(
    stream: TransportStream
): Future[void].Raising([CancelledError]) =
  stream.dataAvailable.wait().join()

proc clearInboundDataAvailable*(stream: TransportStream) =
  stream.dataAvailable.clear()

proc receiveData*(
    stream: TransportStream, sequence: uint64, payload: sink seq[byte]
): InboundDataDisposition =
  if sequence < stream.receiveBase:
    stream.fireShouldSendAckEvent()
    return InboundDataDisposition.Duplicate

  let offset = sequence - stream.receiveBase
  if offset >= ReceiveWindowChunks.uint64:
    return InboundDataDisposition.OutsideWindow
  if stream.acknowledgementBitmap.bitmapContains(offset):
    stream.fireShouldSendAckEvent()
    return InboundDataDisposition.Duplicate

  stream.pendingInbound[sequence] = move(payload)
  stream.acknowledgementBitmap.setBitmapBit(offset)
  stream.dataAvailable.fire()
  stream.fireShouldSendAckEvent()
  InboundDataDisposition.Accepted

proc takeNextInbound*(
    stream: TransportStream
): Opt[tuple[sequence: uint64, payload: seq[byte]]] =
  if not stream.acknowledgementBitmap.bitmapContains(0):
    return Opt.none(tuple[sequence: uint64, payload: seq[byte]])

  stream.pendingInbound.withValue(stream.receiveBase, payload):
    let value = (sequence: stream.receiveBase, payload: move(payload[]))
    stream.pendingInbound.del(stream.receiveBase)
    return Opt.some(value)
  Opt.none(tuple[sequence: uint64, payload: seq[byte]])

proc markInboundDelivered*(stream: TransportStream, sequence: uint64) =
  doAssert sequence == stream.receiveBase
  doAssert stream.acknowledgementBitmap.bitmapContains(0)
  stream.shiftBitmap()
  inc stream.receiveBase
  stream.fireShouldSendAckEvent()
  if stream.acknowledgementBitmap.bitmapContains(0):
    stream.dataAvailable.fire()

func canReserveOutbound*(stream: TransportStream): bool =
  stream.pendingOutbound.len < MaxInflightChunks and
    stream.nextOutboundSequence < stream.remoteReceiveLimit

proc waitForOutboundCapacity*(
    stream: TransportStream
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  while not stream.canReserveOutbound:
    if stream.closed:
      raise newLPStreamClosedError()
    stream.sendStateChanged.clear()
    if not stream.canReserveOutbound:
      await stream.sendStateChanged.wait()

proc reserveOutbound*(
    stream: TransportStream, payload: seq[byte]
): Result[uint64, string] =
  if not stream.canReserveOutbound:
    return err("stream has no outbound capacity")
  let sequence = stream.nextOutboundSequence
  inc stream.nextOutboundSequence
  stream.pendingOutbound[sequence] = payload
  ok(sequence)

proc cancelOutbound*(stream: TransportStream, sequence: uint64) =
  if stream.pendingOutbound.hasKey(sequence):
    stream.pendingOutbound.del(sequence)
    stream.sendStateChanged.fire()

proc applyAcknowledgement*(
    stream: TransportStream, receiveBase: uint64, bitmap: openArray[byte]
): bool =
  if bitmap.len != AckBitmapBytes or receiveBase < stream.remoteReceiveBase or
      receiveBase > stream.nextOutboundSequence:
    return false

  var acknowledged: seq[uint64]
  for sequence in stream.pendingOutbound.keys:
    if sequence < receiveBase:
      acknowledged.add(sequence)
    else:
      let offset = sequence - receiveBase
      if offset < ReceiveWindowChunks.uint64 and bitmap.bitmapContains(offset):
        acknowledged.add(sequence)

  for sequence in acknowledged:
    stream.pendingOutbound.del(sequence)

  let changed = acknowledged.len > 0 or stream.remoteReceiveBase != receiveBase
  stream.remoteReceiveBase = receiveBase
  if changed:
    stream.sendStateChanged.fire()
  changed

proc setWriteHandler*(stream: TransportStream, handler: StreamWriteHandler) =
  doAssert stream.writeHandler.isNil
  doAssert not handler.isNil
  stream.writeHandler = handler

proc establish*(stream: TransportStream) =
  stream.state = StreamState.Established
  stream.resolved.fire()

proc reject*(stream: TransportStream, reason: string) =
  stream.state = StreamState.Rejected
  stream.rejectionReason = reason
  stream.resolved.fire()

proc waitUntilResolved*(stream: TransportStream): Future[void] =
  stream.resolved.wait()

method write*(
    stream: TransportStream, msg: sink seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if stream.writeHandler.isNil:
    raise newException(LPStreamError, "MixTransport stream is not writable")

  await stream.writeLock.acquire()
  defer:
    try:
      stream.writeLock.release()
    except AsyncLockError as exc:
      raiseAssert "stream write lock was not held: " & exc.msg
  await stream.writeHandler(move(msg))

method getWrapped*(stream: TransportStream): Connection =
  nil

method closeImpl*(
    stream: TransportStream
): Future[void] {.async: (raises: [], raw: true).} =
  stream.dataAvailable.fire()
  stream.shouldSendAck.fire()
  stream.sendStateChanged.fire()
  procCall BufferStream(stream).closeImpl()

proc newTransportStream*(
    sessionId: PeerId,
    peerId: PeerId,
    streamId: uint64,
    codec: string,
    direction: StreamDirection,
): TransportStream =
  doAssert sessionId.len > 0, "stream sessionId must not be empty"
  doAssert peerId.len > 0, "stream peerId must not be empty"
  doAssert streamId > 0, "streamId must not be zero"
  doAssert codec.len > 0, "stream codec must not be empty"
  let libp2pDirection =
    if direction == StreamDirection.Outbound: Direction.Out else: Direction.In
  result = TransportStream(
    sessionId: sessionId,
    peerId: peerId,
    streamId: streamId,
    codec: codec,
    direction: direction,
    state: StreamState.Pending,
    resolved: newAsyncEvent(),
    writeLock: newAsyncLock(),
    nextOutboundSequence: 1,
    remoteReceiveBase: 1,
    pendingOutbound: initTable[uint64, seq[byte]](),
    sendStateChanged: newAsyncEvent(),
    receiveBase: 1,
    acknowledgementBitmap: newSeq[byte](AckBitmapBytes),
    pendingInbound: initTable[uint64, seq[byte]](),
    dataAvailable: newAsyncEvent(),
    shouldSendAck: newAsyncEvent(),
    dir: libp2pDirection,
    transportDir: libp2pDirection,
    protocol: codec,
    timeout: ZeroDuration,
  )
  result.initStream()

{.pop.}
