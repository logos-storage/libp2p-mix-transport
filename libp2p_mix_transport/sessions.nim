# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/[deques, tables]

import chronos
import results
import libp2p/peerid
import libp2p/utils/opt
import libp2p_mix
import ./streams
from ./wire import MaxSessionIdBytes, StreamId

const
  ReplyControlReserveGroups* = 2
  ReplyRefillLowWatermarkGroups* = ReplyControlReserveGroups

type
  SessionRole* {.pure.} = enum
    Initiator
    Recipient

  SessionState* {.pure.} = enum
    Pending
    Established

  TransportSession* = ref object
    sessionId: PeerId
    destination: Opt[PeerId]
    role: SessionRole
    state: SessionState
    established: AsyncEvent
    receivedSurbGroups: Deque[seq[SURB]]
    replyCapacityStateChanged: AsyncEvent
    replySendLock: AsyncLock
    pendingRefillBatchId: Opt[uint64]
    nextRefillBatchId: uint64
    streams: Table[StreamId, TransportStream]
    nextOutboundStreamId: Opt[StreamId]

  SessionStore* = ref object
    bySessionId: Table[PeerId, TransportSession]
    byDestination: Table[PeerId, TransportSession]

func sessionId*(session: TransportSession): PeerId =
  session.sessionId

func destination*(session: TransportSession): Opt[PeerId] =
  session.destination

func role*(session: TransportSession): SessionRole =
  session.role

func state*(session: TransportSession): SessionState =
  session.state

func receivedSurbGroupCount*(session: TransportSession): int =
  session.receivedSurbGroups.len

func streamCount*(session: TransportSession): int =
  session.streams.len

func peerId*(session: TransportSession): PeerId =
  ## Identity exposed to consumers of this transport. The initiator knows the
  ## real destination; the recipient knows only the session pseudonym.
  session.destination.get(session.sessionId)

func len*(store: SessionStore): int =
  store.bySessionId.len

proc get*(store: SessionStore, sessionId: PeerId): Opt[TransportSession] =
  store.bySessionId.withValue(sessionId, session):
    return Opt.some(session[])
  Opt.none(TransportSession)

proc getByDestination*(
    store: SessionStore, destination: PeerId
): Opt[TransportSession] =
  store.byDestination.withValue(destination, session):
    return Opt.some(session[])
  Opt.none(TransportSession)

proc addInitiatorSession*(
    store: SessionStore, destination: PeerId, sessionId: PeerId
): Result[TransportSession, string] =
  if destination.len == 0:
    return err("destination must not be empty")
  if sessionId.len == 0:
    return err("sessionId must not be empty")
  if sessionId.len > MaxSessionIdBytes:
    return err("sessionId is too long")
  if store.byDestination.hasKey(destination):
    return err("destination already has a session")
  if store.bySessionId.hasKey(sessionId):
    return err("sessionId is already registered")

  let session = TransportSession(
    sessionId: sessionId,
    destination: Opt.some(destination),
    role: SessionRole.Initiator,
    state: SessionState.Pending,
    established: newAsyncEvent(),
    receivedSurbGroups: initDeque[seq[SURB]](),
    replyCapacityStateChanged: newAsyncEvent(),
    replySendLock: newAsyncLock(),
    pendingRefillBatchId: Opt.none(uint64),
    nextRefillBatchId: 1,
    streams: initTable[StreamId, TransportStream](),
    nextOutboundStreamId: Opt.some(StreamId(1)),
  )
  store.bySessionId[sessionId] = session
  store.byDestination[destination] = session
  ok(session)

proc addRecipientSession*(
    store: SessionStore, sessionId: PeerId
): Result[TransportSession, string] =
  if sessionId.len == 0:
    return err("sessionId must not be empty")
  if sessionId.len > MaxSessionIdBytes:
    return err("sessionId is too long")
  if store.bySessionId.hasKey(sessionId):
    return err("sessionId is already registered")

  let session = TransportSession(
    sessionId: sessionId,
    destination: Opt.none(PeerId),
    role: SessionRole.Recipient,
    state: SessionState.Pending,
    established: newAsyncEvent(),
    receivedSurbGroups: initDeque[seq[SURB]](),
    replyCapacityStateChanged: newAsyncEvent(),
    replySendLock: newAsyncLock(),
    pendingRefillBatchId: Opt.none(uint64),
    nextRefillBatchId: 1,
    streams: initTable[StreamId, TransportStream](),
    nextOutboundStreamId: Opt.some(StreamId(2)),
  )
  store.bySessionId[sessionId] = session
  ok(session)

proc establish*(session: TransportSession) =
  session.state = SessionState.Established
  session.established.fire()

proc waitUntilEstablished*(session: TransportSession): Future[void] =
  session.established.wait()

proc addReceivedSurbGroups*(
    session: TransportSession, groups: sink seq[seq[SURB]]
): Result[void, string] =
  if session.role != SessionRole.Recipient:
    return err("only recipient sessions can store received SURB groups")
  for group in groups:
    if group.len == 0:
      return err("received SURB groups must not be empty")

  for group in groups.mitems:
    session.receivedSurbGroups.addLast(move(group))
  if groups.len > 0:
    session.replyCapacityStateChanged.fire()
  ok()

proc takeReceivedSurbGroup*(session: TransportSession): Result[seq[SURB], string] =
  if session.receivedSurbGroups.len == 0:
    return err("session has no received SURB groups")
  ok(session.receivedSurbGroups.popFirst())

proc takeUnreservedSurbGroup*(session: TransportSession): Result[seq[SURB], string] =
  if session.receivedSurbGroups.len <= ReplyControlReserveGroups:
    return err("session reply capacity is reserved for control traffic")
  ok(session.receivedSurbGroups.popFirst())

proc clearReplyCapacityStateChanged*(session: TransportSession) =
  session.replyCapacityStateChanged.clear()

proc waitForReplyCapacityStateChange*(
    session: TransportSession
): Future[void] {.async: (raises: [CancelledError]).} =
  await session.replyCapacityStateChanged.wait()

proc acquireReplySend*(
    session: TransportSession
): Future[void] {.async: (raises: [CancelledError]).} =
  await session.replySendLock.acquire()

proc releaseReplySend*(session: TransportSession) =
  try:
    session.replySendLock.release()
  except AsyncLockError as exc:
    raiseAssert "session reply-send lock was not held: " & exc.msg

func refillNeeded*(session: TransportSession): bool =
  session.role == SessionRole.Recipient and
    session.receivedSurbGroups.len <= ReplyRefillLowWatermarkGroups and
    session.pendingRefillBatchId.isNone

proc beginRefill*(session: TransportSession): Opt[uint64] =
  if not session.refillNeeded:
    return Opt.none(uint64)
  let batchId = session.nextRefillBatchId
  inc session.nextRefillBatchId
  session.pendingRefillBatchId = Opt.some(batchId)
  Opt.some(batchId)

proc cancelRefill*(session: TransportSession, batchId: uint64) =
  if session.pendingRefillBatchId == Opt.some(batchId):
    session.pendingRefillBatchId = Opt.none(uint64)

func isPendingRefill*(session: TransportSession, batchId: uint64): bool =
  session.pendingRefillBatchId == Opt.some(batchId)

proc completeRefill*(session: TransportSession, batchId: uint64): bool =
  if not session.isPendingRefill(batchId):
    return false
  session.pendingRefillBatchId = Opt.none(uint64)
  session.replyCapacityStateChanged.fire()
  true

func isValidInboundStreamId(session: TransportSession, streamId: StreamId): bool =
  if streamId == 0:
    return false

  case session.role
  of SessionRole.Initiator:
    streamId mod 2 == 0
  of SessionRole.Recipient:
    streamId mod 2 == 1

proc getStream*(session: TransportSession, streamId: StreamId): Opt[TransportStream] =
  session.streams.withValue(streamId, stream):
    return Opt.some(stream[])
  Opt.none(TransportStream)

proc takeNextOutboundStreamId(session: TransportSession): Result[StreamId, string] =
  let streamId = session.nextOutboundStreamId.valueOr:
    return err("stream identifier space is exhausted")

  session.nextOutboundStreamId =
    if streamId > StreamId.high - 2:
      Opt.none(StreamId)
    else:
      Opt.some(streamId + 2)
  ok(streamId)

proc addOutboundStream*(
    session: TransportSession, codec: string
): Result[TransportStream, string] =
  if session.state != SessionState.Established:
    return err("cannot open a stream before the session is established")
  if codec.len == 0:
    return err("stream codec must not be empty")

  let streamId = session.takeNextOutboundStreamId().valueOr:
    return err(error)
  let stream = newTransportStream(
    session.sessionId, session.peerId, streamId, codec, StreamDirection.Outbound
  )
  session.streams[stream.streamId] = stream
  ok(stream)

proc addInboundStream*(
    session: TransportSession, streamId: StreamId, codec: string
): Result[TransportStream, string] =
  if session.state != SessionState.Established:
    return err("cannot accept a stream before the session is established")
  if codec.len == 0:
    return err("stream codec must not be empty")
  if not session.isValidInboundStreamId(streamId):
    return err("stream identifier was not allocated by the remote endpoint")
  if session.streams.hasKey(streamId):
    return err("stream identifier is already registered")

  let stream = newTransportStream(
    session.sessionId, session.peerId, streamId, codec, StreamDirection.Inbound
  )
  session.streams[streamId] = stream
  ok(stream)

proc removeStream*(
    session: TransportSession, streamId: StreamId
): Opt[TransportStream] =
  let stream = session.getStream(streamId).valueOr:
    return Opt.none(TransportStream)
  session.streams.del(streamId)
  Opt.some(stream)

proc remove*(store: SessionStore, sessionId: PeerId): Opt[TransportSession] =
  let session = store.get(sessionId).valueOr:
    return Opt.none(TransportSession)

  store.bySessionId.del(sessionId)
  session.destination.withValue(destination):
    store.byDestination.del(destination)
  Opt.some(session)

proc clear*(store: SessionStore) =
  store.bySessionId.clear()
  store.byDestination.clear()

proc newSessionStore*(): SessionStore =
  SessionStore(
    bySessionId: initTable[PeerId, TransportSession](),
    byDestination: initTable[PeerId, TransportSession](),
  )

{.pop.}
