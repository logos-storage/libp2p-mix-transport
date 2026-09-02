# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/[deques, tables]

import chronos
import results
import libp2p/peerid
import libp2p/utils/opt
import libp2p_mix
import ./streams
from ./wire import MaxSessionIdBytes, RefillRequestId, StreamId

const
  ReplyControlReserveGroups* = 2
  ReplyRefillLowWatermarkGroups* = ReplyControlReserveGroups
  DefaultRefillResponseLifetime* = 30.minutes
  DefaultMaxOutstandingRefillRequests* = 1_024

type
  SessionRole* {.pure.} = enum
    Initiator
    Recipient

  SessionState* {.pure.} = enum
    Pending
    Established
    Closed

  TransportSession* = ref object
    sessionId: PeerId
    destination: Opt[PeerId]
    role: SessionRole
    state: SessionState
    established: AsyncEvent
    receivedSurbGroups: Deque[seq[SURB]]
    replyCapacityStateChanged: AsyncEvent
    replySendLock: AsyncLock
    nextRefillRequestAt: Opt[Moment]
    outstandingRefillRequests: Table[RefillRequestId, Moment]
    nextRefillRequestId: Opt[RefillRequestId]
    refillResponseLifetime: Duration
    maxOutstandingRefillRequests: int
    streams: Table[StreamId, TransportStream]
    nextOutboundStreamId: Opt[StreamId]

  SessionStore* = ref object
    bySessionId: Table[PeerId, TransportSession]
    byDestination: Table[PeerId, TransportSession]
    refillResponseLifetime: Duration
    maxOutstandingRefillRequests: int

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
    nextRefillRequestAt: Opt.none(Moment),
    outstandingRefillRequests: initTable[RefillRequestId, Moment](),
    nextRefillRequestId: Opt.some(RefillRequestId(1)),
    refillResponseLifetime: store.refillResponseLifetime,
    maxOutstandingRefillRequests: store.maxOutstandingRefillRequests,
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
    nextRefillRequestAt: Opt.none(Moment),
    outstandingRefillRequests: initTable[RefillRequestId, Moment](),
    nextRefillRequestId: Opt.some(RefillRequestId(1)),
    refillResponseLifetime: store.refillResponseLifetime,
    maxOutstandingRefillRequests: store.maxOutstandingRefillRequests,
    streams: initTable[StreamId, TransportStream](),
    nextOutboundStreamId: Opt.some(StreamId(2)),
  )
  store.bySessionId[sessionId] = session
  ok(session)

proc establish*(session: TransportSession) =
  session.state = SessionState.Established
  session.established.fire()

proc waitUntilEstablished*(
    session: TransportSession
): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
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
    if session.receivedSurbGroups.len > ReplyRefillLowWatermarkGroups:
      session.nextRefillRequestAt = Opt.none(Moment)
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
): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
  session.replyCapacityStateChanged.wait()

proc acquireReplySend*(
    session: TransportSession
): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
  session.replySendLock.acquire()

proc releaseReplySend*(session: TransportSession) =
  try:
    session.replySendLock.release()
  except AsyncLockError as exc:
    raiseAssert "session reply-send lock was not held: " & exc.msg

func refillRequestDue*(session: TransportSession, now: Moment = Moment.now()): bool =
  if session.role != SessionRole.Recipient or
      session.receivedSurbGroups.len > ReplyRefillLowWatermarkGroups:
    return false
  let nextRefillRequestAt = session.nextRefillRequestAt.valueOr:
    return true
  nextRefillRequestAt <= now

func timeUntilNextRefillRequest*(
    session: TransportSession, now: Moment = Moment.now()
): Duration =
  let nextRefillRequestAt = session.nextRefillRequestAt.valueOr:
    return ZeroDuration
  if nextRefillRequestAt <= now:
    ZeroDuration
  else:
    nextRefillRequestAt - now

proc scheduleNextRefillRequest*(
    session: TransportSession, delay: Duration, now: Moment = Moment.now()
) =
  doAssert delay > ZeroDuration, "refill request delay must be positive"
  session.nextRefillRequestAt = Opt.some(now + delay)

func outstandingRefillRequestCount*(session: TransportSession): int =
  session.outstandingRefillRequests.len

proc purgeExpiredRefillRequests*(
    session: TransportSession, now: Moment = Moment.now()
): int =
  var expired: seq[RefillRequestId]
  for refillRequestId, expiresAt in session.outstandingRefillRequests:
    if expiresAt <= now:
      expired.add(refillRequestId)
  for refillRequestId in expired:
    session.outstandingRefillRequests.del(refillRequestId)
  expired.len

proc registerRefillRequest*(
    session: TransportSession, now: Moment = Moment.now()
): Result[RefillRequestId, string] =
  if session.role != SessionRole.Recipient:
    return err("only recipient sessions can request SURB refills")
  if session.receivedSurbGroups.len > ReplyRefillLowWatermarkGroups:
    return err("session does not need a SURB refill")

  discard session.purgeExpiredRefillRequests(now)
  if session.outstandingRefillRequests.len >= session.maxOutstandingRefillRequests:
    var
      found = false
      oldestRequestId: RefillRequestId
      oldestDeadline: Moment
    for refillRequestId, expiresAt in session.outstandingRefillRequests:
      if not found or expiresAt < oldestDeadline:
        found = true
        oldestRequestId = refillRequestId
        oldestDeadline = expiresAt
    doAssert found, "a full refill request table must contain an entry"
    session.outstandingRefillRequests.del(oldestRequestId)

  let refillRequestId = session.nextRefillRequestId.valueOr:
    return err("refill request identifier space is exhausted")
  session.nextRefillRequestId =
    if refillRequestId == RefillRequestId.high:
      Opt.none(RefillRequestId)
    else:
      Opt.some(refillRequestId + 1)
  session.outstandingRefillRequests[refillRequestId] =
    now + session.refillResponseLifetime
  ok(refillRequestId)

proc cancelRefillRequest*(session: TransportSession, refillRequestId: RefillRequestId) =
  session.outstandingRefillRequests.del(refillRequestId)
  session.nextRefillRequestAt = Opt.none(Moment)
  session.replyCapacityStateChanged.fire()

proc acceptRefillResponse*(
    session: TransportSession,
    refillRequestId: RefillRequestId,
    now: Moment = Moment.now(),
): bool =
  discard session.purgeExpiredRefillRequests(now)
  if not session.outstandingRefillRequests.hasKey(refillRequestId):
    return false
  session.outstandingRefillRequests.del(refillRequestId)
  session.nextRefillRequestAt = Opt.none(Moment)
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

proc takeStreams*(session: TransportSession): seq[TransportStream] =
  result = newSeqOfCap[TransportStream](session.streams.len)
  for stream in session.streams.values:
    result.add(stream)
  session.streams.clear()

proc shutdown*(session: TransportSession): Future[void] {.async: (raises: []).} =
  session.state = SessionState.Closed
  session.established.fire()
  let streams = session.takeStreams()
  var shutdownTasks = newSeqOfCap[Future[void].Raising([])](streams.len)
  for stream in streams:
    shutdownTasks.add(stream.shutdown())
  await noCancel shutdownTasks.allFutures()

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

proc takeSessions*(store: SessionStore): seq[TransportSession] =
  result = newSeqOfCap[TransportSession](store.bySessionId.len)
  for session in store.bySessionId.values:
    result.add(session)
  store.clear()

proc newSessionStore*(
    refillResponseLifetime = DefaultRefillResponseLifetime,
    maxOutstandingRefillRequests = DefaultMaxOutstandingRefillRequests,
): SessionStore =
  doAssert refillResponseLifetime > ZeroDuration,
    "refill response lifetime must be positive"
  doAssert maxOutstandingRefillRequests > 0,
    "maximum outstanding refill requests must be positive"
  SessionStore(
    bySessionId: initTable[PeerId, TransportSession](),
    byDestination: initTable[PeerId, TransportSession](),
    refillResponseLifetime: refillResponseLifetime,
    maxOutstandingRefillRequests: maxOutstandingRefillRequests,
  )

{.pop.}
