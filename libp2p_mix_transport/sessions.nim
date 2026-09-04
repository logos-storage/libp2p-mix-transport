# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/[deques, tables]

import chronicles, chronos
import results
import libp2p/peerid
import libp2p/utils/opt
import libp2p_mix
import ./streams
from ./wire import
  DefaultReplySurbRedundancy, MaxSessionIdBytes, MaxSurbSupplySequence,
  SurbSupplyAckBitmapBytes, SurbSupplySequence, SurbSupplyWindow, StreamId

logScope:
  topics = "mix-transport sessions"

const DefaultRecipientSurbCapacity* = 16

export DefaultReplySurbRedundancy

type
  SessionRole* {.pure.} = enum
    Initiator
    Recipient

  SessionState* {.pure.} = enum
    Pending
    Established
    Closed

  SurbSupplyDisposition* {.pure.} = enum
    Accepted
    Duplicate
    OutsideWindow
    AtCapacity

  SurbSupplySnapshot* = object
    receiveBase*: SurbSupplySequence
    acknowledgementBitmap*: seq[byte]
    supplyLimit*: SurbSupplySequence

  PendingSurbSupply = object
    encodedSurb: seq[byte]
    credentialIdentifier: SURBIdentifier
    nextRetransmissionAt: Opt[Moment]

  TransportSession* = ref object
    sessionId: PeerId
    destination: Opt[PeerId]
    role: SessionRole
    state: SessionState
    established: AsyncEvent
    receivedSurbs: Deque[SURB]
    recipientSurbCapacity: int
    surbSupplyInitialized: bool
    surbSupplyReceiveBase: SurbSupplySequence
    surbSupplyAcknowledgementBitmap: seq[byte]
    surbSupplyLimit: SurbSupplySequence
    replyCapacityStateChanged: AsyncEvent
    replySendLock: AsyncLock
    remoteSurbSupplyReceiveBase: SurbSupplySequence
    remoteSurbSupplyLimit: SurbSupplySequence
    nextSurbSupplySequence: Opt[SurbSupplySequence]
    pendingSurbSupply: Table[SurbSupplySequence, PendingSurbSupply]
    surbSupplyStateChanged: AsyncEvent
    surbSupplierTask: Future[void].Raising([CancelledError])
    nextSurbStatusProbeAt: Opt[Moment]
    unansweredSurbStatusProbes: int
    streams: Table[StreamId, TransportStream]
    nextOutboundStreamId: Opt[StreamId]

  SessionStore* = ref object
    bySessionId: Table[PeerId, TransportSession]
    byDestination: Table[PeerId, TransportSession]
    recipientSurbCapacity: int

func sessionId*(session: TransportSession): PeerId =
  session.sessionId

func destination*(session: TransportSession): Opt[PeerId] =
  session.destination

func role*(session: TransportSession): SessionRole =
  session.role

func state*(session: TransportSession): SessionState =
  session.state

func receivedSurbCount*(session: TransportSession): int =
  session.receivedSurbs.len

func recipientSurbCapacity*(session: TransportSession): int =
  session.recipientSurbCapacity

func pendingSurbSupplyCount*(session: TransportSession): int =
  session.pendingSurbSupply.len

func remoteSurbSupplyLimit*(session: TransportSession): SurbSupplySequence =
  session.remoteSurbSupplyLimit

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
    receivedSurbs: initDeque[SURB](),
    recipientSurbCapacity: store.recipientSurbCapacity,
    surbSupplyAcknowledgementBitmap: newSeq[byte](SurbSupplyAckBitmapBytes),
    replyCapacityStateChanged: newAsyncEvent(),
    replySendLock: newAsyncLock(),
    nextSurbSupplySequence: Opt.some(SurbSupplySequence(0)),
    pendingSurbSupply: initTable[SurbSupplySequence, PendingSurbSupply](),
    surbSupplyStateChanged: newAsyncEvent(),
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
    receivedSurbs: initDeque[SURB](),
    recipientSurbCapacity: store.recipientSurbCapacity,
    surbSupplyAcknowledgementBitmap: newSeq[byte](SurbSupplyAckBitmapBytes),
    replyCapacityStateChanged: newAsyncEvent(),
    replySendLock: newAsyncLock(),
    nextSurbSupplySequence: Opt.some(SurbSupplySequence(0)),
    pendingSurbSupply: initTable[SurbSupplySequence, PendingSurbSupply](),
    surbSupplyStateChanged: newAsyncEvent(),
    streams: initTable[StreamId, TransportStream](),
    nextOutboundStreamId: Opt.some(StreamId(2)),
  )
  store.bySessionId[sessionId] = session
  ok(session)

proc establish*(session: TransportSession) =
  trace "session established successfully", sessionId = session.sessionId, role = session.role
  session.state = SessionState.Established
  session.established.fire()

proc waitUntilEstablished*(
    session: TransportSession
): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
  session.established.wait()

proc addReceivedSurbs*(
    session: TransportSession, surbs: sink seq[SURB]
): Result[void, string] =
  if session.role != SessionRole.Recipient:
    return err("only recipient sessions can store received SURBs")
  if surbs.len > session.recipientSurbCapacity - session.receivedSurbs.len:
    return err("recipient SURB capacity would be exceeded")

  for surb in surbs.mitems:
    session.receivedSurbs.addLast(move(surb))
  if surbs.len > 0:
    session.replyCapacityStateChanged.fire()
  ok()

proc takeReceivedSurbs*(
    session: TransportSession, count: int
): Result[seq[SURB], string] =
  if count <= 0:
    return err("SURB count must be positive")
  if session.receivedSurbs.len < count:
    return err("session does not have enough received SURBs")
  if session.surbSupplyInitialized and
      uint64(session.surbSupplyLimit) + uint64(count) > uint64(SurbSupplySequence.high):
    return err("SURB supply credit space is exhausted")

  var surbs = newSeqOfCap[SURB](count)
  for _ in 0 ..< count:
    surbs.add(session.receivedSurbs.popFirst())
  if session.surbSupplyInitialized:
    session.surbSupplyLimit += SurbSupplySequence(count)
  ok(surbs)

func bitmapContains(bitmap: openArray[byte], offset: SurbSupplySequence): bool =
  let
    byteIndex = int(offset div 8)
    bitIndex = int(offset mod 8)
  (bitmap[byteIndex] and (1'u8 shl bitIndex)) != 0

proc setBitmapBit(bitmap: var seq[byte], offset: SurbSupplySequence) =
  let
    byteIndex = int(offset div 8)
    bitIndex = int(offset mod 8)
  bitmap[byteIndex] = bitmap[byteIndex] or (1'u8 shl bitIndex)

proc shiftSupplyBitmap(session: TransportSession) =
  for index in 0 ..< session.surbSupplyAcknowledgementBitmap.len:
    let carry =
      if index + 1 < session.surbSupplyAcknowledgementBitmap.len:
        (session.surbSupplyAcknowledgementBitmap[index + 1] and 1'u8) shl 7
      else:
        0'u8
    session.surbSupplyAcknowledgementBitmap[index] =
      (session.surbSupplyAcknowledgementBitmap[index] shr 1) or carry

proc initializeSurbSupply*(session: TransportSession): Result[void, string] =
  if session.role != SessionRole.Recipient:
    return err("only recipient sessions advertise SURB supply state")
  if session.surbSupplyInitialized:
    return err("SURB supply state is already initialized")
  if session.receivedSurbs.len > session.recipientSurbCapacity:
    return err("recipient SURB capacity is already exceeded")

  session.surbSupplyLimit =
    SurbSupplySequence(session.recipientSurbCapacity - session.receivedSurbs.len)
  session.surbSupplyInitialized = true
  ok()

proc surbSupplySnapshot*(session: TransportSession): SurbSupplySnapshot =
  doAssert session.role == SessionRole.Recipient
  doAssert session.surbSupplyInitialized
  SurbSupplySnapshot(
    receiveBase: session.surbSupplyReceiveBase,
    acknowledgementBitmap: session.surbSupplyAcknowledgementBitmap,
    supplyLimit: session.surbSupplyLimit,
  )

proc acceptSurbSupply*(
    session: TransportSession, sequence: SurbSupplySequence, surb: sink SURB
): SurbSupplyDisposition =
  doAssert session.role == SessionRole.Recipient
  doAssert session.surbSupplyInitialized

  if sequence < session.surbSupplyReceiveBase:
    return SurbSupplyDisposition.Duplicate
  if sequence >= session.surbSupplyLimit:
    return SurbSupplyDisposition.OutsideWindow

  let offset = sequence - session.surbSupplyReceiveBase
  if offset >= SurbSupplySequence(SurbSupplyWindow):
    return SurbSupplyDisposition.OutsideWindow
  if session.surbSupplyAcknowledgementBitmap.bitmapContains(offset):
    return SurbSupplyDisposition.Duplicate
  if session.receivedSurbs.len >= session.recipientSurbCapacity:
    return SurbSupplyDisposition.AtCapacity

  session.receivedSurbs.addLast(move(surb))
  session.surbSupplyAcknowledgementBitmap.setBitmapBit(offset)
  while session.surbSupplyAcknowledgementBitmap.bitmapContains(0):
    doAssert session.surbSupplyReceiveBase < SurbSupplySequence.high
    session.shiftSupplyBitmap()
    inc session.surbSupplyReceiveBase
  session.replyCapacityStateChanged.fire()
  SurbSupplyDisposition.Accepted

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

func availableSurbSupplySlots*(session: TransportSession): int =
  if session.role != SessionRole.Initiator:
    return 0
  let nextSequence = session.nextSurbSupplySequence.valueOr:
    return 0
  let upperBound = min(
    uint64(session.remoteSurbSupplyLimit),
    uint64(session.remoteSurbSupplyReceiveBase) + uint64(SurbSupplyWindow),
  )
  if uint64(nextSequence) >= upperBound:
    return 0
  int(upperBound - uint64(nextSequence))

proc registerSurbSupply*(
    session: TransportSession,
    encodedSurbs: openArray[seq[byte]],
    credentialIdentifiers: openArray[SURBIdentifier],
): Result[SurbSupplySequence, string] =
  if session.role != SessionRole.Initiator:
    return err("only initiator sessions can supply SURBs")
  if encodedSurbs.len == 0:
    return err("SURB supply must not be empty")
  if credentialIdentifiers.len != encodedSurbs.len:
    return err("every supplied SURB must have a reply credential identifier")
  if encodedSurbs.len > session.availableSurbSupplySlots:
    return err("remote SURB supply credit is exhausted")

  let firstSequence = session.nextSurbSupplySequence.valueOr:
    return err("SURB supply sequence space is exhausted")
  var sequence = firstSequence
  for index, encodedSurb in encodedSurbs:
    session.pendingSurbSupply[sequence] = PendingSurbSupply(
      encodedSurb: encodedSurb,
      credentialIdentifier: credentialIdentifiers[index],
      nextRetransmissionAt: Opt.none(Moment),
    )
    if sequence == MaxSurbSupplySequence:
      session.nextSurbSupplySequence = Opt.none(SurbSupplySequence)
    else:
      inc sequence
      session.nextSurbSupplySequence = Opt.some(sequence)
  ok(firstSequence)

proc registerInitialSurbSupply*(
    session: TransportSession,
    encodedSurbs: openArray[seq[byte]],
    credentialIdentifiers: openArray[SURBIdentifier],
): Result[SurbSupplySequence, string] =
  if session.role != SessionRole.Initiator:
    return err("only initiator sessions can supply SURBs")
  if session.nextSurbSupplySequence != Opt.some(SurbSupplySequence(0)) or
      session.pendingSurbSupply.len != 0:
    return err("initial SURB supply is already registered")
  if encodedSurbs.len == 0:
    return err("initial SURB supply must not be empty")
  if credentialIdentifiers.len != encodedSurbs.len:
    return err("every supplied SURB must have a reply credential identifier")

  var sequence = SurbSupplySequence(0)
  for index, encodedSurb in encodedSurbs:
    session.pendingSurbSupply[sequence] = PendingSurbSupply(
      encodedSurb: encodedSurb,
      credentialIdentifier: credentialIdentifiers[index],
      nextRetransmissionAt: Opt.none(Moment),
    )
    inc sequence
  session.nextSurbSupplySequence = Opt.some(sequence)
  ok(SurbSupplySequence(0))

proc removePendingSurbSupply*(
    session: TransportSession, firstSequence: SurbSupplySequence, count: int
) =
  for offset in 0 ..< count:
    session.pendingSurbSupply.del(firstSequence + SurbSupplySequence(offset))

proc scheduleSurbSupplyRetransmission*(
    session: TransportSession,
    firstSequence: SurbSupplySequence,
    count: int,
    delay: Duration,
    now: Moment = Moment.now(),
) =
  doAssert delay > ZeroDuration, "SURB supply retransmission delay must be positive"
  for offset in 0 ..< count:
    let sequence = firstSequence + SurbSupplySequence(offset)
    session.pendingSurbSupply.withValue(sequence, pending):
      pending.nextRetransmissionAt = Opt.some(now + delay)
  session.surbSupplyStateChanged.fire()

proc takeDueSurbSupplyRetransmission*(
    session: TransportSession, now: Moment = Moment.now()
): Opt[
    tuple[
      sequence: SurbSupplySequence,
      encodedSurb: seq[byte],
      credentialIdentifier: SURBIdentifier,
    ]
] =
  var
    selectedSequence = Opt.none(SurbSupplySequence)
    selectedDeadline = Opt.none(Moment)

  for sequence, pending in session.pendingSurbSupply:
    pending.nextRetransmissionAt.withValue(deadline):
      if deadline > now:
        continue
      if selectedDeadline.isNone or deadline < selectedDeadline.get() or
          (deadline == selectedDeadline.get() and sequence < selectedSequence.get()):
        selectedSequence = Opt.some(sequence)
        selectedDeadline = Opt.some(deadline)

  let sequence = selectedSequence.valueOr:
    return Opt.none(
      tuple[
        sequence: SurbSupplySequence,
        encodedSurb: seq[byte],
        credentialIdentifier: SURBIdentifier,
      ]
    )
  session.pendingSurbSupply.withValue(sequence, pending):
    pending.nextRetransmissionAt = Opt.none(Moment)
    return Opt.some(
      (
        sequence: sequence,
        encodedSurb: pending.encodedSurb,
        credentialIdentifier: pending.credentialIdentifier,
      )
    )
  Opt.none(
    tuple[
      sequence: SurbSupplySequence,
      encodedSurb: seq[byte],
      credentialIdentifier: SURBIdentifier,
    ]
  )

proc removePendingSurbSupply*(session: TransportSession, sequence: SurbSupplySequence) =
  session.pendingSurbSupply.del(sequence)

func earliestSurbSupplyRetransmission*(session: TransportSession): Opt[Moment] =
  var earliest = Opt.none(Moment)
  for pending in session.pendingSurbSupply.values:
    pending.nextRetransmissionAt.withValue(deadline):
      if earliest.isNone or deadline < earliest.get():
        earliest = Opt.some(deadline)
  earliest

proc applySurbSupplySnapshot*(
    session: TransportSession, snapshot: SurbSupplySnapshot
): bool =
  if session.role != SessionRole.Initiator or
      snapshot.acknowledgementBitmap.len != SurbSupplyAckBitmapBytes or
      snapshot.supplyLimit < snapshot.receiveBase:
    return false

  let nextSequence = session.nextSurbSupplySequence.get(SurbSupplySequence.high)
  if snapshot.receiveBase > nextSequence:
    return false

  var acknowledged: seq[SurbSupplySequence]
  for sequence in session.pendingSurbSupply.keys:
    if sequence < snapshot.receiveBase:
      acknowledged.add(sequence)
    else:
      let offset = sequence - snapshot.receiveBase
      if offset < SurbSupplySequence(SurbSupplyWindow) and
          snapshot.acknowledgementBitmap.bitmapContains(offset):
        acknowledged.add(sequence)
  for sequence in acknowledged:
    session.pendingSurbSupply.del(sequence)

  let changed =
    acknowledged.len > 0 or snapshot.receiveBase > session.remoteSurbSupplyReceiveBase or
    snapshot.supplyLimit > session.remoteSurbSupplyLimit
  session.remoteSurbSupplyReceiveBase =
    max(session.remoteSurbSupplyReceiveBase, snapshot.receiveBase)
  session.remoteSurbSupplyLimit =
    max(session.remoteSurbSupplyLimit, snapshot.supplyLimit)
  if changed:
    session.surbSupplyStateChanged.fire()
  true

proc clearSurbSupplyStateChanged*(session: TransportSession) =
  session.surbSupplyStateChanged.clear()

proc waitForSurbSupplyStateChange*(
    session: TransportSession
): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
  session.surbSupplyStateChanged.wait()

proc setSurbSupplierTask*(
    session: TransportSession, task: Future[void].Raising([CancelledError])
) =
  doAssert session.surbSupplierTask.isNil, "SURB supplier task is already set"
  session.surbSupplierTask = task

proc clearSurbSupplierTask*(session: TransportSession) =
  session.surbSupplierTask = nil

proc noteReverseActivity*(
    session: TransportSession, probeInterval: Duration, now: Moment = Moment.now()
) =
  doAssert probeInterval > ZeroDuration, "SURB status probe interval must be positive"
  session.unansweredSurbStatusProbes = 0
  session.nextSurbStatusProbeAt = Opt.some(now + probeInterval)
  session.surbSupplyStateChanged.fire()

proc recordSurbStatusProbeAttempt*(
    session: TransportSession, retryInterval: Duration, now: Moment = Moment.now()
) =
  doAssert retryInterval > ZeroDuration,
    "SURB status probe retry interval must be positive"
  inc session.unansweredSurbStatusProbes
  session.nextSurbStatusProbeAt = Opt.some(now + retryInterval)

func unansweredSurbStatusProbeCount*(session: TransportSession): int =
  session.unansweredSurbStatusProbes

func timeUntilSurbStatusProbe*(
    session: TransportSession, now: Moment = Moment.now()
): Opt[Duration] =
  session.nextSurbStatusProbeAt.withValue(deadline):
    return Opt.some(
      if deadline <= now:
        ZeroDuration
      else:
        deadline - now
    )
  Opt.none(Duration)

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
  session.replyCapacityStateChanged.fire()
  session.surbSupplyStateChanged.fire()
  let surbSupplierTask = session.surbSupplierTask
  session.surbSupplierTask = nil
  if not surbSupplierTask.isNil:
    await noCancel surbSupplierTask.cancelAndWait()
  let streams = session.takeStreams()
  var shutdownTasks = newSeqOfCap[Future[void].Raising([])](streams.len)
  for stream in streams:
    shutdownTasks.add(stream.shutdown())
  await noCancel shutdownTasks.allFutures()

proc remove*(store: SessionStore, sessionId: PeerId): Opt[TransportSession] =
  let session = store.get(sessionId).valueOr:
    return Opt.none(TransportSession)

  trace "removing session", sessionId = session.sessionId,
    role = session.role, state = session.state

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
    recipientSurbCapacity = DefaultRecipientSurbCapacity
): SessionStore =
  doAssert recipientSurbCapacity >= DefaultReplySurbRedundancy,
    "recipient SURB capacity must hold one reply redundancy batch"
  doAssert recipientSurbCapacity <= int(SurbSupplySequence.high),
    "recipient SURB capacity exceeds the supply credit space"
  SessionStore(
    bySessionId: initTable[PeerId, TransportSession](),
    byDestination: initTable[PeerId, TransportSession](),
    recipientSurbCapacity: recipientSurbCapacity,
  )

{.pop.}
