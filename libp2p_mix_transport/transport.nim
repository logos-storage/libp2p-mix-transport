# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronicles, chronos, chronos/asyncsync, results, tables
import libp2p/multistream
import libp2p/peerid
import libp2p/protocols/protocol
import libp2p/stream/bufferstream
import libp2p/stream/connection
import libp2p/utils/opt
import libp2p_mix
from libp2p_mix/serialization import SurbSize
import ./[reply_credentials, sessions, streams, wire]

logScope:
  topics = "mix-transport transport"

const
  DefaultConnectTimeout* = 30.seconds
  DefaultStreamOpenTimeout* = 30.seconds
  DefaultDataRetransmissionTimeout* = 30.seconds
  DefaultSurbSupplyRetransmissionTimeout* = 30.seconds
  DefaultReverseActivityTimeout* = 2.minutes
  DefaultSurbStatusProbeRetryInterval* = 30.seconds
  DefaultMaxSurbStatusProbeAttempts* = 3
  UnknownStreamRejectionReason = "remote rejected the stream for an unknown reason"

type MixTransport* = ref object of RootObj
  mix: MixProtocol
  replyCredentials: ReplyCredentialStore
  sessions: SessionStore
  connectTimeout: Duration
  streamOpenTimeout: Duration
  connLock: AsyncLock
  connectAttempts:
    Table[PeerId, Future[Result[TransportSession, string]].Raising([CancelledError])]
  dataRetransmissionTimeout: Duration
  surbSupplyRetransmissionTimeout: Duration
  reverseActivityTimeout: Duration
  surbStatusProbeRetryInterval: Duration
  maxSurbStatusProbeAttempts: int
  dataRetransmissionsEnabled: bool
  started: bool

type PreparedReplySurbs = object
  encoded: seq[seq[byte]]
  credentials: seq[ReplyCredential]

proc maxSurbCount(frame: MixTransportFrame): int =
  var candidate = frame
  while candidate.surbs.len < MaxTransportFrameBytes div SurbSize:
    candidate.surbs.add(newSeq[byte](SurbSize))
    candidate.firstSurbSequence =
      if candidate.surbs.len > DefaultReplySurbRedundancy:
        Opt.some(SurbSupplySequence(0))
      else:
        Opt.none(SurbSupplySequence)
    if candidate.surbs.len < DefaultReplySurbRedundancy:
      continue
    if candidate.encode().isErr:
      break
    result = candidate.surbs.len

proc handleData(self: MixTransport, frame: MixTransportFrame) {.gcsafe, raises: [].}
proc handleAcknowledgement(
  self: MixTransport, frame: MixTransportFrame
) {.gcsafe, raises: [].}

proc startSurbSupplier(
  self: MixTransport, session: TransportSession
) {.gcsafe, raises: [].}

proc runProtocolHandler(
    session: TransportSession, stream: TransportStream, protocol: LPProtocol
) {.async: (raises: [CancelledError]).} =
  defer:
    stream.clearHandlerTask()
    await noCancel stream.close()
    await noCancel stream.cancelAndWaitForStreamTasks()
    protocol.releaseIncoming(stream.peerId)
    discard session.removeStream(stream.streamId)

  # Binding the template accessor preserves LPProtoHandler's raises list.
  let handler: LPProtoHandler = protocol.handler
  await handler(stream, stream.codec)

proc attachSurbSupplySnapshot(session: TransportSession, frame: var MixTransportFrame) =
  let snapshot = session.surbSupplySnapshot()
  frame.surbSupplyReceiveBase = Opt.some(snapshot.receiveBase)
  frame.surbSupplyAcknowledgementBitmap = Opt.some(snapshot.acknowledgementBitmap)
  frame.surbSupplyLimit = Opt.some(snapshot.supplyLimit)

proc applySurbSupplySnapshot(
    session: TransportSession, frame: MixTransportFrame
): bool =
  if frame.surbSupplyReceiveBase.isNone:
    return true
  session.applySurbSupplySnapshot(
    SurbSupplySnapshot(
      receiveBase: frame.surbSupplyReceiveBase.get(),
      acknowledgementBitmap: frame.surbSupplyAcknowledgementBitmap.get(),
      supplyLimit: frame.surbSupplyLimit.get(),
    )
  )

proc handleReplyFrame(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  logScope:
    sessionId = frame.sessionId
    kind = frame.kind

  let session = self.sessions.get(frame.sessionId).valueOr:
    trace "discard reply frame - unknown session"
    return

  if not session.applySurbSupplySnapshot(frame):
    return
  if frame.surbSupplyReceiveBase.isSome:
    session.noteReverseActivity(self.reverseActivityTimeout)

  case frame.kind
  of FrameKind.ConnectAck:
    if session.role != SessionRole.Initiator:
      trace "discard reply frame - we are not the initiator"
      return
    if session.state != SessionState.Pending:
      trace "discard reply frame - not in Pending state"
      return
    session.establish()
    self.startSurbSupplier(session)
  of FrameKind.StreamAck, FrameKind.StreamReject:
    if session.state != SessionState.Established:
      trace "discard reply frame - session not established"
      return
    let stream = session.getStream(frame.streamId.get()).valueOr:
      trace "discard reply frame - unknown stream id", streamId = frame.streamId.get()
      return
    if stream.direction != StreamDirection.Outbound or
        stream.state != StreamState.Pending:
      return
    if frame.kind == FrameKind.StreamAck:
      stream.establish()
    else:
      let rejectionReason = frame.rejectionReason.get("")
      stream.reject(
        if rejectionReason.len == 0: UnknownStreamRejectionReason else: rejectionReason
      )
  of FrameKind.Data:
    self.handleData(frame)
  of FrameKind.Ack:
    self.handleAcknowledgement(frame)
  of FrameKind.SurbStatus:
    discard
  else:
    discard

method sendWithSurbRedundancyBatch(
    self: MixTransport, surbs: sink seq[SURB], payload: sink seq[byte]
): Future[Result[void, string]] {.async: (raises: [CancelledError]), base.} =
  var sent = false
  for surb in surbs.mitems:
    # Each SURB is consumed once, but every redundant packet needs the same
    # payload. Passing payload without move lets Nim copy it for each send.
    if (await self.mix.sendWithSurb(move(surb), payload)).isOk:
      sent = true

  if not sent:
    return err("could not send through any SURB in the redundancy batch")
  ok()

proc waitForReplySurbs(
    session: TransportSession, count: int
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  while session.receivedSurbCount < count:
    if session.state != SessionState.Established:
      return err("session closed while waiting for reply SURBs")
    session.clearReplyCapacityStateChanged()
    if session.receivedSurbCount < count:
      await session.waitForReplyCapacityStateChange()
  ok()

proc sendStreamFrame(
    self: MixTransport, session: TransportSession, frame: MixTransportFrame
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =

  trace "sending stream frame",
    frameKind = frame.kind, sessionId = session.sessionId, role = session.role

  case session.role
  of SessionRole.Initiator:
    let payload = frame.encode().valueOr:
      return err("could not encode " & $frame.kind & " frame: " & error)
    let destination = session.destination.valueOr:
      return err("initiator session has no destination")
    (
      await self.mix.send(
        MixDestination.exitNode(destination), MixTransportCodec, payload
      )
    ).isOkOr:
      return err("could not send " & $frame.kind & " frame: " & error)
  of SessionRole.Recipient:
    await session.acquireReplySend()
    defer:
      session.releaseReplySend()
    (await session.waitForReplySurbs(DefaultReplySurbRedundancy)).isOkOr:
      return err(error)
    var replyBatch = session.takeReceivedSurbs(DefaultReplySurbRedundancy).valueOr:
      return err(error)
    var replyFrame = frame
    session.attachSurbSupplySnapshot(replyFrame)
    let payload = replyFrame.encode().valueOr:
      return err("could not encode " & $frame.kind & " frame: " & error)
    (await self.sendWithSurbRedundancyBatch(replyBatch, payload)).isOkOr:
      return err("could not send " & $frame.kind & " frame: " & error)
  ok()

proc writeStream(
    self: MixTransport,
    session: TransportSession,
    stream: TransportStream,
    data: sink seq[byte],
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if stream.state != StreamState.Established:
    raise newException(LPStreamError, "MixTransport stream is not established")

  var offset = 0
  while offset < data.len:
    await stream.waitForOutboundCapacity()
    let chunkLength = min(MaxDataPayloadBytes, data.len - offset)
    var chunk = data[offset ..< offset + chunkLength]
    let reservedSequence = stream.reserveOutbound(chunk).valueOr:
      raise newException(LPStreamError, error)
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.Data,
      streamId: Opt.some(stream.streamId),
      sequence: Opt.some(reservedSequence),
      payload: Opt.some(move(chunk)),
    )
    (await self.sendStreamFrame(session, frame)).isOkOr:
      stream.cancelOutbound(reservedSequence)
      raise newException(LPStreamError, error)
    if self.dataRetransmissionsEnabled:
      stream.scheduleOutboundRetransmission(
        reservedSequence, self.dataRetransmissionTimeout
      )
    offset += chunkLength
    stream.activity = true

proc runInboundDelivery(stream: TransportStream) {.async: (raises: [CancelledError]).} =
  while not stream.closed:
    stream.clearInboundDataAvailable()
    while true:
      var inbound = stream.takeNextInbound().valueOr:
        break
      try:
        await stream.pushData(move(inbound.payload))
      except LPStreamError:
        return
      stream.advanceReceiveWindow(inbound.sequence)
    await stream.waitForInboundData()

proc runAcknowledgements(
    self: MixTransport, session: TransportSession, stream: TransportStream
) {.async: (raises: [CancelledError]).} =
  while not stream.closed:
    await stream.waitForShouldSendAck()
    if stream.closed:
      return
    stream.clearShouldSendAck()
    let snapshot = stream.acknowledgementSnapshot()

    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.Ack,
      streamId: Opt.some(stream.streamId),
      receiveBase: Opt.some(snapshot.receiveBase),
      acknowledgementBitmap: Opt.some(snapshot.acknowledgementBitmap),
    )
    if (await self.sendStreamFrame(session, frame)).isErr:
      return

proc runRetransmissions(
    self: MixTransport, session: TransportSession, stream: TransportStream
) {.async: (raises: [CancelledError]).} =
  while not stream.closed:
    stream.clearRetransmissionStateChanged()

    var retransmission = stream.takeDueOutboundRetransmission().valueOr:
      let deadline = stream.earliestRetransmissionDeadline().valueOr:
        await stream.waitForRetransmissionStateChange()
        continue
      let waitTime = deadline - Moment.now()
      if waitTime > ZeroDuration:
        discard await stream.waitForRetransmissionStateChange().withTimeout(waitTime)
      continue

    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.Data,
      streamId: Opt.some(stream.streamId),
      sequence: Opt.some(retransmission.sequence),
      payload: Opt.some(move(retransmission.payload)),
    )
    let sent = await self.sendStreamFrame(session, frame)
    if sent.isErr:
      debug "Could not retransmit Data frame",
        sessionId = session.sessionId,
        streamId = stream.streamId,
        sequence = retransmission.sequence,
        error = sent.error
    stream.scheduleOutboundRetransmission(
      retransmission.sequence, self.dataRetransmissionTimeout
    )

proc configureStream(
    self: MixTransport, session: TransportSession, stream: TransportStream
) =
  let writeHandler: StreamWriteHandler = proc(
      data: sink seq[byte]
  ): Future[void] {.async: (raw: true, raises: [CancelledError, LPStreamError]).} =
    self.writeStream(session, stream, move(data))
  stream.setWriteHandler(writeHandler)
  stream.trackStreamTask(runInboundDelivery(stream))
  stream.trackStreamTask(runAcknowledgements(self, session, stream))
  if self.dataRetransmissionsEnabled:
    stream.trackStreamTask(runRetransmissions(self, session, stream))

proc handleData(self: MixTransport, frame: MixTransportFrame) {.gcsafe, raises: [].} =
  logScope:
    sessionId = frame.sessionId
    frameKind = frame.kind

  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "discarding frame: session not found"
    return
  if session.state != SessionState.Established:
    debug "discarding frame: session not yet established"
    return
  let stream = session.getStream(frame.streamId.get()).valueOr:
    debug "discarding frame: stream not found"
    return
  if stream.state != StreamState.Established:
    debug "discarding frame: stream not yet established"
    return
  discard stream.receiveData(frame.sequence.get(), frame.payload.get())

proc handleAcknowledgement(
    self: MixTransport, frame: MixTransportFrame
) {.gcsafe, raises: [].} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "Dropping Ack frame for unknown session", sessionId = frame.sessionId
    return
  if session.state != SessionState.Established:
    debug "Dropping Ack frame because session is not established",
      sessionId = frame.sessionId, sessionState = session.state
    return
  let stream = session.getStream(frame.streamId.get()).valueOr:
    debug "Dropping Ack frame for unknown stream",
      sessionId = frame.sessionId, streamId = frame.streamId.get()
    return
  if stream.state != StreamState.Established:
    debug "Dropping Ack frame because stream is not established",
      sessionId = frame.sessionId,
      streamId = stream.streamId,
      streamState = stream.state
    return
  discard stream.applyAcknowledgement(
    frame.receiveBase.get(), frame.acknowledgementBitmap.get()
  )

proc handleSurbSupply(
    self: MixTransport, frame: MixTransportFrame
) {.gcsafe, raises: [].} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "Dropping SurbSupply frame for unknown session", sessionId = frame.sessionId
    return
  if session.role != SessionRole.Recipient:
    debug "Dropping SurbSupply frame for session with unexpected role",
      sessionId = frame.sessionId, sessionRole = session.role
    return
  if session.state != SessionState.Established:
    debug "Dropping SurbSupply frame because session is not established",
      sessionId = frame.sessionId, sessionState = session.state
    return

  let firstSequence = frame.firstSurbSequence.get()
  for index, encodedSurb in frame.surbs:
    let sequence = firstSequence + SurbSupplySequence(index)
    let surb = encodedSurb.deserializeSurb().valueOr:
      continue
    discard session.acceptSurbSupply(sequence, surb)

proc sendStreamResponse(
    self: MixTransport,
    session: TransportSession,
    replyBatch: sink seq[SURB],
    streamId: StreamId,
    kind: FrameKind,
    rejectionReason = "",
): Future[bool] {.async: (raises: [CancelledError]).} =
  doAssert kind in {FrameKind.StreamAck, FrameKind.StreamReject}
  doAssert (kind == FrameKind.StreamReject) == (rejectionReason.len > 0)

  logScope:
    sessionId = session.sessionId
    streamId = streamId
    kind = kind

  let boundedRejectionReason =
    if rejectionReason.len > MaxStreamRejectionReasonBytes:
      rejectionReason[0 ..< MaxStreamRejectionReasonBytes]
    else:
      rejectionReason

  trace "sending stream response", rejectionReason = boundedRejectionReason
  var response = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: kind,
    streamId: Opt.some(streamId),
    rejectionReason:
      if kind == FrameKind.StreamReject:
        Opt.some(boundedRejectionReason)
      else:
        Opt.none(string),
  )
  session.attachSurbSupplySnapshot(response)
  let payload = response.encode().valueOr:
    return false
  (await self.sendWithSurbRedundancyBatch(replyBatch, payload)).isOk

proc handleConnect(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  logScope:
    sessionId = frame.sessionId

  if self.sessions.get(frame.sessionId).isSome:
    trace "session already exists, ignoring connect"
    return

  var replyBatch = newSeqOfCap[SURB](DefaultReplySurbRedundancy)
  for index in 0 ..< DefaultReplySurbRedundancy:
    let surb = frame.surbs[index].deserializeSurb().valueOr:
      return
    replyBatch.add(surb)

  let session = self.sessions.addRecipientSession(frame.sessionId).valueOr:
    error "error registering session", error = error
    return

  var keepSession = false
  defer:
    if not keepSession:
      discard self.sessions.remove(frame.sessionId)

  session.initializeSurbSupply().isOkOr:
    return
  if frame.surbs.len > DefaultReplySurbRedundancy:
    let firstSequence = frame.firstSurbSequence.get()
    for index in DefaultReplySurbRedundancy ..< frame.surbs.len:
      let surb = frame.surbs[index].deserializeSurb().valueOr:
        continue
      let sequence =
        firstSequence + SurbSupplySequence(index - DefaultReplySurbRedundancy)
      discard session.acceptSurbSupply(sequence, surb)
  var acknowledgement = MixTransportFrame(
    version: MixTransportVersion, sessionId: frame.sessionId, kind: FrameKind.ConnectAck
  )
  session.attachSurbSupplySnapshot(acknowledgement)
  let payload = acknowledgement.encode().valueOr:
    return

  # Marks the session as established BEFORE sending out ACKs
  # or the other side might try to use it before it's ready
  # and have its frames dropped.
  session.establish()
  (await self.sendWithSurbRedundancyBatch(replyBatch, payload)).isOkOr:
    return

  keepSession = true

proc handleOpenStream(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "Dropping OpenStream frame for unknown session", sessionId = frame.sessionId
    return
  if session.role != SessionRole.Recipient:
    debug "Dropping OpenStream frame for session with unexpected role",
      sessionId = frame.sessionId, sessionRole = session.role
    return
  if session.state != SessionState.Established:
    debug "Dropping OpenStream frame because session is not established",
      sessionId = frame.sessionId, sessionState = session.state
    return

  var replyBatch = newSeqOfCap[SURB](DefaultReplySurbRedundancy)
  for index in 0 ..< DefaultReplySurbRedundancy:
    let surb = frame.surbs[index].deserializeSurb().valueOr:
      return
    replyBatch.add(surb)
  if frame.surbs.len > DefaultReplySurbRedundancy:
    let firstSequence = frame.firstSurbSequence.get()
    for index in DefaultReplySurbRedundancy ..< frame.surbs.len:
      let surb = frame.surbs[index].deserializeSurb().valueOr:
        continue
      let sequence =
        firstSequence + SurbSupplySequence(index - DefaultReplySurbRedundancy)
      discard session.acceptSurbSupply(sequence, surb)

  let protocol = self.mix.switch.ms.lookupProtocol(frame.codec.get()).valueOr:
    discard await self.sendStreamResponse(
      session,
      move(replyBatch),
      frame.streamId.get(),
      FrameKind.StreamReject,
      "requested protocol is not supported",
    )
    return

  if not protocol.reserveIncoming(session.peerId):
    discard await self.sendStreamResponse(
      session,
      move(replyBatch),
      frame.streamId.get(),
      FrameKind.StreamReject,
      "requested protocol cannot accept another incoming stream",
    )
    return

  var keepReservation = false
  defer:
    if not keepReservation:
      protocol.releaseIncoming(session.peerId)

  let streamResult = session.addInboundStream(frame.streamId.get(), frame.codec.get())
  if streamResult.isErr:
    discard await self.sendStreamResponse(
      session,
      move(replyBatch),
      frame.streamId.get(),
      FrameKind.StreamReject,
      streamResult.error,
    )
    return
  let stream = streamResult.get()
  var keepStream = false
  defer:
    if not keepStream:
      discard session.removeStream(stream.streamId)
      await noCancel stream.shutdown()

  # We need to transition our local state machine before sending the ACKs,
  # or the initiator might race us, send data before we're done, and have
  # their data silently dropped.
  stream.establish()
  if not await self.sendStreamResponse(
    session, move(replyBatch), stream.streamId, FrameKind.StreamAck
  ):
    return

  self.configureStream(session, stream)
  let handlerTask = runProtocolHandler(session, stream, protocol)
  # If the handler dies immediately, don't set it: the cleanup in
  # runProtocolHandler has already run, and will fail to clear it.
  if not handlerTask.finished:
    stream.setHandlerTask(handlerTask)

  keepStream = true
  keepReservation = true

proc handleSurbStatusProbe(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    return
  if session.role != SessionRole.Recipient or session.state != SessionState.Established:
    return

  var replyBatch = newSeqOfCap[SURB](DefaultReplySurbRedundancy)
  for encodedSurb in frame.surbs:
    let surb = encodedSurb.deserializeSurb().valueOr:
      continue
    replyBatch.add(surb)
    if replyBatch.len == DefaultReplySurbRedundancy:
      break
  if replyBatch.len < DefaultReplySurbRedundancy:
    return

  var response = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.SurbStatus,
  )
  session.attachSurbSupplySnapshot(response)
  let payload = response.encode().valueOr:
    return
  discard await self.sendWithSurbRedundancyBatch(replyBatch, payload)

proc handleDelivery(
    self: MixTransport, delivery: MixDelivery
): Future[void] {.async: (raises: [CancelledError]).} =
  let frame = MixTransportFrame.decode(delivery.payload).valueOr:
    error "failed to decode mix transport frame", error = error
    return

  trace "handling request transport frame",
    frameKind = frame.kind, sessionId = frame.sessionId
  case frame.kind
  of FrameKind.Connect:
    await self.handleConnect(frame)
  of FrameKind.OpenStream:
    await self.handleOpenStream(frame)
  of FrameKind.Data:
    self.handleData(frame)
  of FrameKind.Ack:
    self.handleAcknowledgement(frame)
  of FrameKind.SurbSupply:
    self.handleSurbSupply(frame)
  of FrameKind.SurbStatusProbe:
    await self.handleSurbStatusProbe(frame)
  else:
    discard

proc handleRawSurbReply(
    self: MixTransport, reply: RawSurbReply
): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
  if self.replyCredentials.isRetiredIdentifier(reply.identifier):
    return RawSurbReplyDisposition.Handled

  let recovered = self.replyCredentials.recoverReply(reply).valueOr:
    return RawSurbReplyDisposition.Handled

  recovered.withValue(value):
    let frame = MixTransportFrame.decode(value.payload).valueOr:
      return RawSurbReplyDisposition.Handled
    if frame.sessionId == value.sessionId:
      await self.handleReplyFrame(frame)
    return RawSurbReplyDisposition.Handled

  RawSurbReplyDisposition.Unhandled

proc newMixTransport*(
    T: type MixTransport,
    mix: MixProtocol,
    connectTimeout = DefaultConnectTimeout,
    streamOpenTimeout = DefaultStreamOpenTimeout,
    dataRetransmissionTimeout = DefaultDataRetransmissionTimeout,
    surbSupplyRetransmissionTimeout = DefaultSurbSupplyRetransmissionTimeout,
    reverseActivityTimeout = DefaultReverseActivityTimeout,
    surbStatusProbeRetryInterval = DefaultSurbStatusProbeRetryInterval,
    maxSurbStatusProbeAttempts = DefaultMaxSurbStatusProbeAttempts,
    enableDataRetransmissions = true,
    recipientSurbCapacity = DefaultRecipientSurbCapacity,
): T =

  doAssert not mix.isNil, "MixProtocol must not be nil"
  doAssert connectTimeout > ZeroDuration, "connect timeout must be positive"
  doAssert streamOpenTimeout > ZeroDuration, "stream open timeout must be positive"
  doAssert dataRetransmissionTimeout > ZeroDuration,
    "Data retransmission timeout must be positive"
  doAssert surbSupplyRetransmissionTimeout > ZeroDuration,
    "SURB supply retransmission timeout must be positive"
  doAssert reverseActivityTimeout > ZeroDuration,
    "reverse activity timeout must be positive"
  doAssert surbStatusProbeRetryInterval > ZeroDuration,
    "SURB status probe retry interval must be positive"
  doAssert maxSurbStatusProbeAttempts > 0,
    "maximum SURB status probe attempts must be positive"
  doAssert recipientSurbCapacity >=
    MaxTransportFrameBytes div SurbSize - DefaultReplySurbRedundancy,
    "recipient SURB capacity must hold the Connect bootstrap supply"
  T(
    mix: mix,
    replyCredentials: ReplyCredentialStore.new(),
    sessions: newSessionStore(recipientSurbCapacity),
    connectTimeout: connectTimeout,
    streamOpenTimeout: streamOpenTimeout,
    connLock: newAsyncLock(),
    dataRetransmissionTimeout: dataRetransmissionTimeout,
    surbSupplyRetransmissionTimeout: surbSupplyRetransmissionTimeout,
    reverseActivityTimeout: reverseActivityTimeout,
    surbStatusProbeRetryInterval: surbStatusProbeRetryInterval,
    maxSurbStatusProbeAttempts: maxSurbStatusProbeAttempts,
    dataRetransmissionsEnabled: enableDataRetransmissions,
  )

proc createReplySurbs(
    self: MixTransport, destination: PeerId, sessionId: PeerId, count: int
): Result[PreparedReplySurbs, string] =
  let mixDestination = MixDestination.exitNode(destination)
  var prepared = PreparedReplySurbs(
    encoded: newSeqOfCap[seq[byte]](count),
    credentials: newSeqOfCap[ReplyCredential](count),
  )

  for _ in 0 ..< count:
    var created = self.mix.createSurb(mixDestination).valueOr:
      return err("could not create SURB: " & error)
    prepared.encoded.add(created.surb.serializeSurb())
    prepared.credentials.add(created.credential)

  self.replyCredentials.add(sessionId, prepared.credentials).isOkOr:
    return err("could not register reply credentials: " & error)

  ok(prepared)

proc retireReplyCredentials(
    self: MixTransport, credentials: openArray[ReplyCredential]
) =
  self.replyCredentials.consume(credentials)

proc sendSurbSupply(
    self: MixTransport,
    session: TransportSession,
    firstSequence: SurbSupplySequence,
    encodedSurbs: seq[seq[byte]],
): Future[void] {.async: (raises: [CancelledError]).} =
  let frame = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.SurbSupply,
    firstSurbSequence: Opt.some(firstSequence),
    surbs: encodedSurbs,
  )
  let sent = await self.sendStreamFrame(session, frame)
  if sent.isErr:
    debug "Could not send SURB supply",
      sessionId = session.sessionId, firstSequence, error = sent.error
  session.scheduleSurbSupplyRetransmission(
    firstSequence, encodedSurbs.len, self.surbSupplyRetransmissionTimeout
  )

proc createAndSendSurbSupply(
    self: MixTransport, session: TransportSession, count: int
): Future[bool] {.async: (raises: [CancelledError]).} =
  let destination = session.destination.valueOr:
    return false
  let prepared = self.createReplySurbs(destination, session.sessionId, count).valueOr:
    debug "Could not create SURB supply", sessionId = session.sessionId, error
    return false
  var credentialIdentifiers = newSeqOfCap[SURBIdentifier](prepared.credentials.len)
  for credential in prepared.credentials:
    credentialIdentifiers.add(credential.identifier)
  let firstSequence = session.registerSurbSupply(
    prepared.encoded, credentialIdentifiers
  ).valueOr:
    self.retireReplyCredentials(prepared.credentials)
    debug "Could not register SURB supply", sessionId = session.sessionId, error
    return false
  await self.sendSurbSupply(session, firstSequence, prepared.encoded)
  true

proc retransmitSurbSupply(
    self: MixTransport,
    session: TransportSession,
    sequence: SurbSupplySequence,
    encodedSurb: seq[byte],
): Future[void] {.async: (raises: [CancelledError]).} =
  await self.sendSurbSupply(session, sequence, @[encodedSurb])

proc sendSurbStatusProbe(
    self: MixTransport, session: TransportSession
): Future[void] {.async: (raises: [CancelledError]).} =
  let destination = session.destination.valueOr:
    return
  let prepared = self.createReplySurbs(
    destination, session.sessionId, DefaultReplySurbRedundancy
  ).valueOr:
    debug "Could not create SURB status probe reply paths",
      sessionId = session.sessionId, error
    return
  let probe = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.SurbStatusProbe,
    surbs: prepared.encoded,
  )
  (await self.sendStreamFrame(session, probe)).isOkOr:
    self.retireReplyCredentials(prepared.credentials)
    return

proc runSurbSupplier(
    self: MixTransport, session: TransportSession
) {.async: (raises: [CancelledError]), gcsafe.} =
  defer:
    session.clearSurbSupplierTask()

  session.noteReverseActivity(self.reverseActivityTimeout)
  while session.state == SessionState.Established:
    session.clearSurbSupplyStateChanged()

    let probeWait = session.timeUntilSurbStatusProbe()
    let statusProbeIsDue = probeWait.isSome and probeWait.get() <= ZeroDuration
    if statusProbeIsDue:
      if session.unansweredSurbStatusProbeCount >= self.maxSurbStatusProbeAttempts:
        error "MixTransport session did not respond to SURB status probes",
          sessionId = session.sessionId,
          attempts = session.unansweredSurbStatusProbeCount
        discard self.sessions.remove(session.sessionId)
        discard self.replyCredentials.removeSession(session.sessionId)
        session.clearSurbSupplierTask()
        await session.shutdown()
        return
      session.recordSurbStatusProbeAttempt(self.surbStatusProbeRetryInterval)
      await self.sendSurbStatusProbe(session)
      continue

    if session.availableSurbSupplySlots > 0:
      let count = min(MaxSurbSupplyPerFrame, session.availableSurbSupplySlots)
      if await self.createAndSendSurbSupply(session, count):
        continue

    let retransmission = session.takeDueSurbSupplyRetransmission()
    if retransmission.isSome:
      let value = retransmission.get()
      discard self.replyCredentials.purgeExpired()
      if self.replyCredentials.get(value.credentialIdentifier).isNone:
        session.removePendingSurbSupply(value.sequence)
        debug "Discarding pending SURB supply without an active reply credential",
          sessionId = session.sessionId, sequence = value.sequence
        continue
      await self.retransmitSurbSupply(session, value.sequence, value.encodedSurb)
      continue

    var waitTime = Opt.none(Duration)
    session.earliestSurbSupplyRetransmission().withValue(deadline):
      waitTime = Opt.some(
        if deadline <= Moment.now():
          ZeroDuration
        else:
          deadline - Moment.now()
      )
    probeWait.withValue(value):
      if waitTime.isNone or value < waitTime.get():
        waitTime = Opt.some(value)

    waitTime.withValue(value):
      if value <= ZeroDuration:
        continue
      discard await session.waitForSurbSupplyStateChange().withTimeout(value)
      continue
    await session.waitForSurbSupplyStateChange()

proc startSurbSupplier(
    self: MixTransport, session: TransportSession
) {.gcsafe, raises: [].} =
  let task = self.runSurbSupplier(session)
  if not task.finished:
    session.setSurbSupplierTask(task)

proc createConnectFrame(
    self: MixTransport, destination: PeerId, session: TransportSession
): Result[MixTransportFrame, string] =
  var frame = MixTransportFrame(
    version: MixTransportVersion, sessionId: session.sessionId, kind: FrameKind.Connect
  )
  let surbCount = frame.maxSurbCount()
  if surbCount < DefaultReplySurbRedundancy:
    return err("Connect frame cannot hold one reply redundancy batch")
  let prepared = self.createReplySurbs(destination, session.sessionId, surbCount).valueOr:
    return err("could not prepare Connect reply SURBs: " & error)
  frame.surbs = prepared.encoded

  let suppliedCount = surbCount - DefaultReplySurbRedundancy
  if suppliedCount > 0:
    var credentialIdentifiers = newSeqOfCap[SURBIdentifier](suppliedCount)
    for index in DefaultReplySurbRedundancy ..< prepared.credentials.len:
      credentialIdentifiers.add(prepared.credentials[index].identifier)
    let firstSequence = session.registerInitialSurbSupply(
      prepared.encoded.toOpenArray(DefaultReplySurbRedundancy, prepared.encoded.high),
      credentialIdentifiers,
    ).valueOr:
      return err("could not register initial SURB supply: " & error)
    frame.firstSurbSequence = Opt.some(firstSequence)

  ok(frame)

proc connectInternal(
    self: MixTransport, destination: PeerId
): Future[Result[TransportSession, string]] {.async: (raises: [CancelledError]).} =
  let sessionId = PeerId.random(self.mix.switch.rng).valueOr:
    return err("could not generate session identifier: " & $error)
  let session = self.sessions.addInitiatorSession(destination, sessionId).valueOr:
    return err(error)

  var keepSession = false
  defer:
    if not keepSession:
      discard self.sessions.remove(sessionId)
      discard self.replyCredentials.removeSession(sessionId)

  let frame = self.createConnectFrame(destination, session).valueOr:
    return err(error)
  let payload = frame.encode().valueOr:
    return err("could not encode Connect frame: " & error)

  (
    await self.mix.send(
      MixDestination.exitNode(destination), MixTransportCodec, payload
    )
  ).isOkOr:
    return err("could not send Connect frame: " & error)

  let suppliedCount = frame.surbs.len - DefaultReplySurbRedundancy
  if suppliedCount > 0:
    session.scheduleSurbSupplyRetransmission(
      frame.firstSurbSequence.get(), suppliedCount, self.surbSupplyRetransmissionTimeout
    )

  if not await session.waitUntilEstablished().withTimeout(self.connectTimeout):
    return err("MixTransport connect timed out")
  if session.state != SessionState.Established:
    return err("MixTransport session closed while connecting")

  keepSession = true
  ok(session)

## Connect operation synchronizer, implemented like this so we can test it in
## isolation.
proc connect[S, K, T](
    self: S,
    destination: K,
    connInternal: (
      proc(self: S, destination: K): Future[Result[T, string]].Raising([CancelledError]) {.
        gcsafe, raises: []
      .}
    ),
    getExisting: (proc(self: S, destination: K): Opt[T] {.gcsafe, raises: [].}),
): Future[Result[T, string]] {.async: (raises: [CancelledError]).} =
  template releaseLock() =
    try:
      self.connLock.release()
    except AsyncLockError:
      doAssert false, "lock release twice"

  trace "acquire connect lock", destination = destination
  await self.connLock.acquire()
  # If a connection already exists and it is established, we return it.
  self.getExisting(destination).withValue(existing):
    trace "return existing connection", destination = destination
    releaseLock()
    # It might happen that the connection is no longer valid here, but
    # that's fine.
    return ok(existing)

  # If a valid connection doesn't exist, then this is officially a connection
  # attempt. Our goal here then becomes to merge all connection attempts
  # into a single operation, with only one of the requesters "owning" the
  # actual attempt while everyone else awaits.

  # Are we the first ones to attempt this connection?
  trace "attempt connection", destination = destination
  if destination in self.connectAttempts:
    # No, someone else owns the attempt, so we just wait.
    # Copies before releasing the lock as otherwise the attempt
    # could complete before we can get a hold of the future.
    var attempt: Future[Result[T, string]].Raising([CancelledError])
    try:
      attempt = self.connectAttempts[destination]
    except exceptions.KeyError:
      doAssert false, "assertion failed"
    releaseLock()
    return await attempt

  # If we're here, we're sure that we're the ones handling this
  # attempt at connecting to this destination.
  let attempt =
    Future[Result[T, string]].Raising([CancelledError]).init("transport.connect")
  # Note that this should never replace an existing attempt.
  doAssert destination notin self.connectAttempts
  self.connectAttempts[destination] = attempt
  # We can release the lock here as we've secured the attempt.
  releaseLock()

  let res =
    try:
      await self.connInternal(destination)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      err(e.msg)
    finally:
      # This is what will officially end an attempt. Once the
      # future is out of the table, another requester can re-attempt
      # the connection. Until then, everyone will see the result of
      # the previous attempt.
      await self.connLock.acquire()
      self.connectAttempts.del(destination)
      releaseLock()

  trace "complete connection attempt", destination = destination, success = res.isOk
  attempt.complete(res)
  res

proc connect*(
    self: MixTransport, destination: PeerId
): Future[Result[TransportSession, string]] {.async: (raises: [CancelledError]).} =
  if not self.started:
    return err("MixTransport is not started")

  proc getExisting(
      self: MixTransport, destination: PeerId
  ): Opt[TransportSession] {.nimcall, gcsafe.} =
    self.sessions.getByDestination(destination).withValue(existing):
      if existing.state == SessionState.Established:
        return Opt.some(existing)
    return Opt.none(TransportSession)

  await connect[MixTransport, PeerId, TransportSession](
    self, destination, connectInternal, getExisting
  )

proc dial*(
    self: MixTransport, destination: PeerId, codec: string
): Future[Result[TransportStream, string]] {.async: (raises: [CancelledError]).} =
  if not self.started:
    return err("MixTransport is not started")
  if codec.len == 0:
    return err("stream codec must not be empty")
  if codec.len > MaxCodecBytes:
    return err("stream codec is too long")

  let session = (await self.connect(destination)).valueOr:
    return err(error)
  let stream = session.addOutboundStream(codec).valueOr:
    return err(error)
  var keepStream = false
  defer:
    if not keepStream:
      discard session.removeStream(stream.streamId)
      await noCancel stream.shutdown()

  var frame = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.OpenStream,
    streamId: Opt.some(stream.streamId),
    codec: Opt.some(codec),
  )
  let maxSurbCount = frame.maxSurbCount()
  if maxSurbCount < DefaultReplySurbRedundancy:
    return err("OpenStream frame cannot hold one reply redundancy batch")
  let
    suppliedCount =
      min(maxSurbCount - DefaultReplySurbRedundancy, session.availableSurbSupplySlots)
    surbCount = DefaultReplySurbRedundancy + suppliedCount
  let prepared = self.createReplySurbs(destination, session.sessionId, surbCount).valueOr:
    return err("could not prepare OpenStream reply SURBs: " & error)
  frame.surbs = prepared.encoded

  var firstSupplySequence = Opt.none(SurbSupplySequence)
  var keepReplyCredentials = false
  defer:
    if not keepReplyCredentials:
      self.retireReplyCredentials(prepared.credentials)
      firstSupplySequence.withValue(sequence):
        session.removePendingSurbSupply(sequence, suppliedCount)

  if suppliedCount > 0:
    var credentialIdentifiers = newSeqOfCap[SURBIdentifier](suppliedCount)
    for index in DefaultReplySurbRedundancy ..< prepared.credentials.len:
      credentialIdentifiers.add(prepared.credentials[index].identifier)
    let firstSequence = session.registerSurbSupply(
      prepared.encoded.toOpenArray(DefaultReplySurbRedundancy, prepared.encoded.high),
      credentialIdentifiers,
    ).valueOr:
      return err("could not register OpenStream SURB supply: " & error)
    firstSupplySequence = Opt.some(firstSequence)
    frame.firstSurbSequence = firstSupplySequence

  let payload = frame.encode().valueOr:
    return err("could not encode OpenStream frame: " & error)
  (
    await self.mix.send(
      MixDestination.exitNode(destination), MixTransportCodec, payload
    )
  ).isOkOr:
    return err("could not send OpenStream frame: " & error)
  keepReplyCredentials = true
  firstSupplySequence.withValue(sequence):
    session.scheduleSurbSupplyRetransmission(
      sequence, suppliedCount, self.surbSupplyRetransmissionTimeout
    )

  if not await stream.waitUntilResolved().withTimeout(self.streamOpenTimeout):
    return err("MixTransport stream opening timed out")
  if stream.closed:
    return err("MixTransport stream closed while opening")
  if stream.state == StreamState.Rejected:
    return err(stream.rejectionReason)

  self.configureStream(session, stream)
  keepStream = true
  ok(stream)

proc start*(
    self: MixTransport
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if self.started:
    return ok()

  let deliveryHandler: MixDeliveryHandler = proc(
      delivery: MixDelivery
  ): Future[void] {.async: (raw: true, raises: [CancelledError]).} =
    self.handleDelivery(delivery)

  self.mix.registerMixDeliveryHandler(MixTransportCodec, deliveryHandler).isOkOr:
    return err(error)

  let rawSurbReplyHandler: RawSurbReplyHandler = proc(
      reply: RawSurbReply
  ): Future[RawSurbReplyDisposition] {.async: (raw: true, raises: [CancelledError]).} =
    self.handleRawSurbReply(reply)

  self.mix.registerRawSurbReplyHandler(rawSurbReplyHandler).isOkOr:
    self.mix.unregisterMixDeliveryHandler(MixTransportCodec)
    return err(error)

  self.started = true
  ok()

proc stop*(self: MixTransport): Future[void] {.async: (raises: [CancelledError]).} =
  if not self.started:
    return

  self.mix.unregisterRawSurbReplyHandler()
  self.mix.unregisterMixDeliveryHandler(MixTransportCodec)
  let sessions = self.sessions.takeSessions()
  var shutdownTasks = newSeqOfCap[Future[void].Raising([])](sessions.len)
  for session in sessions:
    shutdownTasks.add(session.shutdown())
  await noCancel shutdownTasks.allFutures()
  self.replyCredentials.clear()
  self.started = false

{.pop.}
