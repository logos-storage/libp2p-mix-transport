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
import ./[reply_credentials, sessions, streams, wire]

logScope:
  topics = "mix-transport transport"

const
  DefaultConnectTimeout* = 30.seconds
  DefaultStreamOpenTimeout* = 30.seconds
  DefaultRefillRequestTimeout* = 30.seconds
  DefaultDataRetransmissionTimeout* = 30.seconds
  MinimumConnectReplyGroups* = 2
  DefaultConnectReplyGroups* = MinimumConnectReplyGroups
  DefaultConnectSurbRedundancy* = 2
  DefaultOpenStreamReplyGroups* = 2
  DefaultOpenStreamSurbRedundancy* = 2
  DefaultRefillGroups* = MaxRefillGroupsPerFrame.int
  DefaultRefillSurbRedundancy* = 2
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
  refillRequestTimeout: Duration
  dataRetransmissionTimeout: Duration
  dataRetransmissionsEnabled: bool
  started: bool

type PreparedReplyGroups = object
  encoded: seq[SurbGroup]
  credentials: seq[ReplyCredentialGroup]

proc handleRefillRequest(
  self: MixTransport, session: TransportSession, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).}

proc handleData(self: MixTransport, frame: MixTransportFrame) {.gcsafe, raises: [].}
proc handleAcknowledgement(
  self: MixTransport, frame: MixTransportFrame
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

proc handleReplyFrame(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    trace "discard reply frame - unknown session", sessionId = frame.sessionId
    return

  logScope:
    sessionId = frame.sessionId
    kind = frame.kind

  case frame.kind
  of FrameKind.ConnectAck:
    if session.role != SessionRole.Initiator:
      trace "discard reply frame - we are not the initiator"
      return
    if session.state != SessionState.Pending:
      trace "discard reply frame - not in Pending state"
      return
    session.establish()
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
  of FrameKind.RefillRequest:
    if session.role == SessionRole.Initiator and
        session.state == SessionState.Established:
      await self.handleRefillRequest(session, frame)
  else:
    discard

method sendWithSurbGroup(
    self: MixTransport, surbs: sink seq[SURB], payload: sink seq[byte]
): Future[Result[void, string]] {.async: (raises: [CancelledError]), base.} =
  var sent = false
  for surb in surbs.mitems:
    # Each SURB is consumed once, but every redundant packet needs the same
    # payload. Passing payload without move lets Nim copy it for each send.
    if (await self.mix.sendWithSurb(move(surb), payload)).isOk:
      sent = true

  if not sent:
    return err("could not send through any SURB in the reply group")
  ok()

proc requestRefill(
    self: MixTransport, session: TransportSession
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if not session.refillRequestDue:
    return ok()

  let refillRequestId = session.registerRefillRequest().valueOr:
    return err(error)

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
    session.cancelRefillRequest(refillRequestId)
    return err("could not reserve a SURB group for refill: " & error)
  let request = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.RefillRequest,
    refillRequestId: Opt.some(refillRequestId),
    requestedGroups: Opt.some(DefaultRefillGroups.uint32),
  ).encode().valueOr:
    session.cancelRefillRequest(refillRequestId)
    return err("could not encode RefillRequest: " & error)
  session.scheduleNextRefillRequest(self.refillRequestTimeout)
  (await self.sendWithSurbGroup(replyGroup, request)).isOkOr:
    session.cancelRefillRequest(refillRequestId)
    return err("could not send RefillRequest: " & error)
  ok()

proc ensureUnreservedSurbGroup(
    self: MixTransport, session: TransportSession
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  while session.receivedSurbGroupCount <= ReplyControlReserveGroups:
    session.clearReplyCapacityStateChanged()
    (await self.requestRefill(session)).isOkOr:
      return err(error)
    if session.receivedSurbGroupCount <= ReplyControlReserveGroups:
      let waitTime = session.timeUntilNextRefillRequest()
      if waitTime > ZeroDuration:
        discard await session.waitForReplyCapacityStateChange().withTimeout(waitTime)
  ok()

proc sendStreamFrame(
    self: MixTransport, session: TransportSession, frame: MixTransportFrame
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let payload = frame.encode().valueOr:
    return err("could not encode " & $frame.kind & " frame: " & error)

  trace "sending frame",
    frameKind = frame.kind, sessionId = session.sessionId, role = session.role
  case session.role
  of SessionRole.Initiator:
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
    (await self.ensureUnreservedSurbGroup(session)).isOkOr:
      return err(error)
    var replyGroup = session.takeUnreservedSurbGroup().valueOr:
      return err(error)
    (await self.sendWithSurbGroup(replyGroup, payload)).isOkOr:
      return err("could not send " & $frame.kind & " frame: " & error)
    (await self.requestRefill(session)).isOkOr:
      return err(error)
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

proc handleRefill(self: MixTransport, frame: MixTransportFrame) {.gcsafe, raises: [].} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "Dropping Refill frame for unknown session", sessionId = frame.sessionId
    return
  if session.role != SessionRole.Recipient:
    debug "Dropping Refill frame for session with unexpected role",
      sessionId = frame.sessionId, sessionRole = session.role
    return
  if session.state != SessionState.Established:
    debug "Dropping Refill frame because session is not established",
      sessionId = frame.sessionId, sessionState = session.state
    return

  let refillRequestId = frame.refillRequestId.get()
  if not session.acceptRefillResponse(refillRequestId):
    return

  var decodedGroups = newSeqOfCap[seq[SURB]](frame.surbGroups.len)
  for encodedGroup in frame.surbGroups:
    let group = encodedGroup.decodeSurbs().valueOr:
      continue
    decodedGroups.add(group)
  discard session.addReceivedSurbGroups(decodedGroups)

proc sendStreamResponse(
    self: MixTransport,
    session: TransportSession,
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

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
    error "no SURB group available, cannot send reply"
    return false
  let response = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: kind,
    streamId: Opt.some(streamId),
    rejectionReason:
      if kind == FrameKind.StreamReject:
        Opt.some(boundedRejectionReason)
      else:
        Opt.none(string),
  ).encode().valueOr:
    return false
  (await self.sendWithSurbGroup(replyGroup, response)).isOk

proc handleConnect(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  logScope:
    sessionId = frame.sessionId

  if self.sessions.get(frame.sessionId).isSome:
    trace "session already exists, ignoring connect"
    return
  if frame.surbGroups.len < MinimumConnectReplyGroups:
    error "not enough surb groups in connect frame", surbGroups = frame.surbGroups.len
    return

  var decodedGroups = newSeqOfCap[seq[SURB]](frame.surbGroups.len)
  for encodedGroup in frame.surbGroups:
    let group = encodedGroup.decodeSurbs().valueOr:
      error "failed to decode SURBs in connect frame", encodedGroup = encodedGroup
      return
    decodedGroups.add(group)

  let session = self.sessions.addRecipientSession(frame.sessionId).valueOr:
    error "error registering session", error = error
    return

  var keepSession = false
  defer:
    if not keepSession:
      discard self.sessions.remove(frame.sessionId)

  session.addReceivedSurbGroups(decodedGroups).isOkOr:
    error "failed to add received SURB groups", error = error
    return

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
    error "error registering SURB group from connect frame", error = error
    discard self.sessions.remove(frame.sessionId)
    return
  let acknowledgement = MixTransportFrame(
    version: MixTransportVersion, sessionId: frame.sessionId, kind: FrameKind.ConnectAck
  ).encode().valueOr:
    error "failed to create acknowledgment message on session connect", error = error
    discard self.sessions.remove(frame.sessionId)
    return

  # Marks the session as established BEFORE sending out ACKs
  # or the other side might try to use it before it's ready
  # and have its frames dropped.
  session.establish()

  (await self.sendWithSurbGroup(replyGroup, acknowledgement)).isOkOr:
    error "failed to send acknowledgment on session connect", error = error
    discard self.sessions.remove(frame.sessionId)
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

  var decodedGroups = newSeqOfCap[seq[SURB]](frame.surbGroups.len)
  for encodedGroup in frame.surbGroups:
    let group = encodedGroup.decodeSurbs().valueOr:
      return
    decodedGroups.add(group)
  session.addReceivedSurbGroups(decodedGroups).isOkOr:
    return

  let protocol = self.mix.switch.ms.lookupProtocol(frame.codec.get()).valueOr:
    discard await self.sendStreamResponse(
      session,
      frame.streamId.get(),
      FrameKind.StreamReject,
      "requested protocol is not supported",
    )
    return

  if not protocol.reserveIncoming(session.peerId):
    discard await self.sendStreamResponse(
      session,
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
      session, frame.streamId.get(), FrameKind.StreamReject, streamResult.error
    )
    return
  let stream = streamResult.get()
  var keepStream = false
  defer:
    if not keepStream:
      discard session.removeStream(stream.streamId)
     # Make sure to close: will EOF any reader and
      # keep the libp2p counters correct.
      await noCancel stream.shutdown()

  # We need to transition our local state machine before sending the ACKs,
  # or the initiator might race us, send data before we're done, and have
  # their data silently dropped.
  stream.establish()
  if not await self.sendStreamResponse(session, stream.streamId, FrameKind.StreamAck):
    return

  self.configureStream(session, stream)
  discard await self.requestRefill(session)
  let handlerTask = runProtocolHandler(session, stream, protocol)
  # If the handler dies immediately, don't set it: the cleanup in
  # runProtocolHandler has already run, and will fail to clear it.
  if not handlerTask.finished:
    stream.setHandlerTask(handlerTask)
  keepStream = true
  keepReservation = true

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
  of FrameKind.Refill:
    self.handleRefill(frame)
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
    refillRequestTimeout = DefaultRefillRequestTimeout,
    dataRetransmissionTimeout = DefaultDataRetransmissionTimeout,
    enableDataRetransmissions = true,
    refillResponseLifetime = DefaultRefillResponseLifetime,
    maxOutstandingRefillRequests = DefaultMaxOutstandingRefillRequests,
): T =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  doAssert connectTimeout > ZeroDuration, "connect timeout must be positive"
  doAssert streamOpenTimeout > ZeroDuration, "stream open timeout must be positive"
  doAssert refillRequestTimeout > ZeroDuration,
    "refill request timeout must be positive"
  doAssert dataRetransmissionTimeout > ZeroDuration,
    "Data retransmission timeout must be positive"
  T(
    mix: mix,
    replyCredentials: ReplyCredentialStore.new(),
    sessions: newSessionStore(refillResponseLifetime, maxOutstandingRefillRequests),
    connectTimeout: connectTimeout,
    streamOpenTimeout: streamOpenTimeout,
    connLock: newAsyncLock(),
    refillRequestTimeout: refillRequestTimeout,
    dataRetransmissionTimeout: dataRetransmissionTimeout,
    dataRetransmissionsEnabled: enableDataRetransmissions,
  )

proc createReplyGroups(
    self: MixTransport,
    destination: PeerId,
    sessionId: PeerId,
    groupCount: int,
    redundancy: int,
): Result[PreparedReplyGroups, string] =
  let mixDestination = MixDestination.exitNode(destination)
  var prepared = PreparedReplyGroups(
    encoded: newSeqOfCap[SurbGroup](groupCount),
    credentials: newSeqOfCap[ReplyCredentialGroup](groupCount),
  )

  for _ in 0 ..< groupCount:
    var
      surbs = newSeqOfCap[SURB](redundancy)
      credentials = newSeqOfCap[ReplyCredential](redundancy)
    for _ in 0 ..< redundancy:
      var created = self.mix.createSurb(mixDestination).valueOr:
        for group in prepared.credentials:
          self.replyCredentials.consume(group)
        return err("could not create SURB: " & error)
      surbs.add(move(created.surb))
      credentials.add(created.credential)

    let encoded = SurbGroup.init(surbs).valueOr:
      for group in prepared.credentials:
        self.replyCredentials.consume(group)
      return err("could not encode SURB group: " & error)
    let credentialGroup = self.replyCredentials.addGroup(sessionId, credentials).valueOr:
      for group in prepared.credentials:
        self.replyCredentials.consume(group)
      return err("could not register reply credentials: " & error)
    prepared.encoded.add(encoded)
    prepared.credentials.add(credentialGroup)

  ok(prepared)

proc retireReplyGroups(self: MixTransport, groups: openArray[ReplyCredentialGroup]) =
  for group in groups:
    self.replyCredentials.consume(group)

proc handleRefillRequest(
    self: MixTransport, session: TransportSession, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let destination = session.destination.valueOr:
    return
  let prepared = self.createReplyGroups(
    destination,
    session.sessionId,
    frame.requestedGroups.get().int,
    DefaultRefillSurbRedundancy,
  ).valueOr:
    return

  let refill = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.Refill,
    refillRequestId: frame.refillRequestId,
    surbGroups: prepared.encoded,
  )
  (await self.sendStreamFrame(session, refill)).isOkOr:
    self.retireReplyGroups(prepared.credentials)
    return

proc createConnectFrame(
    self: MixTransport, destination: PeerId, sessionId: PeerId
): Result[MixTransportFrame, string] =
  let prepared = self.createReplyGroups(
    destination, sessionId, DefaultConnectReplyGroups, DefaultConnectSurbRedundancy
  ).valueOr:
    return err("could not prepare Connect reply groups: " & error)

  ok(
    MixTransportFrame(
      version: MixTransportVersion,
      sessionId: sessionId,
      kind: FrameKind.Connect,
      surbGroups: prepared.encoded,
    )
  )

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

  let frame = self.createConnectFrame(destination, sessionId).valueOr:
    return err(error)
  let payload = frame.encode().valueOr:
    return err("could not encode Connect frame: " & error)

  (
    await self.mix.send(
      MixDestination.exitNode(destination), MixTransportCodec, payload
    )
  ).isOkOr:
    return err("could not send Connect frame: " & error)

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

  let prepared = self.createReplyGroups(
    destination, session.sessionId, DefaultOpenStreamReplyGroups,
    DefaultOpenStreamSurbRedundancy,
  ).valueOr:
    return err("could not prepare OpenStream reply groups: " & error)
  var keepReplyGroups = false
  defer:
    if not keepReplyGroups:
      self.retireReplyGroups(prepared.credentials)

  let frame = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: FrameKind.OpenStream,
    streamId: Opt.some(stream.streamId),
    codec: Opt.some(codec),
    surbGroups: prepared.encoded,
  )
  let payload = frame.encode().valueOr:
    return err("could not encode OpenStream frame: " & error)
  (
    await self.mix.send(
      MixDestination.exitNode(destination), MixTransportCodec, payload
    )
  ).isOkOr:
    return err("could not send OpenStream frame: " & error)
  keepReplyGroups = true

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
