# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronicles, chronos, results
import libp2p/multistream
import libp2p/peerid
import libp2p/protocols/protocol
import libp2p/stream/bufferstream
import libp2p/stream/connection
import libp2p/utils/future
import libp2p/utils/opt
import libp2p_mix
import ./[reply_credentials, sessions, streams, wire]

logScope:
  topics = "libp2p mix-transport"

const
  DefaultConnectTimeout* = 30.seconds
  DefaultStreamOpenTimeout* = 30.seconds
  DefaultRefillRequestTimeout* = 30.seconds
  MinimumConnectReplyGroups* = 2
  DefaultConnectReplyGroups* = MinimumConnectReplyGroups
  DefaultConnectSurbRedundancy* = 2
  DefaultOpenStreamReplyGroups* = 2
  DefaultOpenStreamSurbRedundancy* = 2
  DefaultRefillGroups* = MaxRefillGroupsPerFrame.int
  DefaultRefillSurbRedundancy* = 2
  UnknownStreamRejectionReason = "remote rejected the stream for an unknown reason"

type MixTransport* = ref object
  mix: MixProtocol
  replyCredentials: ReplyCredentialStore
  sessions: SessionStore
  connectTimeout: Duration
  streamOpenTimeout: Duration
  refillRequestTimeout: Duration
  handlerTasks: seq[Future[void].Raising([CancelledError])]
  streamTasks: seq[Future[void].Raising([CancelledError])]
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
    protocol.releaseIncoming(stream.peerId)
    discard session.removeStream(stream.streamId)
    await noCancel stream.close()

  # Binding the template accessor preserves LPProtoHandler's raises list.
  let handler: LPProtoHandler = protocol.handler
  await handler(stream, stream.codec)

proc handleReplyFrame(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    return

  case frame.kind
  of FrameKind.ConnectAck:
    if session.role != SessionRole.Initiator or session.state != SessionState.Pending:
      return
    session.establish()
  of FrameKind.StreamAck, FrameKind.StreamReject:
    if session.state != SessionState.Established:
      return
    let stream = session.getStream(frame.streamId.get()).valueOr:
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

proc sendWithSurbGroup(
    self: MixTransport, surbs: sink seq[SURB], payload: sink seq[byte]
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
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

proc configureStream(
    self: MixTransport, session: TransportSession, stream: TransportStream
) =
  let writeHandler: StreamWriteHandler = proc(
      data: sink seq[byte]
  ): Future[void] {.async: (raw: true, raises: [CancelledError, LPStreamError]).} =
    self.writeStream(session, stream, move(data))
  stream.setWriteHandler(writeHandler)
  self.streamTasks.trackFut(runInboundDelivery(stream))
  self.streamTasks.trackFut(runAcknowledgements(self, session, stream))

proc handleData(self: MixTransport, frame: MixTransportFrame) {.gcsafe, raises: [].} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    debug "Dropping Data frame for unknown session", sessionId = frame.sessionId
    return
  if session.state != SessionState.Established:
    debug "Dropping Data frame because session is not established",
      sessionId = frame.sessionId, sessionState = session.state
    return
  let stream = session.getStream(frame.streamId.get()).valueOr:
    debug "Dropping Data frame for unknown stream",
      sessionId = frame.sessionId, streamId = frame.streamId.get()
    return
  if stream.state != StreamState.Established:
    debug "Dropping Data frame because stream is not established",
      sessionId = frame.sessionId,
      streamId = stream.streamId,
      streamState = stream.state
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

  let boundedRejectionReason =
    if rejectionReason.len > MaxStreamRejectionReasonBytes:
      rejectionReason[0 ..< MaxStreamRejectionReasonBytes]
    else:
      rejectionReason

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
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
  if self.sessions.get(frame.sessionId).isSome:
    return
  if frame.surbGroups.len < MinimumConnectReplyGroups:
    return

  var decodedGroups = newSeqOfCap[seq[SURB]](frame.surbGroups.len)
  for encodedGroup in frame.surbGroups:
    let group = encodedGroup.decodeSurbs().valueOr:
      return
    decodedGroups.add(group)

  let session = self.sessions.addRecipientSession(frame.sessionId).valueOr:
    return
  var keepSession = false
  defer:
    if not keepSession:
      discard self.sessions.remove(frame.sessionId)

  session.addReceivedSurbGroups(decodedGroups).isOkOr:
    return

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
    return
  let acknowledgement = MixTransportFrame(
    version: MixTransportVersion, sessionId: frame.sessionId, kind: FrameKind.ConnectAck
  ).encode().valueOr:
    return

  # ConnectAck allows the initiator to send session traffic immediately. Make
  # the recipient ready before the first redundant ACK copy can arrive.
  session.establish()
  (await self.sendWithSurbGroup(replyGroup, acknowledgement)).isOkOr:
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
      await noCancel stream.close()

  # StreamAck allows the initiator to send Data immediately. Install the
  # bounded receive path before the first redundant ACK copy can arrive.
  self.configureStream(session, stream)
  stream.establish()
  if not await self.sendStreamResponse(session, stream.streamId, FrameKind.StreamAck):
    return

  keepStream = true
  keepReservation = true
  self.handlerTasks.trackFut(runProtocolHandler(session, stream, protocol))
  discard await self.requestRefill(session)

proc handleDelivery(
    self: MixTransport, delivery: MixDelivery
): Future[void] {.async: (raises: [CancelledError]).} =
  let frame = MixTransportFrame.decode(delivery.payload).valueOr:
    return
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
    mix: MixProtocol,
    connectTimeout = DefaultConnectTimeout,
    streamOpenTimeout = DefaultStreamOpenTimeout,
    refillRequestTimeout = DefaultRefillRequestTimeout,
    refillResponseLifetime = DefaultRefillResponseLifetime,
    maxOutstandingRefillRequests = DefaultMaxOutstandingRefillRequests,
): MixTransport =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  doAssert connectTimeout > ZeroDuration, "connect timeout must be positive"
  doAssert streamOpenTimeout > ZeroDuration, "stream open timeout must be positive"
  doAssert refillRequestTimeout > ZeroDuration,
    "refill request timeout must be positive"
  MixTransport(
    mix: mix,
    replyCredentials: ReplyCredentialStore.new(),
    sessions: newSessionStore(refillResponseLifetime, maxOutstandingRefillRequests),
    connectTimeout: connectTimeout,
    streamOpenTimeout: streamOpenTimeout,
    refillRequestTimeout: refillRequestTimeout,
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

proc connect*(
    self: MixTransport, destination: PeerId
): Future[Result[TransportSession, string]] {.async: (raises: [CancelledError]).} =
  if not self.started:
    return err("MixTransport is not started")

  self.sessions.getByDestination(destination).withValue(existing):
    if existing.state == SessionState.Established:
      return ok(existing)
    return err("session establishment is already in progress")

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

  keepSession = true
  ok(session)

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
  await self.handlerTasks.cancelAndWait()
  self.handlerTasks.setLen(0)
  await self.streamTasks.cancelAndWait()
  self.streamTasks.setLen(0)
  self.replyCredentials.clear()
  self.sessions.clear()
  self.started = false

{.pop.}
