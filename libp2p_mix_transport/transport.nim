# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos, results
import libp2p/multistream
import libp2p/peerid
import libp2p/protocols/protocol
import libp2p/stream/connection
import libp2p/utils/future
import libp2p/utils/opt
import libp2p_mix
import ./[reply_credentials, sessions, streams, wire]

const
  DefaultConnectTimeout* = 30.seconds
  DefaultStreamOpenTimeout* = 30.seconds
  MinimumConnectReplyGroups* = 2
  DefaultConnectReplyGroups* = MinimumConnectReplyGroups
  DefaultConnectSurbRedundancy* = 2
  DefaultOpenStreamReplyGroups* = 2
  DefaultOpenStreamSurbRedundancy* = 2
  UnknownStreamRejectionReason = "remote rejected the stream for an unknown reason"

type MixTransport* = ref object
  mix: MixProtocol
  replyCredentials: ReplyCredentialStore
  sessions: SessionStore
  connectTimeout: Duration
  streamOpenTimeout: Duration
  handlerTasks: seq[Future[void].Raising([CancelledError])]
  started: bool

type PreparedReplyGroups = object
  encoded: seq[SurbGroup]
  credentials: seq[ReplyCredentialGroup]

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

proc sendStreamResponse(
    self: MixTransport,
    session: TransportSession,
    streamId: uint64,
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
  session.addReceivedSurbGroups(decodedGroups).isOkOr:
    discard self.sessions.remove(frame.sessionId)
    return

  var replyGroup = session.takeReceivedSurbGroup().valueOr:
    discard self.sessions.remove(frame.sessionId)
    return
  let acknowledgement = MixTransportFrame(
    version: MixTransportVersion, sessionId: frame.sessionId, kind: FrameKind.ConnectAck
  ).encode().valueOr:
    discard self.sessions.remove(frame.sessionId)
    return

  (await self.sendWithSurbGroup(replyGroup, acknowledgement)).isOkOr:
    discard self.sessions.remove(frame.sessionId)
    return
  session.establish()

proc handleOpenStream(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  let session = self.sessions.get(frame.sessionId).valueOr:
    return
  if session.role != SessionRole.Recipient or session.state != SessionState.Established:
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

  if not await self.sendStreamResponse(session, stream.streamId, FrameKind.StreamAck):
    return

  stream.establish()
  keepStream = true
  keepReservation = true
  self.handlerTasks.trackFut(runProtocolHandler(session, stream, protocol))

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
): MixTransport =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  doAssert connectTimeout > ZeroDuration, "connect timeout must be positive"
  doAssert streamOpenTimeout > ZeroDuration, "stream open timeout must be positive"
  MixTransport(
    mix: mix,
    replyCredentials: ReplyCredentialStore.new(),
    sessions: newSessionStore(),
    connectTimeout: connectTimeout,
    streamOpenTimeout: streamOpenTimeout,
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

  keepStream = true
  ok(stream)

proc start*(
    self: MixTransport
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  if self.started:
    return ok()

  let deliveryHandler: MixDeliveryHandler = proc(
      delivery: MixDelivery
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await self.handleDelivery(delivery)

  self.mix.registerMixDeliveryHandler(MixTransportCodec, deliveryHandler).isOkOr:
    return err(error)

  let rawSurbReplyHandler: RawSurbReplyHandler = proc(
      reply: RawSurbReply
  ): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
    return await self.handleRawSurbReply(reply)

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
  self.replyCredentials.clear()
  self.sessions.clear()
  self.started = false

{.pop.}
