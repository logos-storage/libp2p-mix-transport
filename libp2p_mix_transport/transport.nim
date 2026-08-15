# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos, results
import libp2p/peerid
import libp2p/utils/opt
import libp2p_mix
import ./[reply_credentials, sessions, wire]

const
  DefaultConnectTimeout* = 30.seconds
  MinimumConnectReplyGroups* = 2
  DefaultConnectReplyGroups* = MinimumConnectReplyGroups
  DefaultConnectSurbRedundancy* = 2

type MixTransport* = ref object
  mix: MixProtocol
  replyCredentials: ReplyCredentialStore
  sessions: SessionStore
  connectTimeout: Duration
  started: bool

proc handleReplyFrame(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  if frame.kind != FrameKind.ConnectAck:
    return

  let session = self.sessions.get(frame.sessionId).valueOr:
    return
  if session.role != SessionRole.Initiator or session.state != SessionState.Pending:
    return
  session.establish()

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

proc handleDelivery(
    self: MixTransport, delivery: MixDelivery
): Future[void] {.async: (raises: [CancelledError]).} =
  let frame = MixTransportFrame.decode(delivery.payload).valueOr:
    return
  if frame.kind == FrameKind.Connect:
    await self.handleConnect(frame)

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
    mix: MixProtocol, connectTimeout = DefaultConnectTimeout
): MixTransport =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  doAssert connectTimeout > ZeroDuration, "connect timeout must be positive"
  MixTransport(
    mix: mix,
    replyCredentials: ReplyCredentialStore.new(),
    sessions: newSessionStore(),
    connectTimeout: connectTimeout,
  )

proc createConnectFrame(
    self: MixTransport, destination: PeerId, sessionId: PeerId
): Result[MixTransportFrame, string] =
  let mixDestination = MixDestination.exitNode(destination)
  var groups = newSeqOfCap[SurbGroup](DefaultConnectReplyGroups)

  for _ in 0 ..< DefaultConnectReplyGroups:
    var
      surbs = newSeqOfCap[SURB](DefaultConnectSurbRedundancy)
      credentials = newSeqOfCap[ReplyCredential](DefaultConnectSurbRedundancy)

    for _ in 0 ..< DefaultConnectSurbRedundancy:
      var created = self.mix.createSurb(mixDestination).valueOr:
        discard self.replyCredentials.removeSession(sessionId)
        return err("could not create Connect SURB: " & error)
      surbs.add(move(created.surb))
      credentials.add(created.credential)

    let group = SurbGroup.init(surbs).valueOr:
      discard self.replyCredentials.removeSession(sessionId)
      return err("could not encode Connect SURB group: " & error)
    self.replyCredentials.addGroup(sessionId, credentials).isOkOr:
      discard self.replyCredentials.removeSession(sessionId)
      return err("could not register Connect reply credentials: " & error)
    groups.add(group)

  ok(
    MixTransportFrame(
      version: MixTransportVersion,
      sessionId: sessionId,
      kind: FrameKind.Connect,
      surbGroups: groups,
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
  self.replyCredentials.clear()
  self.sessions.clear()
  self.started = false

{.pop.}
