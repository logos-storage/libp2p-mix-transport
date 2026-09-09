# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, tables, unittest]

import chronicles, chronos, results
import stew/byteutils
import
  libp2p/[
    builders,
    crypto/crypto,
    crypto/secp,
    peerid,
    peerstore,
    protocols/protocol,
    stream/connection,
    switch,
    utils/opt,
  ]
import libp2p_mix
import libp2p_mix/pool
import libp2p_mix/delay_strategy
import libp2p_mix/serialization
import libp2p_mix/curve25519
import protobuf_serialization

import libp2p_mix_transport
import libp2p_mix_transport/connect_attempts
import libp2p_mix_transport/transport {.all.}

import ./logging

privateAccess(MixProtocol)
privateAccess(MixTransport)

proc createMixNodes(count: int): seq[MixProtocol] =
  # Every node is a normal Mix relay. The first and last nodes will also run
  # MixTransport, while the middle nodes provide independent forward and
  # return paths for the actual handshake.
  let
    rng = newRng()
    nodeInfos = MixNodeInfo.generateRandomMany(count, rng)

  for nodeInfo in nodeInfos:
    let
      privateKey = PrivateKey(scheme: Secp256k1, skkey: nodeInfo.libp2pPrivKey)
      switch = SwitchBuilder
        .new()
        .withRng(rng)
        .withPrivateKey(privateKey)
        .withAddress(nodeInfo.multiAddr)
        .withTcpTransport()
        .withMplex()
        .withNoise()
        .build()
      mix = MixProtocol.new(
        nodeInfo,
        switch,
        # Cryptographic routing remains real, but deterministic zero delays
        # keep this transport test focused on delivery rather than timing.
        delayStrategy = Opt.some(DelayStrategy(NoSamplingDelayStrategy.new(rng))),
      )

    mix.nodePool.add(nodeInfos.includeAllExcept(nodeInfo))
    switch.mount(mix)
    result.add(mix)

proc testSurb(marker: byte): SURB =
  var key = newSeq[byte](k)
  for value in key.mitems:
    value = marker

  SURB(
    hop: Hop.init(newSeq[byte](AddrSize)),
    header: Header.init(
      newSeq[byte](AlphaSize), newSeq[byte](BetaSize), newSeq[byte](GammaSize)
    ),
    key: move(key),
  )

const
  TestCodec = "/mix-transport/test/1.0.0"
  UnsupportedCodec = "/mix-transport/unsupported/1.0.0"
  TestRequest = "request through MixTransport"
  TestResponse = "response through MixTransport"
  TestOperationTimeout = 15.seconds

type ProtocolInvocation = object
  stream: Stream
  selectedCodec: string

proc newTestProtocol(
    codec: string,
    invocations: AsyncQueue[ProtocolInvocation],
    requests: AsyncQueue[seq[byte]],
    keepHandlerRunning: AsyncEvent,
): LPProtocol =
  let handler: LPProtoHandler = proc(
      stream: Stream, selectedCodec: string
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await invocations.put(
      ProtocolInvocation(stream: stream, selectedCodec: selectedCodec)
    )
    try:
      let request = await stream.readLp(1024)
      await requests.put(request)
      await stream.writeLp(TestResponse)
    except LPStreamError:
      await requests.put(@[])
    await keepHandlerRunning.wait()
  LPProtocol.new(@[codec], handler)

type RoundTripOutcome = object
  destination: PeerId
  session: TransportSession
  establishedSessionState: SessionState
  reused: TransportSession
  initiatorStream: TransportStream
  recipientStream: TransportStream
  initialRecipientReplySurbs: int
  recipientReplySurbs: int
  rejectionError: string
  initiatorStreamCount: int
  recipientStreamCount: int
  replyDispositions: seq[RawSurbReplyDisposition]
  handlerPeerId: PeerId
  handlerCodec: string
  handlerReceivedStream: bool
  receivedRequest: seq[byte]
  receivedResponse: seq[byte]
  initiatorSessionEvents: seq[SessionEvent]
  recipientSessionEvents: seq[SessionEvent]

proc delayAcknowledgementCopies(transport: MixTransport, interAckDelay: Duration) =
  let originalSurbSender = transport.surbSender
  transport.surbSender = proc(
      surb: sink SURB, payload: sink seq[byte]
  ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
    let
      frame = MixTransportFrame.decode(payload).get()
      isAcknowledgement =
        frame.kind == FrameKind.ConnectAck or frame.kind == FrameKind.StreamAck
      sendResult = await originalSurbSender(move(surb), move(payload))

    if isAcknowledgement:
      info "delaying next ack send", duration = $interAckDelay, frameKind = frame.kind
      await sleepAsync(interAckDelay)

    sendResult

type ConnectionMode {.pure.} = enum
  PeerIdConnect
  AddressConnect
  AddressDial

proc establishSessionAndStream(
    interAckDelay: Opt[Duration] = Opt.none(Duration),
    mode: ConnectionMode = ConnectionMode.PeerIdConnect,
): Future[RoundTripOutcome] {.async: (raises: [CancelledError, LPError]).} =
  let
    nodes = createMixNodes(5)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = newMixTransport(
      initiatorMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
    )
    initiatorSessionEvents = newAsyncQueue[SessionEvent]()
    recipientSessionEvents = newAsyncQueue[SessionEvent]()
    recipient = newMixTransport(
      recipientMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
    )

  if interAckDelay.isSome:
    recipient.delayAcknowledgementCopies(interAckDelay.get())

  let initiatorSessionEventHandler: SessionEventHandler = proc(
      event: SessionEvent
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await initiatorSessionEvents.put(event)
  let recipientSessionEventHandler: SessionEventHandler = proc(
      event: SessionEvent
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await recipientSessionEvents.put(event)
  initiator.addSessionEventHandler(initiatorSessionEventHandler)
  recipient.addSessionEventHandler(recipientSessionEventHandler)

  # StreamAck confirms that the recipient has a mounted handler for the
  # requested application codec. The handler remains active until transport
  # teardown so the test can inspect the connection passed to it.
  let
    protocolInvocations = newAsyncQueue[ProtocolInvocation]()
    receivedRequests = newAsyncQueue[seq[byte]]()
    keepHandlerRunning = newAsyncEvent()
  recipientMix.switch.mount(
    newTestProtocol(
      TestCodec, protocolInvocations, receivedRequests, keepHandlerRunning
    )
  )

  for node in nodes:
    await node.switch.start()
    await node.start()

  defer:
    await initiator.stop()
    await recipient.stop()
    for node in nodes:
      await node.stop()
      await node.switch.stop()

  (await initiator.start()).expect("could not start initiating transport")
  (await recipient.start()).expect("could not start recipient transport")

  # Observe the handler installed by MixTransport without changing its result.
  # Waiting for both observations gives teardown an exact synchronization point
  # after both redundant replies have completed their return paths.
  let
    originalHandler = initiatorMix.rawSurbReplyHandler
    observedReplies = newAsyncQueue[RawSurbReplyDisposition]()
  let observingHandler: RawSurbReplyHandler = proc(
      reply: RawSurbReply
  ): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
    let disposition = await originalHandler(reply)
    await observedReplies.put(disposition)
    disposition
  initiatorMix.rawSurbReplyHandler = observingHandler

  # Exercise three public API paths through the same live network. The address
  # cases must discover the destination from the supplied address, so remove it
  # from the initiator's relay pool before making either call.
  let destination = recipientMix.switch.peerInfo.peerId
  var addrs: seq[MultiAddress]
  if mode != ConnectionMode.PeerIdConnect:
    # The first candidate is deliberately not a mix address, to exercise
    # skipping unusable candidates before trying the advertised destination.
    addrs = @[
      MultiAddress.init("/ip4/127.0.0.1/tcp/1").expect("non-mix address"),
      recipientMix.mixNodeInfo.toMixPubInfo().toMixAddress().expect("mix address"),
    ]
    discard initiatorMix.nodePool.remove(destination)
  let originalPool = initiatorMix.nodePool
  let poolSize = originalPool.len
  var
    session: TransportSession
    initiatorStream: TransportStream
  case mode
  of ConnectionMode.PeerIdConnect:
    session = (await initiator.connect(destination)).expect("PeerId connect")
  of ConnectionMode.AddressConnect:
    let connecting = initiator.connect(destination, addrs)
    # Inspect the pool while the operation is pending, not just after success:
    # unrelated flows must never see the destination as an available relay.
    doAssert initiatorMix.nodePool == originalPool
    doAssert initiatorMix.nodePool.get(destination).isNone
    session = (await connecting).expect("address connect")
  of ConnectionMode.AddressDial:
    # Deliberately do NOT connect first. This case proves that dial(address)
    # performs both handshakes: Connect/ConnectAck and OpenStream/StreamAck.
    # Calling connect beforehand would test only reuse of an existing session.
    let dialing = initiator.dial(destination, addrs, TestCodec)
    doAssert initiatorMix.nodePool == originalPool
    doAssert initiatorMix.nodePool.get(destination).isNone
    initiatorStream = (await dialing).expect("address-only dial")
    session = initiator.sessions.get(initiatorStream.sessionId).expect(
      "dial did not retain the session it created"
    )

  # Once the anonymous round trip has established the session, connecting to
  # the same destination reuses its stable pseudonym instead of sending another
  # Connect frame.
  let reused = (await initiator.connect(destination)).expect(
    "could not reuse established MixTransport session"
  )

  let recipientSession = recipient.sessions.get(session.sessionId).expect(
      "recipient did not retain the established session"
    )
  while recipientSession.receivedSurbCount < recipientSession.recipientSurbCapacity:
    recipientSession.clearReplyCapacityStateChanged()
    if recipientSession.receivedSurbCount < recipientSession.recipientSurbCapacity:
      let supplyChanged = recipientSession.waitForReplyCapacityStateChange()
      if not await supplyChanged.withTimeout(TestOperationTimeout):
        raise newException(LPError, "SURB supplier did not fill its credit")
  let initialRecipientReplySurbs = recipientSession.receivedSurbCount

  # The connect cases still need a stream: dial reuses their established
  # session and completes OpenStream/StreamAck. AddressDial already did both
  # handshakes above. From here all cases run identical data and teardown checks.
  if mode != ConnectionMode.AddressDial:
    initiatorStream = (await initiator.dial(destination, TestCodec)).expect(
      "could not establish MixTransport stream"
    )
  let recipientStream = recipientSession.getStream(initiatorStream.streamId).expect(
      "recipient did not retain the inbound stream"
    )

  # When we're delaying ACKs, we want the initiator to start sending data
  # as quickly as possible, or this will cover up the state transition bugs
  # we're trying to test for.
  let invocationFuture = protocolInvocations.get()
  if interAckDelay.isNone:
    if not await invocationFuture.withTimeout(TestOperationTimeout):
      raise newException(LPError, "recipient protocol handler was not invoked")

  # Application bytes use the ordinary libp2p Connection interface. The
  # transport divides the write into Data frames, restores stream order at the
  # recipient and supplies the bytes to the mounted protocol's read call.
  await initiatorStream.writeLp(TestRequest)
  let receivedRequestFuture = receivedRequests.get()
  if not await receivedRequestFuture.withTimeout(TestOperationTimeout):
    raise newException(LPError, "recipient protocol did not receive stream data")
  let receivedRequest = await receivedRequestFuture

  # The handler writes its response through the same virtual connection. On
  # the recipient this consumes one temporary SURB redundancy batch. The
  # supply snapshot carried by that response grants credit for replacing it.
  let receivedResponseFuture = initiatorStream.readLp(1024)
  if not await receivedResponseFuture.withTimeout(TestOperationTimeout):
    raise newException(LPError, "initiator did not receive stream response")
  let receivedResponse = await receivedResponseFuture

  # The destination has no handler for this codec. It returns StreamReject,
  # allowing dial to fail without waiting for its timeout. The rejected stream
  # is removed on both endpoints while the established session remains usable.
  let rejected = await initiator.dial(destination, UnsupportedCodec)
  if rejected.isOk:
    raise newException(LPError, "unsupported codec unexpectedly opened a stream")

  var replyDispositions: seq[RawSurbReplyDisposition]
  let expectedReplies = 3 * DefaultReplySurbRedundancy
  for _ in 0 ..< expectedReplies:
    let observed = observedReplies.get()
    if not await observed.withTimeout(TestOperationTimeout):
      raise newException(LPError, "redundant reply did not reach the initiator")
    replyDispositions.add(await observed)

  # These futures must've been completed
  let invocation = await invocationFuture

  # Closing the initiating connection emits CloseStream. The recipient waits
  # until every Data sequence named by that frame has entered its BufferStream,
  # then closes the connection passed to the protocol handler.
  await initiatorStream.close()
  let recipientStreamClosed = recipientStream.join()
  if not await recipientStreamClosed.withTimeout(TestOperationTimeout):
    raise newException(LPError, "recipient did not process CloseStream")

  # Once the final stream has closed, Disconnect removes the long-lived
  # pseudonymous session on both endpoints.
  let establishedSessionState = session.state
  (await initiator.disconnect(session)).expect("could not disconnect session")
  let recipientSessionClosed = recipientSession.waitUntilClosed()
  if not await recipientSessionClosed.withTimeout(TestOperationTimeout):
    raise newException(LPError, "recipient did not process Disconnect")

  var
    observedInitiatorSessionEvents: seq[SessionEvent]
    observedRecipientSessionEvents: seq[SessionEvent]
  for _ in 0 ..< 2:
    let initiatorEvent = initiatorSessionEvents.get()
    if not await initiatorEvent.withTimeout(TestOperationTimeout):
      raise newException(LPError, "initiator session event was not published")
    observedInitiatorSessionEvents.add(await initiatorEvent)

    let recipientEvent = recipientSessionEvents.get()
    if not await recipientEvent.withTimeout(TestOperationTimeout):
      raise newException(LPError, "recipient session event was not published")
    observedRecipientSessionEvents.add(await recipientEvent)

  if mode != ConnectionMode.PeerIdConnect:
    doAssert initiatorMix.nodePool == originalPool
    doAssert initiatorMix.nodePool.len == poolSize
    doAssert initiatorMix.nodePool.get(destination).isNone
    doAssert initiator.addressDestinations.len == 0

  RoundTripOutcome(
    destination: destination,
    session: session,
    establishedSessionState: establishedSessionState,
    reused: reused,
    initiatorStream: initiatorStream,
    recipientStream: recipientStream,
    initialRecipientReplySurbs: initialRecipientReplySurbs,
    recipientReplySurbs: recipientSession.receivedSurbCount,
    rejectionError: rejected.error,
    initiatorStreamCount: session.streamCount,
    recipientStreamCount: recipientSession.streamCount,
    replyDispositions: replyDispositions,
    handlerPeerId: invocation.stream.peerId,
    handlerCodec: invocation.selectedCodec,
    handlerReceivedStream: invocation.stream == recipientStream,
    receivedRequest: receivedRequest,
    receivedResponse: receivedResponse,
    initiatorSessionEvents: move(observedInitiatorSessionEvents),
    recipientSessionEvents: move(observedRecipientSessionEvents),
  )

suite "MixTransport session and stream handshakes":
  setup:
    updateLogLevel("INFO;trace:mix-transport")

  test "temporary destination entries restore absent and existing peer-store state":
    for alreadyKnown in [false, true]:
      let
        mix = createMixNodes(1)[0]
        transport = newMixTransport(mix)
        info = MixNodeInfo.generateRandom(4243, newRng()).toMixPubInfo()
        destination = info.peerId
        sessionId = PeerId.random(newRng()).expect("session id")
        mixKeys = mix.switch.peerStore[MixPubKeyBook]
        keys = mix.switch.peerStore[KeyBook]
        observed = mix.switch.peerStore[LastSeenOutboundBook]
        addresses = mix.switch.peerStore[AddressBook]
      var stale = info
      stale.multiAddr = MultiAddress.init("/ip4/127.0.0.1/tcp/4244").expect(
        "stale address"
      )
      stale.mixPubKey = MixNodeInfo.generateRandom(4244, newRng()).mixPubKey
      if alreadyKnown:
        mix.nodePool.add(stale)
        observed[destination] = Opt.some(stale.multiAddr)
      let addressEntries = addresses.entries(destination)
      var notifications = 0
      let onChange: PeerBookChangeHandler =
        proc(peer: PeerId) {.gcsafe, raises: [].} =
          inc notifications
      mixKeys.addHandler(onChange)
      keys.addHandler(onChange)
      observed.addHandler(onChange)
      addresses.addHandler(onChange)

      transport.addressDestinations[sessionId] = info
      # No relays are available, so send fails during route construction. Even
      # this early exit must restore every destination entry without discovery
      # callbacks that could expose it to unrelated flows.
      let sending = transport.sendToDestination(destination, sessionId, @[1.byte])
      check (destination in mixKeys) == alreadyKnown
      check (destination in keys) == alreadyKnown
      check (destination in observed) == alreadyKnown
      check addresses.entries(destination) == addressEntries
      check notifications == 0
      if alreadyKnown:
        check mixKeys[destination].fieldElementToBytes() ==
          stale.mixPubKey.fieldElementToBytes()
        check keys[destination].skkey.getBytes() == stale.libp2pPubKey.getBytes()
        check observed[destination] == Opt.some(stale.multiAddr)
      check (waitFor sending).isErr

  test "failed address connection releases private destination information":
    let mix = createMixNodes(1)[0]
    let transport = newMixTransport(mix)
    let info = MixNodeInfo.generateRandom(4243, newRng()).toMixPubInfo()
    let address = info.toMixAddress().expect("mix address")
    (waitFor transport.start()).expect("start")
    defer:
      waitFor transport.stop()
    check (waitFor transport.connect(info.peerId, @[address])).isErr
    check mix.nodePool.len == 0
    check transport.addressDestinations.len == 0

  test "connect by address works without enrolling destination as a relay":
    let outcome = waitFor establishSessionAndStream(
      mode = ConnectionMode.AddressConnect
    )
    check outcome.session.state == SessionState.Closed
    check outcome.reused == outcome.session

  test "dial by address creates a session and carries traffic without pool membership":
    let outcome = waitFor establishSessionAndStream(
      mode = ConnectionMode.AddressDial
    )
    check outcome.session.state == SessionState.Closed
    check outcome.reused == outcome.session

  test "StreamAck establishes a stream and StreamReject rejects an unsupported codec":
    let outcome = waitFor establishSessionAndStream()

    check:
      outcome.session.role == SessionRole.Initiator
      outcome.establishedSessionState == SessionState.Established
      outcome.session.state == SessionState.Closed
      outcome.session.peerId == outcome.destination
      outcome.reused == outcome.session
      outcome.initiatorStream.sessionId == outcome.session.sessionId
      outcome.initiatorStream.streamId == outcome.recipientStream.streamId
      outcome.initiatorStream.codec == TestCodec
      outcome.recipientStream.codec == TestCodec
      outcome.initiatorStream.peerId == outcome.destination
      outcome.recipientStream.peerId == outcome.session.sessionId
      outcome.initiatorStream.direction == StreamDirection.Outbound
      outcome.recipientStream.direction == StreamDirection.Inbound
      outcome.initiatorStream.state == StreamState.Established
      outcome.recipientStream.state == StreamState.Established
      outcome.initialRecipientReplySurbs == DefaultRecipientSurbCapacity
      outcome.rejectionError == "requested protocol is not supported"
      outcome.initiatorStreamCount == 0
      outcome.recipientStreamCount == 0
      outcome.recipientReplySurbs <= DefaultRecipientSurbCapacity
      outcome.handlerPeerId == outcome.session.sessionId
      outcome.handlerCodec == TestCodec
      outcome.handlerReceivedStream
      outcome.receivedRequest == TestRequest.toBytes()
      outcome.receivedResponse == TestResponse.toBytes()
      outcome.initiatorSessionEvents.len == 2
      outcome.initiatorSessionEvents[0].kind == SessionEventKind.Established
      outcome.initiatorSessionEvents[0].peerId == outcome.destination
      outcome.initiatorSessionEvents[0].sessionId == outcome.session.sessionId
      outcome.initiatorSessionEvents[0].role == SessionRole.Initiator
      outcome.initiatorSessionEvents[1].kind == SessionEventKind.Closed
      outcome.initiatorSessionEvents[1].peerId == outcome.destination
      outcome.initiatorSessionEvents[1].sessionId == outcome.session.sessionId
      outcome.initiatorSessionEvents[1].role == SessionRole.Initiator
      outcome.recipientSessionEvents.len == 2
      outcome.recipientSessionEvents[0].kind == SessionEventKind.Established
      outcome.recipientSessionEvents[0].peerId == outcome.session.sessionId
      outcome.recipientSessionEvents[0].sessionId == outcome.session.sessionId
      outcome.recipientSessionEvents[0].role == SessionRole.Recipient
      outcome.recipientSessionEvents[1].kind == SessionEventKind.Closed
      outcome.recipientSessionEvents[1].peerId == outcome.session.sessionId
      outcome.recipientSessionEvents[1].sessionId == outcome.session.sessionId
      outcome.recipientSessionEvents[1].role == SessionRole.Recipient
      outcome.replyDispositions ==
        @[
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
        ]

  test "numbered supply retains each valid SURB independently":
    let
      mix = createMixNodes(1)[0]
      transport = newMixTransport(mix)
      session = transport.sessions
        .addRecipientSession(
          PeerId.random(mix.rng).expect("could not generate session identifier")
        )
        .expect("could not add recipient session")

    session.initializeSurbSupply().expect("could not initialize SURB supply")
    session.establish()

    (waitFor transport.start()).expect("could not start transport")
    defer:
      waitFor transport.stop()

    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.SurbSupply,
      firstSurbSequence: Opt.some(SurbSupplySequence(0)),
      surbs: @[testSurb(1).serializeSurb(), @[0'u8], testSurb(2).serializeSurb()],
    )

    # Invoke the same delivery callback that Mix uses for proactive supply.
    # Each valid SURB is retained under its wire sequence while the malformed
    # SURB leaves a gap that can be repaired by retransmission.
    check frame.encode().isErr
    waitFor mix.deliveryHandlers[MixTransportCodec](
      MixDelivery(
        service: MixTransportCodec,
        # Bypass the strict local encoder to model malformed bytes received
        # from a remote endpoint. The inbound decoder performs structural
        # validation and leaves each SURB for independent decoding.
        payload: Protobuf.encode(frame),
      )
    )

    check:
      session.receivedSurbCount == 2
      session.surbSupplySnapshot().receiveBase == 1

  test "data packets are not rejected if ACK arrives too fast":
    try:
      discard waitFor establishSessionAndStream(Opt.some(2.seconds))
    except LPError as err:
      raiseAssert "Unexpected error: " & err.msg

type
  Synchronizer = ref object
    connectAttempts: ConnectAttemptCoordinator[string, Session]
    sessions: Table[string, Session]
    gate: AsyncEvent
    operationCancelled: AsyncEvent

  Caller = int
  ConnAttempt = int
  Destination = string
  Session = tuple[attempt: ConnAttempt, caller: Caller]

proc state*(self: Session): SessionState =
  SessionState.Established

proc newSynchronizer(): Synchronizer =
  Synchronizer(
    connectAttempts: newConnectAttemptCoordinator[string, Session](),
    gate: newAsyncEvent(),
    operationCancelled: newAsyncEvent(),
  )

proc getExisting*(self: Synchronizer, dest: Destination): Opt[Session] {.raises: [].} =
  if self.sessions.hasKey(dest):
    try:
      return Opt.some(self.sessions[dest])
    except KeyError:
      doAssert false
  else:
    return Opt.none(Session)

proc connInternal(
    self: Synchronizer, caller: Caller, error: Opt[string] = Opt.none(string)
): ConnectOperation[Destination, Session] =
  proc wrapped(
      dest: Destination
  ): Future[Result[Session, string]] {.async: (raises: [CancelledError]).} =
    # makes sure the connection attempt doesn't end before we can
    # fire the next caller - this is how we ensure that calls get
    # placed into the same attempt.
    try:
      await self.gate.wait()
    except CancelledError as exc:
      self.operationCancelled.fire()
      raise exc

    if error.isSome:
      return err(error.get())

    let attempt =
      try:
        self.sessions[dest].attempt
      except KeyError:
        0

    let session = (attempt + 1, caller)
    self.sessions[dest] = session
    return ok(session)

  wrapped

proc existingConnectionLookup(
    self: Synchronizer
): ExistingConnectionLookup[Destination, Session] =
  proc wrapped(dest: Destination): Opt[Session] {.gcsafe, raises: [].} =
    self.getExisting(dest)

  wrapped

suite "connect behavior under multiple callers":
  test "should create connection when there is only one caller":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()
      transport.gate.fire()

      let (session, existing) = (
        await transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(5),
          transport.existingConnectionLookup(),
        )
      ).get

      check:
        session == (attempt: 1, caller: 5)
        existing == false
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "should return existing connection if there is one":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()
      transport.sessions["destination1"] = (10, 1)
      transport.gate.fire()

      let (session, existing) = (
        await transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(5),
          transport.existingConnectionLookup(),
        )
      ).get

      check:
        session == (attempt: 10, caller: 1)
        existing == true

    waitFor asyncTest()

  test "should await the owner's attempt when there is more than one caller":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()

      let
        first = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(1),
          transport.existingConnectionLookup(),
        )
        second = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(2),
          transport.existingConnectionLookup(),
        )

      transport.gate.fire()

      let
        (firstSession, _) = (await first).get
        (secondSession, _) = (await second).get

      check:
        firstSession == (attempt: 1, caller: 1)
        secondSession == (attempt: 1, caller: 1)
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "cancelling one caller does not cancel an attempt used by another caller":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()
      let
        first = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(1),
          transport.existingConnectionLookup(),
        )
        second = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(2),
          transport.existingConnectionLookup(),
        )

      await first.cancelAndWait()
      check:
        first.cancelled
        transport.connectAttempts.activeAttemptCount == 1

      transport.gate.fire()
      check:
        (await second).get().connection == (attempt: 1, caller: 1)
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "cancelling the only caller cancels the transport-owned attempt":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()
      let caller = transport.connectAttempts.connect(
        "destination1", transport.connInternal(1), transport.existingConnectionLookup()
      )

      await caller.cancelAndWait()
      check:
        caller.cancelled
        await transport.operationCancelled.wait().withTimeout(1.seconds)
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "stopping owned attempts wakes every caller with the stop reason":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()
      let
        first = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(1),
          transport.existingConnectionLookup(),
        )
        second = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(2),
          transport.existingConnectionLookup(),
        )

      await transport.connectAttempts.cancelAll("transport stopped")
      check:
        (await first).error == "transport stopped"
        (await second).error == "transport stopped"
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "should allow another attempt if the previous one failed":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = newSynchronizer()

      let
        first = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(1, Opt.some("ooops, this is an error")),
          transport.existingConnectionLookup(),
        )
        second = transport.connectAttempts.connect(
          "destination1",
          transport.connInternal(2, Opt.some("this is also an error")),
          transport.existingConnectionLookup(),
        )

      transport.gate.fire()
      # Despite the error, the first two calls should end in the same
      # outcome as they are logically the same attempt.
      check:
        (await first).error() == "ooops, this is an error"
        (await second).error() == "ooops, this is an error"
        transport.connectAttempts.activeAttemptCount == 0

      # A third call that happens after the first two complete,
      # however, should be able to go through as it represents
      # a separate attempt.
      transport.gate.clear()
      let third = transport.connectAttempts.connect(
        "destination1", transport.connInternal(3), transport.existingConnectionLookup()
      )
      transport.gate.fire()
      check:
        (await third).get().connection == (attempt: 1, caller: 3)
        transport.connectAttempts.activeAttemptCount == 0

    waitFor asyncTest()

  test "ResetSession closes every stream as a remote reset":
    let
      mix = createMixNodes(1)[0]
      transport = newMixTransport(mix)
      session = transport.sessions
        .addRecipientSession(
          PeerId.random(mix.rng).expect("could not generate session identifier")
        )
        .expect("could not add recipient session")
    session.establish()
    let stream =
      session.addInboundStream(1, TestCodec).expect("could not add inbound stream")
    stream.establish()

    (waitFor transport.start()).expect("could not start transport")
    defer:
      waitFor transport.stop()

    var value: byte
    let pendingRead = stream.readOnce(addr value, 1)
    check not pendingRead.finished

    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.ResetSession,
    )
    waitFor mix.deliveryHandlers[MixTransportCodec](
      MixDelivery(
        service: MixTransportCodec,
        payload: frame.encode().expect("could not encode ResetSession"),
      )
    )

    check:
      session.state == SessionState.Closed
      session.streamCount == 0
      transport.sessions.get(session.sessionId).isNone
      stream.closed
    expect LPStreamResetError:
      discard waitFor pendingRead
