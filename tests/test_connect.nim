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
import libp2p_mix/key_rotation
import libp2p_mix/delay_strategy
import libp2p_mix/serialization
import libp2p_mix/curve25519
import protobuf_serialization

import libp2p_mix_transport
import libp2p_mix_transport/[connect_attempts, reply_credentials]
import libp2p_mix_transport/transport {.all.}

import ./logging

privateAccess(MixProtocol)
privateAccess(MixTransport)
privateAccess(PreparedReplySurbs)

proc createMixNodes(count: int, rotation = false): seq[MixProtocol] =
  # Every node is a normal Mix relay. The first and last nodes will also run
  # MixTransport, while the middle nodes provide independent forward and
  # return paths for the actual handshake.
  let
    rng = newRng()
    nodeInfos = MixNodeInfo.generateRandomMany(count, rng)

  for nodeInfo in nodeInfos:
    let ring =
      if rotation:
        let config = KeyRotationConfig
          .init(
            rotationInterval = 2.seconds,
            graceWindow = 6.seconds,
            replacedRecordLifetime = ZeroDuration,
            maxTransit = 500.milliseconds,
          )
          .expect("test rotation timings")
        let value = MixKeyRing.new(config, rng, replayTableCapacity = 1024)
        value.update()
        Opt.some(value)
      else:
        Opt.none(MixKeyRing)
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
        keyRing = ring,
      )

    mix.nodePool.add(nodeInfos.includeAllExcept(nodeInfo))
    switch.mount(mix)
    result.add(mix)

  if rotation:
    # Simulate prompt publication/discovery of each new record. This test does
    # not implement or exercise DHT discovery.
    let nodes = result
    proc publishRotations(owner: MixProtocol) =
      owner.keyRing.get().setKeyHandler(
        proc(key: FieldElement) {.gcsafe, raises: [].} =
          let info = owner.localMixPubInfo().expect("current rotating record")
          for peer in nodes:
            if peer != owner:
              peer.nodePool.add(info)
      )

    for node in nodes:
      publishRotations(node)

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
  PeerIdDial
  AddressConnect
  AddressDial

proc establishSessionAndStream(
    interAckDelay: Opt[Duration] = Opt.none(Duration),
    mode: ConnectionMode = ConnectionMode.PeerIdConnect,
    rotation = false,
): Future[RoundTripOutcome] {.async: (raises: [CancelledError, LPError]).} =
  let
    nodes = createMixNodes(5, rotation)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = newMixTransport(
      initiatorMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
      reverseActivityTimeout =
        (if rotation: 1.seconds else: DefaultReverseActivityTimeout),
    )
    initiatorSessionEvents = newAsyncQueue[SessionEvent]()
    recipientSessionEvents = newAsyncQueue[SessionEvent]()
    recipient = newMixTransport(
      recipientMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
      reverseActivityTimeout =
        (if rotation: 1.seconds else: DefaultReverseActivityTimeout),
    )

  var droppedStatusCopies = 0
  if rotation:
    initiator.surbStatusProbeRetryInterval = 500.milliseconds
    let originalSender = recipient.surbSender
    recipient.surbSender = proc(
        surb: sink SURB, payload: sink seq[byte]
    ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
      if MixTransportFrame.decode(payload).get().kind == FrameKind.SurbStatus and
          droppedStatusCopies < DefaultReplySurbRedundancy:
        inc droppedStatusCopies
        return ok() # Lose the first complete status response in transit.
      return await originalSender(move(surb), move(payload))

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
  if mode in {ConnectionMode.AddressConnect, ConnectionMode.AddressDial}:
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
  of ConnectionMode.PeerIdDial:
    initiatorStream =
      (await initiator.dial(destination, TestCodec)).expect("PeerId dial")
    session = initiator.sessions.get(initiatorStream.sessionId).expect("dial session")
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

  if rotation:
    let original = initiatorMix.localMixPubInfo().expect("initial key").mixPubKey
    # Outlast replacement plus grace for the original key and all initial SURBs.
    await sleepAsync(9.seconds)
    doAssert initiatorMix.localMixPubInfo().expect("rotated key").mixPubKey != original
    doAssert session.state == SessionState.Established
    doAssert droppedStatusCopies == DefaultReplySurbRedundancy

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
  if mode notin {ConnectionMode.AddressDial, ConnectionMode.PeerIdDial}:
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

  if mode in {ConnectionMode.AddressConnect, ConnectionMode.AddressDial}:
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
  test "rotating reply SURBs require path deadlines from the relay pool":
    let
      nodes = createMixNodes(5, rotation = true)
      mix = nodes[0]
      transport = newMixTransport(mix)
      destination = nodes[^1].switch.peerInfo.peerId
    defer:
      transport.replyCredentials.clear()
      for node in nodes:
        waitFor node.keyRing.get().stop()

    for hasDeadlines in [true, false]:
      if not hasDeadlines:
        # Inject an unaged pool to exercise the missing-deadline safeguard.
        mix.relayPool = MixNodePool.new(mix.switch.peerStore)
      var built = mix.createSurb(MixDestination.exitNode(destination))
      require built.isOk
      defer:
        built.get().credential.wipe()
      check:
        built.get().dispatchBefore.isSome == hasDeadlines
        built.get().credentialsExpireAt.isSome == hasDeadlines
        built.get().replyHoldTime.isSome
        built.get().replyTransitAllowance.isSome
      let prepared = transport.createReplySurbs(destination, destination, 1)
      if hasDeadlines:
        require prepared.isOk
        let encoded = prepared.get().encoded[0]
        let budget = transport.replyCredentials.holdingBudgetMillis(encoded)
        check budget > 0 and
          budget <= uint64(built.get().replyHoldTime.get().milliseconds)
        check transport.replyCredentials.len == 1
        transport.replyCredentials.clear()
      else:
        require prepared.isErr
        check:
          prepared.error == "Mix key rotation requires reply-path record deadlines"
          transport.replyCredentials.len == 0

  test "recipient reuses its anonymous session and opens bidirectional streams":
    proc run() {.async.} =
      let
        nodes = createMixNodes(5)
        initiator = newMixTransport(nodes[0])
        recipient = newMixTransport(nodes[^1])
        invocations = newAsyncQueue[ProtocolInvocation]()
        requests = newAsyncQueue[seq[byte]]()
        keepRunning = newAsyncEvent()
      nodes[0].switch.mount(
        newTestProtocol(TestCodec, invocations, requests, keepRunning)
      )
      for node in nodes:
        await node.switch.start()
        await node.start()
      defer:
        await recipient.stop()
        await initiator.stop()
        for node in nodes:
          await node.stop()
          await node.switch.stop()
      (await initiator.start()).expect("start initiator")
      (await recipient.start()).expect("start recipient")
      let session =
        (await initiator.connect(nodes[^1].switch.peerInfo.peerId)).expect("connect")
      let reverseSession = recipient.sessions.get(session.sessionId).get()
      doAssert (await recipient.connect(reverseSession.peerId)).get() == reverseSession
      for attempt in 0 ..< 2:
        let stream = (await recipient.dial(reverseSession.peerId, TestCodec)).expect(
          "recipient dial"
        )
        doAssert stream.streamId mod 2 == 0
        let invoked = invocations.get()
        doAssert await invoked.withTimeout(TestOperationTimeout)
        doAssert TransportStream((await invoked).stream).streamId == stream.streamId
        await stream.writeLp(TestRequest)
        let request = requests.get()
        doAssert await request.withTimeout(TestOperationTimeout)
        doAssert (await request) == TestRequest.toBytes
        let response = stream.readLp(1024)
        doAssert await response.withTimeout(TestOperationTimeout)
        doAssert (await response) == TestResponse.toBytes
      let rejected = await recipient.dial(reverseSession.peerId, UnsupportedCodec)
      doAssert rejected.isErr
      doAssert rejected.error == "requested protocol is not supported"
      doAssert recipient.sessions.len == 1
      doAssert initiator.sessions.len == 1

    waitFor run()

  setup:
    updateLogLevel("INFO;trace:mix-transport")

  test "explicit destination sends leave absent and existing peer-store state untouched":
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
      stale.multiAddr =
        MultiAddress.init("/ip4/127.0.0.1/tcp/4244").expect("stale address")
      stale.mixPubKey = MixNodeInfo.generateRandom(4244, newRng()).mixPubKey
      if alreadyKnown:
        mix.nodePool.add(stale)
        observed[destination] = Opt.some(stale.multiAddr)
      let addressEntries = addresses.entries(destination)
      var notifications = 0
      let onChange: PeerBookChangeHandler = proc(peer: PeerId) {.gcsafe, raises: [].} =
        inc notifications
      mixKeys.addHandler(onChange)
      keys.addHandler(onChange)
      observed.addHandler(onChange)
      addresses.addHandler(onChange)

      transport.addressDestinations[sessionId] = info
      # No relays are available, so send fails during route construction.
      # Providing destination information must not modify the peer store,
      # including when the pool already contains different information.
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

  test "PeerId-only dial creates a session without an explicit connect":
    let outcome = waitFor establishSessionAndStream(mode = ConnectionMode.PeerIdDial)
    check outcome.session.state == SessionState.Closed

  test "connect by address works without enrolling destination as a relay":
    let outcome =
      waitFor establishSessionAndStream(mode = ConnectionMode.AddressConnect)
    check outcome.session.state == SessionState.Closed
    check outcome.reused == outcome.session

  test "dial by address creates a session and carries traffic without pool membership":
    let outcome = waitFor establishSessionAndStream(mode = ConnectionMode.AddressDial)
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
      surbs: @[
        testSurb(1).serializeSurb().expect("valid test SURB"),
        @[0'u8],
        testSurb(2).serializeSurb().expect("valid test SURB"),
      ],
      surbHoldMillis: @[1000'u64, 1000'u64, 1000'u64],
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

  test "an idle session replenishes reply paths across rotation and grace expiry":
    let outcome = waitFor establishSessionAndStream(rotation = true)
    check:
      outcome.receivedRequest == TestRequest.toBytes()
      outcome.receivedResponse == TestResponse.toBytes()

  test "idle maintenance retires credentials without waiting for another packet":
    proc run() {.async.} =
      let mix = createMixNodes(5)[0]
      let transport = newMixTransport(mix)
      (await transport.start()).expect("start")
      defer:
        await transport.stop()
      var built = mix.createSurb(MixDestination.exitNode(mix.nodePool.peerIds()[0]))
      doAssert built.isOk
      let
        sessionId = PeerId.random(newRng()).expect("session id")
        id = built.get().credential.identifier
        encoded = built.get().surb.serializeSurb().expect("reply block")
      built.get().credentialsExpireAt = Opt.some(Moment.now() + 20.milliseconds)
      var credentials = [move(built.get().credential)]
      transport.replyCredentials.add(sessionId, credentials).expect("store")
      transport.replyCredentials.setSurbBounds(id, built.get(), encoded)
      transport.expiryChanged.fire()
      await sleepAsync(100.milliseconds)
      doAssert transport.replyCredentials.len == 0

    waitFor run()

  test "lost discovery updates close the idle session after bounded probes":
    proc run() {.async.} =
      let nodes = createMixNodes(5, rotation = true)
      let initiator = newMixTransport(
        nodes[0],
        reverseActivityTimeout = 1.seconds,
        surbStatusProbeRetryInterval = 200.milliseconds,
        maxSurbStatusProbeAttempts = 3,
      )
      let recipient = newMixTransport(nodes[^1])
      for node in nodes:
        await node.switch.start()
        await node.start()
      defer:
        await initiator.stop()
        await recipient.stop()
        for node in nodes:
          await node.stop()
          await node.switch.stop()
      (await initiator.start()).expect("initiator")
      (await recipient.start()).expect("recipient")
      let session =
        (await initiator.connect(nodes[^1].switch.peerInfo.peerId)).expect("connect")
      for node in nodes:
        node.keyRing.get().setKeyHandler(nil)
      await sleepAsync(5.seconds)
      doAssert session.state == SessionState.Closed
      doAssert initiator.sessions.get(session.sessionId).isNone

    waitFor run()

suite "MixTransport stream closure":
  proc recipientEndpoint(): Future[(MixTransport, TransportSession)] {.async.} =
    let
      mix = createMixNodes(1)[0]
      transport = newMixTransport(
        mix, streamOpenTimeout = 200.milliseconds, streamCloseTimeout = 25.milliseconds
      )
      session = transport.sessions
        .addRecipientSession(
          PeerId.random(mix.rng).expect("could not generate session identifier")
        )
        .expect("could not add recipient session")
    session.establish()
    session.initializeSurbSupply().expect("could not initialize SURB supply")
    session.addReceivedSurbs(newSeq[SURB](16)).expect("could not supply reply blocks")
    transport.surbSender = proc(
        surb: sink SURB, payload: sink seq[byte]
    ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
      ok()
    (await transport.start()).expect("could not start transport")
    (transport, session)

  proc deliver(
      transport: MixTransport,
      session: TransportSession,
      kind: FrameKind,
      finalSequence: SequenceNumber = 0,
  ) {.async.} =
    var frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: kind,
      streamId: Opt.some(StreamId(2)),
    )
    if kind == FrameKind.CloseStream:
      frame.finalSequence = Opt.some(finalSequence)
    await transport.handleReplyFrame(frame)

  for ackFirst in [false, true]:
    test "dial returns an empty accepted stream, ack first = " & $ackFirst:
      let (transport, session) = waitFor recipientEndpoint()
      defer:
        waitFor transport.stop()
      let opening = transport.dial(session.sessionId, TestCodec)
      if ackFirst:
        waitFor transport.deliver(session, FrameKind.StreamAck)
        waitFor transport.deliver(session, FrameKind.CloseStream)
      else:
        waitFor transport.deliver(session, FrameKind.CloseStream)
        check not opening.finished
        waitFor transport.deliver(session, FrameKind.StreamAck)
      let stream = (waitFor opening.wait(1.seconds)).expect("opening should succeed")
      var value: byte
      check (waitFor stream.readOnce(addr value, 1).wait(1.seconds)) == 0
      check session.streamCount == 0

  test "a graceful close alone cannot resolve the pending open":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.CloseStream)
    check (waitFor opening.wait(1.seconds)).error ==
      "MixTransport stream opening timed out"
    check session.streamCount == 0

  for refusal in [FrameKind.ResetStream, FrameKind.StreamReject]:
    test "a pending close still permits opening failure via " & $refusal:
      let (transport, session) = waitFor recipientEndpoint()
      defer:
        waitFor transport.stop()
      let opening = transport.dial(session.sessionId, TestCodec)
      waitFor transport.deliver(session, FrameKind.CloseStream)
      waitFor transport.deliver(session, refusal)
      let failure = waitFor opening.wait(1.seconds)
      if refusal == FrameKind.ResetStream:
        check failure.error == "MixTransport stream closed while opening"
      else:
        check failure.error == UnknownStreamRejectionReason
      check session.streamCount == 0

  test "Data arriving before acceptance leaves a gap through a pending close":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.CloseStream, 1)
    transport.handleData(
      MixTransportFrame(
        version: MixTransportVersion,
        sessionId: session.sessionId,
        kind: FrameKind.Data,
        streamId: Opt.some(StreamId(2)),
        sequence: Opt.some(SequenceNumber(1)),
        payload: Opt.some(@[1'u8]),
      )
    )
    waitFor sleepAsync(50.milliseconds)
    check not opening.finished
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    var value: byte
    expect LPStreamIncompleteError:
      discard waitFor stream.readOnce(addr value, 1).wait(1.seconds)

  test "a persistent close gap wakes a blocked reader with an incomplete-stream error":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    var value: byte
    let reading = stream.readOnce(addr value, 1)
    waitFor transport.deliver(session, FrameKind.CloseStream, 1)
    expect LPStreamIncompleteError:
      discard waitFor reading.wait(1.seconds)
    expect LPStreamIncompleteError:
      discard waitFor stream.readOnce(addr value, 1)
    check session.streamCount == 0

  test "a resolved close gap preserves all final bytes for a slow reader":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.CloseStream, 2)
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    check stream.receiveData(2, @[3'u8, 4]) == InboundDataDisposition.Accepted
    check stream.receiveData(1, @[1'u8, 2]) == InboundDataDisposition.Accepted
    waitFor sleepAsync(50.milliseconds)
    var value: byte
    for expected in 1'u8 .. 4:
      check (waitFor stream.readOnce(addr value, 1).wait(1.seconds)) == 1
      check value == expected
    check (waitFor stream.readOnce(addr value, 1).wait(1.seconds)) == 0
    waitFor stream.join().wait(1.seconds)
    check session.streamCount == 0

  test "graceful close releases the stream before the last buffered bytes are read":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    check stream.receiveData(1, @[1'u8, 2]) == InboundDataDisposition.Accepted
    waitFor transport.deliver(session, FrameKind.CloseStream, 1)
    waitFor stream.join().wait(1.seconds)
    check session.streamCount == 0
    var value: byte
    for expected in 1'u8 .. 2:
      check (waitFor stream.readOnce(addr value, 1).wait(1.seconds)) == 1
      check value == expected
    check (waitFor stream.readOnce(addr value, 1).wait(1.seconds)) == 0

  test "a close gap still expires while earlier chunks fill the application buffer":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    check stream.receiveData(1, @[1'u8]) == InboundDataDisposition.Accepted
    check stream.receiveData(2, @[2'u8]) == InboundDataDisposition.Accepted
    waitFor transport.deliver(session, FrameKind.CloseStream, 3)
    waitFor stream.join().wait(1.seconds)
    var value: byte
    expect LPStreamIncompleteError:
      discard waitFor stream.readOnce(addr value, 1)
    check session.streamCount == 0

  test "a failed Data submission resets the stream and prevents an ordinary close":
    let (transport, session) = waitFor recipientEndpoint()
    defer:
      waitFor transport.stop()
    var submitted: set[FrameKind]
    transport.surbSender = proc(
        surb: sink SURB, payload: sink seq[byte]
    ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
      let frame = MixTransportFrame.decode(payload).expect("could not decode frame")
      submitted.incl(frame.kind)
      if frame.kind == FrameKind.Data:
        return err("test submission failure")
      ok()
    let opening = transport.dial(session.sessionId, TestCodec)
    waitFor transport.deliver(session, FrameKind.StreamAck)
    let stream = (waitFor opening).expect("opening should succeed")
    expect LPStreamError:
      waitFor stream.write(@[1'u8]).wait(1.seconds)
    waitFor stream.close()
    check:
      stream.closed
      stream.finalOutboundSequence == 1
      stream.pendingOutboundCount == 0
      session.streamCount == 0
      FrameKind.Data in submitted
      FrameKind.ResetStream in submitted
      FrameKind.CloseStream notin submitted

  test "closing a stream cancels an unresolved first submission and sends a reset":
    proc run() {.async: (handleException: true).} =
      let (transport, session) = await recipientEndpoint()
      defer:
        await transport.stop()
      let blocked = newAsyncEvent()
      var resets = 0
      transport.surbSender = proc(
          surb: sink SURB, payload: sink seq[byte]
      ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
        let frame = MixTransportFrame.decode(payload).expect("could not decode frame")
        case frame.kind
        of FrameKind.Data:
          await blocked.wait()
        of FrameKind.ResetStream:
          inc resets
        else:
          discard
        ok()
      let opening = transport.dial(session.sessionId, TestCodec)
      await transport.deliver(session, FrameKind.StreamAck)
      let stream = (await opening).expect("opening should succeed")
      let writing = stream.write(@[1'u8])
      check not writing.finished
      await stream.close().wait(1.seconds)
      expect LPStreamClosedError:
        await writing.wait(1.seconds)
      check resets == DefaultReplySurbRedundancy

    waitFor run()

  proc startInboundHandler(
      transport: MixTransport, session: TransportSession, consume: LPProtoHandler
  ): tuple[stream: TransportStream, task: Future[void].Raising([CancelledError])] =
    let
      stream = session.addInboundStream(1, TestCodec).expect("could not add stream")
      protocol = LPProtocol.new(@[TestCodec], consume)
    doAssert protocol.reserveIncoming(session.peerId)
    stream.establish()
    transport.configureStream(session, stream)
    let task = transport.runProtocolHandler(session, stream, protocol)
    stream.setHandlerTask(task)
    (stream, task)

  proc receiveTeardown(
      transport: MixTransport, session: TransportSession, kind: FrameKind
  ) {.async: (raises: [CancelledError]).} =
    var frame = MixTransportFrame(
      version: MixTransportVersion, sessionId: session.sessionId, kind: kind
    )
    if kind == FrameKind.CloseStream:
      frame.streamId = Opt.some(StreamId(1))
      frame.finalSequence = Opt.some(SequenceNumber(1))
    await transport.handleTeardownFrame(frame)

  proc readFinalChunk(conn: Stream) {.async: (raises: [CancelledError]).} =
    var value: byte
    try:
      doAssert (await conn.readOnce(addr value, 1)) == 1 and value == 7
      doAssert (await conn.readOnce(addr value, 1)) == 0
    except LPStreamError as exc:
      raiseAssert exc.msg

  for disconnectFirst in [false, true]:
    test "handler drains after remote Disconnect, disconnect first = " & $disconnectFirst:
      proc run(disconnectFirst: bool) {.async: (handleException: true).} =
        let (transport, session) = await recipientEndpoint()
        defer:
          await transport.stop()
        let startReading = newAsyncEvent()
        let drained = newAsyncEvent()
        let keepRunning = newAsyncEvent()
        let consume: LPProtoHandler = proc(
            conn: Stream, codec: string
        ) {.async: (raises: [CancelledError]).} =
          await startReading.wait()
          await readFinalChunk(conn)
          drained.fire()
          if disconnectFirst:
            await keepRunning.wait()
        let (stream, handler) = transport.startInboundHandler(session, consume)
        check stream.receiveData(1, @[7'u8]) == InboundDataDisposition.Accepted
        if disconnectFirst:
          await transport.receiveTeardown(session, FrameKind.Disconnect)
        await transport.receiveTeardown(session, FrameKind.CloseStream)
        await stream.join().wait(1.seconds)
        if not disconnectFirst:
          await transport.receiveTeardown(session, FrameKind.Disconnect)
        await session.waitUntilClosed().wait(1.seconds)
        check transport.sessions.get(session.sessionId).isNone
        check transport.closingHandlers.len == 1
        startReading.fire()
        await drained.wait().wait(1.seconds)
        if disconnectFirst:
          await transport.stop().wait(1.seconds)
        await allFutures(handler).wait(1.seconds)
        check handler.cancelled == disconnectFirst
        check transport.closingHandlers.len == 0

      waitFor run(disconnectFirst)

  type HandlerTeardown = enum
    DisconnectSession
    ResetSession
    StopTransport

  for operation in HandlerTeardown:
    test "a handler can tear down its own session after EOF via " & $operation:
      proc run(operation: HandlerTeardown) {.async: (handleException: true).} =
        let (transport, session) = await recipientEndpoint()
        defer:
          await transport.stop()
        let returned = newAsyncEvent()
        let consume: LPProtoHandler = proc(
            conn: Stream, codec: string
        ) {.async: (raises: [CancelledError]).} =
          await readFinalChunk(conn)
          case operation
          of DisconnectSession:
            (await noCancel transport.disconnect(session)).expect(
              "could not disconnect"
            )
          of ResetSession:
            await noCancel transport.resetSession(session)
          of StopTransport:
            await noCancel transport.stop()
          returned.fire()
        let (stream, handler) = transport.startInboundHandler(session, consume)
        check stream.receiveData(1, @[7'u8]) == InboundDataDisposition.Accepted
        await transport.receiveTeardown(session, FrameKind.CloseStream)
        await returned.wait().wait(1.seconds)
        await allFutures(handler).wait(1.seconds)
        check session.state == SessionState.Closed
        check transport.closingHandlers.len == 0

      waitFor run(operation)

  for operation in [ResetSession, StopTransport]:
    test "an active handler can abort its own session via " & $operation:
      proc run(operation: HandlerTeardown) {.async: (handleException: true).} =
        let (transport, session) = await recipientEndpoint()
        defer:
          await transport.stop()
        let start = newAsyncEvent()
        let returned = newAsyncEvent()
        let consume: LPProtoHandler = proc(
            conn: Stream, codec: string
        ) {.async: (raises: [CancelledError]).} =
          await start.wait()
          if operation == ResetSession:
            await noCancel transport.resetSession(session)
          else:
            await noCancel transport.stop()
          returned.fire()
        let (stream, handler) = transport.startInboundHandler(session, consume)
        start.fire()
        await returned.wait().wait(1.seconds)
        await allFutures(handler).wait(1.seconds)
        check stream.closed
        check session.state == SessionState.Closed

      waitFor run(operation)

  test "remote ResetSession aborts an active application handler":
    proc run() {.async: (handleException: true).} =
      let (transport, session) = await recipientEndpoint()
      defer:
        await transport.stop()
      let gate = newAsyncEvent()
      let consume: LPProtoHandler = proc(
          conn: Stream, codec: string
      ) {.async: (raises: [CancelledError]).} =
        await gate.wait()
      let (stream, handler) = transport.startInboundHandler(session, consume)
      await transport.receiveTeardown(session, FrameKind.ResetSession).wait(1.seconds)
      check handler.cancelled
      check stream.closed
      check session.state == SessionState.Closed

    waitFor run()
