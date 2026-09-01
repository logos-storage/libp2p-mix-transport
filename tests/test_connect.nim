# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]

import chronicles, chronos, results, tables
import stew/byteutils
import
  libp2p/[
    builders,
    crypto/crypto,
    crypto/secp,
    peerid,
    protocols/protocol,
    stream/connection,
    switch,
    utils/opt,
  ]
import libp2p_mix
import libp2p_mix/delay_strategy

import libp2p_mix_transport
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
  reused: TransportSession
  initiatorStream: TransportStream
  recipientStream: TransportStream
  recipientReplyGroups: int
  rejectionError: string
  initiatorStreamCount: int
  recipientStreamCount: int
  replyDispositions: seq[RawSurbReplyDisposition]
  handlerPeerId: PeerId
  handlerCodec: string
  handlerReceivedStream: bool
  receivedRequest: seq[byte]
  receivedResponse: seq[byte]

type DelayedAckMixTransport = ref object of MixTransport
  interAckDelay: Duration

method sendWithSurbGroup(
    self: DelayedAckMixTransport, surbs: sink seq[SURB], payload: sink seq[byte]
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
  let
    frame = MixTransportFrame.decode(payload).get()
    isAck = frame.kind == FrameKind.ConnectAck or frame.kind == FrameKind.StreamAck

  var sent = false
  for surb in surbs.mitems:
    # Each SURB is consumed once, but every redundant packet needs the same
    # payload. Passing payload without move lets Nim copy it for each send.
    if (await self.mix.sendWithSurb(move(surb), payload)).isOk:
      sent = true

    if isAck:
      info "delaying next ack send",
        duration = $self.interAckDelay, frameKind = frame.kind
      await sleepAsync(self.interAckDelay)

  if not sent:
    return err("could not send through any SURB in the reply group")
  ok()

proc establishSessionAndStream(
    interAckDelay: Opt[Duration] = Opt.none(Duration)
): Future[RoundTripOutcome] {.async: (raises: [CancelledError, LPError]).} =
  let
    nodes = createMixNodes(5)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = MixTransport.newMixTransport(
      initiatorMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
    )
    recipient =
      if interAckDelay.isSome:
        var transport = DelayedAckMixTransport.newMixTransport(
          recipientMix,
          connectTimeout = TestOperationTimeout,
          streamOpenTimeout = TestOperationTimeout,
        )
        transport.interAckDelay = interAckDelay.get()
        transport
      else:
        MixTransport.newMixTransport(
          recipientMix,
          connectTimeout = TestOperationTimeout,
          streamOpenTimeout = TestOperationTimeout,
        )

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

  # This call sends Connect through the live Mix overlay and completes only
  # after ConnectAck returns through a supplied SURB.
  let destination = recipientMix.switch.peerInfo.peerId
  let session = (await initiator.connect(destination)).expect(
    "could not establish MixTransport session"
  )

  # Once the anonymous round trip has established the session, connecting to
  # the same destination reuses its stable pseudonym instead of sending another
  # Connect frame.
  let reused = (await initiator.connect(destination)).expect(
    "could not reuse established MixTransport session"
  )

  # dial reuses the established session, sends OpenStream, and returns only
  # after the recipient has registered the same stream identifier and sent a
  # StreamAck through one of the session's reply groups.
  let initiatorStream = (await initiator.dial(destination, TestCodec)).expect(
    "could not establish MixTransport stream"
  )

  let recipientSession = recipient.sessions.get(session.sessionId).expect(
      "recipient did not retain the established session"
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
  # the recipient this consumes a SURB group; low reply capacity is replenished
  # before ordinary return traffic is allowed to consume the control reserve.
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
  let expectedReplies =
    DefaultConnectSurbRedundancy + 2 * DefaultOpenStreamSurbRedundancy
  for _ in 0 ..< expectedReplies:
    let observed = observedReplies.get()
    if not await observed.withTimeout(TestOperationTimeout):
      raise newException(LPError, "redundant reply did not reach the initiator")
    replyDispositions.add(await observed)

  # These futures must've been completed
  let invocation = await invocationFuture

  RoundTripOutcome(
    destination: destination,
    session: session,
    reused: reused,
    initiatorStream: initiatorStream,
    recipientStream: recipientStream,
    recipientReplyGroups: recipientSession.receivedSurbGroupCount,
    rejectionError: rejected.error,
    initiatorStreamCount: session.streamCount,
    recipientStreamCount: recipientSession.streamCount,
    replyDispositions: replyDispositions,
    handlerPeerId: invocation.stream.peerId,
    handlerCodec: invocation.selectedCodec,
    handlerReceivedStream: invocation.stream == recipientStream,
    receivedRequest: receivedRequest,
    receivedResponse: receivedResponse,
  )

suite "MixTransport session and stream handshakes":
  setup:
    updateLogLevel("INFO;trace:mix_transport")

  test "StreamAck establishes a stream and StreamReject rejects an unsupported codec":
    let outcome = waitFor establishSessionAndStream()

    check:
      outcome.session.role == SessionRole.Initiator
      outcome.session.state == SessionState.Established
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
      outcome.rejectionError == "requested protocol is not supported"
      outcome.initiatorStreamCount == 1
      outcome.recipientStreamCount == 1
      outcome.recipientReplyGroups >= ReplyControlReserveGroups
      outcome.handlerPeerId == outcome.session.sessionId
      outcome.handlerCodec == TestCodec
      outcome.handlerReceivedStream
      outcome.receivedRequest == TestRequest.toBytes()
      outcome.receivedResponse == TestResponse.toBytes()
      outcome.replyDispositions ==
        @[
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
        ]

  test "data packets are not rejected if ACK arrives too fast":
    try:
      discard waitFor establishSessionAndStream(Opt.some(2.seconds))
    except LPError as err:
      raiseAssert "Unexpected error: " & err.msg

type
  Synchronizer = ref object
    connectAttempts: Table[string, Future[Result[Session, string]].Raising([CancelledError])]
    sessions: Table[string, Session]
    connLock: AsyncLock
    gate: AsyncEvent

  Caller = int
  ConnAttempt = int
  Destination = string
  Session = tuple[attempt: ConnAttempt, caller: Caller]

  ConnInternal = proc(self: Synchronizer, dest: Destination):
    Future[Result[Session, string]].Raising([CancelledError]) {.gcsafe, raises: [].}

proc newSynchronizer*(T: type Synchronizer): T =
  T(connLock: newAsyncLock(), gate: newAsyncEvent())

proc getExisting*(self: Synchronizer, dest: Destination): Opt[Session] {.raises: [].} =
  if self.sessions.hasKey(dest):
    try:
      return Opt.some(self.sessions[dest])
    except KeyError:
      doAssert false
  else:
    return Opt.none(Session)

proc connInternal(caller: Caller, error: Opt[string] = Opt.none(string)): ConnInternal =
  proc wrapped(self: Synchronizer, dest: Destination): Future[Result[Session, string]] {.
      async: (raises: [CancelledError])
  .} =
    # makes sure the connection attempt doesn't end before we can
    # fire the next caller - this is how we ensure that calls get
    # placed into the same attempt.
    await self.gate.wait()

    if error.isSome:
      return err(error.get())

    let attempt = try:
      self.sessions[dest].attempt
    except KeyError:
      0

    let session = (attempt + 1, caller)
    self.sessions[dest] = session
    return ok(session)

  wrapped

suite "connect behavior under multiple callers":

  test "should create connection when there is only one caller":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = Synchronizer.newSynchronizer()
      transport.gate.fire()

      let session = await connect[Synchronizer, Destination, Session](
        transport, "destination1", connInternal(5), getExisting)

      check:
        session.get() == (attempt: 1, caller: 5)
        transport.connectAttempts.len == 0

    waitFor asyncTest()

  test "should return existing connection if there is one":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = Synchronizer.newSynchronizer()
      transport.sessions["destination1"] = (10, 1)
      transport.gate.fire()

      let session = await connect[Synchronizer, Destination, Session](
        transport, "destination1", connInternal(5), getExisting)

      check:
        session.get() == (attempt: 10, caller: 1)

    waitFor asyncTest()

  test "should await the owner's attempt when there is more than one caller":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = Synchronizer.newSynchronizer()

      let
        first = connect[Synchronizer, Destination, Session](
          transport, "destination1", connInternal(1), getExisting)
        second = connect[Synchronizer, Destination, Session](
          transport, "destination1", connInternal(2), getExisting)

      transport.gate.fire()

      let
        firstSession = await first
        secondSession = await second

      check:
        firstSession.get() == (attempt: 1, caller: 1)
        secondSession.get() == (attempt: 1, caller: 1)
        transport.connectAttempts.len == 0

    waitFor asyncTest()

  test "should allow another attempt if the previous one failed":
    proc asyncTest(): Future[void] {.async: (handleException: true).} =
      let transport = Synchronizer.newSynchronizer()

      let
        first = connect[Synchronizer, Destination, Session](
          transport, "destination1", connInternal(1, Opt.some("ooops, this is an error")),
          getExisting)
        second = connect[Synchronizer, Destination, Session](
          transport, "destination1", connInternal(2, Opt.some("this is also an error")),
          getExisting)

      transport.gate.fire()
      # Despite the error, the first two calls should end in the same
      # outcome as they are logically the same attempt.
      check:
        (await first).error() == "ooops, this is an error"
        (await second).error() == "ooops, this is an error"
        transport.connectAttempts.len == 0

      # A third call that happens after the first two complete,
      # however, should be able to go through as it represents
      # a separate attempt.
      transport.gate.clear()
      let third = connect[Synchronizer, Destination, Session](
        transport, "destination1", connInternal(3), getExisting)
      transport.gate.fire()
      check:
        (await third).get() == (attempt: 1, caller: 3)
        transport.connectAttempts.len == 0

    waitFor asyncTest()
