# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, tables, unittest]

import chronos, results
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
  ]
import libp2p_mix
import libp2p_mix/delay_strategy
import libp2p_mix/serialization
import protobuf_serialization

import libp2p_mix_transport

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

proc establishSessionAndStream(): Future[RoundTripOutcome] {.
    async: (raises: [CancelledError, LPError])
.} =
  let
    nodes = createMixNodes(5)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = newMixTransport(
      initiatorMix,
      connectTimeout = TestOperationTimeout,
      streamOpenTimeout = TestOperationTimeout,
    )
    recipient = newMixTransport(
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
  let invocationFuture = protocolInvocations.get()
  if not await invocationFuture.withTimeout(TestOperationTimeout):
    raise newException(LPError, "recipient protocol handler was not invoked")
  let invocation = await invocationFuture

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
  # before return traffic is allowed to consume a group above the control reserve.
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

  test "a refill retains valid groups and permits another short-supply request":
    let
      mix = createMixNodes(1)[0]
      transport = newMixTransport(mix)
      session = transport.sessions
        .addRecipientSession(
          PeerId.random(mix.rng).expect("could not generate session identifier")
        )
        .expect("could not add recipient session")

    session.establish()
    session.addReceivedSurbGroups(@[@[SURB()], @[SURB()]]).expect(
      "could not add initial reply groups"
    )
    let batchId = session.beginRefill().expect("could not begin refill")
    discard session.takeReceivedSurbGroup().expect(
        "could not consume the refill request group"
      )

    (waitFor transport.start()).expect("could not start transport")
    defer:
      waitFor transport.stop()

    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: session.sessionId,
      kind: FrameKind.Refill,
      batchId: Opt.some(batchId),
      partIndex: Opt.some(0'u32),
      partCount: Opt.some(1'u32),
      surbGroups: @[
        SurbGroup.init(@[testSurb(1)]).expect("could not encode valid SURB group"),
        SurbGroup(surbs: @[@[0'u8]]),
      ],
    )

    # Invoke the same delivery callback that Mix uses for a forward Refill.
    # The valid group is retained, the malformed group is ignored, and the
    # completed batch allows the recipient to request more capacity.
    check frame.encode().isErr
    waitFor mix.deliveryHandlers[MixTransportCodec](
      MixDelivery(
        service: MixTransportCodec,
        # Bypass the strict local encoder to model malformed bytes received
        # from a remote endpoint. The inbound decoder performs structural
        # validation and leaves each SURB group for independent decoding.
        payload: Protobuf.encode(frame),
      )
    )

    check:
      session.receivedSurbGroupCount == ReplyControlReserveGroups
      session.beginRefill().isSome
