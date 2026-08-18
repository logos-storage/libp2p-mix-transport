# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]

import chronos, results
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

const
  TestCodec = "/mix-transport/test/1.0.0"
  UnsupportedCodec = "/mix-transport/unsupported/1.0.0"

type ProtocolInvocation = object
  stream: Stream
  selectedCodec: string

proc newTestProtocol(
    codec: string,
    invocations: AsyncQueue[ProtocolInvocation],
    keepHandlerRunning: AsyncEvent,
): LPProtocol =
  let handler: LPProtoHandler = proc(
      stream: Stream, selectedCodec: string
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await invocations.put(
      ProtocolInvocation(stream: stream, selectedCodec: selectedCodec)
    )
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

proc establishSessionAndStream(): Future[RoundTripOutcome] {.
    async: (raises: [CancelledError, LPError])
.} =
  let
    nodes = createMixNodes(5)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = newMixTransport(
      initiatorMix, connectTimeout = 5.seconds, streamOpenTimeout = 5.seconds
    )
    recipient = newMixTransport(
      recipientMix, connectTimeout = 5.seconds, streamOpenTimeout = 5.seconds
    )

  # StreamAck confirms that the recipient has a mounted handler for the
  # requested application codec. The handler remains active until transport
  # teardown so the test can inspect the connection passed to it.
  let
    protocolInvocations = newAsyncQueue[ProtocolInvocation]()
    keepHandlerRunning = newAsyncEvent()
  recipientMix.switch.mount(
    newTestProtocol(TestCodec, protocolInvocations, keepHandlerRunning)
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
  if not await invocationFuture.withTimeout(5.seconds):
    raise newException(LPError, "recipient protocol handler was not invoked")
  let invocation = await invocationFuture

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
    if not await observed.withTimeout(5.seconds):
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
      outcome.recipientReplyGroups == 3
      outcome.handlerPeerId == outcome.session.sessionId
      outcome.handlerCodec == TestCodec
      outcome.handlerReceivedStream
      outcome.replyDispositions ==
        @[
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
          RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled,
        ]
