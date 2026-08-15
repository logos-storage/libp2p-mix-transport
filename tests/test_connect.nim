# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]

import chronos, results
import libp2p/[builders, crypto/crypto, crypto/secp, peerid, switch]
import libp2p_mix
import libp2p_mix/delay_strategy

import libp2p_mix_transport

privateAccess(MixProtocol)

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

type ConnectOutcome = object
  destination: PeerId
  session: TransportSession
  reused: TransportSession
  replyDispositions: seq[RawSurbReplyDisposition]

proc connectRoundTrip(): Future[ConnectOutcome] {.
    async: (raises: [CancelledError, LPError])
.} =
  let
    nodes = createMixNodes(5)
    initiatorMix = nodes[0]
    recipientMix = nodes[^1]
    initiator = newMixTransport(initiatorMix, connectTimeout = 5.seconds)
    recipient = newMixTransport(recipientMix, connectTimeout = 5.seconds)

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

  var replyDispositions: seq[RawSurbReplyDisposition]
  for _ in 0 ..< DefaultConnectSurbRedundancy:
    let observed = observedReplies.get()
    if not await observed.withTimeout(5.seconds):
      raise newException(LPError, "redundant ConnectAck did not reach the initiator")
    replyDispositions.add(await observed)

  ConnectOutcome(
    destination: destination,
    session: session,
    reused: reused,
    replyDispositions: replyDispositions,
  )

suite "MixTransport Connect handshake":
  test "ConnectAck establishes and reuses an anonymous session":
    let outcome = waitFor connectRoundTrip()

    check:
      outcome.session.role == SessionRole.Initiator
      outcome.session.state == SessionState.Established
      outcome.session.peerId == outcome.destination
      outcome.reused == outcome.session
      outcome.replyDispositions ==
        @[RawSurbReplyDisposition.Handled, RawSurbReplyDisposition.Handled]
