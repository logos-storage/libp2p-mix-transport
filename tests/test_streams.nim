# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[crypto/crypto, peerid]
import libp2p_mix

import libp2p_mix_transport

# Callers using the package facade must register streams through their session.
static:
  doAssert not declared(newTransportStream)

proc randomPeerId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate peer identifier")

suite "MixTransport streams":
  test "the two session endpoints allocate disjoint stream identifiers":
    let
      rng = newRng()
      store = newSessionStore()
      initiatorSession = store
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      recipientSession = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    initiatorSession.establish()
    recipientSession.establish()

    let
      firstInitiatorStream = initiatorSession.addOutboundStream("/test/1").expect(
          "could not add first initiator stream"
        )
      secondInitiatorStream = initiatorSession.addOutboundStream("/test/2").expect(
          "could not add second initiator stream"
        )
      firstRecipientStream = recipientSession.addOutboundStream("/test/1").expect(
          "could not add first recipient stream"
        )
      secondRecipientStream = recipientSession.addOutboundStream("/test/2").expect(
          "could not add second recipient stream"
        )

    check:
      firstInitiatorStream.streamId == 1
      secondInitiatorStream.streamId == 3
      firstRecipientStream.streamId == 2
      secondRecipientStream.streamId == 4
      firstInitiatorStream.direction == StreamDirection.Outbound
      firstInitiatorStream.state == StreamState.Pending
      firstInitiatorStream.sessionId == initiatorSession.sessionId
      firstInitiatorStream.codec == "/test/1"

  test "an inbound stream keeps the identifier selected by its remote opener":
    let
      rng = newRng()
      store = newSessionStore()
      initiatorSession = store
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      recipientSession = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    initiatorSession.establish()
    recipientSession.establish()

    let
      streamOpenedByRecipient = initiatorSession.addInboundStream(2, "/test/1").expect(
          "initiator could not accept recipient stream"
        )
      streamOpenedByInitiator = recipientSession.addInboundStream(1, "/test/2").expect(
          "recipient could not accept initiator stream"
        )

    check:
      streamOpenedByRecipient.streamId == 2
      streamOpenedByRecipient.direction == StreamDirection.Inbound
      streamOpenedByInitiator.streamId == 1
      streamOpenedByInitiator.direction == StreamDirection.Inbound
      initiatorSession.addInboundStream(3, "/test/3").isErr
      recipientSession.addInboundStream(4, "/test/3").isErr
      initiatorSession.addInboundStream(2, "/test/1").isErr
      initiatorSession.addInboundStream(0, "/test/1").isErr

  test "stream removal preserves its established transport session":
    let
      rng = newRng()
      store = newSessionStore()
      sessionId = randomPeerId(rng)
      session = store.addInitiatorSession(randomPeerId(rng), sessionId).expect(
          "could not add initiator session"
        )

    check session.addOutboundStream("/test/1").isErr

    session.establish()
    let stream = session.addOutboundStream("/test/1").expect("could not add stream")

    check:
      session.streamCount == 1
      session.getStream(stream.streamId).get() == stream
      session.removeStream(stream.streamId).get() == stream
      session.streamCount == 0
      session.getStream(stream.streamId).isNone
      store.get(sessionId).get() == session

  test "rejection resolves a pending stream without establishing it":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addInitiatorSession(randomPeerId(rng), randomPeerId(rng)).expect(
          "could not add initiator session"
        )

    session.establish()
    let stream = session.addOutboundStream("/test/1").expect("could not add stream")
    stream.reject("test rejection")
    waitFor stream.waitUntilResolved()

    check:
      stream.state == StreamState.Rejected
      stream.rejectionReason == "test rejection"
