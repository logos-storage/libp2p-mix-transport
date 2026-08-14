# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import results
import libp2p/[crypto/crypto, peerid]

import libp2p_mix_transport

proc randomPeerId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate peer identifier")

suite "MixTransport sessions":
  test "initiator sessions retain both destination and pseudonymous identity":
    let
      rng = newRng()
      store = newSessionStore()
      destination = randomPeerId(rng)
      sessionId = randomPeerId(rng)
      session = store.addInitiatorSession(destination, sessionId).expect(
          "could not add initiator session"
        )

    check:
      session.role == SessionRole.Initiator
      session.state == SessionState.Pending
      session.sessionId == sessionId
      session.destination == Opt.some(destination)
      session.peerId == destination
      store.get(sessionId).get() == session
      store.getByDestination(destination).get() == session

    session.establish()
    check session.state == SessionState.Established

  test "recipient sessions expose only the initiator pseudonym":
    let
      rng = newRng()
      store = newSessionStore()
      sessionId = randomPeerId(rng)
      session =
        store.addRecipientSession(sessionId).expect("could not add recipient session")

    check:
      session.role == SessionRole.Recipient
      session.destination.isNone
      session.peerId == sessionId
      store.get(sessionId).get() == session
      store.getByDestination(sessionId).isNone

  test "duplicate indexes are rejected without replacing existing sessions":
    let
      rng = newRng()
      store = newSessionStore()
      destination = randomPeerId(rng)
      sessionId = randomPeerId(rng)
      original = store.addInitiatorSession(destination, sessionId).expect(
          "could not add original session"
        )

    check:
      store.addInitiatorSession(destination, randomPeerId(rng)).isErr
      store.addRecipientSession(sessionId).isErr
      store.len == 1
      store.get(sessionId).get() == original
      store.getByDestination(destination).get() == original

  test "removing an initiator session clears both indexes":
    let
      rng = newRng()
      store = newSessionStore()
      destination = randomPeerId(rng)
      sessionId = randomPeerId(rng)
      session = store.addInitiatorSession(destination, sessionId).expect(
          "could not add initiator session"
        )

    check store.remove(sessionId).get() == session
    check:
      store.len == 0
      store.get(sessionId).isNone
      store.getByDestination(destination).isNone
      store.remove(sessionId).isNone
