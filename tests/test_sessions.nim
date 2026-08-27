# SPDX-License-Identifier: MIT

{.used.}

import std/[sequtils, unittest]

import chronos
import results
import libp2p/[crypto/crypto, peerid]
import libp2p_mix

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

  test "recipient sessions retain and consume complete SURB groups":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
      first = @[SURB(), SURB()]
      second = @[SURB()]

    store.get(session.sessionId).get().addReceivedSurbGroups(@[first, second]).expect(
      "could not add received SURB groups"
    )

    check session.receivedSurbGroupCount == 2
    check session.takeReceivedSurbGroup().expect("could not take first group").len == 2
    check session.receivedSurbGroupCount == 1
    check session.takeReceivedSurbGroup().expect("could not take second group").len == 1
    check session.takeReceivedSurbGroup().isErr

  test "a short refill completes its batch and permits another request":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session
      .addReceivedSurbGroups(toSeq(0 ..< ReplyControlReserveGroups).mapIt(@[SURB()]))
      .expect("could not add initial SURB groups")

    let firstBatchId = session.beginRefill().expect("could not begin first refill")
    discard session.takeReceivedSurbGroup().expect(
        "could not consume the refill request group"
      )
    session.clearReplyCapacityStateChanged()

    # A refill may retain fewer valid groups than requested. Completing the
    # batch wakes the waiting send so that it can recheck the queue and request
    # another refill instead of waiting for an unreserved group indefinitely.
    check session.completeRefill(firstBatchId)
    session.addReceivedSurbGroups(@[@[SURB()]]).expect(
      "could not add the valid part of the refill"
    )
    waitFor session.waitForReplyCapacityStateChange()

    check:
      session.receivedSurbGroupCount == ReplyControlReserveGroups
      session.beginRefill().isSome

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
