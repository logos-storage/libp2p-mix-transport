# SPDX-License-Identifier: MIT

{.used.}

import std/[sequtils, unittest]

import chronos
import results
import libp2p/[crypto/crypto, peerid, stream/connection]
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

  test "a short refill permits another request immediately":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session
      .addReceivedSurbGroups(toSeq(0 ..< ReplyControlReserveGroups).mapIt(@[SURB()]))
      .expect("could not add initial SURB groups")

    let
      now = Moment.now()
      firstRefillRequestId = session.registerRefillRequest(now).expect(
          "could not register first refill request"
        )
    discard session.takeReceivedSurbGroup().expect(
        "could not consume the refill request group"
      )
    session.scheduleNextRefillRequest(30.seconds, now)
    session.clearReplyCapacityStateChanged()

    check not session.refillRequestDue(now + 1.seconds)

    # A response may retain fewer valid groups than requested. Accepting the
    # response wakes the waiting send so that it can recheck the queue and retry
    # instead of waiting for an unreserved group indefinitely.
    check session.acceptRefillResponse(firstRefillRequestId, now + 1.seconds)
    session.addReceivedSurbGroups(@[@[SURB()]]).expect(
      "could not add the valid part of the refill"
    )
    waitFor session.waitForReplyCapacityStateChange()

    check:
      session.receivedSurbGroupCount == ReplyControlReserveGroups
      session.refillRequestDue(now + 1.seconds)

  test "late refill responses contribute their SURB groups exactly once":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session
      .addReceivedSurbGroups(toSeq(0 ..< ReplyControlReserveGroups).mapIt(@[SURB()]))
      .expect("could not add initial SURB groups")

    let firstRequestId =
      session.registerRefillRequest().expect("could not register first request")
    discard session.takeReceivedSurbGroup().expect("could not send first request")
    let retryRequestId =
      session.registerRefillRequest().expect("could not register retry request")
    discard session.takeReceivedSurbGroup().expect("could not send retry request")

    check session.acceptRefillResponse(retryRequestId)
    session.addReceivedSurbGroups(@[@[SURB()], @[SURB()]]).expect(
      "could not add retry response groups"
    )
    check:
      session.refillRequestDue
      session.receivedSurbGroupCount == ReplyControlReserveGroups

    check session.acceptRefillResponse(firstRequestId)
    session.addReceivedSurbGroups(@[@[SURB()], @[SURB()]]).expect(
      "could not add late response groups"
    )
    check:
      not session.refillRequestDue
      session.receivedSurbGroupCount == 2 * ReplyControlReserveGroups
      not session.acceptRefillResponse(firstRequestId)

  test "the oldest refill request is evicted when correlation state is full":
    let
      rng = newRng()
      store = newSessionStore(
        refillResponseLifetime = 30.minutes, maxOutstandingRefillRequests = 2
      )
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
      now = Moment.now()

    session
      .addReceivedSurbGroups(toSeq(0 ..< ReplyControlReserveGroups).mapIt(@[SURB()]))
      .expect("could not add initial SURB groups")
    let
      firstRequestId = session.registerRefillRequest(now).expect("first request")
      secondRequestId =
        session.registerRefillRequest(now + 1.seconds).expect("second request")
      thirdRequestId =
        session.registerRefillRequest(now + 2.seconds).expect("third request")

    check:
      session.outstandingRefillRequestCount == 2
      not session.acceptRefillResponse(firstRequestId, now + 3.seconds)
      session.acceptRefillResponse(secondRequestId, now + 3.seconds)
      session.acceptRefillResponse(thirdRequestId, now + 3.seconds)

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

  test "session shutdown detaches its streams and waits for their tasks":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let
      firstStream =
        session.addInboundStream(1, "/test/1").expect("could not add first stream")
      secondStream =
        session.addInboundStream(3, "/test/2").expect("could not add second stream")
      firstTask = newAsyncEvent().wait()
      secondTask = newAsyncEvent().wait()
    firstStream.trackStreamTask(firstTask)
    secondStream.trackStreamTask(secondTask)

    waitFor session.shutdown()

    check:
      session.state == SessionState.Closed
      session.streamCount == 0
      firstStream.closed
      secondStream.closed
      firstTask.cancelled()
      secondTask.cancelled()

  test "taking sessions clears both store indexes":
    let
      rng = newRng()
      store = newSessionStore()
      destination = randomPeerId(rng)
      initiatorSession = store
        .addInitiatorSession(destination, randomPeerId(rng))
        .expect("could not add initiator session")
      recipientSession = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    let sessions = store.takeSessions()

    check:
      sessions.len == 2
      initiatorSession in sessions
      recipientSession in sessions
      store.len == 0
      store.get(initiatorSession.sessionId).isNone
      store.getByDestination(destination).isNone

  test "session shutdown wakes pending establishment":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
      waitingForEstablishment = session.waitUntilEstablished()

    check not waitingForEstablishment.finished

    waitFor session.shutdown()

    check:
      session.state == SessionState.Closed
      waitingForEstablishment.finished
