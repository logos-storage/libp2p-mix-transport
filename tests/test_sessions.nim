# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

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

  test "recipient sessions store SURBs and form redundancy batches on demand":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
      supplied = @[SURB(), SURB(), SURB()]

    store.get(session.sessionId).get().addReceivedSurbs(supplied).expect(
      "could not add received SURBs"
    )

    check session.receivedSurbCount == 3
    check session.takeReceivedSurbs(2).expect("could not form redundancy batch").len == 2
    check session.receivedSurbCount == 1
    check session.takeReceivedSurbs(2).isErr
    check session.takeReceivedSurbs(1).expect("could not consume final SURB").len == 1

  test "a short refill permits another request immediately":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session.addReceivedSurbs(newSeq[SURB](ReplyControlReserveSurbs)).expect(
      "could not add initial SURBs"
    )

    let
      now = Moment.now()
      firstRefillRequestId = session.registerRefillRequest(now).expect(
          "could not register first refill request"
        )
    discard session.takeReceivedSurbs(DefaultReplySurbRedundancy).expect(
        "could not form the refill request redundancy batch"
      )
    session.scheduleNextRefillRequest(30.seconds, now)
    session.clearReplyCapacityStateChanged()

    check not session.refillRequestDue(now + 1.seconds)

    # A response may retain fewer valid SURBs than requested. Accepting the
    # response wakes the waiting send so that it can recheck the queue and retry
    # instead of waiting for enough unreserved SURBs indefinitely.
    check session.acceptRefillResponse(firstRefillRequestId, now + 1.seconds)
    session.addReceivedSurbs(newSeq[SURB](DefaultReplySurbRedundancy)).expect(
      "could not add valid SURBs from the refill"
    )
    waitFor session.waitForReplyCapacityStateChange()

    check:
      session.receivedSurbCount == ReplyControlReserveSurbs
      session.refillRequestDue(now + 1.seconds)

  test "late refill responses contribute their SURBs exactly once":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session.addReceivedSurbs(newSeq[SURB](ReplyControlReserveSurbs)).expect(
      "could not add initial SURBs"
    )

    let firstRequestId =
      session.registerRefillRequest().expect("could not register first request")
    discard session.takeReceivedSurbs(DefaultReplySurbRedundancy).expect(
        "could not send first request"
      )
    let retryRequestId =
      session.registerRefillRequest().expect("could not register retry request")
    discard session.takeReceivedSurbs(DefaultReplySurbRedundancy).expect(
        "could not send retry request"
      )

    check session.acceptRefillResponse(retryRequestId)
    session.addReceivedSurbs(newSeq[SURB](DefaultRefillSurbs)).expect(
      "could not add retry response SURBs"
    )
    check:
      session.refillRequestDue
      session.receivedSurbCount == ReplyControlReserveSurbs

    check session.acceptRefillResponse(firstRequestId)
    session.addReceivedSurbs(newSeq[SURB](DefaultRefillSurbs)).expect(
      "could not add late response SURBs"
    )
    check:
      not session.refillRequestDue
      session.receivedSurbCount == 2 * ReplyControlReserveSurbs
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

    session.addReceivedSurbs(newSeq[SURB](ReplyControlReserveSurbs)).expect(
      "could not add initial SURBs"
    )
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
