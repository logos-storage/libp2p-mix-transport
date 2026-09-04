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

  test "recipient supply is bounded and duplicate numbered SURBs are ignored":
    let
      rng = newRng()
      store = newSessionStore(recipientSurbCapacity = 6)
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )

    session.initializeSurbSupply().expect("could not initialize SURB supply")

    let initialSnapshot = session.surbSupplySnapshot()
    check:
      session.receivedSurbCount == 0
      initialSnapshot.receiveBase == 0
      initialSnapshot.supplyLimit == 6

    check session.acceptSurbSupply(1, SURB()) == SurbSupplyDisposition.Accepted
    check session.acceptSurbSupply(1, SURB()) == SurbSupplyDisposition.Duplicate
    check session.surbSupplySnapshot().receiveBase == 0
    check session.acceptSurbSupply(0, SURB()) == SurbSupplyDisposition.Accepted
    check session.surbSupplySnapshot().receiveBase == 2
    check session.acceptSurbSupply(3, SURB()) == SurbSupplyDisposition.Accepted
    check session.acceptSurbSupply(2, SURB()) == SurbSupplyDisposition.Accepted
    check session.acceptSurbSupply(5, SURB()) == SurbSupplyDisposition.Accepted
    check session.acceptSurbSupply(4, SURB()) == SurbSupplyDisposition.Accepted
    check:
      session.receivedSurbCount == 6
      session.surbSupplySnapshot().receiveBase == 6
      session.acceptSurbSupply(6, SURB()) == SurbSupplyDisposition.OutsideWindow

    # Consuming two stored SURBs grants exactly two new absolute supply slots.
    discard session.takeReceivedSurbs(2).expect("could not consume received SURBs")
    check session.surbSupplySnapshot().supplyLimit == 8
    check session.acceptSurbSupply(6, SURB()) == SurbSupplyDisposition.Accepted
    check session.acceptSurbSupply(7, SURB()) == SurbSupplyDisposition.Accepted
    check session.receivedSurbCount == session.recipientSurbCapacity

  test "initiator applies absolute supply acknowledgements and credit":
    let
      rng = newRng()
      session = newSessionStore()
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      emptyBitmap = newSeq[byte](SurbSupplyAckBitmapBytes)

    check session.applySurbSupplySnapshot(
      SurbSupplySnapshot(
        receiveBase: 0, acknowledgementBitmap: emptyBitmap, supplyLimit: 4
      )
    )
    check session.availableSurbSupplySlots == 4
    check session
      .registerSurbSupply(
        @[newSeq[byte](1), newSeq[byte](1), newSeq[byte](1), newSeq[byte](1)],
        newSeq[SURBIdentifier](4),
      )
      .expect("could not register SURB supply") == 0
    check:
      session.pendingSurbSupplyCount == 4
      session.availableSurbSupplySlots == 0

    # The snapshot acknowledges sequences 0 and 1 through its receive base and
    # sequence 3 through bitmap offset 1. Sequence 2 remains pending.
    var acknowledgementBitmap = newSeq[byte](SurbSupplyAckBitmapBytes)
    acknowledgementBitmap[0] = 0b00000010
    check session.applySurbSupplySnapshot(
      SurbSupplySnapshot(
        receiveBase: 2, acknowledgementBitmap: acknowledgementBitmap, supplyLimit: 6
      )
    )
    check:
      session.pendingSurbSupplyCount == 1
      session.remoteSurbSupplyLimit == 6
      session.availableSurbSupplySlots == 2

  test "Connect bootstrap supply begins the numbered sequence space":
    let
      rng = newRng()
      session = newSessionStore()
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      encodedSurbs = @[newSeq[byte](1), newSeq[byte](1), newSeq[byte](1)]
      identifiers = newSeq[SURBIdentifier](encodedSurbs.len)

    check session.registerInitialSurbSupply(encodedSurbs, identifiers).expect(
      "could not register bootstrap SURBs"
    ) == 0
    check:
      session.pendingSurbSupplyCount == 3
      session.availableSurbSupplySlots == 0

    # ConnectAck reports that all three bootstrap SURBs arrived and grants
    # credit for filling the rest of the recipient's sixteen-entry queue.
    check session.applySurbSupplySnapshot(
      SurbSupplySnapshot(
        receiveBase: 3,
        acknowledgementBitmap: newSeq[byte](SurbSupplyAckBitmapBytes),
        supplyLimit: DefaultRecipientSurbCapacity,
      )
    )
    check:
      session.pendingSurbSupplyCount == 0
      session.availableSurbSupplySlots == DefaultRecipientSurbCapacity - 3

  test "reverse activity resets unanswered status probe attempts":
    let
      rng = newRng()
      session = newSessionStore()
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      now = Moment.now()

    session.noteReverseActivity(30.seconds, now)
    check:
      session.unansweredSurbStatusProbeCount == 0
      session.timeUntilSurbStatusProbe(now) == Opt.some(30.seconds)

    session.recordSurbStatusProbeAttempt(5.seconds, now + 30.seconds)
    check:
      session.unansweredSurbStatusProbeCount == 1
      session.timeUntilSurbStatusProbe(now + 30.seconds) == Opt.some(5.seconds)

    session.noteReverseActivity(30.seconds, now + 31.seconds)
    check:
      session.unansweredSurbStatusProbeCount == 0
      session.timeUntilSurbStatusProbe(now + 31.seconds) == Opt.some(30.seconds)

  test "unacknowledged SURB supply retains its credential association":
    let
      rng = newRng()
      session = newSessionStore()
        .addInitiatorSession(randomPeerId(rng), randomPeerId(rng))
        .expect("could not add initiator session")
      now = Moment.now()
      firstEncodedSurb = @[1'u8, 2, 3]
      secondEncodedSurb = @[4'u8, 5, 6]
    var
      firstCredentialIdentifier: SURBIdentifier
      secondCredentialIdentifier: SURBIdentifier
    firstCredentialIdentifier[0] = 1
    secondCredentialIdentifier[0] = 2

    check session.applySurbSupplySnapshot(
      SurbSupplySnapshot(
        receiveBase: 0,
        acknowledgementBitmap: newSeq[byte](SurbSupplyAckBitmapBytes),
        supplyLimit: 2,
      )
    )
    let firstSequence = session
      .registerSurbSupply(
        @[firstEncodedSurb, secondEncodedSurb],
        @[firstCredentialIdentifier, secondCredentialIdentifier],
      )
      .expect("could not register SURB supply")
    session.scheduleSurbSupplyRetransmission(firstSequence, 2, 30.seconds, now)

    check:
      session.earliestSurbSupplyRetransmission() == Opt.some(now + 30.seconds)
      session.takeDueSurbSupplyRetransmission(now + 29.seconds).isNone

    let retransmission = session
      .takeDueSurbSupplyRetransmission(now + 30.seconds)
      .expect("first supplied SURB was not ready for retransmission")
    check:
      retransmission.sequence == firstSequence
      retransmission.encodedSurb == firstEncodedSurb
      retransmission.credentialIdentifier == firstCredentialIdentifier

    # Taking a due entry temporarily removes its deadline but retains the
    # serialized public SURB. Sending code schedules its next deadline after
    # the retransmission attempt completes.
    check session.earliestSurbSupplyRetransmission() == Opt.some(now + 30.seconds)

    # The transport removes a pending public serialization instead of
    # retransmitting it after its matching reply credential has expired.
    session.removePendingSurbSupply(retransmission.sequence)
    check session.pendingSurbSupplyCount == 1

    session.scheduleSurbSupplyRetransmission(
      retransmission.sequence, 1, 30.seconds, now + 30.seconds
    )
    check session.earliestSurbSupplyRetransmission() == Opt.some(now + 30.seconds)

    # A later absolute snapshot acknowledges both sequences. Acknowledgement
    # removes the retained serializations even if a retry deadline exists.
    check session.applySurbSupplySnapshot(
      SurbSupplySnapshot(
        receiveBase: 2,
        acknowledgementBitmap: newSeq[byte](SurbSupplyAckBitmapBytes),
        supplyLimit: 2,
      )
    )
    check:
      session.pendingSurbSupplyCount == 0
      session.earliestSurbSupplyRetransmission().isNone

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
