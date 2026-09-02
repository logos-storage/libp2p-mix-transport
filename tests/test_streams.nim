# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]

import chronos, results
import libp2p/[crypto/crypto, peerid, stream/connection]
import libp2p_mix

import libp2p_mix_transport

privateAccess(TransportSession)
privateAccess(TransportStream)

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
      firstInitiatorStream.peerId == initiatorSession.peerId
      firstInitiatorStream.dir == Direction.Out
      firstInitiatorStream.protocol == "/test/1"
      firstInitiatorStream.state == StreamState.Pending
      firstInitiatorStream.sessionId == initiatorSession.sessionId
      firstInitiatorStream.codec == "/test/1"

  test "the final stream identifier is allocated once before exhaustion":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addInitiatorSession(randomPeerId(rng), randomPeerId(rng)).expect(
          "could not add initiator session"
        )
    session.establish()
    session.nextOutboundStreamId = Opt.some(StreamId.high)

    let finalStream = session.addOutboundStream("/test/final").expect(
        "could not allocate final stream identifier"
      )

    check:
      finalStream.streamId == StreamId.high
      session.addOutboundStream("/test/exhausted").isErr

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

  test "the acknowledgement bitmap retains and orders received chunks":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")

    check:
      stream.receiveData(2, @[2'u8]) == InboundDataDisposition.Accepted
      stream.takeNextInbound().isNone
      stream.receiveData(1, @[1'u8]) == InboundDataDisposition.Accepted
      stream.receiveData(2, @[2'u8]) == InboundDataDisposition.Duplicate

    var first = stream.takeNextInbound().expect("sequence 1 was not ready")
    check:
      first.sequence == 1
      first.payload == @[1'u8]
    stream.advanceReceiveWindow(first.sequence)

    var second = stream.takeNextInbound().expect("sequence 2 was not ready")
    check:
      second.sequence == 2
      second.payload == @[2'u8]
    stream.advanceReceiveWindow(second.sequence)

    check:
      stream.receiveBase == 3
      stream.pendingInboundCount == 0

  test "closing wakes a writer blocked by the outbound capacity limit":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")

    for index in 0 ..< MaxInflightChunks:
      discard stream.reserveOutbound(@[byte(index)]).expect(
          "could not fill outbound capacity"
        )

    let waitingForCapacity = stream.waitForOutboundCapacity()
    check not waitingForCapacity.finished

    # TransportStream.closeImpl fires sendStateChanged so a writer blocked on
    # outbound capacity can wake and observe that the stream has closed.
    waitFor stream.close()

    expect LPStreamClosedError:
      waitFor waitingForCapacity

  test "a writer observes sequence exhaustion instead of waiting forever":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")
    stream.nextOutboundSequence = SequenceNumber.high

    check stream.reserveOutbound(@[1'u8]).isErr
    expect LPStreamError:
      waitFor stream.waitForOutboundCapacity()

  test "outbound retransmission deadlines select the earliest due chunk":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")
    let
      now = Moment.now()
      firstSequence =
        stream.reserveOutbound(@[1'u8]).expect("could not reserve first outbound chunk")
      secondSequence = stream.reserveOutbound(@[2'u8]).expect(
          "could not reserve second outbound chunk"
        )

    stream.scheduleOutboundRetransmission(firstSequence, 2.seconds, now)
    stream.scheduleOutboundRetransmission(secondSequence, 1.seconds, now)

    check:
      stream.earliestRetransmissionDeadline().get() == now + 1.seconds
      stream.takeDueOutboundRetransmission(now).isNone

    let retransmission = stream.takeDueOutboundRetransmission(now + 1.seconds).expect(
        "second chunk was not ready for retransmission"
      )
    check:
      retransmission.sequence == secondSequence
      retransmission.payload == @[2'u8]
      stream.earliestRetransmissionDeadline().get() == now + 2.seconds
      stream.takeDueOutboundRetransmission(now + 1.seconds).isNone

  test "finishing an overlapping retransmission does not restore an acknowledged chunk":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")
    let
      now = Moment.now()
      sequence =
        stream.reserveOutbound(@[1'u8]).expect("could not reserve outbound chunk")
    stream.scheduleOutboundRetransmission(sequence, 1.seconds, now)
    discard stream.takeDueOutboundRetransmission(now + 1.seconds).expect(
        "chunk was not ready for retransmission"
      )

    check stream.applyAcknowledgement(2, newSeq[byte](AckBitmapBytes))
    stream.scheduleOutboundRetransmission(sequence, 1.seconds, now + 1.seconds)

    check:
      stream.pendingOutboundCount == 0
      stream.earliestRetransmissionDeadline().isNone

  test "stream shutdown cancels and waits for its owned tasks":
    let
      rng = newRng()
      store = newSessionStore()
      session = store.addRecipientSession(randomPeerId(rng)).expect(
          "could not add recipient session"
        )
    session.establish()
    let stream =
      session.addInboundStream(1, "/test/1").expect("could not add inbound stream")

    let
      streamTaskBlocker = newAsyncEvent()
      handlerTaskBlocker = newAsyncEvent()
      streamTask = streamTaskBlocker.wait()
      handlerTask = handlerTaskBlocker.wait()
    stream.trackStreamTask(streamTask)
    stream.setHandlerTask(handlerTask)
    let waitingForResolution = stream.waitUntilResolved()

    check:
      not streamTask.finished
      not handlerTask.finished
      not waitingForResolution.finished

    waitFor stream.shutdown()

    check:
      stream.closed
      streamTask.cancelled()
      handlerTask.cancelled()
      waitingForResolution.finished
