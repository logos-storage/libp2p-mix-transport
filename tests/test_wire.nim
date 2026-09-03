# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import results
import libp2p/[crypto/crypto, peerid]
import libp2p_mix/serialization

import libp2p_mix_transport

proc randomSessionId(): PeerId =
  PeerId.random(newRng()).expect("could not generate session identifier")

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

suite "MixTransport wire format":
  test "data frame survives a Protobuf round trip":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.Data,
      streamId: Opt.some(StreamId(7)),
      sequence: Opt.some(SequenceNumber(11)),
      payload: Opt.some(@[1'u8, 2, 3]),
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.version == frame.version
      decoded.sessionId == frame.sessionId
      decoded.kind == frame.kind
      decoded.streamId == frame.streamId
      decoded.sequence == frame.sequence
      decoded.payload == frame.payload

  test "fixed Data identifiers give every frame the same payload capacity":
    let
      sessionId = randomSessionId()
      smallIdentifiers = MixTransportFrame(
        version: MixTransportVersion,
        sessionId: sessionId,
        kind: FrameKind.Data,
        streamId: Opt.some(StreamId(1)),
        sequence: Opt.some(SequenceNumber(1)),
        payload: Opt.some(newSeq[byte](MaxDataPayloadBytes)),
      )
      largestIdentifiers = MixTransportFrame(
        version: MixTransportVersion,
        sessionId: sessionId,
        kind: FrameKind.Data,
        streamId: Opt.some(StreamId.high),
        sequence: Opt.some(MaxDataSequenceNumber),
        payload: Opt.some(newSeq[byte](MaxDataPayloadBytes)),
      )
    var reverseIdentifiers = largestIdentifiers
    reverseIdentifiers.surbSupplyReceiveBase = Opt.some(SurbSupplySequence(0))
    reverseIdentifiers.surbSupplyAcknowledgementBitmap =
      Opt.some(newSeq[byte](SurbSupplyAckBitmapBytes))
    reverseIdentifiers.surbSupplyLimit = Opt.some(SurbSupplySequence(16))

    check:
      sessionId.len == MaxSessionIdBytes
      smallIdentifiers.encode().expect("small identifiers did not encode").len ==
        largestIdentifiers.encode().expect("largest identifiers did not encode").len
      largestIdentifiers.encode().expect("largest identifiers did not encode").len <
        MaxTransportFrameBytes
      reverseIdentifiers.encode().expect("reverse identifiers did not encode").len ==
        MaxTransportFrameBytes

    var oversized = largestIdentifiers
    oversized.payload = Opt.some(newSeq[byte](MaxDataPayloadBytes + 1))
    var exhausted = largestIdentifiers
    exhausted.sequence = Opt.some(SequenceNumber.high)
    check:
      oversized.encode().isErr
      exhausted.encode().isErr

  test "acknowledgement bitmap has the fixed receive-window size":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.Ack,
      streamId: Opt.some(StreamId(7)),
      receiveBase: Opt.some(SequenceNumber(12)),
      acknowledgementBitmap: Opt.some(newSeq[byte](AckBitmapBytes)),
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.receiveBase == frame.receiveBase
      decoded.acknowledgementBitmap == frame.acknowledgementBitmap

    var wrongSize = frame
    wrongSize.acknowledgementBitmap = Opt.some(newSeq[byte](AckBitmapBytes - 1))
    check wrongSize.encode().isErr

  test "numbered supply frame preserves individual SURBs":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.SurbSupply,
      firstSurbSequence: Opt.some(SurbSupplySequence(42)),
      surbs: @[newSeq[byte](SurbSize), newSeq[byte](SurbSize)],
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.firstSurbSequence == frame.firstSurbSequence
      decoded.surbs == frame.surbs

  test "SURB supply state is an absolute fixed-size snapshot":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.RefillRequest,
      surbSupplyReceiveBase: Opt.some(SurbSupplySequence(12)),
      surbSupplyAcknowledgementBitmap: Opt.some(newSeq[byte](SurbSupplyAckBitmapBytes)),
      surbSupplyLimit: Opt.some(SurbSupplySequence(28)),
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.surbSupplyReceiveBase == frame.surbSupplyReceiveBase
      decoded.surbSupplyAcknowledgementBitmap == frame.surbSupplyAcknowledgementBitmap
      decoded.surbSupplyLimit == frame.surbSupplyLimit

    var incomplete = frame
    incomplete.surbSupplyLimit = Opt.none(SurbSupplySequence)
    var wrongBitmap = frame
    wrongBitmap.surbSupplyAcknowledgementBitmap =
      Opt.some(newSeq[byte](SurbSupplyAckBitmapBytes - 1))
    check:
      incomplete.encode().isErr
      wrongBitmap.encode().isErr

  test "each SURB uses the canonical Mix serialization boundary":
    let
      original = @[testSurb(1), testSurb(2)]
      firstEncoded = original[0].serializeSurb()
      secondEncoded = original[1].serializeSurb()
      firstDecoded =
        firstEncoded.deserializeSurb().expect("could not decode first SURB")
      secondDecoded =
        secondEncoded.deserializeSurb().expect("could not decode second SURB")

    check:
      firstDecoded.serializeSurb() == firstEncoded
      secondDecoded.serializeSurb() == secondEncoded
      @[0'u8].deserializeSurb().isErr

  test "frame fields must agree with their declared kind - e.g. ConnectAck should not include payload":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.ConnectAck,
      payload: Opt.some(@[1'u8]),
    )

    check frame.encode().isErr

  test "open stream does not require attached SURBs":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.OpenStream,
      streamId: Opt.some(StreamId(1)),
      codec: Opt.some("/example/1.0.0"),
    )

    check frame.encode().isOk

  test "stream rejection identifies the stream it refuses":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.StreamReject,
      streamId: Opt.some(StreamId(3)),
      rejectionReason: Opt.some("requested protocol is not supported"),
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.kind == FrameKind.StreamReject
      decoded.streamId == frame.streamId
      decoded.rejectionReason == frame.rejectionReason

    # An older or malformed peer may omit the diagnostic reason. The receiver
    # still recognizes the rejection and supplies its own fallback message.
    check MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.StreamReject,
      streamId: Opt.some(StreamId(5)),
    ).encode().isOk

  test "unsupported versions and oversized frames are rejected":
    let unsupported = MixTransportFrame(
      version: MixTransportVersion + 1,
      sessionId: randomSessionId(),
      kind: FrameKind.ConnectAck,
    )
    let oversized = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.Data,
      streamId: Opt.some(StreamId(1)),
      sequence: Opt.some(SequenceNumber(1)),
      payload: Opt.some(newSeq[byte](MaxTransportFrameBytes)),
    )

    check:
      unsupported.encode().isErr
      oversized.encode().isErr

  test "decoder rejects malformed and incomplete Protobuf":
    check:
      MixTransportFrame.decode(@[0xff'u8]).isErr
      MixTransportFrame.decode(@[]).isErr
