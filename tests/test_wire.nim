# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import results
import libp2p/[crypto/crypto, peerid]
import libp2p_mix/serialization

import libp2p_mix_transport

proc randomSessionId(): PeerId =
  PeerId.random(newRng()).expect("could not generate session identifier")

suite "MixTransport wire format":
  test "data frame survives a Protobuf round trip":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.Data,
      streamId: Opt.some(7'u64),
      sequence: Opt.some(11'u64),
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

  test "refill frame preserves grouped SURBs":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.Refill,
      batchId: Opt.some(42'u64),
      partIndex: Opt.some(0'u32),
      partCount: Opt.some(1'u32),
      surbGroups: @[SurbGroup(surbs: @[newSeq[byte](SurbSize)])],
    )

    let decoded = MixTransportFrame
      .decode(frame.encode().expect("encode failed"))
      .expect("decode failed")

    check:
      decoded.batchId == frame.batchId
      decoded.partIndex == frame.partIndex
      decoded.partCount == frame.partCount
      decoded.surbGroups == frame.surbGroups

  test "frame fields must agree with their declared kind":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.ConnectAck,
      payload: Opt.some(@[1'u8]),
    )

    check frame.encode().isErr

  test "open stream does not require an attached SURB group":
    let frame = MixTransportFrame(
      version: MixTransportVersion,
      sessionId: randomSessionId(),
      kind: FrameKind.OpenStream,
      streamId: Opt.some(1'u64),
      codec: Opt.some("/example/1.0.0"),
    )

    check frame.encode().isOk

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
      streamId: Opt.some(1'u64),
      sequence: Opt.some(0'u64),
      payload: Opt.some(newSeq[byte](MaxTransportFrameBytes)),
    )

    check:
      unsupported.encode().isErr
      oversized.encode().isErr

  test "decoder rejects malformed and incomplete Protobuf":
    check:
      MixTransportFrame.decode(@[0xff'u8]).isErr
      MixTransportFrame.decode(@[]).isErr
