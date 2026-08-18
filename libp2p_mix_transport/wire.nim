# SPDX-License-Identifier: MIT

{.push raises: [].}

import results
import libp2p/peerid
import libp2p_mix
import libp2p_mix/serialization
import protobuf_serialization
import protobuf_serialization/pkg/results
import protobuf_serialization/std/enums

const
  MixTransportCodec* = "/libp2p/mix-transport/1.0.0"
  MixTransportVersion* = 1'u32
  MaxCodecBytes* = 255
  MaxStreamRejectionReasonBytes* = 255
  MaxAckRanges* = 32

let MaxTransportFrameBytes* = getMaxMessageSizeForCodec(MixTransportCodec, 0).expect(
    "MixTransportCodec framing leaves no room for a transport frame"
  )

type
  FrameKind* {.pure.} = enum
    Connect = 1
    ConnectAck = 2
    OpenStream = 3
    StreamAck = 4
    Data = 5
    Ack = 6
    RefillRequest = 7
    Refill = 8
    CloseStream = 9
    ResetStream = 10
    Disconnect = 11
    ResetSession = 12
    StreamReject = 13

  AckRange* {.proto2.} = object
    first* {.fieldNumber: 1, required, pint.}: uint64
    count* {.fieldNumber: 2, required, pint.}: uint32

  SurbGroup* {.proto2.} = object ## Each entry contains one opaque, serialized SURB.
    surbs* {.fieldNumber: 1.}: seq[seq[byte]]

  MixTransportFrame* {.proto2.} = object
    version* {.fieldNumber: 1, required, pint.}: uint32
    sessionId* {.fieldNumber: 2, required, ext.}: PeerId
    kind* {.fieldNumber: 3, required, ext.}: FrameKind
    streamId* {.fieldNumber: 4, pint.}: Opt[uint64]
    sequence* {.fieldNumber: 5, pint.}: Opt[uint64]
    payload* {.fieldNumber: 6.}: Opt[seq[byte]]
    codec* {.fieldNumber: 7.}: Opt[string]
    ackThrough* {.fieldNumber: 8, pint.}: Opt[uint64]
    ackRanges* {.fieldNumber: 9.}: seq[AckRange]
    batchId* {.fieldNumber: 10, pint.}: Opt[uint64]
    requestedGroups* {.fieldNumber: 11, pint.}: Opt[uint32]
    partIndex* {.fieldNumber: 12, pint.}: Opt[uint32]
    partCount* {.fieldNumber: 13, pint.}: Opt[uint32]
    surbGroups* {.fieldNumber: 14.}: seq[SurbGroup]
    rejectionReason* {.fieldNumber: 15.}: Opt[string]

template require(condition: bool, message: string): untyped =
  if not condition:
    return err(message)

proc init*(T: type SurbGroup, surbs: openArray[SURB]): Result[T, string] =
  require surbs.len > 0, "SURB groups must not be empty"

  var encoded = newSeqOfCap[seq[byte]](surbs.len)
  for surb in surbs:
    encoded.add(surb.serializeSurb())
  ok(T(surbs: encoded))

proc decodeSurbs*(group: SurbGroup): Result[seq[SURB], string] =
  require group.surbs.len > 0, "SURB groups must not be empty"

  var decoded = newSeqOfCap[SURB](group.surbs.len)
  for encoded in group.surbs:
    let surb = encoded.deserializeSurb().valueOr:
      return err("could not deserialize SURB: " & error)
    decoded.add(surb)
  ok(decoded)

proc validateSurbGroups(groups: openArray[SurbGroup]): Result[void, string] =
  var surbCount = 0
  for group in groups:
    require group.surbs.len > 0, "SURB groups must not be empty"
    surbCount += group.surbs.len
    require surbCount <= MaxTransportFrameBytes div SurbSize, "too many SURBs"
    for surb in group.surbs:
      require surb.len == SurbSize, "invalid serialized SURB size"
  ok()

proc validate*(frame: MixTransportFrame): Result[void, string] =
  require frame.version == MixTransportVersion, "unsupported transport frame version"
  require frame.sessionId.len > 0, "sessionId must not be empty"
  require frame.codec.isNone or frame.codec.get().len <= MaxCodecBytes,
    "application codec is too long"
  require frame.rejectionReason.isNone or
    frame.rejectionReason.get().len <= MaxStreamRejectionReasonBytes,
    "stream rejection reason is too long"
  require frame.ackRanges.len <= MaxAckRanges, "too many acknowledgement ranges"

  for ackRange in frame.ackRanges:
    require ackRange.count > 0, "acknowledgement range must not be empty"

  frame.surbGroups.validateSurbGroups().isOkOr:
    return err(error)

  let
    isStreamFrame =
      frame.kind in {
        FrameKind.OpenStream, FrameKind.StreamAck, FrameKind.Data, FrameKind.Ack,
        FrameKind.CloseStream, FrameKind.ResetStream, FrameKind.StreamReject,
      }
    carriesSurbs =
      frame.kind in {FrameKind.Connect, FrameKind.OpenStream, FrameKind.Refill}

  require frame.streamId.isSome == isStreamFrame,
    "streamId does not match the frame kind"
  require frame.sequence.isSome == (frame.kind == FrameKind.Data),
    "sequence does not match the frame kind"
  require frame.payload.isSome == (frame.kind == FrameKind.Data),
    "payload does not match the frame kind"
  require frame.codec.isSome == (frame.kind == FrameKind.OpenStream),
    "codec does not match the frame kind"
  require frame.ackThrough.isSome == (frame.kind == FrameKind.Ack),
    "ackThrough does not match the frame kind"
  require frame.ackRanges.len == 0 or frame.kind == FrameKind.Ack,
    "acknowledgement ranges do not match the frame kind"
  require frame.batchId.isSome ==
    (frame.kind in {FrameKind.RefillRequest, FrameKind.Refill}),
    "batchId does not match the frame kind"
  require frame.requestedGroups.isSome == (frame.kind == FrameKind.RefillRequest),
    "requestedGroups does not match the frame kind"
  require frame.partIndex.isSome == (frame.kind == FrameKind.Refill),
    "partIndex does not match the frame kind"
  require frame.partCount.isSome == (frame.kind == FrameKind.Refill),
    "partCount does not match the frame kind"
  require frame.surbGroups.len == 0 or carriesSurbs,
    "SURB groups do not match the frame kind"
  require frame.rejectionReason.isNone or frame.kind == FrameKind.StreamReject,
    "rejectionReason does not match the frame kind"

  case frame.kind
  of FrameKind.Connect:
    require frame.surbGroups.len > 0, "connect must provide at least one SURB group"
  of FrameKind.OpenStream:
    require frame.codec.get().len > 0, "application codec must not be empty"
  of FrameKind.Data:
    require frame.payload.get().len > 0, "data payload must not be empty"
  of FrameKind.RefillRequest:
    require frame.requestedGroups.get() > 0, "refill must request at least one group"
  of FrameKind.Refill:
    require frame.surbGroups.len > 0, "refill must provide at least one SURB group"
    require frame.partCount.get() > 0, "refill partCount must not be zero"
    require frame.partIndex.get() < frame.partCount.get(),
      "refill partIndex is outside partCount"
  else:
    discard

  ok()

proc encode*(frame: MixTransportFrame): Result[seq[byte], string] =
  frame.validate().isOkOr:
    return err(error)

  let encoded = Protobuf.encode(frame)
  require encoded.len <= MaxTransportFrameBytes, "transport frame is too large"
  ok(encoded)

proc decodeFrame(data: seq[byte]): MixTransportFrame {.raises: [SerializationError].} =
  Protobuf.decode(data, MixTransportFrame)

proc decode*(
    _: type MixTransportFrame, data: seq[byte]
): Result[MixTransportFrame, string] =
  require data.len <= MaxTransportFrameBytes, "transport frame is too large"

  let frame =
    try:
      decodeFrame(data)
    except SerializationError as exc:
      return err("could not decode transport frame: " & exc.msg)
  frame.validate().isOkOr:
    return err(error)
  ok(frame)

{.pop.}
