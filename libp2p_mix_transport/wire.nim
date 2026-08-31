# SPDX-License-Identifier: MIT

{.push raises: [].}

import results
import libp2p/peerid
import libp2p_mix
import libp2p_mix/serialization
import protobuf_serialization
import protobuf_serialization/pkg/results
import protobuf_serialization/std/enums

type
  StreamId* = uint32
  SequenceNumber* = uint32
  RefillRequestId* = uint64

const
  MixTransportCodec* = "/libp2p/mix-transport/1.0.0"
  MixTransportVersion* = 1'u32
  MaxCodecBytes* = 255
  MaxStreamRejectionReasonBytes* = 255
  ReceiveWindowChunks* = 256
  AckBitmapBytes* = ReceiveWindowChunks div 8
  MaxInflightChunks* = 64
  MaxRefillGroupsPerFrame* = 2
  MaxSessionIdBytes* = 39
  MaxDataSequenceNumber* = SequenceNumber.high - 1

  # Every Data-frame field number fits in a one-byte Protobuf field tag.
  ProtobufFieldTagBytes = 1
  SingleByteVarintBytes = 1
  MaxDataPayloadLengthPrefixBytes = 2

  VersionFieldBytes = ProtobufFieldTagBytes + SingleByteVarintBytes
  SessionIdFieldBytes =
    ProtobufFieldTagBytes + SingleByteVarintBytes + MaxSessionIdBytes
  FrameKindFieldBytes = ProtobufFieldTagBytes + SingleByteVarintBytes
  StreamIdFieldBytes = ProtobufFieldTagBytes + sizeof(StreamId)
  SequenceFieldBytes = ProtobufFieldTagBytes + sizeof(SequenceNumber)
  DataPayloadHeaderBytes = ProtobufFieldTagBytes + MaxDataPayloadLengthPrefixBytes

  MaxDataFrameOverheadBytes =
    VersionFieldBytes + SessionIdFieldBytes + FrameKindFieldBytes + StreamIdFieldBytes +
    SequenceFieldBytes + DataPayloadHeaderBytes

static:
  doAssert ReceiveWindowChunks mod 8 == 0
  doAssert MaxInflightChunks <= ReceiveWindowChunks

let MaxTransportFrameBytes* = getMaxMessageSizeForCodec(MixTransportCodec, 0).expect(
    "MixTransportCodec framing leaves no room for a transport frame"
  )

let MaxDataPayloadBytes* = MaxTransportFrameBytes - MaxDataFrameOverheadBytes

doAssert MaxDataPayloadBytes > 0, "MixTransport Data frame has no payload capacity"

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

  SurbGroup* {.proto2.} = object ## Each entry contains one opaque, serialized SURB.
    surbs* {.fieldNumber: 1.}: seq[seq[byte]]

  MixTransportFrame* {.proto2.} = object
    version* {.fieldNumber: 1, required, pint.}: uint32
    sessionId* {.fieldNumber: 2, required, ext.}: PeerId
    kind* {.fieldNumber: 3, required, ext.}: FrameKind
    streamId* {.fieldNumber: 4, fixed.}: Opt[StreamId]
    sequence* {.fieldNumber: 5, fixed.}: Opt[SequenceNumber]
    payload* {.fieldNumber: 6.}: Opt[seq[byte]]
    codec* {.fieldNumber: 7.}: Opt[string]
    receiveBase* {.fieldNumber: 8, fixed.}: Opt[SequenceNumber]
    acknowledgementBitmap* {.fieldNumber: 9.}: Opt[seq[byte]]
    refillRequestId* {.fieldNumber: 10, pint.}: Opt[RefillRequestId]
    requestedGroups* {.fieldNumber: 11, pint.}: Opt[uint32]
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

proc validateSurbGroups(
    groups: openArray[SurbGroup], requireValidEncoding: bool
): Result[void, string] =
  var surbCount = 0
  for group in groups:
    if requireValidEncoding:
      require group.surbs.len > 0, "SURB groups must not be empty"
    surbCount += group.surbs.len
    require surbCount <= MaxTransportFrameBytes div SurbSize, "too many SURBs"
    if requireValidEncoding:
      for surb in group.surbs:
        require surb.len == SurbSize, "invalid serialized SURB size"
  ok()

proc validateFrame(
    frame: MixTransportFrame, requireValidSurbEncoding: bool
): Result[void, string] =
  require frame.version == MixTransportVersion, "unsupported transport frame version"
  require frame.sessionId.len > 0, "sessionId must not be empty"
  require frame.sessionId.len <= MaxSessionIdBytes, "sessionId is too long"
  require frame.codec.isNone or frame.codec.get().len <= MaxCodecBytes,
    "application codec is too long"
  require frame.rejectionReason.isNone or
    frame.rejectionReason.get().len <= MaxStreamRejectionReasonBytes,
    "stream rejection reason is too long"
  frame.surbGroups.validateSurbGroups(requireValidSurbEncoding).isOkOr:
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
  require frame.streamId.isNone or frame.streamId.get() > 0, "streamId must not be zero"
  require frame.sequence.isSome == (frame.kind == FrameKind.Data),
    "sequence does not match the frame kind"
  require frame.payload.isSome == (frame.kind == FrameKind.Data),
    "payload does not match the frame kind"
  require frame.codec.isSome == (frame.kind == FrameKind.OpenStream),
    "codec does not match the frame kind"
  require frame.receiveBase.isSome == (frame.kind == FrameKind.Ack),
    "receiveBase does not match the frame kind"
  require frame.acknowledgementBitmap.isSome == (frame.kind == FrameKind.Ack),
    "acknowledgement bitmap does not match the frame kind"
  require frame.acknowledgementBitmap.isNone or
    frame.acknowledgementBitmap.get().len == AckBitmapBytes,
    "acknowledgement bitmap has the wrong size"
  require frame.refillRequestId.isSome ==
    (frame.kind in {FrameKind.RefillRequest, FrameKind.Refill}),
    "refillRequestId does not match the frame kind"
  require frame.requestedGroups.isSome == (frame.kind == FrameKind.RefillRequest),
    "requestedGroups does not match the frame kind"
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
    require frame.sequence.get() > 0, "data sequence must not be zero"
    require frame.sequence.get() <= MaxDataSequenceNumber,
      "data sequence space is exhausted"
    require frame.payload.get().len > 0, "data payload must not be empty"
    require frame.payload.get().len <= MaxDataPayloadBytes, "data payload is too large"
  of FrameKind.RefillRequest:
    require frame.requestedGroups.get() > 0, "refill must request at least one group"
    require frame.requestedGroups.get() <= MaxRefillGroupsPerFrame,
      "refill requests too many groups"
  of FrameKind.Refill:
    require frame.surbGroups.len > 0, "refill must provide at least one SURB group"
  else:
    discard

  ok()

proc validate*(frame: MixTransportFrame): Result[void, string] =
  frame.validateFrame(requireValidSurbEncoding = true)

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
  frame.validateFrame(requireValidSurbEncoding = false).isOkOr:
    return err(error)
  ok(frame)

{.pop.}
