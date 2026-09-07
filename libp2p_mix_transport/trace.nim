import chronicles
import nimcrypto/[sha2, utils]
import protobuf_serialization
import results

import libp2p
import libp2p_mix
import libp2p_mix/serialization

import ./wire

logScope:
  topics = "mix-transport-messages"

type MsgDir = enum
  Inbound = "in",
  Outbound = "out"

template traceMsg(dir: MsgDir, frame: MixTransportFrame, surbKey: string = "") =
  let payloadDigest = if frame.payload.isSome:
    let payload = frame.payload.get()
    toHex(sha256.digest(payload).data)
  else:
    ""

  template uint32def(value: Opt[uint32]): string =
    if value.isSome:
      $value.get()
    else:
      ""

  trace "msgtrace",
    dir = $dir,
    sessionId = frame.sessionId.shortLog,
    kind = frame.kind,
    payload = payloadDigest,
    streamId = frame.streamId.uint32def,
    sequence = frame.sequence.uint32def,
    receiveBase = frame.receiveBase.uint32def,
    firstSurbSequence = frame.firstSurbSequence.uint32def,
    surbSupplyLimit = frame.surbSupplyLimit.uint32def,
    rejectionReason = frame.rejectionReason.valueOr(""),
    surb = surbKey

template traceMsg(dir: MsgDir, reply: RawSurbReply) =
  # TODO figure out how to extract the key from this
  #   and trace it as we do with outbound SURB messages.
  trace "msgtrace",
    dir = $dir,
    kind = "RawSurbReply"

template traceOutbound*(frame: untyped) =
  traceMsg(Outbound, frame)

template traceInbound*(frame: untyped) =
  traceMsg(Inbound, frame)

template traceOutbound*(surb: SURB, encoded: seq[byte]) =
  let frame = MixTransportFrame.decode(encoded).valueOr:
    error "failed to decode"
    return

  let hex = toHex(surb.key)
  traceMsg(Outbound, frame, hex)

