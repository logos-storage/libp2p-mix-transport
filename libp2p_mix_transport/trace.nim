# SPDX-License-Identifier: MIT

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
  Inbound = "in"
  Outbound = "out"

template payloadDigest(frame: MixTransportFrame): string =
  if frame.payload.isSome:
    toHex(sha256.digest(frame.payload.get()).data)
  else:
    ""

template uint32def(value: Opt[uint32]): string =
  if value.isSome:
    $value.get()
  else:
    ""

proc traceMsg(dir: MsgDir, frame: MixTransportFrame, surbKey: string = "") =
  trace "msgtrace",
    direction = $dir,
    sessionId = frame.sessionId.shortLog,
    kind = frame.kind,
    payload = frame.payloadDigest,
    streamId = frame.streamId.uint32def,
    sequence = frame.sequence.uint32def,
    receiveBase = frame.receiveBase.uint32def,
    firstSurbSequence = frame.firstSurbSequence.uint32def,
    surbSupplyLimit = frame.surbSupplyLimit.uint32def,
    rejectionReason = frame.rejectionReason.valueOr(""),
    surb = surbKey

proc traceMsg(dir: MsgDir, reply: RawSurbReply) =
  # TODO figure out how to extract the key from this
  #   and trace it as we do with outbound SURB messages.
  trace "msgtrace", direction = $dir, kind = "RawSurbReply"

template traceOutbound*(frame: MixTransportFrame) =
  traceMsg(Outbound, frame)

template traceOutbound*(surb: SURB, encoded: seq[byte]) =
  let decoded = MixTransportFrame.decode(encoded)
  if decoded.isErr:
    error "failed to decode"
  else:
    traceMsg(Outbound, decoded.get(), toHex(surb.key))

template traceInbound*(frame: MixTransportFrame) =
  traceMsg(Inbound, frame)

template traceInbound*(reply: RawSurbReply) =
  traceMsg(Inbound, reply)
