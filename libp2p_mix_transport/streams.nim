# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos
import libp2p/peerid

type
  StreamDirection* {.pure.} = enum
    Outbound
    Inbound

  StreamState* {.pure.} = enum
    Pending
    Established
    Rejected

  TransportStream* = ref object
    sessionId: PeerId
    streamId: uint64
    codec: string
    direction: StreamDirection
    state: StreamState
    rejectionReason: string
    resolved: AsyncEvent

func sessionId*(stream: TransportStream): PeerId =
  stream.sessionId

func streamId*(stream: TransportStream): uint64 =
  stream.streamId

func codec*(stream: TransportStream): string =
  stream.codec

func direction*(stream: TransportStream): StreamDirection =
  stream.direction

func state*(stream: TransportStream): StreamState =
  stream.state

func rejectionReason*(stream: TransportStream): string =
  stream.rejectionReason

proc establish*(stream: TransportStream) =
  stream.state = StreamState.Established
  stream.resolved.fire()

proc reject*(stream: TransportStream, reason: string) =
  stream.state = StreamState.Rejected
  stream.rejectionReason = reason
  stream.resolved.fire()

proc waitUntilResolved*(stream: TransportStream): Future[void] =
  stream.resolved.wait()

proc newTransportStream*(
    sessionId: PeerId, streamId: uint64, codec: string, direction: StreamDirection
): TransportStream =
  doAssert sessionId.len > 0, "stream sessionId must not be empty"
  doAssert streamId > 0, "streamId must not be zero"
  doAssert codec.len > 0, "stream codec must not be empty"
  TransportStream(
    sessionId: sessionId,
    streamId: streamId,
    codec: codec,
    direction: direction,
    state: StreamState.Pending,
    resolved: newAsyncEvent(),
  )

{.pop.}
