# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos, results
import libp2p_mix

const MixTransportCodec* = "/libp2p/mix-transport/1.0.0"

type MixTransport* = ref object
  mix: MixProtocol
  started: bool

proc handleDelivery(
    self: MixTransport, delivery: MixDelivery
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Transport-frame dispatch will be added with the wire envelope.
  discard self
  discard delivery

proc handleRawSurbReply(
    self: MixTransport, reply: RawSurbReply
): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
  ## Until the transport owns credentials, every reply belongs to the embedded path.
  discard self
  discard reply
  return RawSurbReplyDisposition.Unhandled

proc newMixTransport*(mix: MixProtocol): MixTransport =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  MixTransport(mix: mix)

proc start*(self: MixTransport): Future[Result[void, string]] {.async: (raises: []).} =
  if self.started:
    return ok()

  let deliveryHandler: MixDeliveryHandler = proc(
      delivery: MixDelivery
  ): Future[void] {.async: (raises: [CancelledError]).} =
    await self.handleDelivery(delivery)

  self.mix.registerMixDeliveryHandler(MixTransportCodec, deliveryHandler).isOkOr:
    return err(error)

  let rawSurbReplyHandler: RawSurbReplyHandler = proc(
      reply: RawSurbReply
  ): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
    return await self.handleRawSurbReply(reply)

  self.mix.registerRawSurbReplyHandler(rawSurbReplyHandler).isOkOr:
    self.mix.unregisterMixDeliveryHandler(MixTransportCodec)
    return err(error)

  self.started = true
  ok()

proc stop*(self: MixTransport): Future[void] {.async: (raises: []).} =
  if not self.started:
    return

  self.mix.unregisterRawSurbReplyHandler()
  self.mix.unregisterMixDeliveryHandler(MixTransportCodec)
  self.started = false

{.pop.}
