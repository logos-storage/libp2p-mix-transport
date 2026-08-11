# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos, results
import libp2p/utils/opt
import libp2p_mix
import ./[reply_credentials, wire]

type MixTransport* = ref object
  mix: MixProtocol
  replyCredentials: ReplyCredentialStore
  started: bool

proc handleFrame(
    self: MixTransport, frame: MixTransportFrame
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Session dispatch will be added with the session store.
  discard self
  discard frame

proc handleDelivery(
    self: MixTransport, delivery: MixDelivery
): Future[void] {.async: (raises: [CancelledError]).} =
  let frame = MixTransportFrame.decode(delivery.payload).valueOr:
    return
  await self.handleFrame(frame)

proc handleRawSurbReply(
    self: MixTransport, reply: RawSurbReply
): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
  let recovered = self.replyCredentials.recoverReply(reply).valueOr:
    return RawSurbReplyDisposition.Handled

  recovered.withValue(value):
    let frame = MixTransportFrame.decode(value.payload).valueOr:
      return RawSurbReplyDisposition.Handled
    if frame.sessionId == value.sessionId:
      await self.handleFrame(frame)
    return RawSurbReplyDisposition.Handled

  RawSurbReplyDisposition.Unhandled

proc newMixTransport*(mix: MixProtocol): MixTransport =
  doAssert not mix.isNil, "MixProtocol must not be nil"
  MixTransport(mix: mix, replyCredentials: ReplyCredentialStore.new())

proc start*(
    self: MixTransport
): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
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

proc stop*(self: MixTransport): Future[void] {.async: (raises: [CancelledError]).} =
  if not self.started:
    return

  self.mix.unregisterRawSurbReplyHandler()
  self.mix.unregisterMixDeliveryHandler(MixTransportCodec)
  self.replyCredentials.clear()
  self.started = false

{.pop.}
