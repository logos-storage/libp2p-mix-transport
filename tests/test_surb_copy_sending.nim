# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]
import chronos, results, libp2p_mix
import libp2p/[crypto/crypto, peerid]
import libp2p_mix_transport/[sessions, wire]
import libp2p_mix_transport/transport {.all.}

privateAccess(MixTransport)

proc checkCopies(kind: FrameKind, cancel = false) {.async.} =
  let gates = [newAsyncEvent(), newAsyncEvent(), newAsyncEvent(), newAsyncEvent()]
  let nextBatchStarted = newAsyncEvent()
  let secondCopyStarted = newAsyncEvent()
  var started = 0
  var active = 0
  let transport = MixTransport()
  transport.surbSender = proc(
      surb: sink SURB, payload: sink seq[byte]
  ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
    let index = started
    inc started
    inc active
    if started == 2:
      secondCopyStarted.fire()
    if started == 4:
      nextBatchStarted.fire()
    try:
      await gates[index].wait()
      return ok()
    finally:
      dec active

  let session = newSessionStore()
    .addRecipientSession(PeerId.random(newRng()).expect("could not create session ID"))
    .expect("could not create recipient session")
  session.establish()
  session.initializeSurbSupply().expect("could not initialize SURB supply")
  session.addReceivedSurbs(newSeq[SURB](4)).expect("could not add reply SURBs")
  var frame = MixTransportFrame(
    version: MixTransportVersion,
    sessionId: session.sessionId,
    kind: kind,
    streamId: Opt.some(StreamId(1)),
  )
  if kind == FrameKind.Data:
    frame.sequence = Opt.some(SequenceNumber(1))
    frame.payload = Opt.some(@[1.byte])
  else:
    frame.receiveBase = Opt.some(SequenceNumber(0))
    frame.acknowledgementBitmap = Opt.some(newSeq[byte](AckBitmapBytes))

  # Exercise frame dispatch, so the normal build must select concurrency itself.
  let sending = transport.sendStreamFrame(session, frame)
  defer:
    await sending.cancelAndWait()
  let expectedStarted = if kind == FrameKind.Data: 2 else: 1
  doAssert started == expectedStarted
  doAssert active == expectedStarted
  doAssert not sending.finished
  if cancel:
    await sending.cancelAndWait()
    doAssert sending.cancelled
  elif kind == FrameKind.Data:
    frame.sequence = Opt.some(SequenceNumber(2))
    let nextSending = transport.sendStreamFrame(session, frame)
    defer:
      await nextSending.cancelAndWait()
    doAssert started == 2
    gates[0].fire()
    await sleepAsync(1.milliseconds)
    doAssert active == 1
    doAssert not sending.finished
    doAssert not nextSending.finished
    doAssert started == 2
    gates[1].fire()
    doAssert (await sending.wait(1.seconds)).isOk
    await nextBatchStarted.wait().wait(1.seconds)
    doAssert started == 4
    doAssert active == 2
    gates[2].fire()
    gates[3].fire()
    doAssert (await nextSending.wait(1.seconds)).isOk
  else:
    gates[0].fire()
    await secondCopyStarted.wait().wait(1.seconds)
    doAssert started == 2
    doAssert active == 1
    doAssert not sending.finished
    gates[1].fire()
    doAssert (await sending.wait(1.seconds)).isOk
  doAssert active == 0
  await session.acquireReplySend().wait(1.seconds)
  session.releaseReplySend()

suite "Redundant SURB sends":
  test "Data copies overlap by default and the next frame waits for both":
    waitFor checkCopies(FrameKind.Data)
  test "cancellation drains both Data copies and releases the session lock":
    waitFor checkCopies(FrameKind.Data, cancel = true)
  test "control copies remain sequential":
    waitFor checkCopies(FrameKind.Ack)
