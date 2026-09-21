# SPDX-License-Identifier: MIT

{.used.}

import std/[importutils, unittest]
import chronos, results, libp2p_mix
import libp2p_mix_transport/transport {.all.}

privateAccess(MixTransport)

proc checkConcurrentCopies(cancel: bool) {.async: (raises: [CancelledError]).} =
  let gates = [newAsyncEvent(), newAsyncEvent()]
  var started = 0
  var active = 0
  let transport = MixTransport()
  transport.surbSender = proc(
      surb: sink SURB, payload: sink seq[byte]
  ): Future[Result[void, string]] {.async: (raises: [CancelledError]).} =
    let index = started
    inc started
    inc active
    try:
      await gates[index].wait()
      return ok()
    finally:
      dec active

  let sending = transport.sendWithSurbRedundancyBatch(
    newSeq[SURB](2), @[], concurrentCopies = true
  )
  doAssert started == 2
  doAssert active == 2
  doAssert not sending.finished
  if cancel:
    await sending.cancelAndWait()
    doAssert sending.cancelled
  else:
    gates[0].fire()
    # Completing one copy must not complete the whole batch.
    await sleepAsync(1.milliseconds)
    doAssert active == 1
    doAssert not sending.finished
    gates[1].fire()
    doAssert (await sending).isOk
  doAssert active == 0

suite "Experimental concurrent SURB copies":
  test "both copies start before either completes and the caller waits for both":
    waitFor checkConcurrentCopies(false)
  test "cancellation drains both owned sends":
    waitFor checkConcurrentCopies(true)
