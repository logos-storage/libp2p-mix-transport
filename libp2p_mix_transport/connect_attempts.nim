# SPDX-License-Identifier: MIT

{.push raises: [].}

import chronos, chronos/asyncsync, results, tables
import libp2p/utils/opt

type
  ConnectOperation*[K, T] = proc(
    key: K
  ): Future[Result[T, string]].Raising([CancelledError]) {.gcsafe, raises: [].}

  ExistingConnectionLookup*[K, T] = proc(key: K): Opt[T] {.gcsafe, raises: [].}

  ConnectAttempt[T] = ref object
    outcome: Future[Result[T, string]].Raising([])
    task: Future[void].Raising([CancelledError])
    waiterCount: int
    cancelling: bool
    cancellationReason: string
    retryAfterCancellation: bool

  ConnectAttemptCoordinator*[K, T] = ref object
    lock: AsyncLock
    attempts: Table[K, ConnectAttempt[T]]

proc newConnectAttemptCoordinator*[K, T](): ConnectAttemptCoordinator[K, T] =
  ConnectAttemptCoordinator[K, T](lock: newAsyncLock())

proc activeAttemptCount*[K, T](coordinator: ConnectAttemptCoordinator[K, T]): int =
  coordinator.attempts.len

proc releaseLock[K, T](coordinator: ConnectAttemptCoordinator[K, T]) =
  try:
    coordinator.lock.release()
  except AsyncLockError:
    doAssert false, "connect-attempt lock released twice"

proc finishAttempt[K, T](
    coordinator: ConnectAttemptCoordinator[K, T],
    key: K,
    attempt: ConnectAttempt[T],
    outcome: Result[T, string],
) {.async: (raises: []).} =
  await noCancel coordinator.lock.acquire()
  try:
    coordinator.attempts.withValue(key, current):
      if current[] == attempt:
        coordinator.attempts.del(key)
    if not attempt.outcome.finished:
      attempt.outcome.complete(outcome)
  finally:
    coordinator.releaseLock()

proc runAttempt[K, T](
    coordinator: ConnectAttemptCoordinator[K, T],
    key: K,
    attempt: ConnectAttempt[T],
    operation: ConnectOperation[K, T],
): Future[void] {.async: (raises: [CancelledError]).} =
  var
    outcome = err(Result[T, string], "connection attempt was cancelled")
    cancellation: ref CancelledError

  try:
    outcome = await operation(key)
  except CancelledError as exc:
    cancellation = exc
    if attempt.cancellationReason.len > 0:
      outcome = err(Result[T, string], attempt.cancellationReason)
  except CatchableError as exc:
    outcome = err(Result[T, string], exc.msg)

  await noCancel coordinator.finishAttempt(key, attempt, outcome)
  if not cancellation.isNil:
    raise cancellation

proc releaseWaiter[K, T](
    coordinator: ConnectAttemptCoordinator[K, T], key: K, attempt: ConnectAttempt[T]
) {.async: (raises: []).} =
  await noCancel coordinator.lock.acquire()
  try:
    doAssert attempt.waiterCount > 0
    dec attempt.waiterCount
    if attempt.waiterCount == 0 and not attempt.outcome.finished and
        not attempt.cancelling:
      attempt.cancelling = true
      attempt.cancellationReason = "connection attempt has no remaining callers"
      attempt.retryAfterCancellation = true
      attempt.task.cancelSoon()
  finally:
    coordinator.releaseLock()

proc connect*[K, T](
    coordinator: ConnectAttemptCoordinator[K, T],
    key: K,
    operation: ConnectOperation[K, T],
    getExisting: ExistingConnectionLookup[K, T],
): Future[Result[tuple[connection: T, existing: bool], string]] {.
    async: (raises: [CancelledError])
.} =
  while true:
    await coordinator.lock.acquire()

    getExisting(key).withValue(existing):
      coordinator.releaseLock()
      return ok((existing, true))

    var
      attempt: ConnectAttempt[T]
      existingAttempt = false
    try:
      attempt = coordinator.attempts[key]
      existingAttempt = true
      if not attempt.cancelling:
        inc attempt.waiterCount
    except KeyError:
      attempt = ConnectAttempt[T](
        outcome: Future[Result[T, string]].Raising([]).init(
            "connect-attempt.outcome", {FutureFlag.OwnCancelSchedule}
          ),
        waiterCount: 1,
      )
      coordinator.attempts[key] = attempt
      attempt.task = coordinator.runAttempt(key, attempt, operation)
    finally:
      coordinator.releaseLock()

    if existingAttempt and attempt.cancelling:
      # Do not overlap a new attempt with a worker that is still unwinding.
      await attempt.outcome.join()
      if attempt.retryAfterCancellation:
        continue
      return err(attempt.cancellationReason)

    var outcome: Result[T, string]
    try:
      await attempt.outcome.join()
      try:
        outcome = attempt.outcome.read()
      except FuturePendingError:
        doAssert false, "joined connection outcome is still pending"
    finally:
      await noCancel coordinator.releaseWaiter(key, attempt)

    let connection = outcome.valueOr:
      return err(error)
    return ok((connection, existingAttempt))

proc cancelAll*[K, T](
    coordinator: ConnectAttemptCoordinator[K, T], reason: string
) {.async: (raises: []).} =
  await noCancel coordinator.lock.acquire()
  var attempts: seq[ConnectAttempt[T]]
  try:
    attempts = newSeqOfCap[ConnectAttempt[T]](coordinator.attempts.len)
    for attempt in coordinator.attempts.values:
      attempt.cancellationReason = reason
      attempt.retryAfterCancellation = false
      if not attempt.cancelling:
        attempt.cancelling = true
        attempt.task.cancelSoon()
      attempts.add(attempt)
  finally:
    coordinator.releaseLock()

  for attempt in attempts:
    await noCancel attempt.task.cancelAndWait()

{.pop.}
