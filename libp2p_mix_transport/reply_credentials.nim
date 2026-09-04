# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/[sets, tables]

import chronos, results
import libp2p/peerid
import libp2p/utils/opt
import libp2p_mix

const
  DefaultReplyCredentialTtl* = 30.minutes
  DefaultMaxReplyCredentials* = 100_000
  DefaultMaxRetiredReplyIdentifiers* = 100_000

type
  StoredReplyCredential* = object
    credential*: ReplyCredential
    sessionId*: PeerId
    expiresAt*: Moment

  RecoveredReply* = object
    sessionId*: PeerId
    payload*: seq[byte]

  ReplyCredentialStore* = ref object
    credentials: Table[SURBIdentifier, StoredReplyCredential]
    retiredIdentifiers: Table[SURBIdentifier, Moment]
    ttl: Duration
    maxCredentials: int
    maxRetiredIdentifiers: int

func len*(store: ReplyCredentialStore): int =
  store.credentials.len

func retiredLen*(store: ReplyCredentialStore): int =
  store.retiredIdentifiers.len

proc purgeExpired*(store: ReplyCredentialStore, now: Moment = Moment.now()): int =
  var
    expiredCredentials: seq[SURBIdentifier]
    expiredRetiredIdentifiers: seq[SURBIdentifier]
  for identifier, stored in store.credentials:
    if stored.expiresAt <= now:
      expiredCredentials.add(identifier)
  for identifier, expiresAt in store.retiredIdentifiers:
    if expiresAt <= now:
      expiredRetiredIdentifiers.add(identifier)

  for identifier in expiredCredentials:
    store.credentials.del(identifier)
  for identifier in expiredRetiredIdentifiers:
    store.retiredIdentifiers.del(identifier)
  expiredCredentials.len + expiredRetiredIdentifiers.len

proc rememberRetiredIdentifier(
    store: ReplyCredentialStore,
    identifier: SURBIdentifier,
    expiresAt: Moment,
    now: Moment,
) =
  if expiresAt <= now:
    return

  if not store.retiredIdentifiers.hasKey(identifier) and
      store.retiredIdentifiers.len >= store.maxRetiredIdentifiers:
    var
      found = false
      evictedIdentifier: SURBIdentifier
      evictedExpiry: Moment
    for candidate, candidateExpiry in store.retiredIdentifiers:
      if not found or candidateExpiry < evictedExpiry:
        found = true
        evictedIdentifier = candidate
        evictedExpiry = candidateExpiry
    doAssert found, "a full retired identifier store must contain an entry"
    if expiresAt <= evictedExpiry:
      return
    store.retiredIdentifiers.del(evictedIdentifier)

  store.retiredIdentifiers[identifier] = expiresAt

func isRetiredIdentifier*(
    store: ReplyCredentialStore, identifier: SURBIdentifier, now: Moment = Moment.now()
): bool =
  if not store.retiredIdentifiers.hasKey(identifier):
    return false
  store.retiredIdentifiers.getOrDefault(identifier) > now

proc add*(
    store: ReplyCredentialStore,
    sessionId: PeerId,
    credentials: openArray[ReplyCredential],
    now: Moment = Moment.now(),
): Result[void, string] =
  if credentials.len == 0:
    return err("reply credentials must not be empty")

  discard store.purgeExpired(now)
  if credentials.len > store.maxCredentials - store.credentials.len:
    return err("reply credential store is at capacity")

  var identifiers = initHashSet[SURBIdentifier]()
  for credential in credentials:
    let identifier = credential.identifier
    if identifier == default(SURBIdentifier):
      return err("reply credential identifier must not be empty")
    if identifier in identifiers or store.credentials.hasKey(identifier) or
        store.retiredIdentifiers.hasKey(identifier):
      return err("reply credential identifier is already registered")
    identifiers.incl(identifier)

  for credential in credentials:
    store.credentials[credential.identifier] = StoredReplyCredential(
      credential: credential, sessionId: sessionId, expiresAt: now + store.ttl
    )
  ok()

proc get*(
    store: ReplyCredentialStore, identifier: SURBIdentifier, now: Moment = Moment.now()
): Opt[StoredReplyCredential] =
  store.credentials.withValue(identifier, stored):
    if stored.expiresAt > now:
      return Opt.some(stored[])
  Opt.none(StoredReplyCredential)

proc consume*(
    store: ReplyCredentialStore, identifier: SURBIdentifier, now: Moment = Moment.now()
) =
  discard store.purgeExpired(now)
  if not store.credentials.hasKey(identifier):
    return

  let expiresAt = store.credentials.getOrDefault(identifier).expiresAt
  store.credentials.del(identifier)
  store.rememberRetiredIdentifier(identifier, expiresAt, now)

proc consume*(
    store: ReplyCredentialStore, credential: ReplyCredential, now: Moment = Moment.now()
) =
  store.consume(credential.identifier, now)

proc consume*(
    store: ReplyCredentialStore,
    credentials: openArray[ReplyCredential],
    now: Moment = Moment.now(),
) =
  for credential in credentials:
    store.consume(credential, now)

proc removeSession*(store: ReplyCredentialStore, sessionId: PeerId): int =
  let now = Moment.now()
  discard store.purgeExpired(now)

  var removed: seq[tuple[identifier: SURBIdentifier, expiresAt: Moment]]
  for identifier, stored in store.credentials:
    if stored.sessionId == sessionId:
      removed.add((identifier, stored.expiresAt))

  for (identifier, expiresAt) in removed:
    store.credentials.del(identifier)
    store.rememberRetiredIdentifier(identifier, expiresAt, now)
  removed.len

proc recoverReply*(
    store: ReplyCredentialStore, reply: RawSurbReply
): Result[Opt[RecoveredReply], ReplyRecoveryError] =
  let stored = store.get(reply.identifier).valueOr:
    return ok(Opt.none(RecoveredReply))

  let payload = recoverReply(stored.credential, reply).valueOr:
    if error.kind == ReplyRecoveryErrorKind.PayloadDecodingFailed:
      store.consume(reply.identifier)
    return err(error)

  store.consume(reply.identifier)
  ok(Opt.some(RecoveredReply(sessionId: stored.sessionId, payload: payload)))

proc clear*(store: ReplyCredentialStore) =
  store.credentials.clear()
  store.retiredIdentifiers.clear()

proc new*(
    T: type ReplyCredentialStore,
    ttl = DefaultReplyCredentialTtl,
    maxCredentials = DefaultMaxReplyCredentials,
    maxRetiredIdentifiers = DefaultMaxRetiredReplyIdentifiers,
): T =
  doAssert ttl > ZeroDuration, "reply credential TTL must be positive"
  doAssert maxCredentials > 0, "maximum reply credentials must be positive"
  doAssert maxRetiredIdentifiers > 0,
    "maximum retired reply identifiers must be positive"
  T(
    credentials: initTable[SURBIdentifier, StoredReplyCredential](),
    retiredIdentifiers: initTable[SURBIdentifier, Moment](),
    ttl: ttl,
    maxCredentials: maxCredentials,
    maxRetiredIdentifiers: maxRetiredIdentifiers,
  )

{.pop.}
