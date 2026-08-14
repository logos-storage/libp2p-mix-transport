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

type
  ReplyCredentialGroup* = ref object
    sessionId: PeerId
    identifiers: HashSet[SURBIdentifier]
    expiresAt: Moment

  StoredReplyCredential* = object
    credential*: ReplyCredential
    group*: ReplyCredentialGroup

  RecoveredReply* = object
    sessionId*: PeerId
    payload*: seq[byte]

  ReplyCredentialStore* = ref object
    credentials: Table[SURBIdentifier, StoredReplyCredential]
    ttl: Duration
    maxCredentials: int

func len*(store: ReplyCredentialStore): int =
  store.credentials.len

func sessionId*(group: ReplyCredentialGroup): PeerId =
  group.sessionId

func expiresAt*(group: ReplyCredentialGroup): Moment =
  group.expiresAt

proc purgeExpired*(store: ReplyCredentialStore, now: Moment = Moment.now()): int =
  var expired: seq[SURBIdentifier]
  for identifier, stored in store.credentials:
    if stored.group.expiresAt <= now:
      expired.add(identifier)

  for identifier in expired:
    store.credentials.del(identifier)
  expired.len

proc addGroup*(
    store: ReplyCredentialStore,
    sessionId: PeerId,
    credentials: openArray[ReplyCredential],
    now: Moment = Moment.now(),
): Result[ReplyCredentialGroup, string] =
  if credentials.len == 0:
    return err("reply credential groups must not be empty")

  discard store.purgeExpired(now)
  if credentials.len > store.maxCredentials - store.credentials.len:
    return err("reply credential store is at capacity")

  var identifiers = initHashSet[SURBIdentifier]()
  for credential in credentials:
    let identifier = credential.identifier
    if identifier == default(SURBIdentifier):
      return err("reply credential identifier must not be empty")
    if identifier in identifiers or store.credentials.hasKey(identifier):
      return err("reply credential identifier is already registered")
    identifiers.incl(identifier)

  let group = ReplyCredentialGroup(
    sessionId: sessionId, identifiers: move(identifiers), expiresAt: now + store.ttl
  )
  for credential in credentials:
    store.credentials[credential.identifier] =
      StoredReplyCredential(credential: credential, group: group)
  ok(group)

proc get*(
    store: ReplyCredentialStore, identifier: SURBIdentifier, now: Moment = Moment.now()
): Opt[StoredReplyCredential] =
  store.credentials.withValue(identifier, stored):
    if stored.group.expiresAt > now:
      return Opt.some(stored[])
  Opt.none(StoredReplyCredential)

proc consume*(store: ReplyCredentialStore, group: ReplyCredentialGroup) =
  if group.isNil:
    return
  for identifier in group.identifiers:
    store.credentials.del(identifier)
  group.identifiers.clear()

proc recoverReply*(
    store: ReplyCredentialStore, reply: RawSurbReply
): Result[Opt[RecoveredReply], ReplyRecoveryError] =
  let stored = store.get(reply.identifier).valueOr:
    return ok(Opt.none(RecoveredReply))

  let payload = recoverReply(stored.credential, reply).valueOr:
    if error.kind == ReplyRecoveryErrorKind.PayloadDecodingFailed:
      store.consume(stored.group)
    return err(error)

  store.consume(stored.group)
  ok(Opt.some(RecoveredReply(sessionId: stored.group.sessionId, payload: payload)))

proc clear*(store: ReplyCredentialStore) =
  store.credentials.clear()

proc new*(
    T: type ReplyCredentialStore,
    ttl = DefaultReplyCredentialTtl,
    maxCredentials = DefaultMaxReplyCredentials,
): T =
  doAssert ttl > ZeroDuration, "reply credential TTL must be positive"
  doAssert maxCredentials > 0, "maximum reply credentials must be positive"
  T(
    credentials: initTable[SURBIdentifier, StoredReplyCredential](),
    ttl: ttl,
    maxCredentials: maxCredentials,
  )

{.pop.}
