# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[crypto/crypto, peerid]
import libp2p_mix/[curve25519, delay, serialization, sphinx]

import libp2p_mix_transport/reply_credentials

proc randomSessionId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate session identifier")

proc createCredential(rng: Rng): ReplyCredential =
  let (_, publicKey) = generateKeyPair().expect("could not generate Mix key pair")
  var identifier: SURBIdentifier
  rng.generate(identifier)

  createSURB(
    @[publicKey], @[NoDelay], @[Hop.init(newSeq[byte](AddrSize))], identifier, rng
  )
    .expect("could not create test SURB").credential

suite "MixTransport reply credentials":
  test "the first successful reply consumes the complete redundancy group":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      first = createCredential(rng)
      second = createCredential(rng)
      group = store.addGroup(randomSessionId(rng), [first, second]).expect(
          "could not add credential group"
        )

    check:
      store.len == 2
      store.get(first.identifier).isSome
      store.get(second.identifier).isSome

    store.consume(group)

    check:
      store.len == 0
      store.get(first.identifier).isNone
      store.get(second.identifier).isNone

    # A late redundant reply may try to consume the group again.
    store.consume(group)
    check store.len == 0

  test "capacity rejects a new group without evicting in-flight credentials":
    let
      rng = newRng()
      store = ReplyCredentialStore.new(maxCredentials = 2)
      sessionId = randomSessionId(rng)
      first = createCredential(rng)
      second = createCredential(rng)
      rejected = createCredential(rng)

    check store.addGroup(sessionId, [first, second]).isOk
    check store.addGroup(sessionId, [rejected]).isErr
    check:
      store.len == 2
      store.get(first.identifier).isSome
      store.get(second.identifier).isSome
      store.get(rejected.identifier).isNone

  test "expired credentials are invisible before they are purged":
    let
      rng = newRng()
      store = ReplyCredentialStore.new(ttl = 1.seconds)
      credential = createCredential(rng)
      createdAt = Moment.now()

    discard store.addGroup(randomSessionId(rng), [credential], createdAt).expect(
        "could not add credential group"
      )

    check:
      store.get(credential.identifier, createdAt).isSome
      store.get(credential.identifier, createdAt + 1.seconds).isNone
      store.len == 1
      store.purgeExpired(createdAt + 1.seconds) == 1
      store.len == 0
