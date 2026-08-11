# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[crypto/crypto, peerid]
import
  libp2p_mix/
    [curve25519, delay, fragmentation, mix_message, serialization, sphinx, tag_manager]

import libp2p_mix_transport/reply_credentials

proc randomSessionId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate session identifier")

type TestReply = object
  surb: SURB
  credential: ReplyCredential
  privateKey: FieldElement

proc createReply(rng: Rng): TestReply =
  let (privateKey, publicKey) =
    generateKeyPair().expect("could not generate Mix key pair")
  var identifier: SURBIdentifier
  rng.generate(identifier)

  let created = createSURB(
      @[publicKey], @[NoDelay], @[Hop.init(newSeq[byte](AddrSize))], identifier, rng
    )
    .expect("could not create test SURB")
  TestReply(surb: created.surb, credential: created.credential, privateKey: privateKey)

proc rawReply(testReply: TestReply, message: Message): RawSurbReply =
  var tagManager = TagManager.new(autoStart = false)
  let
    packet = useSURB(testReply.surb, message)
    processed = processSphinxPacket(packet, testReply.privateKey, tagManager).expect(
        "could not process test reply"
      )

  doAssert processed.status == ProcessingStatus.Reply
  RawSurbReply(
    identifier: testReply.credential.identifier, encryptedPayload: processed.delta_prime
  )

proc replyMessage(payload: seq[byte], sessionId: PeerId): Message =
  MixMessage.init(payload, "").serialize().addPadding(sessionId).serialize()

suite "MixTransport reply credentials":
  test "the first successful reply consumes the complete redundancy group":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      first = createReply(rng).credential
      second = createReply(rng).credential
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
      first = createReply(rng).credential
      second = createReply(rng).credential
      rejected = createReply(rng).credential

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
      credential = createReply(rng).credential
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

  test "an unknown reply remains available to the embedded Mix path":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      unknown = createReply(rng)
      sessionId = randomSessionId(rng)
      recovered = store
        .recoverReply(unknown.rawReply(replyMessage(@[1'u8], sessionId)))
        .expect("unknown replies are not recovery errors")

    check:
      recovered.isNone
      store.len == 0

  test "a valid reply is recovered and consumes its redundancy group":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)
      payload = @[1'u8, 2, 3]

    discard store.addGroup(sessionId, [first.credential, second.credential]).expect(
        "could not add credential group"
      )

    let recovered = store
      .recoverReply(first.rawReply(replyMessage(payload, sessionId)))
      .expect("could not recover reply")
      .get()

    check:
      recovered.sessionId == sessionId
      recovered.payload == payload
      store.len == 0

  test "invalid Sphinx recovery keeps the redundancy group available":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)

    discard store.addGroup(sessionId, [first.credential, second.credential]).expect(
        "could not add credential group"
      )

    let invalid = RawSurbReply(
      identifier: first.credential.identifier,
      encryptedPayload: newSeq[byte](PayloadSize),
    )

    check store.recoverReply(invalid).isErr
    check:
      store.get(first.credential.identifier).isSome
      store.get(second.credential.identifier).isSome

  test "an invalid Mix payload consumes the redundancy group":
    let
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)

    discard store.addGroup(sessionId, [first.credential, second.credential]).expect(
        "could not add credential group"
      )

    let invalidMessage =
      MessageChunk.init(uint16(DataSize + 1), newSeq[byte](DataSize), 0).serialize()

    check store.recoverReply(first.rawReply(invalidMessage)).isErr
    check store.len == 0
