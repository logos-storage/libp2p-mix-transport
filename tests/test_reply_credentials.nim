# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[crypto/crypto, peerid]
import
  libp2p_mix/
    [curve25519, delay, mix_message, padding, serialization, sphinx, tag_manager]
import stew/endians2

import libp2p_mix_transport/reply_credentials

proc randomSessionId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate session identifier")

type TestReply = object
  surb: SURB
  credential: ReplyCredential
  privateKey: FieldElement

proc createReply(rng: Rng): TestReply =
  # The original sender's Mix key pair. The return path ends at this node.
  let (privateKey, publicKey) =
    generateKeyPair().expect("could not generate Mix key pair")
  var identifier: SURBIdentifier
  rng.generate(identifier)

  # MixProtocol.createSurb normally creates this one-time public SURB and its
  # private credential on the original sender. A one-hop path keeps the test
  # focused: the recipient sends directly back to the original sender.
  let created = createSURB(
      @[publicKey], @[NoDelay], @[Hop.init(newSeq[byte](AddrSize))], identifier, rng
    )
    .expect("could not create test SURB")
  TestReply(surb: created.surb, credential: created.credential, privateKey: privateKey)

proc rawReply(testReply: TestReply, message: Message): RawSurbReply =
  # The recipient normally calls MixProtocol.sendWithSurb, which builds this
  # packet with useSURB and sends it to the first return-path Mix node.
  var tagManager = TagManager.new(autoStart = false)
  let
    packet = useSURB(testReply.surb, message)

    # Every return-path node normally reaches processSphinxPacket through
    # MixProtocol.handleMixMessages. Our only hop is the original sender, so
    # it immediately recognizes Reply and exposes the raw encrypted payload.
    processed = processSphinxPacket(packet, testReply.privateKey, tagManager).expect(
        "could not process test reply"
      )

  doAssert processed.status == ProcessingStatus.Reply

  # This is the RawSurbReply that Mix normally offers to MixTransport's
  # registered raw reply handler on the original sender.
  RawSurbReply(
    identifier: testReply.credential.identifier, encryptedPayload: processed.delta_prime
  )

proc replyMessage(payload: seq[byte]): Message =
  # Reproduce the empty-codec Mix envelope and Sphinx-sized padding normally
  # applied by MixProtocol.sendWithSurb before useSURB is called.
  MixMessage.init(payload, "").serialize().addPadding().expect(
    "could not pad test reply"
  )

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

    # The original sender indexes both credentials while either redundant
    # reply can still become the first valid one to arrive.
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
      recovered = store.recoverReply(unknown.rawReply(replyMessage(@[1'u8]))).expect(
          "unknown replies are not recovery errors"
        )

    # This transport never registered the credential, so its handler will
    # return Unhandled and allow Mix's embedded reply path to inspect it.
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

    # The recipient replies through the first public SURB. The original
    # sender recovers the payload with its matching private credential.
    let recovered = store
      .recoverReply(first.rawReply(replyMessage(payload)))
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

    # Cryptographic recovery failed before decoding payload.
    # Keep both credentials because the second redundant reply may be valid.
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

    let invalidMessage = @(uint16(DataSize + 1).toBytesBE()) & newSeq[byte](DataSize)

    # Sphinx recovery succeeds, but the recipient supplied a malformed Mix
    # payload. Every redundant reply carries those same bytes, so the whole
    # reply opportunity is consumed.
    check store.recoverReply(first.rawReply(invalidMessage)).isErr
    check store.len == 0
