# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[crypto/crypto, peerid]
import
  libp2p_mix/
    [curve25519, delay, mix_message, padding, serialization, sphinx, tag_manager]
import stew/endians2

import libp2p_mix
import libp2p_mix/key_rotation
import libp2p_mix_transport/reply_credentials

proc randomSessionId(rng: Rng): PeerId =
  PeerId.random(rng).expect("could not generate session identifier")

type TestReply = object
  identifier: SURBIdentifier
  surb: SURB
  credential: ReplyCredential
  privateKey: FieldElement

proc createReply(rng: Rng, identifier: SURBIdentifier): TestReply =
  # The original sender's Mix key pair. The return path ends at this node.
  let (privateKey, publicKey) =
    generateKeyPair().expect("could not generate Mix key pair")

  # MixProtocol.createSurb normally creates this one-time public SURB and its
  # private credential on the original sender. A one-hop path keeps the test
  # focused: the recipient sends directly back to the original sender.
  var created = createSURB(
    @[publicKey], @[NoDelay], @[Hop.init(newSeq[byte](AddrSize))], identifier, rng
  )
  doAssert created.isOk
  TestReply(
    identifier: identifier,
    surb: created.get().surb,
    credential: move(created.get().credential),
    privateKey: privateKey,
  )

proc createReply(rng: Rng): TestReply =
  createReply(rng, rng.generate(SURBIdentifier))

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
    identifier: testReply.identifier, encryptedPayload: processed.delta_prime
  )

proc replyMessage(payload: seq[byte]): Message =
  # Reproduce the empty-codec Mix envelope and Sphinx-sized padding normally
  # applied by MixProtocol.sendWithSurb before useSURB is called.
  MixMessage.init(payload, "").serialize().addPadding().expect(
    "could not pad test reply"
  )

suite "MixTransport reply credentials":
  test "removing a credential wipes it even while a caller holds its entry":
    type Removal = enum
      Consume
      RemoveSession
      PurgeExpired
      Clear

    for removal in Removal:
      checkpoint $removal
      let
        rng = newRng()
        store = ReplyCredentialStore.new(ttl = 1.minutes)
        sessionId = randomSessionId(rng)
        createdAt = Moment.now()
        payload = @[1'u8, 2, 3]
      var reply = createReply(rng)
      let raw = reply.rawReply(replyMessage(payload))
      var credentials = [move(reply.credential)]
      require store.add(sessionId, credentials, createdAt).isOk
      let held = store.get(reply.identifier, createdAt).get()
      require recoverReply(held.credential, raw).get() == payload

      case removal
      of Consume:
        store.consume(reply.identifier, createdAt)
      of RemoveSession:
        check store.removeSession(sessionId) == 1
      of PurgeExpired:
        check store.purgeExpired(createdAt + 1.minutes) == 1
      of Clear:
        store.clear()
      check recoverReply(held.credential, raw).isErr

  test "consuming one reply credential leaves other credentials active":
    var
      rng = newRng()
      store = ReplyCredentialStore.new()
      first = createReply(rng)
      second = createReply(rng)
      sessionId = randomSessionId(rng)

    var credentials = [move(first.credential), move(second.credential)]
    store.add(sessionId, credentials).expect("could not add reply credentials")

    # Supplying SURBs registers their credentials independently. A redundancy
    # batch is formed later by the recipient and is unknown to this store.
    check:
      store.len == 2
      store.get(first.identifier).isSome
      store.get(second.identifier).isSome

    store.consume(first.identifier)

    check:
      store.len == 1
      store.retiredLen == 1
      store.get(first.identifier).isNone
      store.get(second.identifier).isSome
      store.isRetiredIdentifier(first.identifier)
      not store.isRetiredIdentifier(second.identifier)

    # Reusing an identifier while its tombstone is active would cause the raw
    # reply handler to mistake a new reply for a repeated one.
    var duplicate = createReply(rng, first.identifier)
    var duplicateCredentials = [move(duplicate.credential)]
    check store.add(randomSessionId(rng), duplicateCredentials).isErr

    # Consuming the same credential again is harmless and does not affect an
    # independently registered credential.
    store.consume(first.identifier)
    check:
      store.len == 1
      store.retiredLen == 1
      store.get(second.identifier).isSome

  test "capacity rejects new credentials without evicting active credentials":
    var
      rng = newRng()
      store = ReplyCredentialStore.new(maxCredentials = 2)
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)
      rejected = createReply(rng)

    var credentials = [move(first.credential), move(second.credential)]
    check store.add(sessionId, credentials).isOk
    var rejectedCredentials = [move(rejected.credential)]
    check store.add(sessionId, rejectedCredentials).isErr
    check:
      store.len == 2
      store.get(first.identifier).isSome
      store.get(second.identifier).isSome
      store.get(rejected.identifier).isNone

  test "removing one session preserves credentials owned by another session":
    var
      rng = newRng()
      store = ReplyCredentialStore.new()
      removedSessionId = randomSessionId(rng)
      retainedSessionId = randomSessionId(rng)
      removed = createReply(rng)
      retained = createReply(rng)

    var removedCredentials = [move(removed.credential)]
    store.add(removedSessionId, removedCredentials).expect(
      "could not add credentials for removed session"
    )
    var retainedCredentials = [move(retained.credential)]
    store.add(retainedSessionId, retainedCredentials).expect(
      "could not add credentials for retained session"
    )

    check store.removeSession(removedSessionId) == 1
    check:
      store.get(removed.identifier).isNone
      store.get(retained.identifier).isSome
      store.isRetiredIdentifier(removed.identifier)
      not store.isRetiredIdentifier(retained.identifier)
      store.len == 1

  test "retired reply identifiers expire and remain bounded":
    var
      rng = newRng()
      store = ReplyCredentialStore.new(ttl = 1.seconds, maxRetiredIdentifiers = 1)
      first = createReply(rng)
      second = createReply(rng)
      createdAt = Moment.now()
      sessionId = randomSessionId(rng)

    var credentials = [move(first.credential), move(second.credential)]
    store.add(sessionId, credentials, createdAt).expect(
      "could not add reply credentials"
    )
    store.consume(first.identifier, createdAt)
    store.consume(second.identifier, createdAt)

    # The bound keeps only one tombstone. Which identifier remains depends on
    # hash-table iteration order, but either retained entry still suppresses a
    # repeated reply until the original credential expires.
    check:
      store.len == 0
      store.retiredLen == 1
      store.isRetiredIdentifier(first.identifier, createdAt) !=
        store.isRetiredIdentifier(second.identifier, createdAt)

    # Querying after expiry is pure: an expired identifier is no longer
    # considered retired, but storage cleanup remains explicit.
    check:
      not store.isRetiredIdentifier(first.identifier, createdAt + 1.seconds)
      not store.isRetiredIdentifier(second.identifier, createdAt + 1.seconds)
      store.retiredLen == 1
      store.purgeExpired(createdAt + 1.seconds) == 1
      store.retiredLen == 0

  test "expired credentials are invisible before they are purged":
    var
      rng = newRng()
      store = ReplyCredentialStore.new(ttl = 1.seconds)
      credential = createReply(rng)
      createdAt = Moment.now()

    var credentials = [move(credential.credential)]
    store.add(randomSessionId(rng), credentials, createdAt).expect(
      "could not add reply credential"
    )

    check:
      store.get(credential.identifier, createdAt).isSome
      store.get(credential.identifier, createdAt + 1.seconds).isNone
      store.len == 1
      store.purgeExpired(createdAt + 1.seconds) == 1
      store.len == 0

  test "an unknown reply remains available to the embedded Mix path":
    var
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

  test "each valid redundant reply consumes only its matching credential":
    var
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)
      payload = @[1'u8, 2, 3]

    var credentials = [move(first.credential), move(second.credential)]
    store.add(sessionId, credentials).expect("could not add reply credentials")

    # The recipient replies through the first public SURB. The original
    # sender recovers the payload with its matching private credential.
    let recovered = store
      .recoverReply(first.rawReply(replyMessage(payload)))
      .expect("could not recover reply")
      .get()

    check:
      recovered.sessionId == sessionId
      recovered.payload == payload
      store.len == 1
      store.get(first.identifier).isNone
      store.get(second.identifier).isSome

    # Transport frame semantics suppress the second logical delivery after it
    # is recovered. The credential store still processes each SURB separately.
    let redundant = store
      .recoverReply(second.rawReply(replyMessage(payload)))
      .expect("could not recover redundant reply")
      .get()
    check:
      redundant.payload == payload
      store.len == 0

  test "invalid Sphinx recovery keeps the matching credential available":
    var
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)

    var credentials = [move(first.credential), move(second.credential)]
    store.add(sessionId, credentials).expect("could not add reply credentials")

    let invalid = RawSurbReply(
      identifier: first.identifier, encryptedPayload: newSeq[byte](PayloadSize)
    )

    # Cryptographic recovery failed before decoding payload.
    # Keep the matching credential because a later packet using the same SURB
    # may be valid. The other credential remains independent as well.
    check store.recoverReply(invalid).isErr
    check:
      store.get(first.identifier).isSome
      store.get(second.identifier).isSome

    let payload = @[42.byte]
    let recovered =
      store.recoverReply(first.rawReply(replyMessage(payload))).get().get()
    check recovered.payload == payload

  test "an invalid Mix payload consumes only the matching credential":
    var
      rng = newRng()
      store = ReplyCredentialStore.new()
      sessionId = randomSessionId(rng)
      first = createReply(rng)
      second = createReply(rng)

    var credentials = [move(first.credential), move(second.credential)]
    store.add(sessionId, credentials).expect("could not add reply credentials")

    let invalidMessage = @(uint16(DataSize + 1).toBytesBE()) & newSeq[byte](DataSize)

    # Sphinx recovery succeeds, but the recipient supplied a malformed Mix
    # payload. This SURB cannot produce a different plaintext later, so its
    # credential is consumed. The store does not know which other SURBs the
    # recipient selected for the same temporary redundancy batch.
    check store.recoverReply(first.rawReply(invalidMessage)).isErr
    check:
      store.len == 1
      store.get(first.identifier).isNone
      store.get(second.identifier).isSome

  test "rotation bounds shrink on retries and expired credentials cannot recover":
    let rng = newRng()
    var reply = createReply(rng)
    let store = ReplyCredentialStore.new()
    let now = Moment.now()
    # A stable map key is sufficient here; the one-hop crypto fixture is not
    # a canonical three-hop wire SURB.
    let encoded = @[1.byte]
    var credentials = [move(reply.credential)]
    store.add(randomSessionId(rng), credentials, now).expect("credential")
    let built = SurbBuild(
      surb: reply.surb,
      dispatchBefore: Opt.some(now + 5.seconds),
      credentialsExpireAt: Opt.some(now + 3.seconds),
      replyHoldTime: Opt.some(10.seconds),
      replyTransitAllowance: Opt.some(200.milliseconds),
    )
    store.setSurbBounds(reply.identifier, built, encoded)
    check store.holdingBudgetMillis(encoded, now) == 2800
    check store.holdingBudgetMillis(encoded, now + 1.seconds) == 1800
    check store.holdingBudgetMillis(encoded, now + 3.seconds) == 0
    check store.get(reply.identifier, now + 3.seconds).isNone
    check store.purgeExpired(now + 3.seconds) == 1
    check store.holdingBudgetMillis(encoded, now) == 0
    check store.recoverReply(reply.rawReply(replyMessage(@[42.byte]))).get().isNone

  test "dispatch deadline rejects retransmission even while credential is retained":
    let rng = newRng()
    var reply = createReply(rng)
    let store = ReplyCredentialStore.new()
    let now = Moment.now()
    # A stable map key is sufficient here; the one-hop crypto fixture is not
    # a canonical three-hop wire SURB.
    let encoded = @[1.byte]
    var credentials = [move(reply.credential)]
    store.add(randomSessionId(rng), credentials, now).expect("credential")
    store.setSurbBounds(
      reply.identifier,
      SurbBuild(
        surb: reply.surb,
        dispatchBefore: Opt.some(now + 1.seconds),
        credentialsExpireAt: Opt.some(now + 10.seconds),
        replyHoldTime: Opt.some(5.seconds),
        replyTransitAllowance: Opt.some(200.milliseconds),
      ),
      encoded,
    )
    check store.holdingBudgetMillis(encoded, now + 999.milliseconds) == 5000
    check store.holdingBudgetMillis(encoded, now + 1.seconds) == 0
    check store.get(reply.identifier, now + 1.seconds).isSome

  test "a real SURB reply survives grace but cannot use a destroyed key":
    let rng = newRng()
    let config = KeyRotationConfig
      .init(
        rotationInterval = 1.seconds,
        graceWindow = 3.seconds,
        replacedRecordLifetime = ZeroDuration,
        maxTransit = 100.milliseconds,
      )
      .expect("rotation config")
    let ring = MixKeyRing.new(config, rng, replayTableCapacity = 1024)
    defer:
      waitFor ring.stop()
    let now = Moment.now()
    ring.update(now)
    let oldKey = ring.candidates(now)[0]
    var built = createSURB(
      @[oldKey.publicKey],
      @[NoDelay],
      @[Hop.init(newSeq[byte](AddrSize))],
      rng.generate(SURBIdentifier),
      rng,
    )
    require built.isOk
    let store = ReplyCredentialStore.new()
    defer:
      store.clear()
    let id = built.get().credential.identifier
    var credentials = [move(built.get().credential)]
    store.add(randomSessionId(rng), credentials).expect("credential")
    let packet = useSURB(built.get().surb, replyMessage(@[42.byte]))
    ring.update(now + 1.seconds)
    var matched = matchKey(packet, ring.candidates(now + 2.seconds)).expect("match")
    check matched.status == KeyMatched
    let processed = processSphinxPacket(packet, matched).expect("grace reply")
    check processed.status == ProcessingStatus.Reply
    let recovered = store
      .recoverReply(
        RawSurbReply(identifier: id, encryptedPayload: processed.delta_prime)
      )
      .expect("recover")
      .expect("owned reply")
    check recovered.payload == @[42.byte]
    ring.update(now + 4.seconds)
    check oldKey.isDestroyed
    check matchKey(packet, ring.candidates(now + 4.seconds)).get().status == NoKeyMatched
