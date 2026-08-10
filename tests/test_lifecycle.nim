# SPDX-License-Identifier: MIT

{.used.}

import std/unittest

import chronos, results
import libp2p/[builders, crypto/crypto, crypto/secp, switch]
import libp2p_mix

import libp2p_mix_transport

proc createMixProtocol(): MixProtocol =
  let
    rng = newRng()
    nodeInfo = MixNodeInfo.generateRandom(4242, rng)
    privateKey = PrivateKey(scheme: Secp256k1, skkey: nodeInfo.libp2pPrivKey)
    switch = SwitchBuilder
      .new()
      .withRng(rng)
      .withPrivateKey(privateKey)
      .withAddress(nodeInfo.multiAddr)
      .withTcpTransport()
      .withMplex()
      .withNoise()
      .build()

  MixProtocol.new(nodeInfo, switch)

suite "MixTransport lifecycle":
  test "start and stop own the Mix plug-in registrations":
    let
      mix = createMixProtocol()
      first = newMixTransport(mix)
      second = newMixTransport(mix)

    # The first transport acquires both Mix plug-in registrations.
    check waitFor(first.start()).isOk

    # Starting an already started transport is idempotent.
    check waitFor(first.start()).isOk

    # Another transport cannot acquire the same registrations.
    check waitFor(second.start()).isErr

    # Stopping the owner releases both registrations for another transport.
    waitFor(first.stop())
    check waitFor(second.start()).isOk

    waitFor(second.stop())

    # Stopping an already stopped transport is idempotent.
    waitFor(second.stop())

  test "failed start rolls back the service registration":
    let
      mix = createMixProtocol()
      transport = newMixTransport(mix)

    # Occupy the raw SURB reply handler slot. Transport startup will register
    # its service handler first and then fail to register its SURB handler.
    let surbReplyHandler: RawSurbReplyHandler = proc(
        reply: RawSurbReply
    ): Future[RawSurbReplyDisposition] {.async: (raises: [CancelledError]).} =
      discard reply
      return RawSurbReplyDisposition.Unhandled

    mix.registerRawSurbReplyHandler(surbReplyHandler).expect(
      "could not install SURB reply handler"
    )

    check waitFor(transport.start()).isErr

    # After freeing the SURB handler slot, a replacement can start only if the
    # failed startup rolled its already-registered service handler back.
    mix.unregisterRawSurbReplyHandler()

    let replacement = newMixTransport(mix)
    check waitFor(replacement.start()).isOk
    waitFor(replacement.stop())
