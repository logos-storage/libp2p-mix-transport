# SPDX-License-Identifier: Apache-2.0 OR MIT

## Mix Protocol Ping Example with QUIC Transport
##
## This example demonstrates using the Mix protocol with the Ping protocol.
## It creates a set of mix nodes that form an anonymous overlay network,
## then sends a ping through the mix network to a destination node and
## receives the response via Single Use Reply Blocks (SURBs).
## This example uses QUIC transport for all nodes.

{.used.}

import chronicles, chronos, results
import std/[strformat, sequtils, sugar]
import libp2p/[
    protocols/mix,
    protocols/mix/mix_protocol,
    protocols/mix/curve25519,
    protocols/ping,
    peerid,
    multiaddress,
    switch,
    builders,
    crypto/crypto,
    crypto/secp,
  ]

const NumMixNodes = 10

proc generateLocalQuicMixNodeInfo(): MixNodeInfo =
  let
    (mixPrivKey, mixPubKey) = generateKeyPair().expect("Generate key pair error")
    keyPair = SkKeyPair.random(newRng()[])
    pubKeyProto = PublicKey(scheme: Secp256k1, skkey: keyPair.pubkey)

  MixNodeInfo(
    peerId: PeerId.init(pubKeyProto).expect("PeerId init error"),
    multiAddr: MultiAddress.init(fmt"/ip4/0.0.0.0/udp/0/quic-v1").tryGet(),
    mixPubKey: mixPubKey,
    mixPrivKey: mixPrivKey,
    libp2pPubKey: keyPair.pubkey,
    libp2pPrivKey: keyPair.seckey,
  )

proc generateLocalQuicMixNodeInfos(count: int): seq[MixNodeInfo] =
  var nodeInfos: seq[MixNodeInfo] = newSeq[MixNodeInfo](count)
  for i in 0 ..< count:
    nodeInfos[i] = generateLocalQuicMixNodeInfo()
  nodeInfos

proc createSwitch(
    multiAddr: MultiAddress, libp2pPrivKey: Opt[SkPrivateKey] = Opt.none(SkPrivateKey)
): Switch =
  var rng = newRng()
  let skkey = libp2pPrivKey.valueOr(SkKeyPair.random(rng[]).seckey)
  let privKey = PrivateKey(scheme: Secp256k1, skkey: skkey)
  newStandardSwitchBuilder(privKey = Opt.some(privKey), addrs = multiAddr, transport = TransportType.QUIC).build()


proc mixPingSimulation() {.async: (raises: [Exception]).} =
  let mixNodeInfos = generateLocalQuicMixNodeInfos(NumMixNodes)
  var switches: seq[Switch] = @[]
  var mixProtos: seq[MixProtocol] = @[]

  # Start switches first so wildcard listen addresses are resolved to dialable addresses.
  for nodeInfo in mixNodeInfos:
    var switch = createSwitch(nodeInfo.multiAddr, Opt.some(nodeInfo.libp2pPrivKey))
    await switch.start()
    info "Mix node switch",
      peerId = switch.peerInfo.peerId,
      addrs = switch.peerInfo.addrs,
      listenAddrs = switch.peerInfo.listenAddrs

    switches.add(switch)
  
  defer:
    await switches.mapIt(it.stop()).allFutures()

  let resolvedInfos = collect:
    for i, nodeInfo in mixNodeInfos:
      initMixNodeInfo(
        nodeInfo.peerId,
        switches[i].peerInfo.addrs[0],
        nodeInfo.mixPubKey,
        nodeInfo.mixPrivKey,
        nodeInfo.libp2pPubKey,
        nodeInfo.libp2pPrivKey,
      )
  
  # Mount Mix protocols using the resolved, dialable node addresses.
  for i, nodeInfo in resolvedInfos:
    var switch = switches[i]
    let proto = MixProtocol.new(nodeInfo, switch)

    await proto.start()

    # Populate nodePool with all other nodes' public info
    proto.nodePool.add(resolvedInfos.includeAllExcept(nodeInfo))

    # Register how to read ping responses (32 bytes exactly)
    proto.registerDestReadBehavior(PingCodec, readExactly(32))
    switch.mount(proto)

    mixProtos.add(proto)

  # Create a destination node (not part of the mix network)
  let destNode =
    createSwitch(MultiAddress.init("/ip4/0.0.0.0/udp/0/quic-v1").tryGet())
  defer:
    await destNode.stop()

  let pingProto = Ping.new()
  destNode.mount(pingProto)

  # Start destination switch after mounting Ping.
  await destNode.start()

  # Pick sender (first mix node) and send ping through the mix network
  let senderIndex = 0

  info "Sending ping through mix network",
    sender = switches[senderIndex].peerInfo.peerId,
    destination = destNode.peerInfo.peerId

  # Create a connection through the mix network
  let conn = mixProtos[senderIndex]
    .toConnection(
      MixDestination.init(destNode.peerInfo.peerId, destNode.peerInfo.addrs[0]),
      PingCodec,
      MixParameters(expectReply: Opt.some(true), numSurbs: Opt.some(byte(1))),
    )
    .expect("could not build connection")

  # Send ping and wait for response through the mix network
  let response = await pingProto.ping(conn)
  await conn.close()

  info "Ping response received through mix network", rtt = response

when isMainModule:
  waitFor(mixPingSimulation())
