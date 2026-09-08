# SPDX-License-Identifier: MIT
{.used.}

import std/[base64, unittest]
import libp2p/[crypto/crypto, crypto/secp, multicodec, peerid]
import libp2p_mix/curve25519
import libp2p_mix
import libp2p_mix_transport

suite "Mix transport addresses":
  test "text and binary round trips preserve keys and endpoint":
    let info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
    let address = info.toMixAddress().expect("encode")
    let textAddress = MultiAddress.init($address).expect("text parse")
    let binaryAddress = MultiAddress.init(address.data.buffer).expect("binary parse")
    check textAddress == address
    check binaryAddress == address
    let decoded = fromMixAddress(info.peerId, binaryAddress).expect("decode")
    check decoded.peerId == info.peerId
    check decoded.multiAddr == info.multiAddr
    check decoded.libp2pPubKey.getBytes() == info.libp2pPubKey.getBytes()
    check decoded.mixPubKey.fieldElementToBytes() == info.mixPubKey.fieldElementToBytes()

  test "preserve QUIC, circuit-relay, IPv6 and DNS endpoint components":
    var info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
    let relay = MixNodeInfo.generateRandom(8082, newRng()).peerId
    for endpoint in [
      "/ip4/127.0.0.1/udp/8081/quic-v1",
      "/ip4/127.0.0.1/tcp/8081/p2p/" & $relay & "/p2p-circuit",
      "/ip4/127.0.0.1/udp/8081/quic-v1/p2p/" & $relay & "/p2p-circuit",
      "/ip6/::1/tcp/8081",
      "/dns4/example.com/tcp/8081",
    ]:
      info.multiAddr = MultiAddress.init(endpoint).expect("endpoint")
      let address = info.toMixAddress().expect("encode endpoint")
      let binary = MultiAddress.init(address.data.buffer).expect("binary parse")
      let decoded = fromMixAddress(info.peerId, binary).expect("decode endpoint")
      check decoded.multiAddr == info.multiAddr
      check MultiAddress.init($address).expect("text parse") == binary

  test "reject malformed payloads and misplaced mix components":
    for payload in ["!invalid!", "AAAA", base64.encode(newSeq[byte](66), safe = true)]:
      check MultiAddress.init("/ip4/127.0.0.1/tcp/8081/mix-transport/" & payload).isErr
    let info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
    let address = info.toMixAddress().expect("encode")
    let component = address[^1].expect("mix component")
    for value in [$address & "/tls", $component, $address & $component]:
      check fromMixAddress(info.peerId, MultiAddress.init(value).expect("parse")).isErr
    # Appending a binary component also invokes MultiAddress validation;
    # malformed key lengths are rejected without going through text parsing.
    var shortAddress = info.multiAddr
    let shortComponent = MultiAddress.init(
      multiCodec("mix-transport"), @[1.byte]
    ).expect("short binary component")
    check shortAddress.append(shortComponent).isErr
    let zeroKeys = MultiAddress
      .init(
        "/ip4/127.0.0.1/tcp/8081/mix-transport/" &
          base64.encode(newSeq[byte](65), safe = true)
      )
      .expect("parse")
    check fromMixAddress(info.peerId, zeroKeys).isErr

  test "bind the address public key to the destination PeerId":
    let
      first = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
      second = MixNodeInfo.generateRandom(8082, newRng()).toMixPubInfo()
    check fromMixAddress(second.peerId, first.toMixAddress().expect("encode")).isErr
