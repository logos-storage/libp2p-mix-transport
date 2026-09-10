# SPDX-License-Identifier: MIT
{.used.}

import std/[base64, unittest]
import libp2p/[crypto/crypto, crypto/secp, multicodec, multiaddress, peerid]
import libp2p_mix/curve25519
import libp2p_mix
import libp2p_mix_transport

suite "Mix transport addresses":
  test "should decode to what's been encoded":
    let
      info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
      address = info.toMixAddress().expect("encode")
      textAddress = MultiAddress.init($address).expect("text parse")
      binaryAddress = MultiAddress.init(address.data.buffer).expect("binary parse")

    check textAddress == address
    check binaryAddress == address

    let decoded =
      MixPubInfo.fromMixAddress(binaryAddress, Opt.some(info.peerId)).expect("decode")
    check decoded.peerId == info.peerId
    check decoded.multiAddr == info.multiAddr
    check decoded.libp2pPubKey.getBytes() == info.libp2pPubKey.getBytes()
    check decoded.mixPubKey.fieldElementToBytes() == info.mixPubKey.fieldElementToBytes()

  test "derives the PeerId when no expected destination is supplied":
    let
      info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
      address = info.toMixAddress().expect("encode")
      decoded = MixPubInfo.fromMixAddress(address, Opt.none(PeerId)).expect("decode")
    check decoded.peerId == info.peerId
    check decoded.multiAddr == info.multiAddr

  test "should preserve transport prefixes":
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
      let decoded = MixPubInfo.fromMixAddress(binary, Opt.some(info.peerId)).expect(
          "decode endpoint"
        )
      check decoded.multiAddr == info.multiAddr
      check MultiAddress.init($address).expect("text parse") == binary

  test "should ignore suffixes":
    var info = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
    let address = info.toMixAddress().expect("encode")
    let suffixed = MultiAddress.init($address & "/tls").expect("text parse with suffix")
    let decoded = MixPubInfo.fromMixAddress(suffixed, Opt.some(info.peerId)).expect(
        "decode with suffix"
      )
    check decoded.multiAddr == info.multiAddr

  test "should reject malformed payloads or missing mix components":
    # Bad payloads.
    for payload in ["!invalid!", "AAAA", base64.encode(newSeq[byte](66), safe = true)]:
      check MultiAddress.init("/ip4/127.0.0.1/tcp/8081/mix-transport/" & payload).isErr

    # Missing mix-transport component.
    let peerId = PeerId.random(newRng()).get()
    check MixPubInfo.fromMixAddress(
      MultiAddress.init("/ip4/127.0.0.1/tcp/8081/tls").get(), Opt.some(peerId)
    ).isErr

    # Appending a binary component also invokes MultiAddress validation;
    # malformed key lengths are rejected without going through text parsing.
    var shortAddress = MultiAddress.init("/ip4/127.0.0.1/tcp/8081").get()
    let shortComponent = MultiAddress.init(multiCodec("mix-transport"), @[1.byte]).get()
    check shortAddress.append(shortComponent).isErr

    let zeroKeys = MultiAddress
      .init(
        "/ip4/127.0.0.1/tcp/8081/mix-transport/" &
          base64.encode(newSeq[byte](65), safe = true)
      )
      .get()
    check MixPubInfo.fromMixAddress(zeroKeys, Opt.some(peerId)).isErr

  test "verifies that the address public key matches the destination PeerId":
    let
      first = MixNodeInfo.generateRandom(8081, newRng()).toMixPubInfo()
      second = MixNodeInfo.generateRandom(8082, newRng()).toMixPubInfo()
    check MixPubInfo.fromMixAddress(
      first.toMixAddress().expect("encode"), Opt.some(second.peerId)
    ).isErr
