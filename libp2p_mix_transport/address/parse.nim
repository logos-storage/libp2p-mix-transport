# SPDX-License-Identifier: MIT

{.push raises: [].}
import secp256k1

import
  libp2p/
    [multiaddress, multicodec, peerid, crypto/crypto, crypto/curve25519, crypto/secp]
import libp2p_mix
import libp2p_mix/curve25519

const 
    AddressPayloadSize = SkRawCompressedPublicKeySize + Curve25519KeySize
    # Minimum number of protocol components expected BEFORE the mix-transport component
    MinPrefixLength = 1

proc extract(address: MultiAddress): Result[tuple[prefix: MultiAddress, component: MultiAddress], string] =
  let protocols = ?address.protocols()
  var mixIndex = -1 
  for i, protocol in ?address.protocols():
    if protocol == multiCodec("mix-transport"):
      mixIndex = i
      break

  if mixIndex == -1:
    return err("mix-transport protocol not found in address")

  let
    prefix = ?address[0 ..< mixIndex]
    component = ?address[mixIndex]
  if ?prefix.len < MinPrefixLength:
    return err("address has no transport prefix")
  ok((prefix: prefix, component: component))

proc isMTAddress*(address: MultiAddress): bool = extract(address).isOk

proc fromMixAddress*(
    T: type MixPubInfo, address: MultiAddress, destination: Opt[PeerId],
): Result[MixPubInfo, string] =

  let 
    (endpoint, mix) = ?extract(address)
    payload = ?mix.protoArgument()
  
  if payload.len != AddressPayloadSize:
    return err("invalid payload size")

  let
    publicKey = secp.SkPublicKey.init(
      payload.toOpenArray(0, SkRawCompressedPublicKeySize - 1)
    ).valueOr:
      return err("invalid libp2p public key")
    peer = PeerId.init(PublicKey(scheme: Secp256k1, skkey: publicKey)).valueOr:
      return err("invalid peer public key")

  # Just in case. :-)
  if destination.isSome and peer != destination.get():
    return err("mix address public key does not match destination PeerId")

  let mixKey = bytesToAlphaFieldElement(
    payload.toOpenArray(SkRawCompressedPublicKeySize, AddressPayloadSize - 1)
  ).valueOr:
    return err("invalid mix public key: " & error)

  ok(MixPubInfo.init(peer, endpoint, mixKey, publicKey))

proc toMixAddress*(info: MixPubInfo): Result[MultiAddress, string] =
  ## The payload is compressed secp256k1 (33 bytes), then Curve25519 (32 bytes).
  var keys: seq[byte]
  keys.add(info.libp2pPubKey.getBytes())
  keys.add(info.mixPubKey.fieldElementToBytes())
  let component = ?MultiAddress.init(multiCodec("mix-transport"), keys)
  var address = info.multiAddr
  ?address.append(component)
  ok(address)

{.pop.}
