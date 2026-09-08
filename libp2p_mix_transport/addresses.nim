# SPDX-License-Identifier: MIT

{.push raises: [].}

import results
import libp2p/[multiaddress, multicodec, peerid, crypto/crypto, crypto/secp]
import libp2p_mix/[mix_node, curve25519]

export multiaddress

proc fromMixAddress*(
    destination: PeerId, address: MultiAddress
): Result[MixPubInfo, string] =
  ## MultiAddress has already decoded the text, including the base64 payload.
  ## Strip only our final component, preserving the complete endpoint (including
  ## QUIC or circuit-relay components). Mix validates routability when connecting.
  let protocols = ?address.protocols()
  if protocols.len < 2 or protocols[^1] != multiCodec("mix-transport"):
    return err("expected an endpoint followed by /mix-transport/<keys>")
  for protocol in protocols.toOpenArray(0, protocols.high - 1):
    if protocol == multiCodec("mix-transport"):
      return err("mix-transport must occur only once, at the end")
  let endpoint = ?address[0 .. ^2]
  let component = ?address[^1]
  let decoded = ?component.protoArgument()
  if decoded.len != 65:
    return err("mix address must contain 33 libp2p key bytes and 32 mix key bytes")
  let publicKey = SkPublicKey.init(decoded.toOpenArray(0, 32)).valueOr:
    return err("invalid libp2p public key")
  let peer = PeerId.init(PublicKey(scheme: Secp256k1, skkey: publicKey)).valueOr:
    return err("invalid peer public key")
  if peer != destination:
    return err("mix address public key does not match destination PeerId")
  let mixKey = bytesToAlphaFieldElement(decoded.toOpenArray(33, 64)).valueOr:
    return err("invalid mix public key: " & error)
  ok(MixPubInfo.init(destination, endpoint, mixKey, publicKey))

proc toMixAddress*(info: MixPubInfo): Result[MultiAddress, string] =
  ## The payload is compressed secp256k1 (33 bytes), then Curve25519 (32 bytes).
  var keys: seq[byte]
  keys.add(info.libp2pPubKey.getBytes())
  keys.add(info.mixPubKey.fieldElementToBytes())
  let component = ?MultiAddress.init(multiCodec("mix-transport"), keys)
  var address = info.multiAddr
  ?address.append(component)
  discard fromMixAddress(info.peerId, address).valueOr:
    return err(error)
  ok(address)

{.pop.}
