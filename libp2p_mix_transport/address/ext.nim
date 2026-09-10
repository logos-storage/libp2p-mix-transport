# SPDX-License-Identifier: MIT

# Extension for handling mix-transport addresses in libp2p multiaddress format.
# Needs to be kept as a separate module, with minimal imports as this gets sourced 
# in the middle of `multiaddress.nim`. You should not IMPORT this module, you 
# should source it into your extension file with `includeFile` (see tests/exts/multiaddress).

import std/base64

import secp256k1
import libp2p/crypto/curve25519

proc mixInfoStB(s: string, vb: var VBuffer): bool =
  try:
    let decoded = base64.decode(s)
    if decoded.len != 65 or base64.encode(decoded, safe = true) != s:
      return false
    vb.writeSeq(decoded)
    true
  except ValueError:
    false

proc mixInfoBtS(vb: var VBuffer, s: var string): bool =
  var decoded: string
  if vb.readSeq(decoded) < 0 or
      decoded.len != (SkRawCompressedPublicKeySize + Curve25519KeySize):
    return false
  s = base64.encode(decoded, safe = true)
  true

proc mixInfoVB(vb: var VBuffer): bool =
  var s: string
  mixInfoBtS(vb, s)

# Include this in your `AddressExts` extension array and set libp2p_multiaddress_exts 
# if you intend to use this in your module.
const MixAddressExt = MAProtocol(
  mcodec: multiCodec("mix-transport"),
  kind: Length,
  size: -1,
  coder: Transcoder(
    stringToBuffer: mixInfoStB, bufferToString: mixInfoBtS, validateBuffer: mixInfoVB
  ),
)
