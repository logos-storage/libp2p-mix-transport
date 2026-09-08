import std/base64

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
  if vb.readSeq(decoded) < 0 or decoded.len != 65:
    return false
  s = base64.encode(decoded, safe = true)
  true

proc mixInfoVB(vb: var VBuffer): bool =
  var s: string
  mixInfoBtS(vb, s)

const AddressExts = [
  MAProtocol(
    mcodec: multiCodec("mix-transport"),
    kind: Length,
    size: -1,
    coder: Transcoder(
      stringToBuffer: mixInfoStB, bufferToString: mixInfoBtS, validateBuffer: mixInfoVB
    ),
  )
]
