import std/os

let extsDir = thisDir() / "../../tests/exts"
switch(
  "define",
  "libp2p_multiaddress_exts=" & extsDir / "multiaddress.nim",
)
switch(
  "define",
  "libp2p_multicodec_exts=" & extsDir / "multicodec.nim",
)