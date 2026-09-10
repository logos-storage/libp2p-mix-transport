import std/os

let extsDir = thisDir() / "../../libp2p_mix_transport/address/defaults"
switch("define", "libp2p_multiaddress_exts=" & extsDir / "multiaddress.nim")
switch("define", "libp2p_multicodec_exts=" & extsDir / "multicodec.nim")
