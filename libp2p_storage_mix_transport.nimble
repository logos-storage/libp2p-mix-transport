version = "0.1.0"
author = "local"
description = "Storage transport over libp2p MIX protocol"
license = "MIT"
srcDir = "."
entryPoints = @["mix_ping_tcp.nim", "mix_ping_quic.nim"]

requires "nim >= 2.0.0"
requires "libp2p >= 1.15.3"
requires "chronicles >= 0.11.0"
requires "chronos >= 4.2.2"
requires "results >= 0.5.0"
