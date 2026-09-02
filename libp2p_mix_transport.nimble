packageName = "libp2p_mix_transport"
version = "0.1.0"
author = "local"
description = "Generic byte-stream transport over the libp2p Mix protocol"
license = "MIT"
skipDirs = @["examples", "tests"]
entryPoints = @[
  "libp2p_mix_transport.nim", "tests/test_all.nim", "examples/mix_ping_tcp.nim",
  "examples/mix_ping_quic.nim",
]

requires "nim >= 2.2.4"
requires "libp2p == 2.2.0"
requires "https://github.com/logos-co/nim-libp2p-mix.git#feat/mix-transport"
requires "chronicles >= 0.11.0"
requires "chronos >= 4.2.2"
requires "protobuf_serialization >= 0.5.3"
requires "results >= 0.5.0"

import os, strutils

let
  nimc = getEnv("NIMC", "nim")
  flags = getEnv("NIMFLAGS", "")
  styleFlags = "--styleCheck:usages --styleCheck:error"

proc compile(filename: string) =
  exec nimc & " c " & styleFlags & " " & flags & " " & filename

proc compile(filename: string, output: string) =
  exec nimc & " c " & styleFlags & " " & flags & " -o:" & output & " " & filename

proc buildExample(filename: string) =
  compile("examples/" & filename)
  rmFile("examples/" & filename.changeFileExt("").toExe)

task test, "Run tests":
  compile("tests/test_all.nim")
  exec "./tests/test_all"
  rmFile "tests/test_all"

task example, "Build examples":
  buildExample("mix_ping_tcp.nim")
  buildExample("mix_ping_quic.nim")

task node, "Build standalone node":
  compile("tools/node/cli.nim", "tools/node/node")
