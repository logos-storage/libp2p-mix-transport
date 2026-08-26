import std/httpclient
import std/json
import std/jsonutils
import std/os
import std/parseopt
import std/strformat
import std/strutils

import pkg/chronos
import pkg/chronos/apps/http/httpserver
import pkg/libp2p_mix
import pkg/results

import ./[node, apiserver]

const
  DefaultApiPort = 8080.uint
  DefaultListenPort = 0.uint
  DefaultListenIp = "127.0.0.1"

template echoerr(msg: string) =
  stderr.writeLine(msg)

proc collectMixConfigs(urls: seq[string]): seq[MixPubInfo] =
  # Use blocking client cause we're not running inside of chronos
  let client = newHttpClient()
  result = newSeq[MixPubInfo](urls.len)
  echoerr "Contacting " & $urls.len & " mix nodes..."
  for i, url in urls:
    let content = parseJson(client.getContent(fmt"{url}/status"))
    fromJson(result[i], content["mixInfo"])

proc printUsage() =
  echo "Usage: node [options] <listen-ip>"
  echo "Options:"
  echo "  -h, --help            Show this help message"
  echo "  -a, --api-port <port> API port (default: 8080)"
  echo "  -l, --listen-port <port> Listen port (default: 0)"
  echo "  -i  --listen-ip <ipv4> Listen IP (default: 127.0.0.1)"

proc main() =
  var
    apiPort: uint = DefaultApiPort
    listenPort: uint = DefaultListenPort
    listenIp: string = DefaultListenIp
    positionalArgs: seq[string]

  var optparser = initOptParser(quoteShellCommand(commandLineParams()))
  for kind, key, val in optparser.getopt():
    case kind
    of cmdArgument:
      positionalArgs.add(key)
    of cmdLongOption, cmdShortOption:
      case key
      of "help", "h":
        printUsage()
        quit(0)
      of "api-port", "a":
        apiPort = val.parseUint()
      of "listen-port", "l":
        listenPort = val.parseUint()
      of "listen-ip", "i":
        listenIp = val
      else:
        echoerr("Unknown option: " & key)
        printUsage()
        quit(1)
    of cmdEnd:
      assert(false) # should not happen

  echoerr "Positional args: " & $positionalArgs
  let mixNodes = collectMixConfigs(positionalArgs)

  echoerr "Mix nodes: " & $mixNodes

  let
    node = Node.init(mixNodes, listenIp, listenPort).valueOr:
      echoerr("Failed to initialize node: " & $error)
      quit(1)
    server = newServer(node, listenIp, apiPort).valueOr:
      echoerr("Failed to create server: " & $error)
      quit(1)

  waitFor node.start()
  server.start()

  while true:
    chronos.poll()

main()
