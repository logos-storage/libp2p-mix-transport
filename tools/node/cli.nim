import std/httpclient
import std/json
import std/os
import std/parseopt
import std/strformat
import std/strutils

import pkg/chronicles
import pkg/chronos
import pkg/chronos/apps/http/httpserver
import pkg/libp2p_mix
import pkg/results

import ./[node, apiserver]

logScope:
  topics = "node mix_transport"

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
  if urls.len == 0:
    warn "No mix nodes to contact"
    return

  info "Contacting mix nodes", len = urls.len
  for i, url in urls:
    debug "Contacting mix node", url = url
    let content = parseJson(client.getContent(fmt"{url}/status"))
    fromJson(result[i], content["mixInfo"])
    debug "Added mix config for node", url = url

proc printUsage() =
  echoerr "Usage: node [options] <listen-ip>"
  echoerr "Options:"
  echoerr "  -h, --help            Show this help message"
  echoerr "  -a, --api-port <port> API port (default: 8080)"
  echoerr "  -l, --listen-port <port> Listen port (default: 0)"
  echoerr "  -i  --listen-ip <ipv4> Listen IP (default: 127.0.0.1)"

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

  let
    mixNodes = collectMixConfigs(positionalArgs)
    node = Node.init(mixNodes, listenIp, listenPort).valueOr:
      error "Failed to initialize node", msg = error
      quit(1)
    server = newServer(node, listenIp, apiPort).valueOr:
      error "Failed to create server", msg = error
      quit(1)

  (waitFor node.start()).isOkOr:
    error "Failed to start node", msg = error
    quit(1)

  server.start()
  node.running = true

  while true:
    chronos.poll()

main()
