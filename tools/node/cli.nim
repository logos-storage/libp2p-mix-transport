import std/httpclient
import std/json
import std/os
import std/parseopt
import std/strformat
import std/strutils

import pkg/chronicles
import pkg/chronicles/helpers
import pkg/chronicles/topics_registry
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
  DefaultLogLevel = "INFO"
  DefaultMaxConnections = 50 # same as libp2p

template echoerr(msg: string) =
  stderr.writeLine(msg)

proc updateLogLevel*(logLevel: string) {.raises: [ValueError].} =
  let directives = logLevel.split(";")
  try:
    setLogLevel(parseEnum[LogLevel](directives[0].toUpperAscii))
  except ValueError:
    raise (ref ValueError)(
      msg:
        "Please specify one of: trace, debug, " & "info, notice, warn, error or fatal"
    )

  if directives.len > 1:
    for topicName, settings in parseTopicDirectives(directives[1 ..^ 1]):
      if not setTopicState(topicName, settings.state, settings.logLevel):
        warn "Unrecognized logging topic", topic = topicName

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
    info "Added mix config for node", url = url

  info "Node mix pool has nodes", n = result.len

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
    maxConnections: int = DefaultMaxConnections
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
      of "max-connections", "m":
        maxConnections = val.parseInt()
      of "log-level", "e":
        updateLogLevel(val)
      else:
        echoerr("Unknown option: " & key)
        printUsage()
        quit(1)
    of cmdEnd:
      assert(false) # should not happen

  let
    mixNodes = collectMixConfigs(positionalArgs)
    node = Node.init(mixNodes, listenIp, listenPort, maxConnections).valueOr:
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
