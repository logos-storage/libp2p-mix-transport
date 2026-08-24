import std/base64
import std/httpclient
import std/json
import std/os
import std/parseopt
import std/strformat
import std/sugar

import pkg/chronos
import pkg/chronos/apps/http/httpserver
import pkg/chronicles
import pkg/libp2p
import pkg/libp2p/crypto/secp
import pkg/libp2p_mix
import pkg/libp2p_mix/curve25519

const
    DefaultApiPort = 8080.uint
    DefaultListenPort = 0.uint
    DefaultListenIp = "127.0.0.1"

type
  MixPool* = seq[MixPubInfo]
  Node* = object
    info: MixNodeInfo
    switch: Switch
    proto: MixProtocol

template echoerr(msg: string) =
  stderr.writeLine(msg)

type MixPubInfo* = object
  peerId*: PeerId
  multiAddr*: MultiAddress
  mixPubKey*: FieldElement
  libp2pPubKey*: SkPublicKey

proc createSwitch(
    multiAddr: MultiAddress,
    rng: Rng,
    libp2pPrivKey: Opt[SkPrivateKey] = Opt.none(SkPrivateKey),
): Switch =
  let
    skkey = libp2pPrivKey.valueOr(SkKeyPair.random(rng).seckey)
    privKey = PrivateKey(scheme: Secp256k1, skkey: skkey)
  SwitchBuilder
    .new()
    .withRng(rng)
    .withPrivateKey(privKey)
    .withAddress(multiAddr)
    .withTcpTransport()
    .withMplex()
    .withNoise()
    .build()

proc createMixNodeInfo(info: PeerInfo, listenAddress: MultiAddress): MixNodeInfo =
  let (mixPrivKey, mixPubKey) = generateKeyPair().expect("Generate key pair error")
  MixNodeInfo(
    peerId: info.peerId,
    multiAddr: listenAddress,
    mixPrivKey: mixPrivKey,
    mixPubKey: mixPubKey,
    libp2pPrivKey: info.privateKey.skkey,
    libp2pPubKey: info.publicKey.skkey,
  )

proc init*(T: type Node, mixPool: MixPool, listenIp: string, listenPort: uint): Result[Node, string] =
  let listenAddress = ? MultiAddress.init(fmt"/ip4/{listenIp}/tcp/{listenPort}")

  let
    rng = newRng()
    switch = createSwitch(listenAddress, rng)
    mixNodeInfo = createMixNodeInfo(switch.peerInfo, listenAddress)
    mixProto = MixProtocol.new(mixNodeInfo, switch)

  mixProto.nodePool.add(mixPool)
  switch.mount(mixProto)

  Node(
    info: mixNodeInfo,
    switch: switch,
    proto: mixProto
  ).ok

proc start*(node: Node): Future[void] {.async: (raises: [CancelledError, LPError]).} =
  await node.switch.start()
  await node.proto.start()

####### HTTP Server #######

proc toJson*(obj: MixPubInfo): JsonNode =
  %*{
    # libp2p uses base58 but we'll go for base64 so we don't
    # have to deal with different APIs
    "peerId": base64.encode(obj.peerId.data),
    "multiAddr": $obj.multiAddr,
    "mixPubKey": base64.encode(obj.mixPubKey.fieldElementToBytes),
    "libp2pPubKey": base64.encode(obj.libp2pPubKey.getBytes),
  }

proc toJson*(obj: MixNodeInfo): JsonNode =
  MixPubInfo(
    peerId: obj.peerId,
    multiAddr: obj.multiAddr,
    mixPubKey: obj.mixPubKey,
    libp2pPubKey: obj.libp2pPubKey,
  ).toJson()


proc toJson*(obj: Node): JsonNode =
  %*{
    "mixInfo": obj.info.toJson(),
  }

proc fromJson*(obj: var MixPubInfo, json: JsonNode)  =
  obj.multiAddr = MultiAddress.init(json["multiAddr"].str).valueOr:
    raise newException(ValueError, "Invalid multiaddress")

  let mixPubKeyStr = base64.decode(json["mixPubKey"].str)
  obj.mixPubKey = bytesToFieldElement(
    mixPubKeyStr.toOpenArrayByte(0, mixPubKeyStr.len - 1)).valueOr:
    raise newException(ValueError, "Failed to decode mix public key")

  let libp2pPubKeyStr = base64.decode(json["libp2pPubKey"].str)
  obj.libp2pPubKey = SkPublicKey.init(
    libp2pPubKeyStr.toOpenArrayByte(0, libp2pPubKeyStr.len - 1)).valueOr:
    raise newException(ValueError, "Invalid libp2p public key")

  let peerIdStr = base64.decode(json["peerId"].str)
  if not obj.peerId.init(peerIdStr.toOpenArrayByte(0, peerIdStr.len - 1)):
    raise newException(ValueError, "Failed to decode libp2p peer id")

proc newServer*(node: Node, listenAddress: string, apiPort: uint): Result[HttpServerRef, string] =

  proc apiHandler(reqfence: RequestFence): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
    let request = reqfence.valueOr:
      return defaultResponse()

    try:
      case request.uri.path
      of "/status":
        let headers = HttpTable.init([("Content-Type", "application/json")])
        await request.respond(Http200, $node.toJson(), headers)
      else:
        await request.respond(Http404, "Not found.")
    except HttpError as exc:
      defaultResponse(exc)

  HttpServerRef.new(
    initTAddress(fmt"{listenAddress}:{apiPort}"),
    apiHandler,
  )

####### CLI ########

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
    node = Node.init(@[], listenIp, listenPort).valueOr:
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
