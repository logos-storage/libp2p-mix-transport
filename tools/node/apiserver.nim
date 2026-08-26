import std/base64
import std/json
import std/strformat

import pkg/chronos/apps/http/httpserver
import pkg/libp2p
import pkg/libp2p/crypto/secp
import pkg/libp2p_mix
import pkg/libp2p_mix/curve25519

import ./node

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
  %*{"mixInfo": obj.info.toJson()}

proc fromJson*(obj: var PeerId, json: JsonNode) =
  let peerIdStr = base64.decode(json.str)
  if not obj.init(peerIdStr.toOpenArrayByte(0, peerIdStr.len - 1)):
    raise newException(ValueError, "Failed to decode libp2p peer id")

proc fromJson*(obj: var MultiAddress, json: JsonNode) =
  obj = MultiAddress.init(json.str).valueOr:
    raise newException(ValueError, "Invalid multiaddress")

proc fromJson*(obj: var MixPubInfo, json: JsonNode) =
  fromJson(obj.multiAddr, json["multiAddr"])
  fromJson(obj.peerId, json["peerId"])

  let mixPubKeyStr = base64.decode(json["mixPubKey"].str)
  obj.mixPubKey = bytesToFieldElement(
    mixPubKeyStr.toOpenArrayByte(0, mixPubKeyStr.len - 1)
  ).valueOr:
    raise newException(ValueError, "Failed to decode mix public key")

  let libp2pPubKeyStr = base64.decode(json["libp2pPubKey"].str)
  obj.libp2pPubKey = SkPublicKey.init(
    libp2pPubKeyStr.toOpenArrayByte(0, libp2pPubKeyStr.len - 1)
  ).valueOr:
    raise newException(ValueError, "Invalid libp2p public key")

proc newServer*(
    node: Node, listenAddress: string, apiPort: uint
): Result[HttpServerRef, string] =
  proc apiHandler(
      reqfence: RequestFence
  ): Future[HttpResponseRef] {.async: (raises: [CancelledError]).} =
    let req = reqfence.valueOr:
      return defaultResponse()

    try:
      case req.uri.path
      of "/status":
        let headers = HttpTable.init([("Content-Type", "application/json")])
        return await req.respond(Http200, $node.toJson(), headers)
      of "/request":
        let
          body = await req.getBody()
          data =
            try:
              parseJson(bytesToString(body))
            except ValueError as exc:
              return await req.respond(Http400, "Failed to parse: " & exc.msg)
          size = data{"size"}.getInt().int32

        if size <= 0:
          return await req.respond(Http400, "Size must be present and greater than 0")

        # If we have a PeerId, use mix transport.
        if data.hasKey("peerId"):
          var peerIdObj: PeerId
          fromJson(peerIdObj, data["peerId"])
          let res = await node.request(peerIdObj, size)
          if res.isErr:
            return await req.respond(Http500, "Failed to request: " & res.error)

        # Otherwise, contact peer directly.
        elif data.hasKey("address"):
          var addrObj: MultiAddress
          fromJson(addrObj, data["address"])
          await node.request(addrObj, size)
        else:
          return await req.respond(Http400, "Either peerId or address must be present")
      else:
        return await req.respond(Http404, "Not found.")
    except CatchableError as exc:
      return defaultResponse(exc)

  HttpServerRef.new(initTAddress(fmt"{listenAddress}:{apiPort}"), apiHandler)
