# SPDX-License-Identifier: MIT

{.push raises: [].}

import std/tables

import results
import libp2p/peerid
import libp2p/utils/opt

type
  SessionRole* {.pure.} = enum
    Initiator
    Recipient

  SessionState* {.pure.} = enum
    Pending
    Established

  TransportSession* = ref object
    sessionId: PeerId
    destination: Opt[PeerId]
    role: SessionRole
    state: SessionState

  SessionStore* = ref object
    bySessionId: Table[PeerId, TransportSession]
    byDestination: Table[PeerId, TransportSession]

func sessionId*(session: TransportSession): PeerId =
  session.sessionId

func destination*(session: TransportSession): Opt[PeerId] =
  session.destination

func role*(session: TransportSession): SessionRole =
  session.role

func state*(session: TransportSession): SessionState =
  session.state

func peerId*(session: TransportSession): PeerId =
  ## Identity exposed to consumers of this transport. The initiator knows the
  ## real destination; the recipient knows only the session pseudonym.
  session.destination.get(session.sessionId)

func len*(store: SessionStore): int =
  store.bySessionId.len

proc get*(store: SessionStore, sessionId: PeerId): Opt[TransportSession] =
  store.bySessionId.withValue(sessionId, session):
    return Opt.some(session[])
  Opt.none(TransportSession)

proc getByDestination*(
    store: SessionStore, destination: PeerId
): Opt[TransportSession] =
  store.byDestination.withValue(destination, session):
    return Opt.some(session[])
  Opt.none(TransportSession)

proc addInitiatorSession*(
    store: SessionStore, destination: PeerId, sessionId: PeerId
): Result[TransportSession, string] =
  if destination.len == 0:
    return err("destination must not be empty")
  if sessionId.len == 0:
    return err("sessionId must not be empty")
  if store.byDestination.hasKey(destination):
    return err("destination already has a session")
  if store.bySessionId.hasKey(sessionId):
    return err("sessionId is already registered")

  let session = TransportSession(
    sessionId: sessionId,
    destination: Opt.some(destination),
    role: SessionRole.Initiator,
    state: SessionState.Pending,
  )
  store.bySessionId[sessionId] = session
  store.byDestination[destination] = session
  ok(session)

proc addRecipientSession*(
    store: SessionStore, sessionId: PeerId
): Result[TransportSession, string] =
  if sessionId.len == 0:
    return err("sessionId must not be empty")
  if store.bySessionId.hasKey(sessionId):
    return err("sessionId is already registered")

  let session = TransportSession(
    sessionId: sessionId,
    destination: Opt.none(PeerId),
    role: SessionRole.Recipient,
    state: SessionState.Pending,
  )
  store.bySessionId[sessionId] = session
  ok(session)

proc establish*(session: TransportSession) =
  session.state = SessionState.Established

proc remove*(store: SessionStore, sessionId: PeerId): Opt[TransportSession] =
  let session = store.get(sessionId).valueOr:
    return Opt.none(TransportSession)

  store.bySessionId.del(sessionId)
  session.destination.withValue(destination):
    store.byDestination.del(destination)
  Opt.some(session)

proc clear*(store: SessionStore) =
  store.bySessionId.clear()
  store.byDestination.clear()

proc newSessionStore*(): SessionStore =
  SessionStore(
    bySessionId: initTable[PeerId, TransportSession](),
    byDestination: initTable[PeerId, TransportSession](),
  )

{.pop.}
