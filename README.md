# storage transport over libp2p MIX protocol

TBD...

## Quick Start

Prerequisites:

- Nim 2.0 or newer
- Nimble
- Git

From a fresh clone:

```bash
nimble setup -l
nim c -r mix_ping_tcp.nim
```

`nimble setup -l` enables project-local dependency mode, installs dependencies
under `nimbledeps/`, and generates `nimble.paths` and `nimble.develop`.

## Local Files

The repository uses `config.nims` to keep Nim build output in the local
`nimcache/` directory and to include `nimble.paths` when it exists.

These files and directories are local artifacts and should not be committed:

- `nimbledeps/`
- `nimble.paths`
- `nimble.develop`
- `nimcache/`
- `mix_ping_tcp`
- `mix_ping_quic`

## Clean Rebuild

To verify the project can be rebuilt from committed files:

```bash
rm -rf nimbledeps nimble.paths nimble.develop nimcache \
  mix_ping_tcp mix_ping_quic
nimble setup -l
nim c -r mix_ping_tcp.nim
nim c -r mix_ping_quic.nim
```

If `nimble setup -l` reports that it cannot determine the VCS revision, make at
least one Git commit first, then rerun the command.
