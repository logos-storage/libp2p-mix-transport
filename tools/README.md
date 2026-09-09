# Measurement harness

`tools/` holds everything needed to run repeatable transfer experiments over the
Mix transport on a single machine: a standalone node binary, a Bash harness
library that starts networks and times transfers, an optional network-emulation
layer, the experiments themselves, and R notebooks for analysis.

```
tools/
├── node/                     Nim node used as the experiment workload
├── harness/                  Bash library: config, node control, netem emulation
└── experiments/
    └── multitransfer/        Concurrent-transfer experiment + R analysis
```

Everything runs on the loopback interface: node `i` listens on `127.0.0.(i+1)`,
so a run is a real libp2p network of independent processes, just without a real
network between them.

## Requirements

- Bash 5 (the harness uses `EPOCHREALTIME`), `curl`, `jq`, `shuf`
- Nim 2.2.4+ and Nimble, plus the project dependencies (`make setup`)
- For network emulation: Linux, `iproute2` (`ip`, `tc`) and passwordless-ish
  `sudo` (see [Network emulation](#network-emulation))
- For `emu-test.bash`: `iperf3`, `ss`
- For the analysis notebooks: R with `renv`

## The node

`tools/node/` is a small Nim program that speaks a trivial request/response
protocol (`/test/simple-transfer/1.0.0`): the client asks for `size` bytes
generated from a PRNG seed, the server streams them, and the client validates
the stream byte-for-byte as it arrives. That makes a transfer both a load
generator and a correctness check.

Each node runs a libp2p switch (TCP + Mplex + Noise), mounts `MixProtocol` and
the transfer protocol, and exposes a small HTTP API.

Build it with the Nimble tasks:

```bash
nimble node        # -> tools/node/node          (release)
nimble debugNode   # -> tools/node/node-debug    (release, JSON chronicles sink)
```

`node-debug` writes structured JSON logs, which is what the message-level
analysis notebook consumes.

The harness uses `tools/node/node` by default. Select the debug executable
before sourcing the harness so the choice is also propagated into a network
namespace:

```bash
export TR_NODE_BINARY="$PWD/tools/node/node-debug"
source tools/harness/harness.bash
```

If the harness is already loaded, set `TR_NODE_BINARY` and run `reload`.

### CLI

```
node [options] <peer-api-url>...
```

| Option | Default | Meaning |
| --- | --- | --- |
| `-a, --api-port` | `8080` | HTTP API port |
| `-l, --listen-port` | `0` | libp2p listen port |
| `-i, --listen-ip` | `127.0.0.1` | libp2p + API bind address |
| `-x, --mix-config` | `default` | Delay strategy preset: `default` (no sampling) or `exponential` |
| `-e, --log-level` | `INFO` | Chronicles level, optionally with topic directives (`INFO;trace:mix-transport`) |
| `-m, --max-connections` | `50` | libp2p connection-manager limit |

The `default` Mix configuration uses `NoSamplingDelayStrategy`: the sender
encodes a concrete delay of 0, 1 or 2 ms for every intermediate relay, and the
relay uses that value directly. The `exponential` configuration encodes a mean
of 100 ms; every intermediate relay independently samples its actual holding
time from a bounded exponential distribution. The exit node has no intentional
delay. These protocol-level holding times are independent of the IP-level
delay, loss and rate limits introduced by `netem`.

Positional arguments are the API URLs of *already running* nodes. At startup the
node `GET`s `/status` on each of them and adds the returned `mixInfo` to its mix
pool. There is no discovery: a node's view of the network is exactly the set of
URLs it was given.

### HTTP API

`GET /status` — returns the node's own mix descriptor and readiness:

```json
{
  "mixInfo": {
    "peerId": "<base64>",
    "multiAddr": "/ip4/127.0.0.4/tcp/9000",
    "mixPubKey": "<base64>",
    "libp2pPubKey": "<base64>"
  },
  "running": true
}
```

Keys are base64 rather than libp2p's base58, so both ends can use stock
encoders.

`POST /request` — asks this node to pull `size` bytes from a peer. The call is
**synchronous**: it returns `200 ok` only once the whole stream has been
received and validated, which is what makes wall-clock timing around `curl`
meaningful.

```jsonc
{"peerId": "<base64>", "size": 1000000}                          // over Mix
{"address": "/ip4/127.0.0.5/tcp/9000/", "size": 1000000}         // direct libp2p
```

`peerId` routes through the Mix transport; `address` dials directly and is the
baseline to compare against.

## The harness library

`tools/harness/harness.bash` is the single entry point — source it and you get
the whole API. It sources, in order, `utils.bash`, `config.bash`, `emu.bash` and
`transport.bash`, and installs an `EXIT` trap that kills every node it started.

```bash
source tools/harness/harness.bash
```

It detects interactive shells: sourcing it from your prompt leaves the caller's
shell options unchanged and skips the exit trap. The executable experiment
scripts enable their own strict shell options. `reload` re-sources the library,
which is handy while editing it.

### Configuration (`config.bash`)

Every knob is an environment variable prefixed `TR_`. Overrides already present
in the environment win; anything unset gets a default. All `TR_*` variables are
exported *and* collected into `TR_ENV`, which is how they survive the hop into
the network namespace.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TR_NODE_BINARY` | `tools/node/node` | Node binary to run |
| `TR_BASE` | `<repo>/experiment-output` | Root for all output (experiments override this) |
| `TR_LOG_LEVEL` | `INFO` | Passed to `--log-level`; use `INFO;trace:mix-transport` or `INFO;trace:mix-transport-messages` for tracing |
| `TR_API_PORT` | `8000` | API port for every node |
| `TR_LISTEN_PORT` | `9000` | libp2p port for every node |
| `TR_RUN_ID` | `<timestamp>-<random>` | Identifies the run |
| `TR_RUNTIME_FOLDER` | `$TR_BASE/$TR_RUN_ID` | Per-run directory |
| `TR_LOGS_FOLDER` | `$TR_RUNTIME_FOLDER/logs` | Node logs |

Since all nodes share the same ports and differ only by bind address, the
per-node address is derived from its index: `127.0.0.$((index + 1))`.

### Running nodes (`transport.bash`)

| Function | What it does |
| --- | --- |
| `tr_init` | Creates the output folders, resets the node table, writes the CSV header |
| `tr_start_network N [args...]` | Starts nodes `0..N-1` sequentially, waiting for each to report `running: true` before the next; extra args go to every node |
| `tr_start_node IDX [args...]` | Starts a single node, passing the API URLs of all previously started nodes |
| `tr_transfer_regular SRC DST SIZE` | Times a direct libp2p transfer |
| `tr_transfer_mix SRC DST SIZE` | Times a transfer over Mix |
| `tr_status IDX` | Pretty-printed `/status` |
| `tr_peer_id IDX` | That node's peer id |
| `tr_list_nodes` | Index, PID, alive/dead and peer id for every node |
| `tr_kill_node IDX` / `tr_kill_nodes` | Stop nodes (`tr_kill_nodes` also runs from the exit trap) |
| `tr_field NAME VALUE` | Adds a constant column to the measurements CSV — call **before** `tr_init` |

Because `tr_start_node` only ever hands a node the nodes started before it, node
`i` knows peers `0..i-1`. Mix paths are 3 hops (`MIX_PATH_LENGTH`), so
`tr_transfer_mix` refuses source indices below 3 — such a node cannot possibly
know enough peers to build a path.

`tr_transfer_*` are just `curl` wrapped in Bash's `time`, with `TIMEFORMAT` set
to emit a CSV row; run them in the background to generate concurrency.

### Output layout

```
$TR_BASE/
├── $TR_RUN_ID-transfer-times.csv     measurements
└── $TR_RUN_ID/
    └── logs/
        ├── node-<i>.log              one per node
        └── transfers/
            └── {mix,regular}-<src>-<dst>-<rand>.log
```

The measurements CSV always starts with:

```
timestamp,filesize,source,destination,wallclock,cpu
```

followed by one column per `tr_field`, e.g.:

```
timestamp,filesize,source,destination,wallclock,cpu,concurrent,emulator,strategy,netsize,mix
2026-09-07 14:45:29.528234-0300,1000000,23,9,24.082,0.003,80,none,exponential,100,true
```

`wallclock` and `cpu` are `time`'s real and user seconds for the `curl` call.
The extra columns come from a Bash associative array, so **their order is not
stable across runs** — parse by header name, never by position. Note also that
a *failed* transfer still produces a row; the CSV carries no status column, so
cross-check the per-transfer logs when a run looks odd.

## Network emulation

`emu.bash` runs the whole experiment inside a dedicated network namespace
(`mixtests`) with `netem` applied to loopback, so you can shape delay, jitter,
loss and bandwidth without touching the host's networking.

The interesting part is that API traffic is deliberately left unshaped. A `prio`
qdisc with two bands sits at the root; the priomap sends everything to band 1
(`netem`), and two `u32` filters matching `TR_API_PORT` as source *or*
destination divert API packets to band 0 (a plain `pfifo`). Both directions are
matched because clients pick random source ports. Without this, the control
plane would be shaped along with the traffic under test and measurements would
mostly reflect the emulator.

The namespace's `lo` is also brought up with MTU 1500 instead of the loopback
default of 65536, so packet sizes resemble a real link.

```bash
emu_set_params delay 10ms 2ms distribution normal loss 0.1% rate 100mbit
emu_enter                 # must come after emu_set_params
```

`emu_enter` takes the arguments straight from `tc ... netem`. With no command it
does one of two things: in a script it re-executes that script inside the
namespace and exits when it finishes; in an interactive shell it drops you into
`bash -i` there (you then need to re-source `harness.bash`). Pass a command to
run just that. Teardown happens automatically; `emu_teardown` is available for
manual cleanup.

This needs `sudo`. For long batches, `start_sudo_keepalive` refreshes the
credential in the background and `stop_sudo_keepalive` (also called from the
exit trap) stops it. Making this work rootless is still an open TODO.

`emu-profiles.bash` names the useful parameter sets:

| Profile | netem parameters |
| --- | --- |
| `wired` | `delay 10ms 2ms distribution normal loss 0.1%` |
| `wired-lossy` | `delay 10ms 2ms distribution normal loss 1%` |
| `wired-capped` | `wired` + `rate 100mbit` |
| `wired-very-capped` | `wired` + `rate 1mbit` |
| `hi-delay-jittery` | `delay 100ms 30ms distribution normal loss 0.1%` |
| `hi-delay-jittery-lossy` | `delay 100ms 30ms distribution normal loss 1%` |

Rate caps apply to the shared loopback, so a capped profile splits that
bandwidth across the whole network rather than giving it to each node. The
profile values are placeholders picked by hand — grounding them in real
measurements is a TODO.

Sanity-check the setup with `emu-test.bash`, which shapes loopback to 0.5 Mbit
and runs `iperf3` against both the libp2p port (should be slow) and the API port
(should stay fast).

## The multitransfer experiment

`experiments/multitransfer/multitransfer.bash` builds a network, then keeps a
fixed number of transfers in flight between randomly chosen node pairs until the
requested total has completed.

```bash
bash tools/experiments/multitransfer/multitransfer.bash \
    <n_nodes> <n_transfers> <concurrent> <filesize_bytes> <use_mix> <strategy> <emu_profile>
```

| Position | Default | Meaning |
| --- | --- | --- |
| 1 | `40` | Network size |
| 2 | `50` | Total transfers to complete |
| 3 | `5` | Transfers in flight |
| 4 | `1048576` | Bytes per transfer |
| 5 | `true` | `true` for Mix, `false` for direct libp2p |
| 6 | `default` | Mix delay strategy (`default` or `exponential`) |
| 7 | `none` | Emulation profile, or `none` |

Output goes to `experiments/multitransfer/output/` (override with `TR_BASE`).
Network size, concurrency, mix on/off, strategy and profile are all recorded as
CSV columns via `tr_field`, so results from many runs can simply be concatenated.

Pairs are drawn with `shuf` from `[MIX_PATH_LENGTH, n_nodes)` and sorted
descending, which satisfies `tr_transfer_mix`'s constraint that the source knows
at least three peers. The scheduler uses `wait -n` to block until any transfer
exits, then reconciles the in-flight list with `kill -0` — Bash's `wait -n`
doesn't say *which* job finished.

`multitransfer-set.bash` runs a batch of parameter combinations back to back
(network sizes, concurrency levels, delay strategies, emulation profiles),
holding `sudo` alive for the duration. Edit the `PARAMS` array to change the
sweep.

Run the complete configured sweep from the repository root with:

```bash
TR_BASE="$PWD/tools/experiments/multitransfer/output" \
  bash tools/experiments/multitransfer/multitransfer-set.bash
```

Setting `TR_BASE` explicitly places the generated CSV files and node logs in the
directory read by the bundled analysis notebooks.

## Analysis

`experiments/multitransfer/analysis/` holds two R Markdown notebooks with an
`renv` lockfile:

- `analysis.Rmd` — reads every CSV under `../output`, and plots transfer time
  against concurrency, faceted by delay strategy and network size.
- `analysis-messages.Rmd` — parses `mix-transport-messages` trace lines out of
  the per-node JSON logs and compares message counts across experiments. This
  one needs nodes built with `nimble debugNode` (JSON sink) and run with
  `TR_LOG_LEVEL="INFO;trace:mix-transport-messages"`. It reads named folders
  under `../output`, so rename or symlink run directories to match the labels
  in the notebook.

```bash
cd tools/experiments/multitransfer/analysis
R -e 'renv::restore()'
```

Then knit from RStudio, VS Code, or `rmarkdown::render()`.

## Gotchas

- **Node count is capped by the loopback address space.** Addresses are
  `127.0.0.(i+1)`, so indices stop being usable somewhere below 254.
- **Mix sources must have index ≥ 3.** Lower indices don't know enough peers to
  build a path.
- **Interactive sourcing changes behaviour** (`set +e`, no exit trap), so stop
  nodes yourself with `tr_kill_nodes` when experimenting from a prompt.
