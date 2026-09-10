# libp2p Mix transport

`libp2p_mix_transport` provides a generic byte-stream transport over the libp2p
Mix protocol. It is under active development.

## Building

The project requires Nim 2.2.4 or newer, Nimble and Git. From a fresh clone:

```bash
make setup NIMBLE_FLAGS="-y"
make test
make example
```

`make setup` installs dependencies in the repository-local `nimbledeps/`
directory and generates `nimble.paths`. It uses the currently selected system
compiler instead of allowing Nimble to install another Nim version. If
Choosenim is available, the Makefile resolves its selected underlying compiler;
otherwise it uses `nim` from `PATH`. Set `NIMBLE_NIM` explicitly to override
that selection.

`NIMBLE_FLAGS` is passed through to Nimble. For example, omit `-y` for an
interactive setup or use a different dependency solver when troubleshooting:

```bash
make setup NIMBLE_FLAGS="--solver:legacy"
```

`NIMFLAGS` is consumed by the test and example tasks:

```bash
make test NIMFLAGS="-d:release"
```

The main development commands are:

```bash
make test     # run the test suite
make example  # compile the TCP and QUIC examples
make format   # format tracked Nim files with nph
make clean    # remove generated dependencies and build output
```

`make format` expects `nph` to be installed and available in `PATH`.

The two current demos exercise Mix over TCP and QUIC and can also be run
directly after setup:

```bash
nim c -r examples/mix_ping_tcp.nim
nim c -r examples/mix_ping_quic.nim
```

They are retained while the transport is being built, but they do not yet
demonstrate the transport API. A transport-specific example will later model a
realistic bidirectional protocol with separate streams, similar to the stream
arrangement used by block exchange.

## Dialing a mix address

`toMixAddress(info: MixPubInfo)` returns an address such as
`/ip4/10.0.0.1/tcp/8081/mix-transport/<base64url-keys>`. Pass that address to
`transport.connect(peerId, @[address])` or
`transport.dial(peerId, @[address], codec)`. The existing PeerId-only calls
remain available for peers already in the node pool or established sessions.

The payload is 65 bytes: a compressed secp256k1 libp2p public key (33 bytes),
followed by a Curve25519 mix public key (32 bytes), encoded as padded URL-safe
base64. The public key must match the supplied PeerId. The address format
preserves any MultiAddress endpoint prefix, including QUIC and circuit relays.
Actual routing remains subject to the underlying Mix packet format, which
currently supports IPv4 TCP and QUIC-v1, including circuit relays. The multicodec
`0x300001` is a private-use assignment, not an upstream registered code.

Supplied destinations stay attached to their transport session and are removed
on failure or teardown. For each forward frame, MixTransport passes the stored
`MixPubInfo` to Mix's explicit-destination `send` overload. Mix uses that
information for the final hop and selects the preceding hops from its relay
pool, excluding the destination. No destination entries are installed in the
peer store, and the provider does not become a relay candidate for other sends.
For the current three-hop path, an explicit send needs two eligible relay nodes.
Creating the return SURBs still requires the normal pool used by `createSurb`.
Addresses are tried in order, skipping invalid entries and continuing after
connection failures; each attempt uses the configured connection timeout.

The test and standalone-node build tasks enable libp2p's extension hooks using
the example files in `tests/exts/`. Those files are not installed with the
package. Applications must provide their own extension files and select them
with `libp2p_multicodec_exts` and `libp2p_multiaddress_exts` at compile time.
Use `tests/exts/multicodec.nim` as the example for the `CodecExts` entry. For
multiaddresses, use `tests/exts/multiaddress.nim` as the example: adapt its
`includeFile` path to the installed `libp2p_mix_transport/address/ext.nim` and
include `MixAddressExt` in `AddressExts`. Applications with existing extensions
should add these entries to their own arrays.

## Docs

In the `docs` folder there are some Markdown documents. Some of the them may contain more of less sophisticated math formatting. I am using Obsidian to render math expressions in Markdown. Obsidian has excelent support for Math and it works somoothly (I am speaking about you HackMD!). So, it you are serious about anything in your life ;), please use Obsidian to access the documentation. You can find our Obsidian vault at [logos-storage/logos-storage-docs-obsidian](https://github.com/logos-storage/logos-storage-docs-obsidian).

If for you need to work with the docs in VSCode, the please consider installing [Markdown Preview Enhanced](https://marketplace.visualstudio.com/items?itemName=shd101wyy.markdown-preview-enhanced) extenssion and make sure `MathJax` is selcted for rendering. This repository comes with the VSCode workspace settings and it already does this for you: 

```json
"markdown-preview-enhanced.mathRenderingOption": "MathJax"
```

If the two formulas below renders without errors (and looks kind of nice), your environment is most probably ok:

$$
\begin{array}{|c|c|c|c|c|c|c|c|}
\hline
0&0&0&0&0&0&0&0
\\
\hline
\hspace{-2em}\oplus\hspace{1.4em}
\mathcal S_i[16] &
\mathcal S_i[17] &
\mathcal S_i[18] &
\mathcal S_i[19] &
\mathcal S_i[20] &
\mathcal S_i[21] &
\mathcal S_i[22] &
\mathcal S_i[23]
\\
\hline
\hspace{-2em}=\hspace{1.2em}
\mathcal S_i[16] &
\mathcal S_i[17] &
\mathcal S_i[18] &
\mathcal S_i[19] &
\mathcal S_i[20] &
\mathcal S_i[21] &
\mathcal S_i[22] &
\mathcal S_i[23]
\\
\hline
\end{array}
$$

$$
\begin{array}{l}
(A_3\,\Vert\,D_2\,\Vert\,\gamma_3) \\
\,\left\vert
\begin{matrix}
\small\;\mathcal{S}_0[a,b) \\
\small\;\mathcal{S}_1[c,d) \\
\small\;\enclose{horizontalstrike}{\mathcal{S}_3[e,f)} \\
\small\;\enclose{horizontalstrike}{\mathcal{S}_3[e,f)}
\end{matrix}  
\right.  
\end{array}
$$

> If you are reading this on GitHub you already see that GitHub Markdown rendering is absolutly not sufficient...


## Local Files

The repository uses `config.nims` to keep Nim build output in the local
`nimcache/` directory and to include `nimble.paths` when it exists.

These files and directories are local artifacts and should not be committed:

- `nimbledeps/`
- `nimble.paths`
- `nimble.develop`
- `nimcache/`
- `examples/mix_ping_tcp`
- `examples/mix_ping_quic`

## Dependency Cache Troubleshooting

The Mix dependency currently follows a mutable Git branch. Nimble can keep using an older checkout from its global `pkgcache` after that branch advances, even when the project-local `nimbledeps/` directory is rebuilt.

First inspect the cached Mix checkouts:

```bash
nimble_dir="${NIMBLE_DIR:-$HOME/.nimble}"
find "$nimble_dir/pkgcache" -mindepth 1 -maxdepth 1 -type d -name '*nimlibp2pmix*' -print
```

When the stale directory has been identified, remove that exact directory manually and rebuild the local dependencies:

```bash
rm -rf "$nimble_dir/pkgcache/<verified-mix-cache-directory>"
make clean
make setup NIMBLE_FLAGS="-y"
```

Do not pass an unresolved wildcard to `rm`. Inspect the paths first and remove only the intended checkout.

If selective cleanup is insufficient, completely reset Nimble's downloaded package and resolver state before retrying:

```bash
rm -rf "$nimble_dir/pkgcache" "$nimble_dir/pkgs2" "$nimble_dir/buildtemp"
rm -f "$nimble_dir/nimbledata2.json" \
  "$nimble_dir/packages_official.json" "$nimble_dir/packages_temp.json"
make clean
make setup NIMBLE_FLAGS="-y"
```

This is destructive global cleanup. It affects other projects, removes globally installed package sources, and can leave launchers in `$nimble_dir/bin` that need to be reinstalled.

For a less destructive metadata-only retry, use:

```bash
make clean-all
make setup NIMBLE_FLAGS="-y"
```

`clean-nimble-cache` removes only the cached package registry and SAT tag index. It does not remove downloaded package checkouts. `clean-nimbledeps` removes only this project's `nimbledeps/`, `nimble.paths`, and `nimble.develop`.

## Clean Rebuild

To verify the project can be rebuilt from committed files:

```bash
make clean
make setup NIMBLE_FLAGS="-y"
make test
make example
```

If `nimble setup -l` reports that it cannot determine the VCS revision, make at
least one Git commit first, then rerun the command.
