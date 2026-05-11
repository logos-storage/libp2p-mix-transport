# storage transport over libp2p MIX protocol

TBD...

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
