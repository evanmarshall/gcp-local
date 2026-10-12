# Decision: BigQuery runs on an embedded GoogleSQL engine

BigQuery queries run on [goccy/googlesqlite](https://github.com/goccy/googlesqlite) v0.3.1 embedded in the core: GoogleSQL's own parser and analyzer (compiled to WebAssembly, then to Go) over SQLite. It is pure Go, so the core stays `CGO_ENABLED=0` and cross-compiles. Authorization parses each script with the same GoogleSQL parser before anything runs.

## Why

Every option needs a GoogleSQL parser in the core: IAM must resolve each table a statement reads or writes before it runs, and no backend reports that. The only GoogleSQL parsers are ZetaSQL's C++ (CGO) and its wasm-to-Go port. With that port already in the core, the engine adds about 34 MB and one dependency tree.

Measured 2026-10-12 with Go 1.26.8:

| Option | Build | Size | Fit |
| --- | --- | --- | --- |
| googlesqlite, embedded (chosen) | Clean core build 47 s linux/amd64, 70 s linux/arm64 (cross-compiled on an M-series host), 223 s on a 2-vCPU arm64 VM with `GOGC=25`. Compiling `go-googlesql` peaks at 5.7 GiB RSS, or 4.2 GiB with `GOGC=25`; cached afterwards | Engine program 91 MB amd64 stripped; the parser alone is 58 MB; the core was 51 MB | Single state owner per project; the parse step and the engine share one grammar |
| [go-zetasqlite](https://github.com/goccy/go-zetasqlite) v0.20.0 (CGO) | 178 s clean on darwin/arm64 with clang; needs a C++ toolchain per target, so the Dockerfile's cross-compiling build stage can't build it | 157 MB | Same engine family, superseded upstream by googlesqlite; pins 2023-era dependencies |
| [bigquery-emulator](https://github.com/goccy/bigquery-emulator) v0.8.1 as a child process | Prebuilt binaries for linux and darwin, amd64 and arm64 (~44 MB compressed) | Core still needs the parser (+58 MB), plus the emulator binary | Two owners of dataset and table metadata; its REST shapes differ from the recordings; no authorization hook |
| DuckDB with a dialect shim | CGO | — | A different dialect: GoogleSQL types, functions, and scripting would be approximated |

## Rules that follow

- Each project gets a private in-memory engine through the backend manager. Reset discards it. The provider owns datasets and table metadata; the engine owns columns and rows.
- The authorization parse rewrites every table path to its canonical `` `project`.`dataset`.`table` ``. The engine resolves names loosely (a bare `t` can match any dataset's `t`) and folds case. Names the engine can't keep apart are refused: case-only differences, and paths that share a storage name (`ds.t_x` and `ds_t.x`).
- The process runs in UTC (`RequireUTC`). googlesqlite decodes DATE values through `time.Local`.
- Building the core needs about 6 GiB of memory; moo machines get 8 GiB ([moo](../moo.md)).
