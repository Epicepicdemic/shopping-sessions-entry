# Shopping Sessions (Databricks)

A click-stream sessionisation pipeline for the Shopping Sessions track. Entry for the Builders Buildathon, November 2026.

## Layout

- `reference/`: a slow, simple implementation of `rules.md` that is meant to be obviously correct. Every fast version is compared with it. `reference/README.md` explains how to run it and the harness, and lists the open questions about the rules.
- `harness/`: `check.py` runs any implementation and compares its two Parquet tables with the reference, row for row. The folder also holds deliberately broken copies (`broken/`), a mutation survey (`mutants.py`), a layout scrambler (`scramble.py`) and the fixtures (`fixtures/kit`, `fixtures/adversarial`).

The generated data sets are not in the repository.

## Third-party code and libraries

- **pyarrow** (Apache License 2.0). The reference uses it to write Parquet and the harness uses it to read Parquet.
- **The organisers' starter kit fixtures.** `harness/fixtures/kit/` is an unchanged copy of the 36 edge-case fixtures from the track's starter kit. The kit's README says to copy them in when publishing a starter as its own repository.
- **The organisers' dataset generator** (`generator.zip`, Rust). It was used only to make the test data, and it is not included here.

No other code written before the event is used.

## Fast pipeline

`fast/` is the independent Rust implementation. It uses the Rust standard library
and system libraries (GNU glibc and GCC libgcc_s on Linux; Apple libSystem for
native macOS timing), with **no third-party crates**. The root Dockerfile builds a linux/amd64 image when built with the platform below; its
runtime runs as uid 1000 and writes only to `OUTPUT_DIR`.

```sh
docker build --platform linux/amd64 -t shopping-fast .
docker run --rm --platform linux/amd64 --read-only --network none \
  --user 1000:1000 --cpus 8 --memory 24g --memory-swap 24g \
  --pids-limit 4096 --tmpfs /tmp:rw,size=64m \
  -v "$DATA:/data:ro" -v "$OUT:/output" -v "$SCRATCH:/scratch" \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 shopping-fast
```

Input/output directories can also be supplied as two positional arguments.
Use a fresh output directory for each run. `SHOPPING_TIMING=1` logs sequential
stage wall times, the chosen scanner, and Linux peak RSS to stderr.

The pipeline splits every file into newline-aligned work ranges, validates all
CSV fields structurally, keeps invalid earliest-ingest event copies as dedup
markers, and compares IDs exactly. IDs up to ten supported characters pack
reversibly into 60 bits; all others use length-prefixed byte arenas with 63-bit
offsets. Links use transitive union-find and bytewise minimum names. Event dedup
uses 2,048 shards and hash bits independent of the shard bits for table slots.
Surviving events and clicks are grouped by resolved person. Timestamp sorting,
a latest-click sweep and active campaign counts produce the tables. Outputs
are plain, uncompressed Parquet with nullable UTF-8 categories, DATE days and
64-bit counts/revenue. Catalog indices are 32-bit; wide windows/large campaign
catalogs use sparse daily accumulators. Timestamp and ingest order are not
clamped to the generator's ranges.

`SHOPPING_USE_BINS=1` enables exact half-hour counting order for people with at
least 64 events and windows of at most 2,048 bins; all other people/windows use
sorting. `SHOPPING_ASSISTS=rescan` selects the slower direct assist scan for
measurement. Build with `cargo build --release --features scalar` to exercise
the portable scanners. The normal amd64 build detects AVX2 at runtime; arm64
uses baseline NEON. There is no required AVX instruction on the scalar path.

The unchanged `harness/` compares outputs against `reference/`. Extra general
cases are generated outside the repository by:

```sh
python fast/tests/extended.py /path/outside/repository/extended -- /path/to/shopping-fast
```

The unit tests compare the vector/scalar CSV parsers on malformed and long
lines, exercise timestamp/price/ID edges, and compare bins against sorting on
1,000 deterministic random histories. `cargo test` also runs during Docker
builds. `fast/tools/build_macos.py` can compile in an existing Rust Docker
container and link with Xcode to measure a native macOS arm64 binary without
installing Rust on the host; its `--output` directory and this repository must
be mounted at matching paths in that container.

Local verification of runtime source `b138351`: all 22 cached-reference data
sets, 36 kit fixtures, 19 adversarial fixtures, both scrambled inputs and six
extra general cases match. The constrained amd64 image matches dev and 20M.
A fresh seed-3001 60M set completes with matching native/amd64 outputs; no
independent 60M oracle was computed.

| Local runtime | 20M wall median | 60M wall median | 60M peak RSS |
| --- | ---: | ---: | ---: |
| macOS arm64, 4 repetitions | 1.906 s | 15.466 s | 4.824 GiB |
| Emulated linux/amd64, 3 repetitions | 23.606 s | 69.214 s | 7.365 GiB |

These are shared-machine Apple M5 Pro measurements with uncontrolled cache/load,
not scorer estimates. Native uses eight workers; the Docker VM has six pinned
vCPUs with `NCPU=8` and about 10.4 GB VM RAM (24 GB container cgroup limit).
Stage timings and algorithm ablations are recorded in the parent project's
`progress/shopping-sessions.md`; raw data is outside this repository.

## License

MIT. See [LICENSE](LICENSE).
