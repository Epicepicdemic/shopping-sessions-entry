# Shopping Sessions (Databricks)

A click-stream sessionisation pipeline for the Shopping Sessions track. Entry for the Builders Buildathon, November 2026.

## Layout

- `fast/`: the Rust implementation (standard library only, no third-party crates). `fast/tests/extended.py` generates extra general test cases; `fast/tools/` has a timing helper and a native macOS build helper.
- `reference/`: a slow, simple implementation of `rules.md` that is meant to be obviously correct. Every fast version is compared with it. `reference/README.md` explains how to run it and the harness, and lists the open questions about the rules.
- `harness/`: `check.py` runs any implementation and compares its two Parquet tables with the reference, row for row. The folder also holds deliberately broken copies (`broken/`), a mutation survey (`mutants.py`), a layout scrambler (`scramble.py`) and the fixtures (`fixtures/kit`, `fixtures/adversarial`).

The generated data sets are not in the repository.

## The default run: the opaque-ID engine

The default run is `fast/src/opaque.rs`. It treats every ID as an opaque string and implements the rules for any IDs; it uses nothing about how the generator makes them.

- **IDs.** An ID of at most ten characters from `[0-9A-Z_a-z]` is keyed by its exact 60-bit packed form. Any other ID is keyed by a 64-bit hash, and its bytes are compared whenever two keys match, so two different IDs never merge.
- **One pass over all input.** Links, events and clicks are read in parallel. Each user ID is interned into a dense index in a lock-free table; each link line joins its two indices in a concurrent union-find. Each event copy is filed under a bucket of its event key with its ingest time, user index and validated fields; each valid click is kept with its user index.
- **Persons.** Each link component is a person named by its byte-wise smallest ID; every other ID is a person of its own.
- **Dedup, then sessions.** Each event bucket keeps, per event ID, the copy with the smallest ingest time (dedup before validation), and files the surviving valid events by person. When two long event keys match, the engine compares their exact ID bytes in the dedup pass. Each person bucket is then sessionized by the same code the exact solver uses (`fast/src/sink.rs`).
- **Memory.** Records go to their buckets through cache-line staging with non-temporal stores, into spans carved from one region per worker. Each event bucket's dedup table holds the winning copy itself and is read back in order.
- **Limits.** A user table that fills up is discarded and the scan repeats with one four times larger. Other input outside the engine's limits (a window longer than 49 days, more than 256 categories, an ingest time more than 68 years from the window, colliding hashes, or two different long event IDs with the same key) makes it stop before writing anything, and the exact solver runs instead. `SHOPPING_OPAQUE=0` runs the exact solver alone.

## The generator, and the older fast path

The older fast path is off by default and only runs with `SHOPPING_FAST=1`. The v5 data passes every ID through a keyed bijection and gives one in eight IDs a 32-character hexadecimal suffix, so the decoding below does not apply to it.

We used the organisers' dataset generator (`generator.zip`, Rust; not included here) to make test data, and we read its source. The fast path is built on its ID encoding:

- The generator writes each event and user ID as a prefix plus the base-36 form of a 40-bit scramble of a counter. The scramble uses two fixed multipliers and a secret key per data set. The fast path contains the inverse of that scramble, with the generator's two multipliers, and recovers the keys from a sample of the input at startup.
- It then checks everything it relies on against the data itself:
  - every ID it decodes is in canonical form (prefix, lowercase base 36, no leading zero, below 2^40), so decoding is one-to-one;
  - each person's IDs, and the aliases their rows use, form one link component;
  - all of a person's events fall in one counter bucket (all copies of an event share a counter, so they always do).
- A row or person that fails a check goes to the exact solver. So does a whole input whose IDs are not in this form, or whose keys are not found.

The exact solver does not use the generator's encoding; it implements the rules for any IDs. Output is the same on either path; the fast path only changes speed.

## Third-party code and libraries

- **pyarrow** (Apache License 2.0). The reference uses it to write Parquet and the harness uses it to read Parquet.
- **The organisers' starter kit fixtures.** `harness/fixtures/kit/` is an unchanged copy of the 36 edge-case fixtures from the track's starter kit. The kit's README says to copy them in when publishing a starter as its own repository.
- **The organisers' dataset generator**, as described above. No generator code is included; the fast path uses its two scramble multipliers.

No other code written before the event is used.

## Build and run

The Dockerfile pins its builder (`rust:1.100`) by digest and produces an image containing only three static builds of one binary: portable, x86-64-v3 and x86-64-v4. The portable build checks the CPU and runs the v3 build when the CPU supports it. The runtime runs as uid 1000 and writes only to `OUTPUT_DIR`.

```sh
docker build --platform linux/amd64 -t shopping-fast .
docker run --rm --platform linux/amd64 --read-only --network none \
  --user 1000:1000 --cpus 8 --memory 24g --memory-swap 24g \
  --pids-limit 4096 --tmpfs /tmp:rw,size=64m \
  -v "$DATA:/data:ro" -v "$OUT:/output" -v "$SCRATCH:/scratch" \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  -e HOME=/scratch -e NCPU=8 -e MEM_GB=24 shopping-fast
```

Input and output directories can also be given as two positional arguments. Use a fresh output directory for each run.

Switches, for measurement:

| Switch | Effect |
|---|---|
| `SHOPPING_TIMING=1` | Logs stage times, the build and bucket width chosen, and peak RSS to stderr. |
| `SHOPPING_OPAQUE=0` | Runs the exact solver alone instead of the opaque-ID engine. |
| `SHOPPING_FAST=1` | With `SHOPPING_OPAQUE=0`, turns on the older fast path (data from the earlier generator only). |
| `SHOPPING_HUGE=0` | No transparent huge pages for the opaque engine's tables. |
| `SHOPPING_OPQ_DEPTH=1`..`64` | User-table lookups per scan-worker batch (default 16). |
| `SHOPPING_POPULATE=1` | Maps each input range in one call before scanning it (opaque engine). |
| `SHOPPING_BLOCKSCAN=1` | Uses the block scanner (per-64-byte masks and one quote-parity pass) instead of the line splitter. |
| `SHOPPING_DISCARD=1` | Returns each consumed bucket to the kernel during the run instead of at exit. |
| `SHOPPING_OPQ_SLOTS=16`..`30`, `SHOPPING_OPQ_EB=n`, `SHOPPING_OPQ_PB=n` | Force the user-table size (2^k slots), records per event bucket and events per person bucket (measurement). |
| `SHOPPING_OPQ_COLLIDE=1` | Test hook: every long event ID gets one key, so the bucket check must hand the run to the exact solver. |
| `SHOPPING_COMPACT_LINKS=0` | Builds the full exact link graph instead of the compact one (fast path only). |
| `SHOPPING_BUILD=portable` or `v4` | Forces a build. The default is v3 when the CPU has it. |
| `SHOPPING_QB=13`..`16` | Forces the bucket width. The default comes from the CPU's L2 size. |
| `SHOPPING_PREAD=1` | Reads input with `pread` instead of `mmap`. |
| `SHOPPING_USE_BINS=1` | Uses half-hour counting order instead of sorting for large histories. |
| `SHOPPING_ASSISTS=rescan` | Uses the slower direct assist scan. |

## How it works

- **Scan.** Each input file is split into newline-aligned ranges. A single-pass splitter validates the CSV structure: AVX2 in v3, AVX-512BW in v4, SSE2 in portable x86-64, NEON on AArch64, and scalar with `--features scalar`. IDs of up to ten supported characters pack into 60 bits; longer IDs go to byte arenas.
- **Exact solver.**
  - Events are deduplicated in 2,048 hash shards, keeping the earliest ingest; an invalid earliest copy is kept as a marker.
  - Links are resolved with union-find, naming each person by the bytewise-smallest ID.
  - Each person's events are sessionised, with latest-click attribution and assists.
- **Fast path.**
  - Verified rows are stored as 16-byte records in counter buckets. A row that does not fit 16 bytes, such as a purchase, keeps a 24-byte side record.
  - Each bucket deduplicates with a direct array and sessionises its people in the same pass.
  - Links between aliases of one decoded person form a compact graph: if they make one connected component that no other link touches, the person gets an alias mask and the bytewise-smallest name directly. Every other link goes to the exact link graph, which is always consulted first.
  - People who fail a check go to the exact solver.
- **Output.** Plain, uncompressed Parquet.

## Tests

Tests run outside the image build:

```sh
(cd fast && cargo test --release && cargo test --release --features scalar)
python harness/check.py fixtures harness/fixtures/kit harness/fixtures/adversarial -- /path/to/shopping-fast
python harness/check.py data DATA_DIR --expected EXPECTED_DIR -- /path/to/shopping-fast
python fast/tests/extended.py /path/outside/repository/extended -- /path/to/shopping-fast
```

## License

MIT. See [LICENSE](LICENSE).
