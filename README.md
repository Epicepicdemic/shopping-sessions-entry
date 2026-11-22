# Shopping Sessions v5 · Wave 5

This repository is our team's own Wave-5 engine for the Shopping Sessions track: a C++20 program the team wrote for the v5 data, which treats every ID as an opaque string. It replaces the team's earlier Rust entry, which stays in this repository's history. The image sets `SHOPPING_IO_MODE=pread`, so it reads input with positioned reads; `-e SHOPPING_IO_MODE=mmap` restores memory mapping.

Third-party code: the [mimalloc](https://github.com/microsoft/mimalloc) 2.2.7 allocator (MIT license, Microsoft), downloaded at build time with a pinned SHA-256 checksum and linked statically; its license ships in the image at `/licenses/mimalloc.txt` and in [`proto/third_party`](proto/third_party/mimalloc-LICENSE.txt). The Docker build compiles with Alpine Linux's GCC and musl (pinned package versions; base image pinned by digest). The Python tools in `proto/tools` and `proto/tests` use pyarrow and duckdb for benchmarks and comparisons. Test data comes from the organisers' v5 generator, which is not included.

C++20 CSV-to-Parquet engine for the v5 contract. It deduplicates by earliest ingest, resolves transitive links to the bytewise-minimum identity, and writes `user_daily` and `campaign_daily`. The checked path supports opaque byte strings, wide dates and categories, and variable-width rows. [Results and verification](BREAKTHROUGH-V5-W5.md)

## Linux x86-64 drop-in

Use `bin/shopping-w5-linux-x86_64` as the scorer executable, or build the image from this directory. The executable accepts input and output directories as two positional arguments or via `DATA_DIR` and `OUTPUT_DIR`, which default to `/data` and `/output`. Start with an empty output directory writable by UID 1000.

```sh
mkdir -p work/output
chmod a+w work/output
bin/shopping-w5-linux-x86_64 work/input work/output
```

```sh
docker build --platform linux/amd64 --provenance=false -t shopping-w5 .
docker run --rm --platform linux/amd64 --read-only --network none --user 1000:1000 \
  --cpus 8 --memory 12g --memory-swap 12g -e W5_TIMING=1 \
  --mount "type=bind,src=$PWD/work/input,dst=/data,readonly" \
  --mount "type=bind,src=$PWD/work/output,dst=/output" shopping-w5
```

The static musl executable links the pinned mimalloc object first. A baseline entry point reads CPUID and XGETBV and dispatches the complete engine to x86-64-v4, v3, or v2. AVX-512BW and AVX2 CSV classifiers are both included. Linux uses mapped input by default. The engine reserves two lazily committed pools at setup and reuses the dedup pool for grouped histories. [Dockerfile](Dockerfile) · [Static linkage](evidence/linux-elf.log) · [Instruction excerpts](evidence/linux-instructions.log) · [Allocator license](proto/third_party/mimalloc-LICENSE.txt)

| Setting | Default | Effect |
|---|---|---|
| `NCPU` | Automatic | Linux uses CPU affinity and visible cgroup v1 or v2 CPU quotas, rounded up, with a 64-worker limit. The explicit override accepts 1–64. macOS defaults to up to 8 workers. |
| `SHOPPING_IO_MODE` | Linux `mmap`, macOS `pread` | Supports `pread`, `mmap`, `sequential` and `willneed`. Linux positioned reads use `POSIX_FADV_SEQUENTIAL`. `W4_IO` remains an alias. |
| `SHOPPING_THP` | `0` | Set `1` to advise transparent huge pages on Linux pools. |
| `SHOPPING_SCALAR` | `0` | Set `1` to use the scalar classifier and the baseline x86 pipeline. `W4_SCALAR` remains an alias. |
| `SHOPPING_AVX2` | `0` | Set `1` to bypass AVX-512 dispatch on x86-64. `W4_AVX2` remains an alias. |
| `SHOPPING_BASELINE` | `0` | Set `1` to run the v2 x86 pipeline while retaining SIMD classifier dispatch. |
| `W5_TIMING` | `0` | Emit ISA, worker count, phase clocks, peak process RSS, faults and available Linux cgroup counters. |
| `W4_EVENT_BITS`, `W4_USER_BITS` | From event input bytes | Override the radix partition widths. |
| `W4_CHUNK_KIB` | `2048` | Scan job size in KiB. |
| `W4_CELL_THRESHOLD` | `16` | Minimum history size for 30-minute time cells on the compact path. |
| `W2_SYNC` | `0` | Sync each Parquet part before close. |

## Native build and verification

```sh
mkdir -p work/bin work/tmp
export TMPDIR="$PWD/work/tmp/"
clang++ -std=c++20 -O3 -DNDEBUG -mcpu=native -flto=thin -pthread \
  proto/src/main.cpp -o work/bin/shopping-w5
```

Use the current kit generator with seeds 2005 and 6005 and event/click counts of 20M/5M and 60M/15M. Keep one generated large dataset at a time. Use the published seed-1 input and expected tables for dev. The native comparator requires `duckdb` and `pyarrow`.

```sh
NCPU=8 W4_IO=pread python3 -B proto/tools/bench.py work/input \
  --temp-root work/bench --keep-first work/check --pairs 7 \
  --candidate work/bin/shopping-w5 --control work/bin/shopping-w4
python3 -B proto/tools/compare.py work/check/candidate work/check/control --temp-root work/compare
```

`proto/tools/bench_docker.py` takes matching candidate and control image names and applies the amd64 restrictions above. Its receipts capture process and host wall times, RSS, cgroup peaks, and per-phase clocks. Set `SHOPPING_FAST=0` for the independent Rust control. The comparator checks both schemas, keyed values, duplicate keys and cross-table totals.

With `CONTROL_REPO` set to the supplied checkout, run its fixture harness once in each scanner mode. `proto/tests/opaque_cases.py` creates the additional stress inputs for the supplied Python reference.

```sh
NCPU=8 SHOPPING_SCALAR=0 python3 -B "$CONTROL_REPO/harness/check.py" fixtures \
  "$CONTROL_REPO/harness/fixtures/kit" "$CONTROL_REPO/harness/fixtures/adversarial" \
  -- "$PWD/work/bin/shopping-w5"

clang++ -std=c++20 -O1 -gline-tables-only -mcpu=native -pthread \
  -fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer \
  -DSHOPPING_CLEAN_EXIT proto/src/main.cpp -o work/bin/shopping-w5-sanitized
```

Sanitizer builds retain normal C++ teardown. Production closes both Parquet outputs, flushes C streams, and uses `_exit(0)` to reclaim the pools and other transient state together.
