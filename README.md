# Shopping Sessions v5 · Wave 4

This repository is our team's own Wave-4 engine for the Shopping Sessions track: a C++20 program the team wrote for the v5 data, which treats every ID as an opaque string. It replaces the team's earlier Rust entry, which stays in this repository's history. The engine uses only the C++ standard library and the operating system's interfaces. The Docker build compiles it with Alpine Linux's GCC and musl (`build-base`, base image pinned by digest). The Python tools in `proto/tools` and `proto/tests` use pyarrow and duckdb for benchmarks and comparisons. Test data comes from the organisers' v5 generator, which is not included. Paths in `evidence/` logs from the developer's machine are shortened to `<repo>` and `<home>`.

A C++20 CSV-to-Parquet engine for the v5 contract. The engine reads opaque IDs, resolves linked IDs transitively, keeps the first ingest per event ID and writes the two Parquet tables. [Implementation](proto/src/main.cpp) · [Dev, 20M and 60M results](BREAKTHROUGH-V5-W4.md)

## Linux x86-64

The executable accepts two positional directories or the `DATA_DIR` and `OUTPUT_DIR` environment variables. The defaults are `/data` and `/output`. Start each run with an empty output directory. [Static executable](bin/shopping-w4-linux-x86_64) · [Dockerfile](Dockerfile)

```sh
NCPU=8 W4_TIMING=1 bin/shopping-w4-linux-x86_64 work/input work/output
```

```sh
docker build --platform linux/amd64 -t shopping-w4 .
mkdir -p work/output
chmod a+w work/output
docker run --rm --platform linux/amd64 --read-only --network none --user 1000:1000 \
  --cpus 8 --memory 12g --memory-swap 12g --tmpfs /tmp:size=64m \
  -e NCPU=8 -e W4_TIMING=1 \
  -v "$PWD/work/input:/data:ro" \
  -v "$PWD/work/output:/output" shopping-w4
```

The image uses Alpine musl and a static x86-64-v2 executable. The scanner selects AVX-512BW or AVX2 after checking CPU and OS support. ARM64 builds use NEON. The CSV parser also provides a scalar path.

## Native macOS build

```sh
mkdir -p work/bin work/tmp
export TMPDIR="$PWD/work/tmp/"
clang++ -std=c++20 -O3 -DNDEBUG -mcpu=native -flto=thin -pthread \
  proto/src/main.cpp -o work/bin/shopping-w4
NCPU=8 W4_TIMING=1 work/bin/shopping-w4 work/input work/output
```

| Setting | Default | Effect |
|---|---|---|
| `NCPU` | Up to 8 | Worker count, 1–64 |
| `W4_IO` | `mmap` on Linux, `pread` on macOS | Input mode. Also accepts `sequential` and `willneed` for mmap advice |
| `W4_TIMING` | 0 | Phase clocks, peak process RSS, faults and Linux cgroup counters |
| `W4_SCALAR` | 0 | Use the scalar CSV classifier |
| `W4_AVX2` | 0 | Bypass AVX-512 dispatch on x86-64 |
| `W4_EVENT_BITS`, `W4_USER_BITS` | From event input bytes | Override the radix partition widths |
| `W4_CHUNK_KIB` | 2048 | Scan job size in KiB |
| `W4_CELL_THRESHOLD` | 16 | Minimum history size for 30-minute time cells on the compact path |
| `W2_SYNC` | 0 | Sync each Parquet part before close |

## Paired benchmarks and exact checks

Use the official v5 generator. The measured datasets used seed 1 with 1M events and 250K clicks, seed 2005 with 20M and 5M, and seed 6005 with 60M and 15M. Keep one generated dataset at a time.

```sh
mkdir -p work/bin work/tmp
export TMPDIR="$PWD/work/tmp/"
python3 -m venv work/venv
work/venv/bin/pip install --no-cache-dir duckdb pyarrow
../shopping-w3/work/bin/generate --seed 2005 --events 20000000 --clicks 5000000 --output work/input

NCPU=8 work/venv/bin/python -B proto/tools/bench.py work/input \
  --temp-root work/bench --keep-first work/check --pairs 7 \
  --candidate work/bin/shopping-w4 --control ../shopping-w3/work/bin/shopping-w3
work/venv/bin/python -B proto/tools/compare.py work/check/candidate work/check/control --temp-root work/compare
```

The comparator checks both schemas, every keyed field, duplicate keys and cross-table totals. It accepts published CSV answers or reference Parquet. The adversarial tests cover long and embedded-NUL keys, cross-file deduplication, a 32-bit hash collision, cross-year offsets, and a link chain with a links-only canonical ID. [Adversarial input builder](proto/tests/opaque_cases.py)

To run the supplied fixtures, set `CONTROL_REPO` to the team's read-only checkout containing the fixture harness and the byte-oriented Python reference.

```sh
NCPU=8 work/venv/bin/python -B "$CONTROL_REPO/harness/check.py" fixtures \
  "$CONTROL_REPO/harness/fixtures/kit" "$CONTROL_REPO/harness/fixtures/adversarial" \
  -- "$PWD/work/bin/shopping-w4"
```
