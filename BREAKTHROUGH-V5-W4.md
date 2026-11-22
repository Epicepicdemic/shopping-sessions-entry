# Shopping Sessions v5 · Wave 4

Wave-4 reached **0.405207 s at 20M** and **1.551688 s at 60M** in paired native runs. The supplied Wave-3 binary measured **0.453058 s** and **1.892875 s** in those same cohorts. The static Linux x86-64 build matched the complete 60M reference and its largest observed Docker cgroup peak was **9.466 GiB**. Native Linux x86-64 scorer time and leaderboard rank remain unmeasured. [Implementation](proto/src/main.cpp) · [Static executable](bin/shopping-w4-linux-x86_64) · [Reproduction](README.md)

## Paired native benchmarks

The Apple M5 Pro host ran both engines with eight workers, positioned reads, and fresh Parquet output directories. The input was on APFS for the table below. Each cohort alternated engine order and discarded its first pair as warm-up. Wall time includes process launch and exit. Peak RSS is the largest measured process RSS per engine. Wave-3 previously reported **0.842517 s** at 20M and **3.612317 s** at 60M on this Mac under its earlier test conditions. [Host and binary hashes](evidence/environment.log) · [Wave-3 report](../shopping-w3/BREAKTHROUGH-V5.md)

| v5 dataset | Measured pairs | Wave-3 median | Wave-4 median | Wave-4 best | Median paired speedup | Wave-3 peak RSS | Wave-4 peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|
| Published dev, seed 1 | 7 | 0.050322 s | **0.047745 s** | 0.033611 s | 1.054× | 124.812 MiB | 105.672 MiB |
| 20M events, seed 2005 | 7 | 0.453058 s | **0.405207 s** | 0.386976 s | 1.108× | 1.726 GiB | 1.359 GiB |
| 60M events, seed 6005 | 5 | 1.892875 s | **1.551688 s** | 1.416785 s | 1.161× | 4.724 GiB | 3.932 GiB |

[Dev trials](evidence/final-dev-pairs.jsonl) · [20M trials](evidence/final-20m-pairs.jsonl) · [60M trials](evidence/apfs-60m-pairs.jsonl) · [Unrounded memory receipts](evidence/measurements-summary.log)

| Median phase wall time, ms | Dev W3 | Dev W4 | 20M W3 | 20M W4 | 60M W3 | 60M W4 |
|---|---:|---:|---:|---:|---:|---:|
| Setup | 6.428 | 6.421 | 6.275 | 6.199 | 7.554 | 7.376 |
| CSV scan and scatter | 17.788 | 16.662 | 221.929 | 203.371 | 797.447 | 791.786 |
| Event deduplication and link endpoint interning | 4.806 | 3.766 | 62.742 | 45.314 | 309.336 | 165.102 |
| Resolve linked components and release event index | 1.172 | 1.114 | 11.433 | 11.422 | 48.111 | 44.987 |
| Group aliases and route link histories | 3.978 | 4.627 | 58.263 | 55.977 | 342.382 | 238.645 |
| Session reduction and user Parquet | 6.444 | 7.100 | 60.171 | 63.990 | 232.281 | 248.245 |
| Campaign Parquet | 1.735 | 1.600 | 1.140 | 1.151 | 1.564 | 1.931 |

Wave-4 routes records by alias during CSV scanning. Wave-3 routes surviving events during deduplication. Wave-4 also releases the raw alias buffers during its reduction phase. Each row has its own median. The 60M deduplication and grouping phases fell 47% and 30%, respectively. The paired reduction phase was slower at both large sizes. The scan targets of 180 ms and 700 ms, the 20M grouping target of 50 ms, and the 60M reduction target of 240 ms remain open. [Phase receipts](evidence/measurements-summary.log) · [Phase chart](evidence/native-phase-comparison.html)

## Docker x86-64

Both images were built from their respective Dockerfiles with Alpine GCC and static musl. The local test VM ran ARM64 Linux with Rosetta, eight vCPUs and virtiofs input. Each run used a read-only container, a read-only input mount, UID 1000, no network, a **12 GiB** cgroup limit and zero cgroup swap. The times below use the engine clock inside the container. [Build and platform](evidence/final-docker-build.log) · [VM properties](evidence/environment.log) · [ELF and AVX-512 instructions](evidence/linux-elf.log)

| Dataset and input mode | Pairs | W3 engine wall | W4 engine wall | W3 peak RSS | W4 peak RSS | W3 cgroup peak | W4 cgroup peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| Dev, APFS, mmap | 2 | 0.284187 s | 0.348387 s | 0.230 GiB | 0.208 GiB | 0.233 GiB | 0.210 GiB |
| 20M, APFS, mmap | 2 | 3.076243 s | 2.836243 s | 3.519 GiB | 3.186 GiB | 3.533 GiB | 3.199 GiB |
| 60M, APFS, mmap | 1 | 20.533169 s | 24.665732 s | 10.348 GiB | 9.429 GiB | 10.386 GiB | 9.464 GiB |
| 60M, RAM-backed HFS Plus, mmap | 2 | 55.501410 s | 73.850506 s | 10.358 GiB | 9.430 GiB | 10.394 GiB | 9.465 GiB |
| 60M, RAM-backed HFS Plus, pread | 1 | 52.256725 s | 44.933467 s | 4.585 GiB | 3.662 GiB | 10.391 GiB | **9.466 GiB** |

The largest Wave-4 peak was **10,163,564,544 bytes**. On the emulator, Wave-4’s default mmap path was slower at 60M despite lower memory use. Its RAM-backed pread trial reduced major faults from roughly 13,000 with mmap to 24, and its engine wall time was faster in that separate paired trial. These are local emulation results. The deployed binary selected AVX2 in the VM. The exported executable also passed the published dev answer with the scalar scanner enabled. [Dev trials](evidence/docker-dev-pairs.jsonl) · [20M trials](evidence/docker-20m-pairs.jsonl) · [APFS 60M pair](evidence/docker-apfs-60m-pair.jsonl) · [RAM-backed mmap pairs](evidence/docker-60m-pairs.jsonl) · [RAM-backed pread pair](evidence/docker-60m-pread-pair.jsonl) · [Exported binary scalar check](evidence/static-scalar-dev-exact.log)

## Exact verification

Each check compared the Parquet schemas and every value by key, with zero missing rows, extra rows, differing rows or duplicate keys. The comparator checked cross-table session and revenue totals and the assist invariant.

| Dataset | Reference | User rows | Campaign rows | Sessions | Revenue cents |
|---|---|---:|---:|---:|---:|
| Dev, seed 1 | Published v5 answer | 118,107 | 27,449 | 181,032 | 352,767,593 |
| 20M, seed 2005 | Wave-3 and Rust opaque-ID control | 2,218,605 | 28,014 | 3,356,800 | 6,371,532,161 |
| 60M, seed 6005 | Wave-3 and Rust opaque-ID control | 6,627,110 | 28,014 | 10,014,286 | 20,105,187,622 |

The independent Rust control came from the supplied `0c01719ae433ba97271b46352488b50a1e0221c6` checkout and ran with `SHOPPING_FAST=0`. Both the native and Docker builds passed full comparisons. [Dev](evidence/final-dev-exact.log) · [20M vs Wave-3](evidence/final-20m-paired-exact.log) · [20M vs Rust](evidence/final-20m-reference-exact.log) · [60M vs Wave-3](evidence/apfs-60m-paired-exact.log) · [60M vs Rust](evidence/apfs-60m-reference-exact.log) · [Docker dev](evidence/docker-dev-exact.log) · [Docker 20M](evidence/docker-20m-reference-exact.log) · [Docker 60M](evidence/docker-apfs-60m-exact.log)

All **55 supplied fixtures** passed with both the default and scalar scanners. Three additional datasets matched the byte-oriented Python reference under combined AddressSanitizer and UndefinedBehaviorSanitizer in both NEON/pread and scalar/mmap configurations. The cases include a 600 KB ID, an exact fingerprint collision, earliest invalid copies across files, wide dates and categories, a link chain with 310 intermediate IDs, cross-year lookback offsets and tied clicks. [Default fixtures](evidence/final-fixtures.log) · [Scalar fixtures](evidence/scalar-fixtures.log) · [Opaque](evidence/opaque-sanitized-neon.log) · [Wide](evidence/wide-sanitized-neon.log) · [Boundaries](evidence/boundaries-sanitized-neon.log) · [Scalar opaque](evidence/opaque-sanitized-scalar.log) · [Scalar wide](evidence/wide-sanitized-scalar.log) · [Scalar boundaries](evidence/boundaries-sanitized-scalar.log)

## Architecture and profiling

- A **12-byte exact event-ID reference** and a **16-byte user-side event** replace the full dedup record followed by a second event scatter. Four-lane fingerprint probes fit the per-shard table into a smaller working set. Matching fingerprints still compare the complete key. Deduplication invalidates losing rows in place, including when the earliest copy fails validation. [Event pipeline](proto/src/main.cpp) · [Radix storage](proto/src/radix.hpp) · [Opaque keys](proto/src/keys.hpp)
- The scanner validates quote boundaries with bit masks and prefix parity. It extracts and counts the six event delimiters together for common row lengths, and caches adjacent calendar years. Malformed batches and long rows use checked parsing. AVX-512BW, AVX2, NEON and scalar classifiers share that parser. The original Time Profiler trace sampled the Wave-3 field splitter 516 times, the general timestamp parser 303 times and `pread` 518 times. The Wave-4 CPU Counters trace measured a 24.82% cycle-weighted discarded fraction, which includes branch misprediction and pipeline restarts. [Scanner](proto/src/input.hpp) · [Time samples](evidence/final-time-profile-summary.log) · [Cycle counters](evidence/cpu-counter-summary.log)
- Only active linked aliases emit cross-partition histories. A bytewise-minimum union finds the canonical spelling even when that ID has no events or clicks. Connected single-session histories can sweep unsorted clicks while applying the exact recency and ID tie-break. One dictionary run per person reduces user Parquet encoding work and bytes. Across the native runs, median total Parquet output dropped from 61,952,951 to 59,907,950 bytes at 20M and from 182,957,533 to 175,947,204 bytes at 60M. [Identity resolution](proto/src/main.cpp) · [Reduction](proto/src/aggregate.hpp) · [Parquet writer](proto/src/parquet.hpp)
- Eager canonical lookup passed 20M exactness and raised the paired median from 0.470716 s to 0.849863 s as lookup work shifted into the scanner. In-place linked-list grouping increased reduction time, so the final build retains contiguous per-alias histories. Paired results for write combining, software prefetch, time-only click sorting and file striping varied or regressed. [Eager routing](evidence/direct-20m-pairs.jsonl) · [In-place grouping](evidence/chains-20m-pairs.jsonl) · [Write combining](evidence/apfs-60m-staged-pairs.jsonl) · [Prefetch](evidence/apfs-60m-prefetch-pairs.jsonl) · [Click sorting](evidence/apfs-60m-clicksort-pairs.jsonl) · [File ordering](evidence/native-60m-striped-pairs.jsonl)

The first 60M generation outgrew the available APFS space. The partial input was removed and regenerated on a local RAM disk. Once disk space returned, the same input moved to APFS one file at a time for the primary benchmark. Each dataset was removed before the next was generated. [60M transfer](evidence/60m-migration.log) · [Final cleanup](evidence/cleanup-final.log)
