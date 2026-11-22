# Shopping Sessions v5 · Wave 5

**The Real Problem Solvers · Ohio State Builders Buildathon**

Wave-5 implements the five Linux changes from the live audit and exports a static musl amd64 executable. It matched the published dev answer and Wave-4 and the independent Rust control at 20M and 60M. The paired Apple M5 Pro medians were **0.036521 s dev**, **0.495155 s at 20M**, and **2.529683 s at 60M**. The dev target passed. The large targets remain open. Wave-5 has no measured native x86-64 leaderboard time or Finals retime. [Executable](bin/shopping-w5-linux-x86_64) · [Drop-in instructions](README.md) · [Dockerfile](Dockerfile)

The live feedback reported **1.4163 s** for Wave-4 with `pread`, **1.1488 s** for Rust `5051166`, a **0.724 s** top-30 cutoff and a **0.4293 s** leader. The Linux default is mapped input. The macOS and Rosetta results below measure their respective local environments.

## Paired Apple M5 Pro results

Both engines used eight workers, positioned reads and the same APFS input. Runs alternated order and discarded the first pair. Wall time includes launch and exit. Peak RSS is the largest measured value in each cohort. The historical Wave-4 medians were 0.047745 s, 0.405207 s and 1.551688 s. The rebuilt controls below capture host conditions during Wave-5 testing. [Host and binary hashes](evidence/environment.log)

| Dataset | Pairs | Wave-4 wall | Wave-5 wall | Median paired speedup | W4 peak RSS | W5 peak RSS | Target |
|---|---:|---:|---:|---:|---:|---:|---|
| Published dev, seed 1 | 11 | 0.036975 s | **0.036521 s** | 0.994× | 106.6 MiB | 105.3 MiB | <0.040 s · met |
| 20M, seed 2005 | 9 | 0.508902 s | **0.495155 s** | 1.028× | 1.409 GiB | 1.245 GiB | <0.34 s · missed |
| 60M, seed 6005 | 7 | 2.605735 s | **2.529683 s** | 1.111× | 3.866 GiB | 3.711 GiB | <1.25 s · missed |

[Dev trials](evidence/native-dev-pairs.jsonl) · [20M trials](evidence/native-20m-pairs.jsonl) · [60M trials](evidence/native-60m-pairs.jsonl) · [Native wall-time chart](evidence/native-wall-time.html)

| Median phase wall time, ms | Dev W4 | Dev W5 | 20M W4 | 20M W5 | 60M W4 | 60M W5 |
|---|---:|---:|---:|---:|---:|---:|
| Setup | 6.819 | 5.261 | 8.960 | 7.206 | 8.709 | 5.859 |
| CSV scan and scatter | 12.038 | 12.613 | 252.370 | 249.720 | 1425.215 | 1277.227 |
| Event deduplication and link endpoint interning | 2.455 | 2.696 | 54.205 | 53.512 | 468.243 | 440.978 |
| Resolve links and retire event index | 0.719 | 0.896 | 12.711 | 13.809 | 55.968 | 55.485 |
| Group aliases and route linked histories | 2.829 | 2.888 | 70.290 | 65.508 | 351.700 | 314.397 |
| Session reduction and user Parquet | 5.093 | 5.243 | 84.550 | 83.211 | 315.671 | 300.202 |
| Campaign Parquet | 1.698 | 1.765 | 1.569 | 1.607 | 1.869 | 3.808 |

Wave-4 profiling found `pread`, dedup probing, timestamp parsing and radix first-touch stores among the most sampled instructions or call sites. On the 60M paired cohort the scan median fell **148.0 ms**, grouping fell **37.3 ms**, and peak RSS fell **158.5 MiB**. Each phase has its own median. [Wave-4 time profile and instruction offsets](evidence/w4-hotspots.log)

## Restricted Linux amd64 verification

Both images ran in a local ARM64 Linux VM translating amd64 through Rosetta. The containers had eight CPU equivalents, a read-only root and input mount, no network, UID 1000, a 12 GiB memory limit, and zero cgroup swap. The measured Wave-5 path selected x86-64-v3 and AVX2. The v4 instruction receipts verify the shipped AVX-512BW code. [Image and guest platform](evidence/docker-60m-images.log) · [Build](evidence/docker-revised-build.log) · [ELF and allocator symbols](evidence/linux-elf.log) · [Disassembly](evidence/linux-instructions.log)

| Dataset · mapped input | Pairs | W4 engine wall | W5 engine wall | W4 host wall | W5 host wall | W4 peak cgroup | W5 peak cgroup |
|---|---:|---:|---:|---:|---:|---:|---:|
| Dev | 5 | 0.148040 s | 0.152827 s | 0.289716 s | 0.310045 s | 0.210 GiB | 0.208 GiB |
| 20M | 5 | 4.111908 s | 4.512809 s | 4.514300 s | 4.969395 s | 3.201 GiB | 3.337 GiB |
| 60M | 3 | 26.246940 s | 20.640588 s | 28.104958 s | 21.914601 s | 9.467 GiB | 9.806 GiB |

[Dev trials](evidence/docker-dev-final-pairs.jsonl) · [20M trials](evidence/docker-20m-final-pairs.jsonl) · [60M trials](evidence/docker-60m-final-pairs.jsonl)

The 60M mapped Wave-5 engine median fell **21.4%**. Its largest cgroup peak in that cohort was **10,528,919,552 bytes**, leaving **2.194 GiB** under the test cap. Median group time fell from **1,959 ms to 1,083 ms**.

| Same-engine comparison | Control | Alternative | Median paired speedup |
|---|---:|---:|---:|
| APFS 20M · `pread` vs mapped `sequential` | 0.599 s | 0.623 s | 0.951× |
| Docker 60M · `mmap` vs `pread` | 17.922 s | 17.090 s | 1.049× |
| Docker 60M · THP off vs on | 21.655 s | 25.232 s | 0.895× |
| Docker 60M · `mmap` vs mapped `sequential` | 16.008 s | 16.587 s | 0.965× |

Linux positioned reads cut the 60M process RSS from **9.535 GiB to 3.783 GiB** and median major faults from **8,704 to 23**. The cgroup peak was about **9.81 GiB** in each mode because cached input still counted. The translated 20M positioned-read trial was slower. Linux ships with mapped input and THP advice disabled. Native scorer I/O and huge-page timings remain open. [APFS I/O](evidence/native-20m-io.jsonl) · [Docker 20M I/O](evidence/docker-20m-io.jsonl) · [60M I/O](evidence/docker-60m-io.jsonl) · [60M THP](evidence/docker-60m-thp.jsonl) · [60M mapping advice](evidence/docker-60m-mmap-sequential.jsonl)

## Linux audit fixes

1. **Mapped pools and teardown.** Two large anonymous pools replace per-extent mappings and alignment trims. Workers bump-allocate extents and reuse the dead dedup pool for histories. Production keeps storage through reduction, checks Parquet close, flushes streams and calls `_exit(0)`. Sanitizer and `SHOPPING_CLEAN_EXIT` builds return through normal destruction. [Pool and cleanup](proto/src/radix.hpp)
2. **Static allocator.** The pinned mimalloc object supplies `malloc`, `calloc`, `realloc`, `free` and C++ `new` in the exported ELF. Disassembly verifies its thread-local fast path. Allocator contention on native x86-64 remains unmeasured. [Linkage and allocator aliases](evidence/linux-elf.log) · [Microsoft mimalloc static linking](https://github.com/microsoft/mimalloc/tree/v2.2.7#static-override)
3. **Full x86 dispatch.** The baseline CPUID/XGETBV shim selects independent v2, v3 and v4 builds of the entire engine. `tzcnt`, `blsr`, `shrx` and `bzhi` appear in both the v3 and v4 checked event splitters. The classifier loops use 32-byte YMM and 64-byte ZMM comparisons. Dedup uses 16-lane AVX2 and AVX-512BW tag probes with full-key checks. [Dispatch](proto/src/dispatch.cpp) · [Instruction excerpts](evidence/linux-instructions.log)
4. **CPU allocation.** Linux derives the automatic workers from affinity and visible cgroup v1 or v2 quota ceilings, up to 64. Restricted v2 tests selected 3 workers for 2.5 CPU equivalents and 2 for a two-CPU affinity mask. `NCPU=10` overrode automatic sizing, and the 64-worker boundary run matched the reference. [Worker selection](evidence/linux-worker-selection.log) · [64-worker exactness](evidence/workers-64-exact.log)
5. **Opaque keys and I/O.** A sampled 87.47% of 20M event-ID spellings fit the literal representation. Another 12.51% used the canonical 33-byte lowercase hex spelling. The SIMD path encodes those losslessly in 16 bytes and reconstructs exact spelling at output. The short opaque path allocates pages incrementally and caches repeated user IDs. Linux defaults to `mmap` with positioned reads and mapping advice available through `SHOPPING_IO_MODE`. [ID sample](evidence/id-spellings.log) · [Key implementation](proto/src/keys.hpp)

The checked scanner narrows delimiter extraction to two word windows when row geometry permits, caches exact Gregorian dates, parses short decimal fields with SWAR, and keeps the variable-width parser for fallback. User groups store two exclusive ends, omit unlinked click-only identities, and use an unsorted one-session reduction for connected small histories. [Scanner](proto/src/input.hpp) · [Grouping and reduction](proto/src/main.cpp)

GCC 15.2 with LTO remains in the Dockerfile. The 20M translated compiler trials compared bare musl, Clang 21, GCC with RTTI and explicit frame-pointer omission, and GCC PGO derived from a dev subset. PGO's paired v3 speedup was 1.096× in one noisy cohort. The v4 branch was untrained in this VM, so the shipped compile uses the same non-PGO flags for all three pipelines. Exceptions remain enabled for input, worker and output errors. [Bare allocator comparison](evidence/compiler-bare-20m.jsonl) · [Clang](evidence/compiler-clang-20m.jsonl) · [GCC flags](evidence/compiler-rtti-20m.jsonl) · [PGO trial](evidence/compiler-pgo-20m.jsonl)

## Exactness and cleanup

Both output schemas, all keyed values, duplicate keys and cross-table totals matched. The supplied independent Rust control ran with `SHOPPING_FAST=0` from commit `0c01719ae433ba97271b46352488b50a1e0221c6`.

| Dataset | Reference | User rows | Campaign rows | Sessions | Revenue cents |
|---|---|---:|---:|---:|---:|
| Dev | Published answer | 118,107 | 27,449 | 181,032 | 352,767,593 |
| 20M | Wave-4 and Rust opaque-ID control | 2,218,605 | 28,014 | 3,356,800 | 6,371,532,161 |
| 60M | Wave-4 and Rust opaque-ID control | 6,627,110 | 28,014 | 10,014,286 | 20,105,187,622 |

[Native dev](evidence/native-dev-exact.log) · [Native 20M Wave-4](evidence/native-20m-wave4-exact.log) · [Native 20M Rust](evidence/native-20m-rust-exact.log) · [Native 60M Wave-4](evidence/native-60m-wave4-exact.log) · [Native 60M Rust](evidence/native-60m-rust-exact.log) · [Docker dev](evidence/docker-dev-exact.log) · [Docker 20M Wave-4](evidence/docker-20m-wave4-exact.log) · [Docker 20M Rust](evidence/docker-20m-rust-exact.log) · [Docker 60M Wave-4](evidence/docker-60m-wave4-exact.log) · [Docker 60M Rust](evidence/docker-60m-rust-exact.log)

All **55 supplied fixtures** passed in both native scanner modes. The opaque-ID, wide-date/category and boundary datasets matched the byte-oriented Python reference under ASan and UBSan in both modes. The Linux ARM64 runs also enabled LeakSanitizer. Cases cover a 600 KB ID, case-sensitive packed hex IDs, exact tag collisions, invalid earliest copies, New Year offsets and transitive links with tied clicks. [SIMD fixtures](evidence/fixtures-simd.log) · [Scalar fixtures](evidence/fixtures-scalar.log) · [Sanitizer receipts](evidence/sanitizer-summary.log)

The 20M dataset and outputs were removed before the 60M generation. The 60M dataset, all temporary outputs, and the isolated test VM were removed after verification. **Zero generated CSV or Parquet files remain.** `df -h /` reported **21Gi available**. [20M cleanup](evidence/cleanup-20m.log) · [Final disk receipt](evidence/cleanup-final.log)
