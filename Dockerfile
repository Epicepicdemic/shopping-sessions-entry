# Shopping Sessions fast pipeline: three static builds of one zero-dependency
# Rust binary in an otherwise empty image.
#   /app/shopping-fast     portable x86-64 build, the entry point
#   /app/shopping-fast-v3  x86-64-v3 (AVX2, BMI2, FMA, ...) build
#   /app/shopping-fast-v4  x86-64-v4 (AVX-512) build
# The portable build checks the CPU and re-executes the widest build whose
# every feature is present, so an older scorer CPU still runs correct code.
#
# The builder is pinned by digest so a rebuild uses the same toolchain:
# rust:1.100, image index sha256:223cc32d... (linux/amd64 manifest
# sha256:ebc54bef33ec8f9a5cd05799366010f35efbb73b354fb4f2cb086ec6b9460ef5,
# created 2026-11-12). Tests run outside the image build (cargo test, and the
# harness against the reference); see README.md.
FROM rust:1.100@sha256:223cc32ddca8f3ee90c436dbc94b6b5f052ea7f9653c5201e9612f08ad7d21b8 AS builder

WORKDIR /build
COPY fast/Cargo.toml fast/Cargo.lock /build/
COPY fast/src/ /build/src/

# Each build is one codegen unit with LTO, so mostly one core: run all three
# at once. `wait PID` returns that build's status, so any failure fails the step.
RUN set -e; \
    build() { RUSTFLAGS="-C target-feature=+crt-static $2" \
        cargo build --release --offline --target x86_64-unknown-linux-gnu --target-dir "/build/t-$1"; }; \
    build portable "" & p1=$!; \
    build v3 "-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq -C llvm-args=-inline-threshold=10000" & p2=$!; \
    build v4 "-C target-cpu=x86-64-v4 -C target-feature=+pclmulqdq -C llvm-args=-inline-threshold=10000" & p3=$!; \
    wait $p1; wait $p2; wait $p3; \
    mkdir -p /out; \
    cp /build/t-portable/x86_64-unknown-linux-gnu/release/shopping-fast /out/shopping-fast; \
    cp /build/t-v3/x86_64-unknown-linux-gnu/release/shopping-fast /out/shopping-fast-v3; \
    cp /build/t-v4/x86_64-unknown-linux-gnu/release/shopping-fast /out/shopping-fast-v4

FROM scratch
COPY --from=builder /out/ /app/
USER 1000:1000
WORKDIR /app
ENTRYPOINT ["/app/shopping-fast"]
