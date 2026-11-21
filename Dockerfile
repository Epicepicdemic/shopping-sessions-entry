# Shopping Sessions fast pipeline: two static builds of one zero-dependency
# Rust binary in an otherwise empty image.
#   /app/shopping-fast     portable x86-64 build, the entry point
#   /app/shopping-fast-v3  x86-64-v3 (AVX2, BMI2, FMA, ...) build
# The portable build checks the CPU and re-executes the v3 build when every v3
# feature is present, so an older scorer CPU still runs correct code.
FROM rust:1.100 AS builder

WORKDIR /build
COPY fast/Cargo.toml fast/Cargo.lock /build/
COPY fast/src/ /build/src/

RUN cargo test --release
RUN RUSTFLAGS="-C target-feature=+crt-static" \
        cargo build --release --target x86_64-unknown-linux-gnu --target-dir /build/t-portable \
 && RUSTFLAGS="-C target-feature=+crt-static -C target-cpu=x86-64-v3" \
        cargo build --release --target x86_64-unknown-linux-gnu --target-dir /build/t-v3 \
 && mkdir -p /out \
 && cp /build/t-portable/x86_64-unknown-linux-gnu/release/shopping-fast /out/shopping-fast \
 && cp /build/t-v3/x86_64-unknown-linux-gnu/release/shopping-fast /out/shopping-fast-v3

FROM scratch
COPY --from=builder /out/ /app/
USER 1000:1000
WORKDIR /app
ENTRYPOINT ["/app/shopping-fast"]
