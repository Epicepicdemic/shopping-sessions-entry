# Multi-stage build for Shopping Sessions fast pipeline.
# Stage 1 compiles the zero-dependency Rust binary with full LTO.
FROM rust:1.88 AS builder

WORKDIR /build
COPY fast/Cargo.toml /build/Cargo.toml
COPY fast/src/ /build/src/

RUN cargo test && cargo build --release && strip /build/target/release/shopping-fast

# Stage 2: minimal Debian runtime matching rust:1.88's glibc, configured for uid 1000.
FROM debian:bookworm-slim

RUN useradd --uid 1000 --no-create-home --home-dir /scratch app

COPY --from=builder /build/target/release/shopping-fast /app/shopping-fast

USER 1000:1000
WORKDIR /app
CMD ["/app/shopping-fast"]
