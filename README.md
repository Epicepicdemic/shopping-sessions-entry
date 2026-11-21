# Shopping Sessions (Databricks)

Entry for the Shopping Sessions track of the Builders Buildathon, November 2026:
a native Rust pipeline that reads the click-stream CSV inputs from `$DATA_DIR`
and writes the two daily Parquet tables to `$OUTPUT_DIR`, as specified in the
track's `rules.md`.

- `fast/src/`: the pipeline (no third-party crates).
- `Dockerfile`: multi-stage build; the runtime image runs `/app/shopping-fast`
  as uid 1000.

## Third-party code and libraries

No third-party crates. Build image `rust:1.88`, runtime image
`debian:bookworm-slim`. No code written before the event and no other team's
code is used.

## License

MIT. See [LICENSE](LICENSE).
