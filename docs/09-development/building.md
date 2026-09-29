# Building and Testing

## Prerequisites

- Rust 1.70 or later (install via [rustup](https://rustup.rs))
- Git
- `protoc` (protobuf compiler) for the cluster gRPC code generation
- `nasm` 2.14 or newer, on x86_64 only

`nasm` assembles the vendored Intel ISA-L sources in `ee/s4-codec-isal`, which
back the accelerated erasure coding codec. Any command that covers the whole
workspace builds that crate, so `nasm` is needed even without the `isal`
feature. On aarch64 the crate compiles to nothing and `nasm` is not used.

```bash
# Debian / Ubuntu
apt-get install -y nasm protobuf-compiler
```

## Build Commands

```bash
# Check compilation (fast, no binary output)
cargo check --workspace

# Build debug binary
cargo build --workspace

# Build release binary (optimized)
cargo build --release

# Enterprise Edition
cargo build --release --features enterprise

# Enterprise Edition with ISA-L accelerated erasure coding (x86_64 only).
# `isal` implies `enterprise`.
cargo build --release --features isal
```

The release binary is at `target/release/s4-server`.

## Benchmarks

```bash
# Portable codec baseline, all six EC profiles
cargo bench -p s4-codec

# Portable engine against ISA-L, in one process
cargo bench -p s4-codec-isal
```

Recorded results live next to the plan that produced them, in
`plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/`.

## Running Tests

```bash
# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p s4-core
cargo test -p s4-api
cargo test -p s4-features

# Run specific integration tests
cargo test --workspace --test crash_recovery
cargo test -p s4-core --test deduplication
cargo test -p s4-api --test api_integration
cargo test -p s4-api --test signature_v4_integration
cargo test -p s4-api --test versioning_integration
cargo test -p s4-api --test lifecycle_integration
cargo test -p s4-api --test object_lock_integration
cargo test -p s4-api --test metrics_integration
```

## Code Quality

```bash
# Format code
cargo fmt

# Check formatting (CI mode — fails if not formatted)
cargo fmt --check

# Run linter
cargo clippy --workspace --all-targets -- -D warnings
```

## Full CI Check

Run all checks at once (same as CI pipeline):

```bash
./scripts/check.sh
```

This runs: `cargo fmt --check`, `cargo clippy`, and `cargo test --workspace`.

## Running the Server

```bash
# Development mode (default settings)
cargo run --bin s4-server

# With custom settings
S4_ACCESS_KEY_ID=mykey S4_SECRET_ACCESS_KEY=mysecret cargo run --bin s4-server

# Release mode
./target/release/s4-server
```

The server listens on `http://127.0.0.1:9000` by default.

## Testing Environment Variables

These variables are intended for development and CI testing only. They are **not** for production use.

| Variable | Description |
|---|---|
| `S4_COMPACTION_MULTIPART_TTL_SECS` | Overrides the compactor's multipart session TTL in seconds. When set, takes priority over `S4_MULTIPART_UPLOAD_TTL_HOURS` for the compactor only. Useful for testing abandoned multipart upload cleanup without waiting 24 hours. Example: `S4_COMPACTION_MULTIPART_TTL_SECS=1` expires sessions after 1 second. |
