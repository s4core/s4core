# Building and Testing

## Prerequisites

- Rust 1.70 or later (install via [rustup](https://rustup.rs))
- Git

## Build Commands

```bash
# Check compilation (fast, no binary output)
cargo check --workspace

# Build debug binary
cargo build --workspace

# Build release binary (optimized)
cargo build --release
```

The release binary is at `target/release/s4-server`.

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
