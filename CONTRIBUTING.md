# Contributing to S4

Thank you for your interest in contributing to S4! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Git
- Linux or macOS (Windows support is experimental)

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/s4.git
   cd s4
   ```
3. Build the project:
   base check
   ```bash
   cargo check --workspace
   ```

   ```bash
   cargo build
   
   ```
4. Run tests:
   ```bash
   cargo test
   ```

## Code Standards

### Naming Conventions

- **Modules**: `snake_case` (e.g., `volume_writer.rs`)
- **Types**: `PascalCase` (e.g., `VolumeWriter`, `IndexRecord`)
- **Functions**: `snake_case` (e.g., `write_blob()`, `create_bucket()`)
- **Constants**: `SCREAMING_SNAKE_CASE` (e.g., `MAX_VOLUME_SIZE`)

### Documentation

- All `pub` functions, structs, and traits **must** have documentation
- Use English for all comments and documentation
- Follow Rust documentation conventions with `///` for items and `//!` for modules

Example:
```rust
/// Writes a blob to the current volume.
///
/// # Arguments
///
/// * `header` - Blob header
/// * `key` - Object key
/// * `data` - Blob data
///
/// # Returns
///
/// Returns the volume ID and offset where the blob was written.
pub async fn write_blob(
    &mut self,
    header: &BlobHeader,
    key: &str,
    data: &[u8],
) -> Result<(u32, u64), StorageError> {
    // ...
}
```

### Error Handling

- Use `thiserror` for error types
- Never use `unwrap()` or `expect()` in production code (only in tests)
- Provide context in error messages

### Testing

- Write unit tests for each module (in `#[cfg(test)]` blocks)
- Write integration tests in `tests/` directory
- All tests must be independent and deterministic
- Aim for high test coverage

### Code Formatting

Run before committing:
```bash
cargo fmt
cargo clippy -- -D warnings
```

## Project Structure

The project is organized as a Cargo workspace:

- `s4-core/` - Core storage engine
- `s4-api/` - HTTP API and S3 compatibility layer
- `s4-features/` - Advanced features (atomic ops, extended metadata)
- `s4-compactor/` - Background compaction utility
- `s4-server/` - Main binary entry point

## Development Workflow

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes following the code standards

3. Write or update tests

4. Ensure all checks pass:
   ```bash
   cargo fmt --check
   cargo clippy -- -D warnings
   cargo test
   cargo doc --no-deps
   ```

5. Commit your changes:
   ```bash
   git commit -m "Add feature: description"
   ```

6. Push to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

7. Create a Pull Request on GitHub

## Pull Request Process

1. Ensure your PR addresses an existing issue or describes the problem it solves
2. Update documentation if needed
3. Add tests for new functionality
4. Ensure CI passes (GitHub Actions will run automatically)
5. Request review from maintainers

## Code Review Checklist

Before submitting a PR, ensure:

- [ ] Code follows naming conventions
- [ ] All `pub` APIs have documentation
- [ ] Unit tests are included for new functionality
- [ ] `cargo clippy -- -D warnings` passes
- [ ] `cargo fmt --check` passes
- [ ] No `unwrap()` or `expect()` in production code
- [ ] Error types are used correctly
- [ ] Code is organized into logical modules

## Communication

- Use English for all code, comments, and documentation
- Be respectful and constructive in discussions
- Ask questions if something is unclear

## Questions?

If you have questions, please:
- Open an issue for bug reports or feature requests
- Check existing documentation first
- Ask in discussions if available

Thank you for contributing to S4!
