# Contributing

Thank you for your interest in contributing to S4. This guide will help you get started.

## Getting Started

1. Fork the repository
2. Clone your fork
3. Create a feature branch: `git checkout -b feature/my-feature`
4. Make your changes
5. Run tests: `cargo test --workspace`
6. Run lints: `cargo clippy --workspace --all-targets -- -D warnings`
7. Format code: `cargo fmt`
8. Commit and push
9. Open a pull request

## Development Environment

### Required Tools

- **Rust 1.70+** — install via [rustup](https://rustup.rs)
- **Git** — version control
- **AWS CLI** (optional) — for manual testing

### Recommended Tools

- **cargo-watch** — auto-rebuild on file changes: `cargo install cargo-watch`
- **jq** — for parsing JSON responses in test scripts

## Project Structure

See the [Workspace Structure](../02-architecture/workspace-structure.md) page for a detailed overview of the crate layout.

## Workflow

### 1. Find or Create an Issue

Before starting work, check existing issues or create a new one describing the change.

### 2. Write Code

Follow the [Code Standards](code-standards.md):

- English-only comments and documentation
- Document all public APIs
- Use proper error types (`thiserror`)
- No `unwrap()` in production code
- Include unit tests

### 3. Write Tests

- Unit tests in the same file: `#[cfg(test)] mod tests { ... }`
- Integration tests in `tests/` directory
- Aim for meaningful test coverage, not just line count

### 4. Run CI Checks Locally

Before pushing, run the full CI check:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Or use the convenience script:

```bash
./scripts/check.sh
```

### 5. Submit a Pull Request

- Write a clear PR title and description
- Reference the related issue
- Ensure all CI checks pass
- Be responsive to review feedback

## Commit Messages

Use clear, descriptive commit messages:

```
Add lifecycle policy background worker

Implements a background worker that periodically evaluates
lifecycle rules and deletes expired objects. Supports dry-run
mode for safe testing.
```

## Questions?

If you have questions about the codebase or architecture, check the [Architecture](../02-architecture/) section or open a discussion on GitHub.
