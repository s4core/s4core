# Code Standards

S4 follows strict coding standards to maintain quality and readability across the team.

## Rust Standards

### Safety

- **`unsafe_code = "forbid"`** — no unsafe Rust code allowed anywhere
- **`missing_docs = "warn"`** — all public APIs must be documented
- No `unwrap()` or `expect()` in production code (only in tests)

### Error Handling

- Use `thiserror` for all error types
- Provide contextual error messages
- Each crate has its own error type (e.g., `StorageError`, `AuthError`)
- Convert between error types via the `From` trait

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Volume not found: {volume_id}")]
    VolumeNotFound { volume_id: u32 },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] redb::Error),
}
```

### Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Files and modules | `snake_case` | `volume_writer.rs`, `signature_v4.rs` |
| Structs, enums, traits | `PascalCase` | `VolumeWriter`, `IndexRecord` |
| Functions | `snake_case` | `write_blob()`, `verify_signature()` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_VOLUME_SIZE`, `DEFAULT_CHUNK_SIZE` |
| Boolean functions | `is_` or `has_` prefix | `is_valid()`, `has_permission()` |

### Documentation

All public items must have documentation comments:

```rust
/// Writes data to append-only volume files.
///
/// Volumes are rotated when they reach the configured size limit.
///
/// # Arguments
///
/// * `header` - Blob header with metadata
/// * `data` - Object data bytes
///
/// # Returns
///
/// Returns the byte offset where the blob was written.
pub async fn write_blob(
    &mut self,
    header: &BlobHeader,
    data: &[u8],
) -> Result<u64, StorageError> {
    // ...
}
```

Documentation rules:

- All comments and documentation in **English only**
- Explain "why", not "what" (the code explains "what")
- Document non-obvious design decisions

## Module Organization

Each module should:

- Have a clear, single responsibility (visible from the name)
- Export only necessary items (`pub` only where needed)
- Include a module-level documentation comment (`//!`)
- Contain unit tests in `#[cfg(test)] mod tests`

## Testing Standards

- Unit tests live in the same file as the code (`#[cfg(test)]`)
- Integration tests live in `tests/` directories
- All tests must be independent (no shared mutable state)
- Tests can use `unwrap()` and `expect()` freely

## Dependencies

- One dependency per task — avoid duplicating functionality
- Use `workspace.dependencies` for shared versions
- Pin explicit versions in `Cargo.toml`
- Minimize the dependency tree

## Code Review Checklist

Before submitting code:

- [ ] Follows naming conventions
- [ ] All `pub` APIs have documentation
- [ ] Unit tests for new functionality
- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy -- -D warnings` passes
- [ ] No `unwrap()`/`expect()` in production code
- [ ] Uses proper error types
- [ ] Code is organized into logical modules

## License Headers

All Rust source files must include the Apache 2.0 license header:

```rust
// Copyright 2026 S4Core Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
```
