# Workspace Structure

S4 is organized as a Cargo workspace with five crates, each with a clear responsibility.

## Crate Overview

```
s4core/
+-- s4-core/       # Storage engine (volumes, metadata, dedup, crash recovery)
+-- s4-api/        # HTTP API and S3 compatibility (handlers, auth, middleware)
+-- s4-features/   # Advanced features (lifecycle, object lock, IAM)
+-- s4-compactor/  # Background garbage collection and volume compaction
+-- s4-server/     # Main binary, configuration, startup
```

## Dependency Graph

```
s4-server
  +-- s4-api
  |     +-- s4-core
  |     +-- s4-features
  |           +-- s4-core
  +-- s4-compactor
        +-- s4-core
```

## Crate Details

### s4-core

The foundation of S4. Contains the storage engine, metadata indexing, deduplication logic, and crash recovery.

**Key modules:**
- `storage/engine.rs` — `StorageEngine` trait defining the storage API
- `storage/bitcask.rs` — Bitcask-style implementation
- `storage/volume.rs` — Append-only volume file reader/writer
- `storage/index.rs` — fjall metadata index wrapper
- `storage/dedup.rs` — Content-addressable deduplication
- `storage/recovery.rs` — Crash recovery logic
- `types/` — Core data types (`BlobHeader`, `IndexRecord`)
- `error.rs` — Storage error types

### s4-api

The HTTP layer. Handles S3 API compatibility, AWS Signature V4 authentication, and request routing.

**Key modules:**
- `server.rs` — Axum server setup and route registration
- `auth/signature_v4.rs` — AWS Signature V4 verification
- `handlers/bucket.rs` — Bucket operations (create, list, delete)
- `handlers/object.rs` — Object operations (PUT, GET, DELETE, HEAD)
- `handlers/multipart.rs` — Multipart upload
- `handlers/bucket_config.rs` — Versioning, lifecycle, Object Lock config
- `handlers/object_lock.rs` — Object retention and legal hold
- `handlers/admin.rs` — Admin API endpoints
- `handlers/stats.rs` — Metrics and stats endpoints
- `s3/xml.rs` — S3-compatible XML response generation
- `s3/errors.rs` — S3 error codes
- `middleware/` — Auth, metrics, and logging middleware

### s4-features

Business logic for advanced features that build on top of s4-core.

**Key modules:**
- `lifecycle/` — Lifecycle policy types, XML parsing, rule evaluation
- `object_lock/` — Object Lock types and XML handling
- `iam/` — User management, JWT authentication, RBAC

### s4-compactor

Background service for garbage collection and volume compaction.

**Key modules:**
- `compactor.rs` — GC logic for reclaiming space from deleted objects
- `scrubber.rs` — CRC verification for data integrity checks

### s4-server

The main binary that ties everything together.

**Key modules:**
- `main.rs` — Application entry point
- `config.rs` — Configuration loading from environment variables
- `app.rs` — Application initialization (storage, IAM, lifecycle worker)
- `lifecycle_worker.rs` — Background lifecycle policy evaluation
