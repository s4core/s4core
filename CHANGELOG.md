# Changelog

All notable changes to S4 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Core storage engine with append-only volumes (Bitcask-style)
- Metadata indexing with redb (ACID transactions)
- Content-addressable deduplication (SHA-256)
- S3 API compatibility (AWS Signature V4)
- Bucket operations (create, list, delete)
- Object operations (PUT, GET, DELETE, HEAD, copy)
- Multipart upload support
- Object versioning with delete markers
- Lifecycle policies (object expiration, noncurrent version cleanup)
- Basic Object Lock (retention periods)
- CORS configuration
- TLS/HTTPS support
- IAM and Admin API with RBAC (Reader, Writer, SuperUser roles)
- JWT authentication for Admin API
- Prometheus metrics endpoint (`/metrics`)
- JSON stats endpoint (`/api/stats`)
- Crash recovery from volume headers
- Third-party tool compatibility (AWS CLI, rclone, restic)

### Infrastructure
- Cargo workspace structure (s4-core, s4-api, s4-features, s4-compactor, s4-server)
- Comprehensive test suite (unit and integration tests)
- Manual test scripts for feature validation
- CI-ready code quality checks (fmt, clippy, tests)
