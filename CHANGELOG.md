# v0.0.1-alpha-24

Add S3 Select SQL engine, fix versioning/Object Lock/multipart, improve S3 compatibility

S3 Select:
- New s4-select crate with custom SQL engine (planner, evaluator, validator)
- CSV, JSON format support with input/output serialization
- Aggregate, conditional, conversion, date, and string functions
- Multi-object query support
- API handler and integration tests for SelectObjectContent

Versioning fixes:
- Fix ListObjects lexicographic ordering after strip_version_ids_from_keys
- Normalize version_id "null" to None in check_object_locks

Object Lock fixes:
- Add Years support to DefaultRetention struct and XML serialization
- Fix Mode/Days ordering in XML output, add xmlns attribute

Multipart upload fixes:
- Fix multipart upload handling

S3 API compatibility improvements:
- Add AWS Signature V2 authentication support (825 lines)
- Add storage class validation (reject invalid values like REDUCED)
- Add bucket encryption config stubs (GET/PUT/DELETE ?encryption)
- Return ServerSideEncryptionConfigurationNotFound after DELETE
- Change POST fallback from NotImplemented (501) to MethodNotAllowed (405)
- Remove BucketKeyEnabled from encryption XML for SDK compatibility
- Add MinIO-compatible Prometheus metrics/healthcheck endpoints
- Fix V2 Signature empty sub-resource values
- Fix ListObjectVersions response format

# v0.0.1-alpha-22

- Fix DeleteBucket to reject buckets with versioned objects and delete markers

  Per S3 spec, a bucket must have no entries at all — including old
  versions and delete markers — before it can be deleted. The previous
  implementation filtered these out via strip_version_ids_from_keys,
  which could allow deletion of non-empty versioned buckets.  

# v0.0.1-alpha-21

- Support x-amz-bypass-governance-retention and retention clearing
- Add CopyObject (PUT with x-amz-copy-source) support
- Implement server-side object copy with COPY/REPLACE metadata directives, versioning, and Object Lock default retention.
- Route requests with x-amz-copy-source header to the new handler

# Changelog

All notable changes to S4 will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Core storage engine with append-only volumes (Bitcask-style)
- Metadata indexing with fjall (LSM-tree, MVCC, atomic cross-keyspace batches)
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
