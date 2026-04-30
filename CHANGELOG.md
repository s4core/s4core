# v1.0.0-beta-federation

# S4-Federation, Cluster Recovery & Compaction

**Release Overview**
In this major update (67 commits), full support for distributed mode (S4-Federation). Alongside clustering, this update brings critical improvements to crash recovery, optimizes startup memory consumption, and resolves fundamental garbage collection (compactor) issues related to multipart uploads and "dead" volumes.

## 🚀 New Features
* **S4-Federation:** Completed the implementation of the distributed mode. Added support for load balancers, proper cluster node resolution, and reliable state synchronization.
* **Enterprise Edition:** Added the architectural foundation and build support for the Enterprise Edition.
* **Configurable Daily Compaction:** Added the ability to schedule a daily full garbage collector run (default is 02:00 AM) to completely free up unused disk space.

## ⚡ Performance & Architecture Optimizations
* **Fast Startup (O(1) Memory):** The `count_inline_objects` algorithm now uses O(1) memory. This completely eliminates multi-minute hangs during cold starts on large keyspaces.
* **Metadata Journal Compaction:** Implemented the `compact_journal_all` method to prevent unbounded growth of the metadata journal.
* **WAL and LSM Optimization:** Added a `persist()` method to forcefully flush the write-ahead log (fjall) to disk and immediately trigger LSM compaction after bulk object deletions.
* **Deduplicator Optimization:** During normal system startup, deduplicator rebuilding is now skipped if the deduplication data is already persistent.

## 🛡️ Crash Recovery
* **Safe Volume Handling:** The garbage collector (compactor) now safely bypasses unreadable and corrupted volumes (bit-rot protection) and correctly cleans up completely orphaned multipart sessions and "dead" volumes with no active references.

## 🐛 Bug Fixes
* **Memory/Space Leak on Multipart Object Deletion:** Fixed a critical bug in `bitcask.rs` where deleting multipart objects did not decrement the reference count in `DedupEntry`. This caused deleted segments to be forever considered "alive" and endlessly copied by the compactor (reported as ~11GB of unprocessed dead data).
* **Pagination Fix in ListObjects:** Resolved a bug with duplicate directory prefixes (fixed by advancing the cursor with the `U+10FFFF` character).
* **UTF-8 Correctness in AWS SigV4:** Fixed the `percent_decode` logic for correct UTF-8 handling during AWS Signature V4 canonicalization.
* **Handling 500 Errors (DeleteObject):** Added graceful handling for `BlobRefNotFound`, eliminating the 500 error when deleting an object with a missing manifest.
* **Ghost Objects Fix:** Introduced a series of fixes (ghost scripts/objects) that stabilize the cleanup process and prevent "ghost" objects from appearing during compaction. Added corresponding regression CI tests. Added a `/health` endpoint for the compactor.


# v0.0.7-alpha-fix-journal-compaction

fix: implement metadata journal compaction and optimize startup

- Add `compact_journal_all` to clear the metadata journal keyspace and prevent unbounded growth.
- Add `persist()` to flush the fjall write-ahead log to disk and trigger LSM compaction after bulk deletes.
- Integrate journal compaction and WAL flushing into `VolumeCompactor` via the `compact_journal` config flag.
- Optimize `count_inline_objects` to use O(1) memory iteration, preventing multi-minute startup hangs on large keyspaces.
- Skip deduplicator rebuilding on normal startup if the dedup data is already persistent.
- Add unit tests for `IndexDb` operations and architectural documentation for S4-Federation and Erasure Coding.


# v0.0.6-alpha-fix-compaction-dead-volumes

fix: compactor safely handles unreadable volumes, purges orphaned dead volumes and multipart sessions

  - Add fast-path to purge all non-active volumes when both dedup and blob_ref indices are empty
  - Treat unreadable volumes (0 readable blobs, file_size > 0) as dead only if no live index entries reference them
  - Protect against bit-rot: skip corrupted volumes that still have live references
  - Use on_demand() config for Admin API compaction (min_dead_bytes=0, unlimited volumes per run)
  - Add integration test for full-delete + compaction purge scenario
  - Add /health endpoint


# v0.0.5-alpha-fix-delimiter-list-objects

fix: implement delimiter/CommonPrefixes support for ListObjects/ListObjectsV2

S3-compatible directory-like listing was broken — aws s3 ls s3://bucket/ showed all objects in a flat list instead of grouping them into virtual directories
via CommonPrefixes.

Changes:
- Implement delimiter filtering with iterative batch scanning (objects collapsing into CommonPrefixes require scanning beyond the first batch)
- Add Marker/NextMarker to V1 responses and pass NextContinuationToken from handler in V2 for correct pagination with delimiter
- Fix KeyCount in V2 to include both Contents and CommonPrefixes per S3 spec
- Guard against panic on prefix slicing, infinite loop with versioned keys, and unbounded scans (100k object cap)


# v0.0.4-alpha-fjall-fix-unregister-dedup

fix: unregister dedup entries when deleting composite (multipart) objects

  When composite objects were deleted, composite_delete_ref_ops() decremented
  BlobRefEntry.ref_count_committed for segment blobs but never called
  make_unregister_op() to decrement the corresponding DedupEntry.ref_count.
  This left orphaned DedupEntries with ref_count >= 1, causing the compactor
  to treat dead segment blobs as live and copy them to new volumes forever.

  Customer reported ~11GB of dead data remaining after compaction with all
  buckets empty. After multiple compaction cycles, ~3.7GB (one ISO worth of
  unique segment data) persisted indefinitely — matching exactly the size of
  orphaned dedup entries.

  Fixed in 4 places in bitcask.rs:
  - composite_delete_ref_ops: unregister dedup for each segment on delete
  - abort_multipart_native: unregister dedup for each part on abort
  - complete_multipart_native: unregister dedup for unselected parts
  - upload_part_streaming: unregister dedup for old content on part overwrite

  Also added live_blobs/dead_blobs/volumes_skipped fields to the compaction
  API response for better diagnostics.

  Added regression test: test_compaction_reclaims_deleted_composite_segments


# v0.0.4-alpha-fjall-fix-compaction-edge-cases

fix: harden compaction against edge cases

Two edge cases in compaction that could cause silent data loss or crash entire batches:

    1. Blob read errors during volume scan — when read_blob() fails (corrupted header,
    truncated file), the scan loop breaks silently with no log output. In compact_volume
    this means live blobs past the corruption point aren't relocated, and the old volume
    gets deleted. Now both analyze_volume and compact_volume log a warning with
    volume_id and offset before breaking. The break itself stays — without a valid header
    we can't compute the next blob offset. A future improvement would be adding magic
    bytes or alignment markers to the volume format so the scanner can skip past corrupted
    regions and recover the rest.

    2. UpdateDedupLocation hard error on missing entry — if an object is deleted between
    batch construction and commit, the dedup entry no longer exists and batch_write
    returns a hard error, failing potentially thousands of unrelated operations in the
    same batch. Since the entry was intentionally removed (ref_count hit 0), this is now
    a no-op skip instead of an error.

Co-authored-by: doc-johnson <hustler@mail.ru>


# v0.0.3-alpha-fjall-fix-compactor-delete-iam
 
fix: compaction now preserves IAM records by scanning all keyspaces

Volume compaction was silently destroying IAM user data. The root cause:
scan_objects_by_volume() only scanned the `objects` keyspace, but IAM 
IndexRecords (users, credentials, access keys) are stored in the `iam`
keyspace via route_key(). When compaction relocated blobs to new volumes
and deleted old ones, IAM IndexRecords still pointed to the deleted
volume files, causing "Volume not found" errors and permanent data loss.

The compactor also had a hardcoded `KeyspaceId::Objects` when writing
back updated IndexRecords, so even if IAM records were found, they
would be written to the wrong keyspace.

Changes:
 - scan_objects_by_volume() now scans both `objects` and `iam` keyspaces,
   returning (KeyspaceId, key, IndexRecord) tuples
 - Compactor writes relocated records back to their origin keyspace
   instead of hardcoded Objects
 - Added unit test verifying IAM records are found during volume scan
 - Added integration test: IAM data survives compaction cycle
 - Added reproduction script (scripts/15-iam-compaction-bug-test.sh)

Reported-by: Alexander

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
