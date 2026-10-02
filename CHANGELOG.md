# v1.2.0-extended-metrics

# Cluster Observability, Hint Delivery and Gossip Rejoin

**Release Overview**
Cluster nodes now report their state through Prometheus metrics and the cluster admin API. Cluster mode also gets a set of fixes: a replica that was down catches up on what it missed, a node declared dead rejoins by itself, and buckets, batch deletes and the web console cover the whole cluster instead of one node.

## 📊 Observability (All Editions)
* **Cluster metrics:** membership, quorum operations, calls between nodes, hinted handoff, read repair, anti-entropy, tombstones and disk usage. Reference and suggested alerts: `docs/06-monitoring/prometheus-metrics.md`.
* **Build and licence metrics:** `s4_build_info{version,edition}` on every node, `s4_license_expiry_timestamp_seconds` under an Enterprise licence.
* **Grafana dashboard:** `docs/06-monitoring/grafana/s4-cluster.json`.
* **Cluster admin API:** `/api/admin/cluster/health`, `/cluster/topology`, `/node/health`, `/cluster/repair-status` and `/scrubber/status` report the real state of the cluster; health is `healthy`, `degraded` or `critical`.

## 🐛 Bug Fixes (All Editions)
* **HTTP metrics:** `http_requests_total` stayed at zero, `/health` checks were counted as bucket listings, and `http_request_duration_seconds` was a summary that cannot be combined across nodes.
* **Deleted buckets kept their Object Lock and encryption settings,** and a new bucket with the same name inherited them.
* **Hinted handoff (cluster mode):** A replica that was down now gets the writes and deletes it missed within about 30 seconds of its return. Hints used to be written for every write and never delivered.
* **A node declared dead rejoins by itself** after a long pause or a network split, without a restart.
* **Objects stored before a node joined the cluster:** A delete made while that node was down now reaches it when it returns; the object used to stay there, listed and answering `503`.
* **Missing buckets in cluster mode answer `NoSuchBucket`,** as in S3. A write into a bucket that never existed, or was deleted through another node, used to succeed.
* **DeleteObjects in cluster mode** (`aws s3 rm --recursive`, `mc rm --recursive`) removed the keys on one node only, and they came back.
* **Web console and admin API in cluster mode:** Creating and deleting buckets, bucket statistics, browsing a bucket and the dashboard counters covered only the node that took the request; they now cover the whole cluster.
* **A write could be lost in cluster mode** if it landed while hinted handoff or an erasure-coding move was removing an older copy of the same key.

## 📚 Documentation
* **Cluster monitoring guide:** what each metric and cluster admin route reports.
* **Switching a single node to cluster mode** does not copy its data to the other nodes: reads of its objects answer `503`, and the objects are lost with that node. The clustering guide shows how to move the data into a new cluster through S3.

## 🏗️ Build & Deployment
* **Rust 1.99.0:** Pinned in `rust-toolchain.toml`; rustup installs it when you build from source. Docker images are built with the same release.

## ⚠️ Upgrade Notes
* **`http_request_duration_seconds` is a histogram:** Use `histogram_quantile` over `http_request_duration_seconds_bucket`; the `quantile` series are gone.
* **`POST /api/admin/node/drain` answers `501`:** Stop a node with SIGTERM; it finishes the requests in flight.
* **Cluster health checks:** `/api/admin/cluster/health` returns `degraded` or `critical` while a member is down, so a check that expects `healthy` fails.
* **`NoSuchBucket` in cluster mode:** Create a bucket before writing into it. Each request into a bucket now reads the bucket through the quorum first.
* **`/api/stats` and the admin object listing** gain a `complete` field, and `max-keys` of the admin listing is limited to 1–1000. Use `/health`, not `/api/stats`, for health checks.
* **Hint files of earlier releases** are removed at start.
* **Cluster protocol:** Unchanged.


# v1.1.1-secret-files

# Secrets from Files and a Safe Admin JWT Secret

**Release Overview**
This release (8 commits) closes a security hole in the admin API and lets every credential come from Docker and Kubernetes secret files. Up to v1.1.0-erasure-coding, a server started without `S4_JWT_SECRET` signed admin tokens with a fixed secret that is public in the source code, so anyone who could reach its admin API could sign a SuperUser token for it. The admin JWT secret now has no built-in value, short secrets are refused, and a cluster that needs one shared secret will not start without it. If you cannot upgrade yet, set `S4_JWT_SECRET` to a random value of at least 32 bytes.

## 🔒 Security (All Editions)
* **No built-in JWT secret:** Without `S4_JWT_SECRET` a single node signs admin tokens with a random secret generated at every start. Tokens signed with the old fixed secret are rejected.
* **Short JWT secrets are refused:** An `S4_JWT_SECRET` shorter than 32 bytes stops the start. Any issued token is enough to guess a short secret offline and then sign a SuperUser token with it.
* **Cluster and gateway nodes need a shared secret:** With IAM in use — `S4_ROOT_PASSWORD` set, or IAM users on disk — a node in cluster or gateway mode refuses to start without `S4_JWT_SECRET`, before it joins the cluster. Otherwise every node would sign with a secret of its own, and a token would work only on the node that issued it.
* **Random development S3 keys:** The keys used when `S4_ACCESS_KEY_ID` and `S4_SECRET_ACCESS_KEY` are unset come from the OS random generator instead of a hash of the start time.
* **Secrets stay out of logs:** Secret values are redacted in debug output of the configuration.

## 🚀 New Features (All Editions)
* **Secrets from files (`_FILE`):** `S4_ROOT_USERNAME_FILE`, `S4_ROOT_PASSWORD_FILE`, `S4_ACCESS_KEY_ID_FILE`, `S4_SECRET_ACCESS_KEY_FILE` and `S4_JWT_SECRET_FILE` read the value from a file, the Docker and Kubernetes secrets convention: the values stay out of `docker inspect`, the process environment and the compose file. A leading byte order mark and trailing line breaks are stripped. Both forms set, or a file that is missing, empty, not UTF-8 or larger than 64 KiB, stops the start with the variable named.

## 📚 Documentation
* **Admin JWT Secret** and **Secrets from Files** sections in `docs/01-getting-started/configuration.md`, and a Docker Compose example with secrets in `docs/08-deployment/docker.md`, including the file permissions the container's `s4` user (uid 1000) needs.
* **Cluster admin API paths:** The routes live under `/api/admin/...` and need a SuperUser token; the docs gave them as `/admin/...`. Three documented routes that never existed — `/cluster/pool`, `/node/bootstrap`, `/cluster/scrub` — are removed; the manual scrub is `POST /api/admin/scrubber/run`.

## 🏗️ Build & Deployment
* **New CI test:** `scripts/54-FILE-ENV-and-jwt-test.sh` checks the `_FILE` forms and every JWT secret rule against the CE build, the EE build without a licence and, where the licence is available, licensed EE.
* **EC acceptance runs once per change:** The EC v1 and EC v2 acceptance suites, about an hour together, run on the feature branch and no longer again on `main`, where every change arrives already tested.
* **CI tests the Community Edition binary again:** The ISA-L build had replaced it with the EE one at the same path, so the test jobs of a feature branch had been testing EE without a licence. A CI step now checks which edition each binary is.
* **Release notes from the annotated tag:** `actions/checkout` turned the release tag into a lightweight one, and the release notes steps fell back to the tagged commit's message. That is how the GitHub release of v1.1.0-erasure-coding came out with one line from a CI commit instead of its notes. The tags are now fetched again before they are read.
* **Website:** s4core.com is redesigned around the Enterprise erasure coding features.

## ⚠️ Upgrade Notes
* **Clusters and gateways with IAM:** Set the same `S4_JWT_SECRET`, at least 32 bytes, on every node before upgrading; the previous release already reads it. A node upgraded without it stops at once, and the rest keep quorum.
* **`S4_JWT_SECRET` shorter than 32 bytes** refuses to start in every mode. Replace it with the output of `openssl rand -hex 32`, generated once and kept.
* **Admin sessions:** Tokens issued before the upgrade are rejected unless `S4_JWT_SECRET` was set and stays the same; log in again. A single node without `S4_JWT_SECRET` ends admin sessions on every restart.
* **Empty values:** An empty `S4_ROOT_USERNAME`, `S4_ROOT_PASSWORD` or `S4_JWT_SECRET` counts as unset. An empty `S4_ACCESS_KEY_ID` or `S4_SECRET_ACCESS_KEY` still switches login with these keys off.
* **Trailing line breaks** in a credential set directly are stripped, as in its file form.
* **Data format and cluster protocol:** Unchanged. Nodes of this release and of v1.1.0-erasure-coding can run side by side during a rolling upgrade.


# v1.1.0-erasure-coding

# Federation Erasure Coding (Enterprise), Bit-Rot Healing & Cluster Resilience

**Release Overview**
This release (83 commits) brings erasure coding to S4 Federation as an Enterprise Edition pool type. Objects are stored as data and parity shards across a fixed-width erasure set, at 1.375x–1.75x of their size instead of the 3x of RF=3 replication. It ships with an ISA-L accelerated codec, repair, anti-entropy, scrubbing and garbage collection, and the full EC v2 feature set: auto-tiering between RF=3 and erasure coding, multi-set pools, packed segments for small objects, topology-aware LRC placement, node replacement, damage-ordered repair and minimal reads. Every cluster, Community Edition included, gets bit-rot healing that actually rewrites the damaged bytes, restarts that no longer need every node up, and a fix for a storage-engine defect in which an interrupted write could make later objects unreadable.

## 🚀 New Features (Enterprise Edition)
* **Erasure-coded pools (`S4_POOL_TYPE=ec`):** Licensed by the `erasure_coding` capability. Six built-in profiles — RS(4,2), RS(6,3), RS(8,3), LRC(4,2,1), LRC(6,2,2), LRC(10,2,2) — plus custom RS and LRC. A write is acknowledged on its RF=3 replicas and moved into erasure coding in the background: the object is encoded, its manifest is committed through the metadata quorum, the object is read back from the shards and checked against its SHA-256, and only then are the hot copies released, after a grace period. Objects below `S4_EC_MIN_OBJECT_SIZE` (1 MiB) stay replicated. Clients see no difference: body, ETag, size, Content-Type, Last-Modified and storage class are preserved.
* **ISA-L accelerated codec:** `cargo build --release --features isal` (x86_64, nasm 2.14 or newer), selected with `S4_EC_CODEC_ENGINE=auto|isal|pure`. On `ec-rs-standard`, one core, 6 MiB chunks: encode 8.2x, degraded read 10.9x and shard repair 7.8x faster than the portable engine. Both engines produce byte-identical shards, so nodes can mix engines and switch with a restart. The x86_64 EE Docker image is built with it.
* **Self-healing pools:** Repair worker, manifest anti-entropy, shard scrubbing and tombstone and orphan shard GC. The repair queue is ordered by damage: the chunk missing the most shards is rebuilt first. A scrub pass that keeps failing on the medium reports the disk as suspect instead of declaring its shards corrupt.
* **LRC recovery:** Local XOR, global Reed-Solomon and a unified solver that recovers every loss pattern the code can recover; the decoder takes the cheapest route. `GET /api/admin/ec/pools/{pool}/failure-matrix` lists the failure patterns of a pool and the recovery path each one takes.
* **Minimal reads (on by default):** A read pulls only the `k` data shards it assembles from, and a range read only the shards the range covers. Parity is read only for a shard that is missing or fails its hash.
* **Auto-tiering (off by default):** Cold objects move from RF=3 into erasure coding and objects that become hot come back. Bucket policies with prefix and tag exclusions, a dry run, admission and rate limits, and a pause between migrations of one object that grows with each migration. `POST /api/admin/ec/tiering/{bucket}/promote` brings objects back to RF=3 on demand, with auto-tiering switched off too.
* **Multi-set pools (off by default):** Grow a pool by whole erasure sets, place new objects `round_robin` or by `free_space`, and stop a set taking new objects once it fills.
* **Packed segments for small objects (off by default):** Small objects are gathered into large segments that are erasure coded as a whole, with repacking to reclaim the space of deleted objects and an unpacker to roll back.
* **Topology-aware placement:** Zone, rack and host labels (`S4_EC_NODE_TOPOLOGY`), LRC local groups spread over a failure domain (`S4_EC_LRC_GROUP_DOMAIN`), a recorded layout per erasure set with drift detection, and repair bandwidth limits per zone.
* **Node replacement:** A new machine takes over a dead node's identity (`S4_EXPECTED_NODE_IDENTITY`) and is refilled by repair. A start is refused while a live node already answers for the same identity, and a node that was away longer than `S4_MAX_REJOIN_DOWNTIME_DAYS` serves no erasure-coded reads until its shards are rebuilt.
* **Admin API and metrics:** Pool health with release gates, repair status, erasure sets, verification, repair, GC, tiering and packed-segment routes under `/api/admin/ec/*`, a read-only duplicate content report, and Prometheus metrics for every worker.

## 🛡️ Reliability (All Editions)
* **Bit-rot healing that heals:** The scrubber now writes the healthy copy it fetches from a replica back to disk. Before, it fetched the copy and counted the blob as healed while the damaged bytes stayed where they were. A cycle that could not heal something runs again after `S4_SCRUBBER_UNHEALED_RETRY_SECS` (default 5 minutes) instead of waiting out the full scan period, and the first cycle waits `S4_SCRUBBER_START_DELAY_SECS` for membership to settle.
* **Damaged blobs leave deduplication:** A blob that fails its hash on read is no longer handed to new writes of the same content, and a healing write always stores a fresh copy instead of pointing back at the damage.
* **Restarts with a node down:** A cluster node no longer refuses to start until every pool member is visible. Members it cannot see are named from what it recorded the last time they answered, so a rolling restart works with one node dead.
* **Start order no longer matters:** A node keeps announcing itself to its seeds until it finds a peer, so a whole cluster can be started at once.

## 🐛 Bug Fixes
* **Interrupted writes corrupted later objects:** A write cancelled mid-flight — a client disconnecting during an upload, or a replica write timing out — left the volume file position ahead of the writer's recorded offset. Objects written afterwards were stored away from the offset their index entry recorded and failed to read. The writer now discards an unfinished write before the next one.
* **CopyObject in cluster mode:** The source is now read and the copy written through the quorum coordinators. Before, both went to the local storage of the node that took the request: the copy was not replicated, and a node that held no replica of the source answered `NoSuchKey`.
* **Admin login no longer reveals which accounts exist:** An unknown account and a wrong password now both answer `401 InvalidCredentials`.
* **Admin API errors:** Every failure is JSON with a human-readable `error` and a stable `code`; anything the caller can correct is `4xx`, and `500` means a server fault only. A write the cluster has no room for answers `507 InsufficientStorage` instead of `500`.
* **Quorum reads:** A cancelled quorum read no longer leaves its replica reads running in the background.

## 🏗️ Build & Deployment
* **No OpenSSL:** TLS is rustls end to end and OpenSSL is no longer a build dependency; the workspace cross-compiles to aarch64. ISA-L is x86_64 only, and the aarch64 build uses the portable codec.
* **Docker Compose:** Three-node cluster files behind HAProxy with health checks — `docker-compose-cluster.yml` for the released image, `docker-compose-cluster-dev.yml` built from source. `docker-compose-single-node.yml` reads its credentials from the shell. The README and docs referred to a `docker-compose.yml` that does not exist; they now name the real files.
* **Documentation:** One complete configuration reference in `docs/01-getting-started/configuration.md`; erasure coding documented in `docs/04-features/erasure-coding.md`.
* **CI:** ISA-L and aarch64 builds, the EC v1 and EC v2 acceptance suites, and CE and EE cluster crash-recovery and bit-rot durability tests.

## ⚠️ Upgrade Notes
* **Data format:** Unchanged for existing data. Index records written by v1.0.0-beta-federation are read as before. New records carry a reserved `_s4_storage_class` metadata key that is never returned to clients.
* **Cluster protocol:** Version 3, compatible down to version 1, so nodes of this release and of v1.0.0-beta-federation can run side by side during a rolling upgrade. EC v2 features that need every node — auto-tiering, adding erasure sets — stay blocked until every pool member runs this release.
* **Restarting with a node down** works from the second start of each node on this release: the record of peer identities it relies on is written by the first.
* **Admin API status codes:** Deleting a non-empty bucket answers `409 BucketNotEmpty` (was `400`); a login with an unknown account answers `401 InvalidCredentials` (was `404`). Clients should branch on the `code` field rather than on the status code or the message text.
* **EC production readiness:** Pool health reports `production_ready=true` only for an EE build attested with `S4_EC_V1_CI_ACCEPTANCE_PASSED=true`.

## Known Limitations
* Hot RF=3 replicas are placed by the hash ring, which does not know about zones. In a pool spread over three zones, losing one zone makes writes unavailable for the keys that had two of their three replicas in it.
* One generation of an object is erasure coded at most once: an object brought back to RF=3 stays there until a client writes it again.
* Auto-tiering does not migrate objects assembled from multipart parts; such objects written into an EC pool are still encoded on their way in.
* Adding an erasure set does not rebalance existing objects.
* Deduplication across erasure-coded content is not implemented; a read-only report measures how much it would save.


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
