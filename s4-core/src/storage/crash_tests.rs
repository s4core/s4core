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

//! Atomicity tests for versioning operations.
//!
//! # Phase 3: All crash windows eliminated
//!
//! With Phase 3 atomic batch writes, all multi-step versioning operations
//! are performed as a single atomic fjall batch. This means:
//!
//! - PUT versioned (Enabled): version record + version list + current pointer
//!   are all committed in one batch.
//! - DELETE versioned (Enabled): delete marker + version list + current pointer
//!   removal are all committed in one batch.
//! - DELETE specific version: version deletion + version list update + current
//!   pointer recalculation are all committed in one batch.
//!
//! These tests verify that the system is always in a consistent state after
//! every operation — no orphaned versions, no stale pointers, no dangling
//! references.

use crate::storage::BitcaskStorageEngine;
use crate::storage::StorageEngine;
use crate::types::VersioningStatus;
use std::collections::HashMap;
use std::path::Path;
use tempfile::TempDir;

/// Creates a test engine with standard settings.
///
/// Uses 4KB inline threshold and strict_sync=true for durability testing.
/// Data written to the engine should be above the inline threshold (>4KB)
/// to exercise volume storage paths.
async fn create_engine(base_path: &Path) -> BitcaskStorageEngine {
    let volumes_dir = base_path.join("volumes");
    let metadata_path = base_path.join("metadata_db");
    std::fs::create_dir_all(&volumes_dir).unwrap();

    BitcaskStorageEngine::new(
        &volumes_dir,
        &metadata_path,
        50 * 1024 * 1024, // 50MB volume size
        4096,             // 4KB inline threshold
        true,             // strict_sync
    )
    .await
    .unwrap()
}

/// Returns test data larger than the inline threshold to ensure volume storage.
fn volume_sized_data(suffix: &str) -> Vec<u8> {
    let mut data = vec![b'X'; 5000]; // 5KB, above 4KB inline threshold
    data.extend_from_slice(suffix.as_bytes());
    data
}

/// Verifies that PUT versioned (Enabled) is fully atomic.
///
/// After a successful PUT, the version record, version list, AND current pointer
/// must all be consistent. There should be zero orphaned versions.
#[tokio::test]
async fn test_put_versioned_is_atomic() {
    let temp_dir = TempDir::new().unwrap();
    let data = volume_sized_data("_v1");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (etag, version_id) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();

    let version_id = version_id.expect("Should have version ID");

    // Verify: version record exists
    let version_key = engine.test_make_version_key("bucket", "key", &version_id);
    let version_record = engine.index_db().get(&version_key).await.unwrap();
    assert!(version_record.is_some(), "Version record must exist");
    assert_eq!(version_record.unwrap().etag, etag);

    // Verify: version list references this version
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 1);
    assert_eq!(version_list.versions[0], version_id);
    assert_eq!(version_list.current_version, Some(version_id.clone()));

    // Verify: current pointer exists and matches
    let current = engine.index_db().get("bucket/key").await.unwrap();
    assert!(current.is_some(), "Current pointer must exist");
    assert_eq!(current.unwrap().etag, etag);

    // Verify: zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0, "No orphaned versions should exist");
}

/// Verifies atomicity of multiple sequential versioned PUTs.
///
/// After two PUTs, the version list should have both versions, the current
/// pointer should point to the latest, and there should be zero orphans.
#[tokio::test]
async fn test_multiple_versioned_puts_are_consistent() {
    let temp_dir = TempDir::new().unwrap();
    let data_v1 = volume_sized_data("_v1");
    let data_v2 = volume_sized_data("_v2");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (_, vid1) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v1,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid1 = vid1.unwrap();

    let (etag_v2, vid2) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v2,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid2 = vid2.unwrap();

    // Version list should have both, newest first
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 2);
    assert_eq!(version_list.versions[0], vid2);
    assert_eq!(version_list.versions[1], vid1);
    assert_eq!(version_list.current_version, Some(vid2.clone()));

    // Current pointer should return v2
    let (retrieved_data, record) = engine.get_object("bucket", "key").await.unwrap();
    assert_eq!(retrieved_data, data_v2);
    assert_eq!(record.etag, etag_v2);
    assert_eq!(record.version_id, Some(vid2));

    // v1 should still be accessible by version ID
    let (v1_data, _) = engine.get_object_version("bucket", "key", &vid1).await.unwrap();
    assert_eq!(v1_data, data_v1);

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}

/// Verifies that DELETE versioned (Enabled, no version_id) atomically creates
/// a delete marker, updates the version list, and removes the current pointer.
#[tokio::test]
async fn test_delete_marker_creation_is_atomic() {
    let temp_dir = TempDir::new().unwrap();
    let data = volume_sized_data("_v1");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (_, vid) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid = vid.unwrap();

    // Delete (creates delete marker)
    let delete_result = engine
        .delete_object_versioned("bucket", "key", None, VersioningStatus::Enabled)
        .await
        .unwrap();

    assert!(delete_result.delete_marker);
    let marker_vid = delete_result.version_id.unwrap();

    // Verify: current pointer is gone
    let current = engine.index_db().get("bucket/key").await.unwrap();
    assert!(current.is_none(), "Current pointer must be removed");

    // Verify: GET returns ObjectNotFound (no current pointer)
    let get_result = engine.get_object("bucket", "key").await;
    assert!(get_result.is_err());

    // Verify: version list has both the original and the delete marker
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 2);
    assert_eq!(version_list.versions[0], marker_vid);
    assert_eq!(version_list.versions[1], vid);
    assert!(version_list.current_version.is_none());

    // Verify: delete marker record exists
    let marker_key = engine.test_make_version_key("bucket", "key", &marker_vid);
    let marker_record = engine.index_db().get(&marker_key).await.unwrap();
    assert!(marker_record.is_some());
    assert!(marker_record.unwrap().is_delete_marker);

    // Verify: original version still accessible by version ID
    let (v1_data, _) = engine.get_object_version("bucket", "key", &vid).await.unwrap();
    assert_eq!(v1_data, data);

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}

/// Verifies that deleting a specific version atomically removes it from
/// both the index and the version list, and recalculates the current pointer.
#[tokio::test]
async fn test_specific_version_delete_is_atomic() {
    let temp_dir = TempDir::new().unwrap();
    let data_v1 = volume_sized_data("_v1");
    let data_v2 = volume_sized_data("_v2");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (_, vid1) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v1,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid1 = vid1.unwrap();

    let (_, vid2) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v2,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid2 = vid2.unwrap();

    // Delete v1 specifically
    let delete_result = engine
        .delete_object_versioned("bucket", "key", Some(&vid1), VersioningStatus::Enabled)
        .await
        .unwrap();

    assert!(!delete_result.delete_marker);
    assert_eq!(delete_result.version_id, Some(vid1.clone()));

    // Verify: v1 is gone from the index
    let v1_key = engine.test_make_version_key("bucket", "key", &vid1);
    let v1_record = engine.index_db().get(&v1_key).await.unwrap();
    assert!(v1_record.is_none(), "v1 must be deleted from index");

    // Verify: v1 is gone from the version list
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 1);
    assert_eq!(version_list.versions[0], vid2);
    assert!(!version_list.versions.contains(&vid1));

    // Verify: current pointer still points to v2
    let (retrieved_data, record) = engine.get_object("bucket", "key").await.unwrap();
    assert_eq!(retrieved_data, data_v2);
    assert_eq!(record.version_id, Some(vid2.clone()));

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}

/// Verifies that deleting the current (latest) version correctly recalculates
/// the current pointer to the previous version.
#[tokio::test]
async fn test_delete_current_version_recalculates_pointer() {
    let temp_dir = TempDir::new().unwrap();
    let data_v1 = volume_sized_data("_v1");
    let data_v2 = volume_sized_data("_v2");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (_, vid1) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v1,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid1 = vid1.unwrap();

    let (_, vid2) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v2,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid2 = vid2.unwrap();

    // Delete the current version (v2)
    engine
        .delete_object_versioned("bucket", "key", Some(&vid2), VersioningStatus::Enabled)
        .await
        .unwrap();

    // Current pointer should now point to v1
    let (retrieved_data, record) = engine.get_object("bucket", "key").await.unwrap();
    assert_eq!(retrieved_data, data_v1);
    assert_eq!(record.version_id, Some(vid1.clone()));

    // Version list should only have v1
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 1);
    assert_eq!(version_list.current_version, Some(vid1));

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}

/// Verifies that deleting all versions properly cleans up the version list
/// and current pointer.
#[tokio::test]
async fn test_delete_all_versions_cleans_up() {
    let temp_dir = TempDir::new().unwrap();
    let data = volume_sized_data("_v1");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    let (_, vid) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();
    let vid = vid.unwrap();

    // Delete the only version
    engine
        .delete_object_versioned("bucket", "key", Some(&vid), VersioningStatus::Enabled)
        .await
        .unwrap();

    // Current pointer should be gone
    let current = engine.index_db().get("bucket/key").await.unwrap();
    assert!(current.is_none());

    // Version list should be empty
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert!(version_list.is_empty());

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}

/// Verifies consistency after reopen — all atomic writes must survive restart.
#[tokio::test]
async fn test_consistency_survives_reopen() {
    let temp_dir = TempDir::new().unwrap();
    let data_v1 = volume_sized_data("_v1");
    let data_v2 = volume_sized_data("_v2");
    let metadata = HashMap::new();

    let vid1;
    let vid2;
    let etag_v2;

    // Phase 1: Create two versions
    {
        let engine = create_engine(temp_dir.path()).await;

        let (_, v1) = engine
            .put_object_versioned(
                "bucket",
                "key",
                &data_v1,
                "text/plain",
                &metadata,
                VersioningStatus::Enabled,
            )
            .await
            .unwrap();
        vid1 = v1.unwrap();

        let (e2, v2) = engine
            .put_object_versioned(
                "bucket",
                "key",
                &data_v2,
                "text/plain",
                &metadata,
                VersioningStatus::Enabled,
            )
            .await
            .unwrap();
        vid2 = v2.unwrap();
        etag_v2 = e2;
    }

    // Phase 2: Reopen and verify everything is consistent
    {
        let engine = create_engine(temp_dir.path()).await;

        // Version list should have both
        let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
        assert_eq!(version_list.len(), 2);
        assert_eq!(version_list.versions[0], vid2);
        assert_eq!(version_list.versions[1], vid1);

        // Current pointer should return v2
        let (retrieved_data, record) = engine.get_object("bucket", "key").await.unwrap();
        assert_eq!(retrieved_data, data_v2);
        assert_eq!(record.etag, etag_v2);

        // Both versions accessible
        let (v1_data, _) = engine.get_object_version("bucket", "key", &vid1).await.unwrap();
        assert_eq!(v1_data, data_v1);

        // Zero orphans
        let orphan_count = engine.audit_version_consistency().await.unwrap();
        assert_eq!(orphan_count, 0);
    }
}

/// Verifies that diagnostic counters are properly initialized at startup.
#[tokio::test]
async fn test_diagnostic_counters_initialization() {
    let temp_dir = TempDir::new().unwrap();

    // Phase 1: Create engine and write some objects
    {
        let engine = create_engine(temp_dir.path()).await;
        let metadata = HashMap::new();

        // Write an inline object (< 4KB)
        engine
            .put_object("bucket", "tiny", b"small", "text/plain", &metadata)
            .await
            .unwrap();

        // Write a volume object (> 4KB)
        let big_data = vec![b'B'; 5000];
        engine
            .put_object(
                "bucket",
                "big",
                &big_data,
                "application/octet-stream",
                &metadata,
            )
            .await
            .unwrap();
    }

    // Phase 2: Re-open and check counters
    {
        let engine = create_engine(temp_dir.path()).await;
        let diag = engine.diagnostics();

        // Phase 5: All new objects go to volumes, so inline count should be 0
        assert_eq!(
            diag.inline_objects_count, 0,
            "Phase 5: new objects should not be inline"
        );

        // Dedup rebuild should have completed (duration >= 0)
        assert!(
            diag.dedup_rebuild_duration_ms < 10000,
            "Dedup rebuild should complete in reasonable time"
        );

        // No orphans initially
        assert_eq!(
            diag.orphaned_versions_found, 0,
            "No orphans should be detected at startup"
        );
    }
}

/// Verifies that the audit correctly reports zero orphans for well-formed data.
#[tokio::test]
async fn test_audit_reports_zero_orphans_for_atomic_writes() {
    let temp_dir = TempDir::new().unwrap();
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    // Create multiple versioned objects with various operations
    let data1 = volume_sized_data("_obj1_v1");
    let data2 = volume_sized_data("_obj1_v2");
    let data3 = volume_sized_data("_obj2_v1");

    // Object 1: two versions
    engine
        .put_object_versioned(
            "bucket",
            "obj1",
            &data1,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();

    let (_, vid2) = engine
        .put_object_versioned(
            "bucket",
            "obj1",
            &data2,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();

    // Object 2: one version
    engine
        .put_object_versioned(
            "bucket",
            "obj2",
            &data3,
            "text/plain",
            &metadata,
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();

    // Delete marker on object 2
    engine
        .delete_object_versioned("bucket", "obj2", None, VersioningStatus::Enabled)
        .await
        .unwrap();

    // Delete specific version on object 1
    engine
        .delete_object_versioned(
            "bucket",
            "obj1",
            Some(&vid2.unwrap()),
            VersioningStatus::Enabled,
        )
        .await
        .unwrap();

    // After all operations, audit should find zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(
        orphan_count, 0,
        "All atomic batch operations should leave zero orphans"
    );

    let diag = engine.diagnostics();
    assert_eq!(diag.orphaned_versions_found, 0);
}

/// Verifies suspended mode versioning with null version replacement.
#[tokio::test]
async fn test_suspended_mode_null_version_replacement() {
    let temp_dir = TempDir::new().unwrap();
    let data_v1 = volume_sized_data("_v1");
    let data_v2 = volume_sized_data("_v2");
    let metadata = HashMap::new();

    let engine = create_engine(temp_dir.path()).await;

    // First put in suspended mode
    let (_, vid1) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v1,
            "text/plain",
            &metadata,
            VersioningStatus::Suspended,
        )
        .await
        .unwrap();
    assert_eq!(vid1.as_deref(), Some("null"));

    // Second put replaces the null version
    let (_, vid2) = engine
        .put_object_versioned(
            "bucket",
            "key",
            &data_v2,
            "text/plain",
            &metadata,
            VersioningStatus::Suspended,
        )
        .await
        .unwrap();
    assert_eq!(vid2.as_deref(), Some("null"));

    // Version list should have only one null version
    let version_list = engine.test_get_version_list("bucket", "key").await.unwrap();
    assert_eq!(version_list.len(), 1);

    // Current pointer should return v2 data
    let (retrieved_data, _) = engine.get_object("bucket", "key").await.unwrap();
    assert_eq!(retrieved_data, data_v2);

    // Zero orphans
    let orphan_count = engine.audit_version_consistency().await.unwrap();
    assert_eq!(orphan_count, 0);
}
