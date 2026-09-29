// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

use std::time::Duration;

use s4_cluster::{HlcTimestamp, NodeId};
use s4_ee::ec_metadata::{
    EcMetadataError, EcMetadataStore, EcObjectVersionKey, ObjectDataLayout, PackedSegmentDeletion,
    PackedSegmentObjectPlacement, ReplicatedSmallObjectPlacement,
};
use s4_ee::ec_packed_segments::{
    PackedObjectIdentity, PackedSegment, PackedSegmentConfig, PackedSegmentEntry, PackedSegmentId,
    PackedSegmentJournalPayload, PackedSegmentState, SealReason,
};

fn test_config() -> PackedSegmentConfig {
    PackedSegmentConfig {
        enabled: true,
        target_bytes: 1024 * 1024,
        max_age: Duration::from_secs(300),
        max_objects: 100,
        max_open_segments: 4,
    }
}

fn test_nodes() -> Vec<NodeId> {
    vec![NodeId(1), NodeId(2), NodeId(3)]
}

/// Helper to open a new segment and record its opening in the journal before memory usage.
async fn open_segment(
    store: &EcMetadataStore,
    segment_id: PackedSegmentId,
    erasure_set_id: u64,
    config: &PackedSegmentConfig,
    created_at: u64,
) -> PackedSegment {
    let segment = PackedSegment::open(segment_id, erasure_set_id, config, created_at);
    store
        .append_packed_segment_journal(&segment.journal_open())
        .await
        .expect("journal open append should succeed");
    segment
}

/// Helper that drives one object commit durably:
/// reserve -> journal_append -> commit_packed_object -> commit.
/// Durability always precedes in-memory commit.
async fn commit_object(
    store: &EcMetadataStore,
    segment: &mut PackedSegment,
    object: EcObjectVersionKey,
    length: u64,
    content_hash_sha256: [u8; 32],
    hlc: HlcTimestamp,
    hot_replica_nodes: Vec<NodeId>,
) -> (PackedSegmentObjectPlacement, PackedSegmentEntry) {
    let reservation = segment.reserve(length).expect("segment reserve should succeed");
    let item = segment
        .journal_append(
            &reservation,
            PackedObjectIdentity::new(object.clone(), content_hash_sha256, hlc),
        )
        .expect("journal append should succeed");
    let placement = match &item.payload {
        PackedSegmentJournalPayload::ObjectAppended { entry } => {
            PackedSegmentObjectPlacement::from_entry(segment, entry, hot_replica_nodes)
                .expect("placement derivation from entry should succeed")
        }
        _ => unreachable!("journal item payload must be ObjectAppended"),
    };
    store
        .commit_packed_object(&object, &placement, &item)
        .await
        .expect("commit packed object should succeed");
    let entry = segment
        .commit(&reservation, &item)
        .expect("segment commit should succeed")
        .clone();
    (placement, entry)
}

#[tokio::test]
async fn segment_record_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = PackedSegment::open(segment_id, 1, &config, 1000);

    let reservation = segment.reserve(512).unwrap();
    let obj = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let item = segment
        .journal_append(
            &reservation,
            PackedObjectIdentity::new(obj, [0xaa; 32], HlcTimestamp::zero()),
        )
        .unwrap();
    segment.commit(&reservation, &item).unwrap();
    segment.seal(2000, SealReason::TargetSizeReached).unwrap();

    store.put_packed_segment(&segment).await.unwrap();

    let retrieved = store.get_packed_segment(segment_id).await.unwrap();
    assert_eq!(retrieved, Some(segment.clone()));

    let list = store.list_packed_segments(100).await.unwrap();
    assert!(list.iter().any(|s| s == &segment));

    store.delete_packed_segment(segment_id).await.unwrap();

    let after_delete = store.get_packed_segment(segment_id).await.unwrap();
    assert_eq!(after_delete, None);

    let list_after = store.list_packed_segments(100).await.unwrap();
    assert!(!list_after.iter().any(|s| s.segment_id == segment_id));
}

#[tokio::test]
async fn an_open_segment_has_no_record_only_a_journal() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "key-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "key-2", None);

    let (placement1, entry1) = commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        512,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement2, entry2) = commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        1024,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    assert_eq!(placement1.offset, 0);
    assert_eq!(placement2.offset, 512);

    let record = store.get_packed_segment(segment_id).await.unwrap();
    assert_eq!(
        record, None,
        "open segment must not have a record in ec_packed_segments"
    );

    let recovery = store.load_packed_segments(100).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    let recovered = &recovery.segments[0];
    assert_eq!(recovered.segment_id, segment_id);
    assert_eq!(recovered.state, PackedSegmentState::Open);
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.entries[0].offset, 0);
    assert_eq!(recovered.entries[0].length, 512);
    assert_eq!(recovered.entries[0].object, obj1);
    assert_eq!(recovered.entries[1].offset, 512);
    assert_eq!(recovered.entries[1].length, 1024);
    assert_eq!(recovered.entries[1].object, obj2);
    assert_eq!(recovered.entries[0], entry1);
    assert_eq!(recovered.entries[1], entry2);
    assert!(recovered.validate().is_ok());
}

#[tokio::test]
async fn committing_an_object_writes_layout_and_index_together() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj = EcObjectVersionKey::new("test-bucket", "object-layout-test", None);
    let length = 768u64;
    let hash = [42u8; 32];
    let timestamp = HlcTimestamp::zero();
    let nodes = test_nodes();

    let (placement, _entry) = commit_object(
        &store,
        &mut segment,
        obj.clone(),
        length,
        hash,
        timestamp,
        nodes.clone(),
    )
    .await;

    let layout = store.get_object_layout(&obj).await.unwrap();
    assert!(layout.is_some(), "object layout must exist after commit");
    let Some(ObjectDataLayout::PackedSegment(read_placement)) = layout else {
        panic!("expected ObjectDataLayout::PackedSegment, got {layout:?}");
    };
    assert_eq!(read_placement.segment_id, segment_id);
    assert_eq!(read_placement.offset, 0);
    assert_eq!(read_placement.length, length);
    assert_eq!(read_placement.content_hash_sha256, hash);
    assert_eq!(read_placement.hlc, timestamp);
    assert_eq!(read_placement.hot_replica_nodes, nodes);
    assert_eq!(read_placement, placement);

    let recovery = store.load_packed_segments(100).await.unwrap();
    let recovered_seg = recovery
        .segments
        .iter()
        .find(|s| s.segment_id == segment_id)
        .expect("segment must be found in recovery");
    let recovered_entry = recovered_seg
        .entries
        .iter()
        .find(|e| e.object == obj)
        .expect("object must be found in segment entries");
    assert_eq!(recovered_entry.offset, read_placement.offset);
    assert_eq!(recovered_entry.length, read_placement.length);
    assert_eq!(
        recovered_entry.content_hash_sha256,
        read_placement.content_hash_sha256
    );
}

#[tokio::test]
async fn an_open_segment_survives_reopening_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("ec-meta");
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();

    let mut expected_entries = Vec::new();
    {
        let store = EcMetadataStore::open(&meta_path).unwrap();
        let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

        for i in 0..5 {
            let obj = EcObjectVersionKey::new("test-bucket", format!("key-{i}"), None);
            let length = 100 * (i + 1);
            let mut hash = [0u8; 32];
            hash[0] = i as u8;

            let (_placement, entry) = commit_object(
                &store,
                &mut segment,
                obj,
                length,
                hash,
                HlcTimestamp::zero(),
                test_nodes(),
            )
            .await;
            expected_entries.push(entry);
        }
        // Drop store
    }

    let reopened_store = EcMetadataStore::open(&meta_path).unwrap();
    let recovery = reopened_store.load_packed_segments(100).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    let recovered = &recovery.segments[0];
    assert_eq!(recovered.segment_id, segment_id);
    assert_eq!(recovered.state, PackedSegmentState::Open);
    assert_eq!(recovered.entries.len(), 5);
    for (recovered_entry, expected) in recovered.entries.iter().zip(&expected_entries) {
        assert_eq!(recovered_entry.object, expected.object);
        assert_eq!(recovered_entry.offset, expected.offset);
        assert_eq!(recovered_entry.length, expected.length);
        assert_eq!(
            recovered_entry.content_hash_sha256,
            expected.content_hash_sha256
        );
    }
    assert!(recovered.validate().is_ok());
}

#[tokio::test]
async fn sealing_replaces_the_journal_with_a_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj = EcObjectVersionKey::new("test-bucket", "seal-me", None);
    let (_placement, _entry) = commit_object(
        &store,
        &mut segment,
        obj,
        512,
        [7u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();

    let record = store.get_packed_segment(segment_id).await.unwrap();
    assert_eq!(record, Some(segment.clone()));
    assert_eq!(record.unwrap().state, PackedSegmentState::Sealed);

    let recovery = store.load_packed_segments(100).await.unwrap();
    assert_eq!(
        recovery.segments.len(),
        1,
        "load_packed_segments must return exactly one segment, not two"
    );
    let loaded = &recovery.segments[0];
    assert_eq!(loaded.segment_id, segment_id);
    assert_eq!(loaded.state, PackedSegmentState::Sealed);
    assert!(recovery.discarded.is_empty(), "discarded must be empty");
    assert!(recovery.rejected.is_empty(), "rejected must be empty");
    assert!(loaded.validate().is_ok());
}

#[tokio::test]
async fn deleting_a_packed_object_charges_dead_bytes_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("ec-meta");
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();

    let obj1 = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "obj-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "obj-3", None);

    let len1 = 100u64;
    let len2 = 200u64;
    let len3 = 300u64;
    let total_used = len1 + len2 + len3;

    {
        let store = EcMetadataStore::open(&meta_path).unwrap();
        let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

        commit_object(
            &store,
            &mut segment,
            obj1.clone(),
            len1,
            [1u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;
        commit_object(
            &store,
            &mut segment,
            obj2.clone(),
            len2,
            [2u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;
        commit_object(
            &store,
            &mut segment,
            obj3.clone(),
            len3,
            [3u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        segment.seal(2000, SealReason::TargetSizeReached).unwrap();
        store.seal_packed_segment(&segment).await.unwrap();

        let deletion = store.record_packed_object_deleted(&obj2).await.unwrap();
        assert_eq!(
            deletion,
            Some(PackedSegmentDeletion {
                segment_id,
                freed_bytes: len2,
            })
        );
        // Drop store
    }

    let reopened_store = EcMetadataStore::open(&meta_path).unwrap();
    let recovery = reopened_store.load_packed_segments(100).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    let recovered = &recovery.segments[0];

    assert_eq!(recovered.dead_bytes, len2);
    assert_eq!(recovered.entries.len(), 2);
    assert_eq!(recovered.live_bytes(), len1 + len3);
    assert_eq!(recovered.used_bytes, total_used);
    assert!(!recovered.entries.iter().any(|e| e.object == obj2));
    assert!(recovered.entries.iter().any(|e| e.object == obj1));
    assert!(recovered.entries.iter().any(|e| e.object == obj3));
    assert!(recovered.validate().is_ok());
}

#[tokio::test]
async fn deleting_a_packed_object_drops_its_layout() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj = EcObjectVersionKey::new("test-bucket", "drop-layout-test", None);
    commit_object(
        &store,
        &mut segment,
        obj.clone(),
        512,
        [9u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    assert!(store.get_object_layout(&obj).await.unwrap().is_some());

    let deletion = store.record_packed_object_deleted(&obj).await.unwrap();
    assert_eq!(
        deletion,
        Some(PackedSegmentDeletion {
            segment_id,
            freed_bytes: 512,
        })
    );

    let layout_after = store.get_object_layout(&obj).await.unwrap();
    assert_eq!(layout_after, None);
}

#[tokio::test]
async fn charging_the_same_deletion_twice_counts_it_once() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("ec-meta");
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();

    let obj1 = EcObjectVersionKey::new("test-bucket", "charge-twice-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "charge-twice-2", None);
    let len1 = 256u64;
    let len2 = 512u64;

    {
        let store = EcMetadataStore::open(&meta_path).unwrap();
        let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

        commit_object(
            &store,
            &mut segment,
            obj1.clone(),
            len1,
            [1u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;
        commit_object(
            &store,
            &mut segment,
            obj2.clone(),
            len2,
            [2u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        segment.seal(2000, SealReason::TargetSizeReached).unwrap();
        store.seal_packed_segment(&segment).await.unwrap();

        let first = store.record_packed_object_deleted(&obj1).await.unwrap();
        assert_eq!(
            first,
            Some(PackedSegmentDeletion {
                segment_id,
                freed_bytes: len1,
            })
        );

        let second = store.record_packed_object_deleted(&obj1).await.unwrap();
        assert_eq!(
            second, None,
            "second deletion must return None because layout is already removed"
        );
        // Drop store
    }

    let reopened_store = EcMetadataStore::open(&meta_path).unwrap();
    let recovery = reopened_store.load_packed_segments(100).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    let recovered = &recovery.segments[0];
    assert_eq!(
        recovered.dead_bytes, len1,
        "dead_bytes must be counted exactly once"
    );
    assert_eq!(recovered.entries.len(), 1);
    assert_eq!(recovered.entries[0].object, obj2);
    assert_eq!(recovered.live_bytes(), len2);
    assert_eq!(recovered.used_bytes, len1 + len2);
    assert!(recovered.validate().is_ok());
}

#[tokio::test]
async fn dead_bytes_are_charged_for_an_open_segment_too() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("ec-meta");
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();

    let obj1 = EcObjectVersionKey::new("test-bucket", "open-del-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "open-del-2", None);
    let len1 = 150u64;
    let len2 = 250u64;

    {
        let store = EcMetadataStore::open(&meta_path).unwrap();
        let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

        commit_object(
            &store,
            &mut segment,
            obj1.clone(),
            len1,
            [1u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;
        commit_object(
            &store,
            &mut segment,
            obj2.clone(),
            len2,
            [2u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        let deletion = store.record_packed_object_deleted(&obj1).await.unwrap();
        assert_eq!(
            deletion,
            Some(PackedSegmentDeletion {
                segment_id,
                freed_bytes: len1,
            })
        );
        // Drop store
    }

    let reopened_store = EcMetadataStore::open(&meta_path).unwrap();
    let recovery = reopened_store.load_packed_segments(100).await.unwrap();
    assert_eq!(recovery.segments.len(), 1);
    let recovered = &recovery.segments[0];
    assert_eq!(recovered.state, PackedSegmentState::Open);
    assert_eq!(recovered.dead_bytes, len1);
    assert_eq!(recovered.entries.len(), 1);
    assert_eq!(recovered.entries[0].object, obj2);
    assert_eq!(recovered.live_bytes(), len2);
    assert_eq!(recovered.used_bytes, len1 + len2);
    assert!(recovered.validate().is_ok());
}

#[tokio::test]
async fn record_packed_object_deleted_ignores_an_object_that_is_not_packed() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();

    let obj = EcObjectVersionKey::new("test-bucket", "not-packed-obj", None);
    let small_placement =
        ReplicatedSmallObjectPlacement::new(test_nodes(), [5u8; 32], 500, HlcTimestamp::zero())
            .unwrap();
    let layout = ObjectDataLayout::ReplicatedSmall(small_placement);

    store.put_object_layout(&obj, &layout).await.unwrap();

    let deletion = store.record_packed_object_deleted(&obj).await.unwrap();
    assert_eq!(
        deletion, None,
        "deleting a non-packed object must return None"
    );

    let retrieved_layout = store.get_object_layout(&obj).await.unwrap();
    assert_eq!(
        retrieved_layout,
        Some(layout),
        "non-packed object layout must remain intact"
    );
}

#[tokio::test]
async fn deleting_a_segment_removes_its_record_journal_and_dead_rows() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();

    // Segment A: to be deleted
    let segment_id_a = PackedSegmentId::new_random();
    let mut segment_a = open_segment(&store, segment_id_a, 1, &config, 1000).await;
    let obj_a1 = EcObjectVersionKey::new("test-bucket", "a1", None);
    let obj_a2 = EcObjectVersionKey::new("test-bucket", "a2", None);
    commit_object(
        &store,
        &mut segment_a,
        obj_a1.clone(),
        200,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;
    commit_object(
        &store,
        &mut segment_a,
        obj_a2.clone(),
        300,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;
    segment_a.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment_a).await.unwrap();
    // Charge a deletion to segment A (creates a row in ec_packed_segment_dead)
    store.record_packed_object_deleted(&obj_a1).await.unwrap();

    // Segment B: to remain
    let segment_id_b = PackedSegmentId::new_random();
    let mut segment_b = open_segment(&store, segment_id_b, 1, &config, 1000).await;
    let obj_b = EcObjectVersionKey::new("test-bucket", "b1", None);
    commit_object(
        &store,
        &mut segment_b,
        obj_b,
        400,
        [3u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    // Delete segment A
    store.delete_packed_segment(segment_id_a).await.unwrap();

    // Verify get_packed_segment returns None for segment A
    let record_a = store.get_packed_segment(segment_id_a).await.unwrap();
    assert_eq!(record_a, None);

    // Verify load_packed_segments does not return segment A at all
    let recovery = store.load_packed_segments(100).await.unwrap();
    assert!(
        !recovery.segments.iter().any(|s| s.segment_id == segment_id_a),
        "deleted segment A must not appear in loaded segments"
    );
    assert!(
        !recovery.discarded.iter().any(|t| t.segment_id == segment_id_a),
        "deleted segment A must not appear in discarded tails"
    );
    assert!(
        !recovery.rejected.iter().any(|r| r.segment_id == segment_id_a),
        "deleted segment A must not appear in rejected journals"
    );

    // Segment B should still be present
    assert!(
        recovery.segments.iter().any(|s| s.segment_id == segment_id_b),
        "unrelated segment B must remain loaded"
    );
}

#[tokio::test]
async fn switching_a_segment_releases_every_object_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "obj-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "obj-3", None);

    let (placement1, _) = commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        100,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement2, _) = commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        200,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement3, _) = commit_object(
        &store,
        &mut segment,
        obj3.clone(),
        300,
        [3u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();
    segment.transition_to(PackedSegmentState::Encoded).unwrap();
    store.put_packed_segment(&segment).await.unwrap();

    let switched = store.switch_packed_segment_to_ec(&segment).await.unwrap();
    assert_eq!(
        switched,
        vec![obj1.clone(), obj2.clone(), obj3.clone()],
        "switched keys list must contain all three objects"
    );

    for (obj, orig_placement) in [
        (&obj1, &placement1),
        (&obj2, &placement2),
        (&obj3, &placement3),
    ] {
        let layout = store.get_object_layout(obj).await.unwrap().expect("layout must exist");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert!(
                    p.hot_replica_nodes.is_empty(),
                    "hot replica nodes must be empty after switch"
                );
                assert_eq!(p.segment_id, segment_id);
                assert_eq!(p.offset, orig_placement.offset);
                assert_eq!(p.length, orig_placement.length);
                assert_eq!(p.content_hash_sha256, orig_placement.content_hash_sha256);
                assert_eq!(p.erasure_set_id, orig_placement.erasure_set_id);
                assert_eq!(p.hlc, orig_placement.hlc);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }

    let stored_segment =
        store.get_packed_segment(segment_id).await.unwrap().expect("segment must exist");
    assert_eq!(
        stored_segment.state,
        PackedSegmentState::Purged,
        "segment state must be Purged"
    );
}

#[tokio::test]
async fn switching_refuses_a_segment_that_is_not_encoded() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "obj-2", None);

    let (placement1, _) = commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        100,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement2, _) = commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        200,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();

    let err = store
        .switch_packed_segment_to_ec(&segment)
        .await
        .expect_err("switching a sealed segment must return an error");

    assert!(
        matches!(err, EcMetadataError::InvalidLayout { .. }),
        "expected InvalidLayout error, got: {err:?}"
    );

    for (obj, orig_placement) in [(&obj1, &placement1), (&obj2, &placement2)] {
        let layout = store.get_object_layout(obj).await.unwrap().expect("layout must exist");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert_eq!(
                    p.hot_replica_nodes,
                    test_nodes(),
                    "hot replica nodes must remain intact"
                );
                assert_eq!(p.segment_id, segment_id);
                assert_eq!(p.offset, orig_placement.offset);
                assert_eq!(p.length, orig_placement.length);
                assert_eq!(p.content_hash_sha256, orig_placement.content_hash_sha256);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }

    let stored_segment =
        store.get_packed_segment(segment_id).await.unwrap().expect("segment must exist");
    assert_eq!(
        stored_segment.state,
        PackedSegmentState::Sealed,
        "segment state must remain Sealed"
    );
}

#[tokio::test]
async fn switching_skips_an_object_that_now_points_at_another_segment() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "obj-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "obj-3", None);

    let (placement1, _) = commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        100,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement2, _) = commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        200,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement3, _) = commit_object(
        &store,
        &mut segment,
        obj3.clone(),
        300,
        [3u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();
    segment.transition_to(PackedSegmentState::Encoded).unwrap();
    store.put_packed_segment(&segment).await.unwrap();

    // Overwrite obj2 layout to point to a different segment ID
    let alien_segment_id = PackedSegmentId::new_random();
    let mut alien_placement = placement2.clone();
    alien_placement.segment_id = alien_segment_id;
    store
        .put_object_layout(&obj2, &ObjectDataLayout::PackedSegment(alien_placement))
        .await
        .unwrap();

    let switched = store.switch_packed_segment_to_ec(&segment).await.unwrap();
    assert_eq!(
        switched,
        vec![obj1.clone(), obj3.clone()],
        "overwritten object must be excluded from switched list"
    );

    // obj2 layout must be untouched (alien segment id and hot replica nodes intact)
    let layout2 = store
        .get_object_layout(&obj2)
        .await
        .unwrap()
        .expect("layout for obj2 must exist");
    match layout2 {
        ObjectDataLayout::PackedSegment(p) => {
            assert_eq!(
                p.segment_id, alien_segment_id,
                "alien segment id must remain untouched"
            );
            assert_eq!(
                p.hot_replica_nodes,
                test_nodes(),
                "hot replica nodes for alien segment must remain intact"
            );
            assert_eq!(p.offset, placement2.offset);
            assert_eq!(p.length, placement2.length);
        }
        other => panic!("expected PackedSegment layout, got {other:?}"),
    }

    // obj1 and obj3 must have hot replica nodes cleared
    for (obj, orig_placement) in [(&obj1, &placement1), (&obj3, &placement3)] {
        let layout = store.get_object_layout(obj).await.unwrap().expect("layout must exist");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert!(p.hot_replica_nodes.is_empty());
                assert_eq!(p.segment_id, segment_id);
                assert_eq!(p.offset, orig_placement.offset);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }

    let stored_segment =
        store.get_packed_segment(segment_id).await.unwrap().expect("segment must exist");
    assert_eq!(stored_segment.state, PackedSegmentState::Purged);
}

#[tokio::test]
async fn switching_skips_an_object_whose_layout_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "obj-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "obj-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "obj-3", None);

    let (placement1, _) = commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        100,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        200,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    let (placement3, _) = commit_object(
        &store,
        &mut segment,
        obj3.clone(),
        300,
        [3u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();
    segment.transition_to(PackedSegmentState::Encoded).unwrap();
    store.put_packed_segment(&segment).await.unwrap();

    // Delete layout of obj2
    store.record_packed_object_deleted(&obj2).await.unwrap();
    assert_eq!(store.get_object_layout(&obj2).await.unwrap(), None);

    let switched = store.switch_packed_segment_to_ec(&segment).await.unwrap();
    assert_eq!(
        switched,
        vec![obj1.clone(), obj3.clone()],
        "deleted object must not be in switched list"
    );

    assert_eq!(
        store.get_object_layout(&obj2).await.unwrap(),
        None,
        "deleted object layout must remain None"
    );

    for (obj, orig_placement) in [(&obj1, &placement1), (&obj3, &placement3)] {
        let layout = store.get_object_layout(obj).await.unwrap().expect("layout must exist");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert!(p.hot_replica_nodes.is_empty());
                assert_eq!(p.segment_id, segment_id);
                assert_eq!(p.offset, orig_placement.offset);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }

    let stored_segment =
        store.get_packed_segment(segment_id).await.unwrap().expect("segment must exist");
    assert_eq!(stored_segment.state, PackedSegmentState::Purged);
}

#[tokio::test]
async fn switching_is_all_or_nothing_across_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let meta_path = dir.path().join("ec-meta");
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();

    let obj1 = EcObjectVersionKey::new("test-bucket", "reopen-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "reopen-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "reopen-3", None);

    {
        let store = EcMetadataStore::open(&meta_path).unwrap();
        let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

        commit_object(
            &store,
            &mut segment,
            obj1.clone(),
            100,
            [1u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        commit_object(
            &store,
            &mut segment,
            obj2.clone(),
            200,
            [2u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        commit_object(
            &store,
            &mut segment,
            obj3.clone(),
            300,
            [3u8; 32],
            HlcTimestamp::zero(),
            test_nodes(),
        )
        .await;

        segment.seal(2000, SealReason::TargetSizeReached).unwrap();
        store.seal_packed_segment(&segment).await.unwrap();
        segment.transition_to(PackedSegmentState::Encoded).unwrap();
        store.put_packed_segment(&segment).await.unwrap();

        let switched = store.switch_packed_segment_to_ec(&segment).await.unwrap();
        assert_eq!(switched.len(), 3);
        // Explicitly drop store to test persistence across reopen
    }

    let reopened = EcMetadataStore::open(&meta_path).unwrap();

    for obj in [&obj1, &obj2, &obj3] {
        let layout = reopened
            .get_object_layout(obj)
            .await
            .unwrap()
            .expect("layout must exist after reopen");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert!(
                    p.hot_replica_nodes.is_empty(),
                    "hot replica nodes must be empty after switch and reopen"
                );
                assert_eq!(p.segment_id, segment_id);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }

    let stored_segment = reopened
        .get_packed_segment(segment_id)
        .await
        .unwrap()
        .expect("segment must exist after reopen");
    assert_eq!(
        stored_segment.state,
        PackedSegmentState::Purged,
        "segment state must be Purged across reopen"
    );
}

#[tokio::test]
async fn switching_an_already_purged_segment_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
    let config = test_config();
    let segment_id = PackedSegmentId::new_random();
    let mut segment = open_segment(&store, segment_id, 1, &config, 1000).await;

    let obj1 = EcObjectVersionKey::new("test-bucket", "purged-1", None);
    let obj2 = EcObjectVersionKey::new("test-bucket", "purged-2", None);
    let obj3 = EcObjectVersionKey::new("test-bucket", "purged-3", None);

    commit_object(
        &store,
        &mut segment,
        obj1.clone(),
        100,
        [1u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    commit_object(
        &store,
        &mut segment,
        obj2.clone(),
        200,
        [2u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    commit_object(
        &store,
        &mut segment,
        obj3.clone(),
        300,
        [3u8; 32],
        HlcTimestamp::zero(),
        test_nodes(),
    )
    .await;

    segment.seal(2000, SealReason::TargetSizeReached).unwrap();
    store.seal_packed_segment(&segment).await.unwrap();
    segment.transition_to(PackedSegmentState::Encoded).unwrap();
    store.put_packed_segment(&segment).await.unwrap();

    let first_switched = store.switch_packed_segment_to_ec(&segment).await.unwrap();
    assert_eq!(first_switched.len(), 3);

    let purged_segment = store
        .get_packed_segment(segment_id)
        .await
        .unwrap()
        .expect("segment must exist in store");
    assert_eq!(purged_segment.state, PackedSegmentState::Purged);

    // Second switch call on the purged segment must be refused with InvalidLayout
    let err = store
        .switch_packed_segment_to_ec(&purged_segment)
        .await
        .expect_err(
            "second switch call on a purged segment must fail with InvalidLayout because the segment is no longer encoded",
        );

    let reason = match err {
        EcMetadataError::InvalidLayout { reason } => reason,
        other => panic!("expected InvalidLayout error, got {other:?}"),
    };
    assert!(
        reason.to_lowercase().contains("purged"),
        "error message must mention purged state, got: {reason}"
    );

    // Verify metadata was not corrupted and hot replica nodes remain empty
    for obj in [&obj1, &obj2, &obj3] {
        let layout = store.get_object_layout(obj).await.unwrap().expect("layout must exist");
        match layout {
            ObjectDataLayout::PackedSegment(p) => {
                assert!(p.hot_replica_nodes.is_empty());
                assert_eq!(p.segment_id, segment_id);
            }
            other => panic!("expected PackedSegment layout, got {other:?}"),
        }
    }
}
