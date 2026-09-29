// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Repacking a fragmented packed segment, end to end over durable metadata.
//!
//! The unit tests around the coordinator prove each step in isolation. What
//! these prove is that the steps join up across a restart: a repack is staged in
//! one cycle, encoded in another and cut over in a third, and a node that dies
//! between any two of them resumes rather than starts again — or, before the
//! cutover, simply carries on serving every object from the segment it was
//! already serving them from.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use s4_cluster::HlcTimestamp;
use s4_ee::ec_metadata::{
    EcMetadataStore, EcObjectVersionKey, ObjectDataLayout, PackedRepackMove,
    PackedSegmentObjectPlacement,
};
use s4_ee::ec_packed_segments::{
    PackedObjectIdentity, PackedSegment, PackedSegmentConfig, PackedSegmentId, PackedSegmentState,
    SealReason,
};

const ERASURE_SET_ID: u64 = 9;
const OBJECT_LENGTH: u64 = 128;

fn test_config() -> PackedSegmentConfig {
    PackedSegmentConfig {
        enabled: true,
        target_bytes: 1024 * 1024,
        max_age: Duration::from_secs(300),
        max_objects: 100,
        max_open_segments: 4,
    }
}

fn object(index: usize) -> EcObjectVersionKey {
    EcObjectVersionKey::new("photos", format!("packed-{index}.bin"), None)
}

fn open_store(root: &Path) -> EcMetadataStore {
    EcMetadataStore::open(&root.join("ec-meta")).expect("metadata store should open")
}

/// A purged segment whose first `dead` objects have been deleted from it.
///
/// Deleting from the front leaves the survivors at a non-zero offset, so a
/// replacement genuinely has to close a hole rather than copy a prefix.
fn fragmented_segment(live: usize, dead: usize) -> PackedSegment {
    let config = test_config();
    let mut segment = PackedSegment::open(
        PackedSegmentId::new_random(),
        ERASURE_SET_ID,
        &config,
        1_000,
    );

    for index in 0..(live + dead) {
        let reservation = segment.reserve(OBJECT_LENGTH).expect("reserve should succeed");
        let identity = PackedObjectIdentity::new(
            object(index),
            [index as u8 + 1; 32],
            HlcTimestamp::from_record(100 + index as u64, 0, 1),
        )
        .with_s3_metadata(
            format!("\"etag-{index}\""),
            "application/octet-stream",
            HashMap::from([("x-amz-meta-index".to_string(), index.to_string())]),
        );
        let item = segment
            .journal_append(&reservation, identity)
            .expect("append record should build");
        segment.commit(&reservation, &item).expect("commit should succeed");
    }

    segment.seal(2_000, SealReason::TargetSizeReached).expect("seal should succeed");
    segment
        .transition_to(PackedSegmentState::Encoded)
        .expect("encode should be allowed");
    segment
        .transition_to(PackedSegmentState::Purged)
        .expect("purge should be allowed");

    for index in 0..dead {
        segment
            .record_object_deleted(&object(index))
            .expect("the object is in the segment");
    }
    segment
}

/// Store a fragmented segment together with the layouts of its survivors.
async fn store_fragmented_segment(store: &EcMetadataStore, segment: &PackedSegment) {
    store
        .put_packed_segment(segment)
        .await
        .expect("segment record should be written");
    for entry in &segment.entries {
        // No hot replicas: a purged segment's objects are served by its shards,
        // which is what makes its dead space unreclaimable without a repack.
        let placement = PackedSegmentObjectPlacement::from_entry(segment, entry, Vec::new())
            .expect("placement should be valid");
        store
            .put_object_layout(&entry.object, &ObjectDataLayout::PackedSegment(placement))
            .await
            .expect("layout should be written");
    }
}

/// Build the replacement a repack stages, and persist it sealed.
async fn stage_replacement(store: &EcMetadataStore, source: &PackedSegment) -> PackedSegment {
    let config = test_config();
    let mut destination =
        PackedSegment::open_repack(PackedSegmentId::new_random(), source, &config, 3_000);
    for entry in &source.entries {
        let identity =
            PackedObjectIdentity::new(entry.object.clone(), entry.content_hash_sha256, entry.hlc)
                .with_s3_metadata(
                    entry.etag.clone(),
                    entry.content_type.clone(),
                    entry.user_metadata.clone(),
                );
        destination
            .append_without_journal(identity, entry.length)
            .expect("the replacement is sized to hold the source's live part");
    }
    destination.seal(3_000, SealReason::Repacked).expect("seal should succeed");
    store
        .put_packed_segment(&destination)
        .await
        .expect("replacement should be written");
    destination
}

/// Mark a staged replacement encoded, as the transcode pipeline does.
async fn encode_replacement(store: &EcMetadataStore, destination: &PackedSegment) -> PackedSegment {
    let mut encoded = store
        .get_packed_segment(destination.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("the replacement should still be recorded");
    encoded
        .transition_to(PackedSegmentState::Encoded)
        .expect("encode should be allowed");
    store.put_packed_segment(&encoded).await.expect("replacement should be written");
    encoded
}

fn moves_onto(destination: &PackedSegment) -> Vec<PackedRepackMove> {
    destination
        .entries
        .iter()
        .map(|entry| PackedRepackMove {
            object: entry.object.clone(),
            placement: PackedSegmentObjectPlacement::from_entry(destination, entry, Vec::new())
                .expect("placement should be valid"),
        })
        .collect()
}

async fn layout_segment_of(
    store: &EcMetadataStore,
    object: &EcObjectVersionKey,
) -> Option<PackedSegmentId> {
    match store.get_object_layout(object).await.expect("metadata read should succeed") {
        Some(ObjectDataLayout::PackedSegment(placement)) => Some(placement.segment_id),
        _ => None,
    }
}

#[tokio::test]
async fn a_crash_before_the_cutover_leaves_every_object_on_the_source() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = fragmented_segment(2, 2);
    let destination_id;
    {
        let store = open_store(dir.path());
        store_fragmented_segment(&store, &source).await;
        let destination = stage_replacement(&store, &source).await;
        encode_replacement(&store, &destination).await;
        destination_id = destination.segment_id;
    }

    // The node dies here: the replacement exists and is encoded, but nothing
    // points at it.
    let store = open_store(dir.path());

    for entry in &source.entries {
        assert_eq!(
            layout_segment_of(&store, &entry.object).await,
            Some(source.segment_id),
            "an unfinished repack must leave every object where it was"
        );
    }
    let stored_source = store
        .get_packed_segment(source.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("the source should still be recorded");
    assert_eq!(stored_source.state, PackedSegmentState::Purged);
    assert_eq!(stored_source.entries.len(), 2);

    let stored_destination = store
        .get_packed_segment(destination_id)
        .await
        .expect("metadata read should succeed")
        .expect("the replacement should have survived the restart");
    assert_eq!(
        stored_destination.repacked_from,
        Some(source.segment_id),
        "the link is what lets the next cycle finish the repack instead of starting a new one"
    );
}

#[tokio::test]
async fn a_repack_cutover_survives_reopening_the_store() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = fragmented_segment(3, 3);
    let destination_id;
    {
        let store = open_store(dir.path());
        store_fragmented_segment(&store, &source).await;
        let destination = stage_replacement(&store, &source).await;
        let encoded = encode_replacement(&store, &destination).await;
        let cutover = store
            .commit_packed_repack(&source, &encoded, &moves_onto(&encoded), 9_000)
            .await
            .expect("cutover should succeed");
        assert_eq!(cutover.switched.len(), 3);
        assert!(cutover.abandoned.is_empty());
        assert_eq!(cutover.reclaimed_bytes, source.used_bytes);
        destination_id = encoded.segment_id;
    }

    let store = open_store(dir.path());

    for entry in &source.entries {
        assert_eq!(
            layout_segment_of(&store, &entry.object).await,
            Some(destination_id),
            "the cutover must survive the restart for every object at once"
        );
    }
    let retired = store
        .get_packed_segment(source.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("a retired segment keeps its record until its shards go");
    assert_eq!(retired.state, PackedSegmentState::Dead);
    assert_eq!(retired.dead_at, Some(9_000));
    let replacement = store
        .get_packed_segment(destination_id)
        .await
        .expect("metadata read should succeed")
        .expect("the replacement should be recorded");
    assert_eq!(replacement.state, PackedSegmentState::Purged);
}

#[tokio::test]
async fn every_live_object_keeps_its_content_and_metadata_through_a_repack() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = fragmented_segment(3, 3);
    let store = open_store(dir.path());
    store_fragmented_segment(&store, &source).await;
    let destination = stage_replacement(&store, &source).await;
    let encoded = encode_replacement(&store, &destination).await;

    store
        .commit_packed_repack(&source, &encoded, &moves_onto(&encoded), 9_000)
        .await
        .expect("cutover should succeed");

    let replacement = store
        .get_packed_segment(encoded.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("the replacement should be recorded");
    assert_eq!(
        replacement.used_bytes,
        source.live_bytes(),
        "the holes are closed"
    );
    assert_eq!(replacement.dead_bytes, 0);

    let mut expected_offset = 0u64;
    for source_entry in &source.entries {
        let moved = replacement
            .entry_for(&source_entry.object)
            .expect("every live object moved across");
        assert_eq!(
            moved.offset, expected_offset,
            "the replacement holds no holes"
        );
        assert_eq!(moved.length, source_entry.length);
        assert_eq!(moved.content_hash_sha256, source_entry.content_hash_sha256);
        // A HEAD after the move has to answer exactly as it did before it: the
        // index entry is the only thing left describing the object.
        assert_eq!(moved.etag, source_entry.etag);
        assert_eq!(moved.content_type, source_entry.content_type);
        assert_eq!(moved.user_metadata, source_entry.user_metadata);
        expected_offset += moved.length;

        match store
            .get_object_layout(&source_entry.object)
            .await
            .expect("metadata read should succeed")
            .expect("a live object keeps a layout")
        {
            ObjectDataLayout::PackedSegment(placement) => {
                assert_eq!(placement.segment_id, replacement.segment_id);
                assert_eq!(placement.offset, moved.offset);
                assert_eq!(placement.length, moved.length);
                assert_eq!(placement.content_hash_sha256, moved.content_hash_sha256);
                assert!(
                    placement.hot_replica_nodes.is_empty(),
                    "a repacked object is served by shards and names no hot copies"
                );
            }
            other => panic!("expected a packed layout, got {other:?}"),
        }
    }

    // The objects deleted before the repack stay deleted.
    for index in 0..3 {
        assert!(
            store
                .get_object_layout(&object(index))
                .await
                .expect("metadata read should succeed")
                .is_none(),
            "a repack must not bring a deleted object back"
        );
    }
}

#[tokio::test]
async fn a_retired_segment_keeps_its_index_for_readers_that_started_before_the_switch() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = fragmented_segment(2, 2);
    let store = open_store(dir.path());
    store_fragmented_segment(&store, &source).await;

    // The snapshot a read takes on the way in, before anything moves.
    let reading = source.entries[0].object.clone();
    let in_flight = match store
        .get_object_layout(&reading)
        .await
        .expect("metadata read should succeed")
        .expect("the object is packed")
    {
        ObjectDataLayout::PackedSegment(placement) => placement,
        other => panic!("expected a packed layout, got {other:?}"),
    };

    let destination = stage_replacement(&store, &source).await;
    let encoded = encode_replacement(&store, &destination).await;
    store
        .commit_packed_repack(&source, &encoded, &moves_onto(&encoded), 9_000)
        .await
        .expect("cutover should succeed");

    // Everything that read holds still resolves: the record it named, and the
    // index entry it takes the object's ETag and content type from.
    let retired = store
        .get_packed_segment(in_flight.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("a retired segment keeps its record for the readers still on it");
    assert_eq!(retired.state, PackedSegmentState::Dead);
    let entry = retired.entry_for(&reading).expect("the entry a read resolves its facts from");
    assert_eq!(entry.offset, in_flight.offset);
    assert_eq!(entry.length, in_flight.length);
    assert!(
        store
            .packed_segment_referrers(&retired, 64)
            .await
            .expect("metadata read should succeed")
            .is_empty(),
        "no read starting now can reach the retired segment"
    );
}

#[tokio::test]
async fn an_object_deleted_during_the_repack_is_not_brought_back() {
    let dir = tempfile::tempdir().expect("temp dir");
    let source = fragmented_segment(2, 2);
    let store = open_store(dir.path());
    store_fragmented_segment(&store, &source).await;
    let destination = stage_replacement(&store, &source).await;
    let encoded = encode_replacement(&store, &destination).await;

    // A DELETE lands after the replacement was encoded and before the cutover.
    let deleted = source.entries[0].object.clone();
    store
        .record_packed_object_deleted(&deleted)
        .await
        .expect("charging the deletion should succeed");

    let cutover = store
        .commit_packed_repack(&source, &encoded, &moves_onto(&encoded), 9_000)
        .await
        .expect("cutover should succeed");

    assert_eq!(cutover.abandoned, vec![deleted.clone()]);
    assert_eq!(cutover.switched.len(), 1);
    assert!(
        store
            .get_object_layout(&deleted)
            .await
            .expect("metadata read should succeed")
            .is_none(),
        "the cutover must not resurrect an object that was deleted while it ran"
    );

    let replacement = store
        .get_packed_segment(encoded.segment_id)
        .await
        .expect("metadata read should succeed")
        .expect("the replacement should be recorded");
    assert!(replacement.entry_for(&deleted).is_none());
    assert_eq!(
        replacement.dead_bytes, OBJECT_LENGTH,
        "the replacement was encoded holding the object and cannot give the room back"
    );
}
