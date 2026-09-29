// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

use std::sync::Mutex;
use std::time::Duration;

use s4_cluster::HlcTimestamp;
use s4_ee::ec_metadata::EcObjectVersionKey;
use s4_ee::ec_packed_segments::{
    recover_segments, PackedObjectIdentity, PackedSegment, PackedSegmentConfig, PackedSegmentEntry,
    PackedSegmentError, PackedSegmentJournal, PackedSegmentJournalItem, PackedSegmentPool,
    PackedSegmentReservation,
};

/// In-memory implementation of [`PackedSegmentJournal`] for testing crash recovery.
struct MemoryJournal {
    records: Mutex<Vec<PackedSegmentJournalItem>>,
}

impl MemoryJournal {
    fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    fn len(&self) -> usize {
        self.records.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).len()
    }

    fn is_empty(&self) -> bool {
        self.records.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).is_empty()
    }

    /// Truncate the journal to `keep` records, simulating a torn write tail on crash.
    fn truncate_to(&self, keep: usize) {
        let mut records = self.records.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        records.truncate(keep);
    }
}

impl Default for MemoryJournal {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl PackedSegmentJournal for MemoryJournal {
    async fn append(&self, item: &PackedSegmentJournalItem) -> Result<(), PackedSegmentError> {
        let mut records = self.records.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        records.push(item.clone());
        Ok(())
    }

    async fn load(&self) -> Result<Vec<PackedSegmentJournalItem>, PackedSegmentError> {
        let records = self.records.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        Ok(records.clone())
    }
}

/// Helper that drives one object through the durability contract in the strictly required order:
/// 1. `pool.reserve(len, now)`
/// 2. Write all `admission.prerequisites` to journal
/// 3. `pool.journal_append(...)`
/// 4. Write obtained item to journal
/// 5. `pool.commit(&reservation, &item)`
///
/// Durability must always precede commit: an object is considered acknowledged only after durable write.
async fn append_object_durably(
    pool: &mut PackedSegmentPool,
    journal: &MemoryJournal,
    object: EcObjectVersionKey,
    length: u64,
    content_hash_sha256: [u8; 32],
    hlc: HlcTimestamp,
    now: u64,
) -> Result<(PackedSegmentReservation, PackedSegmentEntry), PackedSegmentError> {
    let admission = pool.reserve(length, now)?;
    for prereq in &admission.prerequisites {
        journal.append(prereq).await?;
    }
    let item = pool.journal_append(
        &admission.reservation,
        PackedObjectIdentity::new(object, content_hash_sha256, hlc),
    )?;
    journal.append(&item).await?;
    let entry = pool.commit(&admission.reservation, &item)?;
    Ok((admission.reservation, entry))
}

#[tokio::test]
async fn pool_survives_a_simulated_restart() {
    let config = PackedSegmentConfig {
        enabled: true,
        target_bytes: 4096,
        max_age: Duration::from_secs(300),
        max_objects: 64,
        max_open_segments: 2,
    };
    let erasure_set_id = 1;
    let mut pool = PackedSegmentPool::new(erasure_set_id, config);
    let journal = MemoryJournal::new();
    let now = 1_000_000_000u64;

    let mut recorded_placements = Vec::new();
    for i in 1..=12 {
        let obj = EcObjectVersionKey::new("test-bucket", format!("key-{i}"), None);
        let mut hash = [0u8; 32];
        hash[0] = i as u8;

        let (reservation, entry) = append_object_durably(
            &mut pool,
            &journal,
            obj.clone(),
            512,
            hash,
            HlcTimestamp::zero(),
            now,
        )
        .await
        .expect("durable append should succeed");

        assert_eq!(reservation.offset, entry.offset);
        recorded_placements.push((obj, reservation.segment_id, entry.offset));
    }

    // Verify more than one segment was opened.
    let total_segments_before = pool.open_segments().len() + pool.sealed_segments().len();
    assert!(
        total_segments_before > 1,
        "expected more than 1 segment, found {total_segments_before}"
    );

    // Simulated restart: reload journal and recover segments.
    let items = journal.load().await.expect("journal load should succeed");
    let recovery = recover_segments(items);

    assert!(
        recovery.rejected.is_empty(),
        "recovery rejected should be empty: {:?}",
        recovery.rejected
    );
    assert!(
        recovery.discarded.is_empty(),
        "recovery discarded should be empty: {:?}",
        recovery.discarded
    );

    let recovered_pool =
        PackedSegmentPool::from_recovered(erasure_set_id, config, recovery.segments)
            .expect("pool recovery should succeed");

    // Verify each of the 12 objects is present at the exact same segment and offset.
    for (expected_obj, expected_seg_id, expected_offset) in &recorded_placements {
        let segment = recovered_pool
            .open_segments()
            .iter()
            .chain(recovered_pool.sealed_segments().iter())
            .find(|s| s.segment_id == *expected_seg_id)
            .unwrap_or_else(|| panic!("segment {expected_seg_id} not found in recovered pool"));

        let entry =
            segment.entries.iter().find(|e| e.object == *expected_obj).unwrap_or_else(|| {
                panic!(
                    "object {} not found in segment {expected_seg_id}",
                    expected_obj.key
                )
            });

        assert_eq!(
            entry.offset, *expected_offset,
            "object {} offset mismatch in segment {}",
            expected_obj.key, expected_seg_id
        );
    }

    // Verify each recovered segment validates cleanly.
    for segment in recovered_pool
        .open_segments()
        .iter()
        .chain(recovered_pool.sealed_segments().iter())
    {
        segment.validate().expect("recovered segment must be valid");
    }
}

#[tokio::test]
async fn restart_after_a_torn_tail_keeps_every_acknowledged_object() {
    let config = PackedSegmentConfig {
        enabled: true,
        target_bytes: 4096,
        max_age: Duration::from_secs(300),
        max_objects: 64,
        max_open_segments: 2,
    };
    let erasure_set_id = 1;
    let mut pool = PackedSegmentPool::new(erasure_set_id, config);
    let journal = MemoryJournal::new();
    let now = 1_000_000_000u64;

    let mut recorded_placements = Vec::new();
    for i in 1..=12 {
        let obj = EcObjectVersionKey::new("test-bucket", format!("key-{i}"), None);
        let mut hash = [0u8; 32];
        hash[0] = i as u8;

        let (reservation, entry) = append_object_durably(
            &mut pool,
            &journal,
            obj.clone(),
            512,
            hash,
            HlcTimestamp::zero(),
            now,
        )
        .await
        .expect("durable append should succeed");

        assert_eq!(reservation.offset, entry.offset);
        recorded_placements.push((obj, reservation.segment_id, entry.offset));
    }

    // Simulate crash with a torn tail by truncating the last record.
    assert!(!journal.is_empty());
    journal.truncate_to(journal.len() - 1);

    // Simulated restart: replay truncated journal.
    let items = journal.load().await.expect("journal load should succeed");
    let recovery = recover_segments(items);

    assert!(
        recovery.rejected.is_empty(),
        "recovery rejected should be empty: {:?}",
        recovery.rejected
    );

    // Each recovered segment must pass validate().
    for segment in &recovery.segments {
        segment.validate().expect("every recovered segment must pass validate()");
    }

    let recovered_pool =
        PackedSegmentPool::from_recovered(erasure_set_id, config, recovery.segments)
            .expect("pool recovery should succeed");

    // The first 11 acknowledged objects must be present with unchanged (segment_id, offset).
    for (expected_obj, expected_seg_id, expected_offset) in &recorded_placements[..11] {
        let segment = recovered_pool
            .open_segments()
            .iter()
            .chain(recovered_pool.sealed_segments().iter())
            .find(|s| s.segment_id == *expected_seg_id)
            .unwrap_or_else(|| panic!("segment {expected_seg_id} not found in recovered pool"));

        let entry =
            segment.entries.iter().find(|e| e.object == *expected_obj).unwrap_or_else(|| {
                panic!(
                    "object {} not found in segment {expected_seg_id}",
                    expected_obj.key
                )
            });

        assert_eq!(
            entry.offset, *expected_offset,
            "object {} offset mismatch in segment {}",
            expected_obj.key, expected_seg_id
        );
    }

    // The 12th object (torn tail) must be absent.
    let (last_obj, _, _) = &recorded_placements[11];
    let last_found = recovered_pool
        .open_segments()
        .iter()
        .chain(recovered_pool.sealed_segments().iter())
        .any(|s| s.entries.iter().any(|e| e.object == *last_obj));
    assert!(
        !last_found,
        "the 12th object ({}) must be absent after torn tail",
        last_obj.key
    );

    let total_recovered: usize = recovered_pool
        .open_segments()
        .iter()
        .chain(recovered_pool.sealed_segments().iter())
        .map(|s| s.entries.len())
        .sum();
    assert_eq!(total_recovered, 11);
}

#[tokio::test]
async fn restart_reopens_pool_and_continues_appending() {
    let config = PackedSegmentConfig {
        enabled: true,
        target_bytes: 4096,
        max_age: Duration::from_secs(300),
        max_objects: 64,
        max_open_segments: 2,
    };
    let erasure_set_id = 1;
    let mut pool = PackedSegmentPool::new(erasure_set_id, config);
    let journal = MemoryJournal::new();
    let now = 1_000_000_000u64;

    for i in 1..=12 {
        let obj = EcObjectVersionKey::new("test-bucket", format!("key-{i}"), None);
        let mut hash = [0u8; 32];
        hash[0] = i as u8;

        append_object_durably(
            &mut pool,
            &journal,
            obj,
            512,
            hash,
            HlcTimestamp::zero(),
            now,
        )
        .await
        .expect("initial durable append should succeed");
    }

    // Simulated restart as in test (a).
    let items = journal.load().await.expect("journal load should succeed");
    let recovery = recover_segments(items);
    assert!(recovery.rejected.is_empty());
    assert!(recovery.discarded.is_empty());

    let mut recovered_pool =
        PackedSegmentPool::from_recovered(erasure_set_id, config, recovery.segments)
            .expect("pool recovery should succeed");

    // Append 3 more objects to the recovered pool using the same helper.
    for i in 13..=15 {
        let obj = EcObjectVersionKey::new("test-bucket", format!("key-{i}"), None);
        let mut hash = [0u8; 32];
        hash[0] = i as u8;

        append_object_durably(
            &mut recovered_pool,
            &journal,
            obj,
            512,
            hash,
            HlcTimestamp::zero(),
            now,
        )
        .await
        .expect("append to recovered pool should succeed");
    }

    // Verify all segments: validate() returns Ok and intervals [offset, offset + length) do not overlap.
    let all_segments: Vec<&PackedSegment> = recovered_pool
        .open_segments()
        .iter()
        .chain(recovered_pool.sealed_segments().iter())
        .collect();

    let mut total_objects = 0;
    for segment in &all_segments {
        segment.validate().expect("each segment must validate successfully");

        total_objects += segment.entries.len();

        // Check pairwise non-overlap of [offset, offset + length)
        let entries = &segment.entries;
        for (i, entry_i) in entries.iter().enumerate() {
            let start_i = entry_i.offset;
            let end_i = start_i + entry_i.length;
            for entry_j in entries.iter().skip(i + 1) {
                let start_j = entry_j.offset;
                let end_j = start_j + entry_j.length;
                assert!(
                    end_i <= start_j || end_j <= start_i,
                    "segment {} has overlapping object intervals: [{}, {}) and [{}, {})",
                    segment.segment_id,
                    start_i,
                    end_i,
                    start_j,
                    end_j
                );
            }
        }
    }

    assert_eq!(
        total_objects, 15,
        "expected 15 total objects across segments"
    );
}
