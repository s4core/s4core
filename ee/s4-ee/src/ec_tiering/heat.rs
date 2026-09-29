// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Object activity tracking for auto-tiering.
//!
//! Auto-tiering needs to know when an object was last used. Recording that on
//! the request path is not free: a metadata write costs an fsync, so a mark
//! written per GET would make cold storage slower than hot storage — the exact
//! opposite of the point. Decision [EC2-020] resolves this in three steps, and
//! this module implements them:
//!
//! 1. The request path only sends a signal into a bounded channel and never
//!    waits. A full channel drops the signal; that is normal, not an error.
//! 2. A background task collapses signals per object, so any number of accesses
//!    between two flushes becomes one entry.
//! 3. A durable write happens only once the stored mark is older than the
//!    configured resolution. An hour of accuracy against a threshold measured
//!    in days is a deliberate trade.
//!
//! The mark is node-local. Reads of one object spread across whichever nodes
//! serve them, so what a single node stores is a lower bound on activity. The
//! migration decision has to take the newest mark across the nodes of the set;
//! that fan-out belongs to the tiering worker, not here.
//!
//! [EC2-020]: `EC-v2/adr/EC2-020-heat-tracking.md`

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::ec_metadata::{
    EcHeatWindow, EcMetadataError, EcMetadataStore, EcObjectHeatRecord, EcObjectVersionKey,
};
use crate::ec_metrics;
use crate::ec_runtime::unix_time_nanos;

/// Runtime settings of the activity tracker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcHeatConfig {
    /// Whether access marks are recorded at all.
    pub enabled: bool,
    /// How much newer a mark has to be before it is written durably.
    pub resolution: Duration,
    /// Interval between two flushes of collapsed signals.
    pub flush_interval: Duration,
    /// Capacity of the bounded signal channel.
    pub queue_capacity: usize,
    /// Width of the window that counts reads for reverse tiering.
    pub window: Duration,
}

/// What kind of access produced a signal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EcAccessKind {
    /// A successful GET or HEAD.
    Read,
    /// A successful write commit.
    Write,
}

/// Counters the tracker exposes to the admin status and to Prometheus.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct EcHeatCountersSnapshot {
    /// Signals accepted into the channel.
    pub signals: u64,
    /// Signals dropped because the channel was full.
    pub dropped: u64,
    /// Flush cycles completed.
    pub flushes: u64,
    /// Marks actually written to durable metadata.
    pub records_written: u64,
    /// Objects held in the batch at the last flush.
    pub pending: u64,
}

#[derive(Debug, Default)]
struct EcHeatCounters {
    signals: AtomicU64,
    dropped: AtomicU64,
    flushes: AtomicU64,
    records_written: AtomicU64,
    pending: AtomicU64,
}

impl EcHeatCounters {
    fn snapshot(&self) -> EcHeatCountersSnapshot {
        EcHeatCountersSnapshot {
            signals: self.signals.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            records_written: self.records_written.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
struct HeatSignal {
    object: EcObjectVersionKey,
    access_nanos: u64,
    kind: EcAccessKind,
}

/// One object's accesses between two flushes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingHeat {
    last_access_nanos: u64,
    reads: u32,
}

/// Signals collapsed per object.
///
/// Kept separate from the flusher task so that collapsing can be tested on its
/// own: it is the property the whole design rests on.
///
/// The map is capped. A scan that touches hundreds of thousands of distinct
/// keys in one interval would otherwise hold them all in memory and then write
/// them one durable record at a time, and while that ran the flusher would not
/// be draining the channel — turning a wide read pattern into exactly the
/// signal loss the design exists to avoid. Past the cap a new object's mark is
/// dropped, which costs accuracy and nothing else.
#[derive(Debug)]
struct HeatBatch {
    entries: HashMap<EcObjectVersionKey, PendingHeat>,
    max_entries: usize,
}

impl HeatBatch {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.max(1),
        }
    }

    /// Fold one signal in. Returns false when the batch is full and the object
    /// is not already in it.
    fn record(&mut self, signal: HeatSignal) -> bool {
        if !self.entries.contains_key(&signal.object) && self.entries.len() >= self.max_entries {
            return false;
        }
        let entry = self.entries.entry(signal.object).or_insert(PendingHeat {
            last_access_nanos: 0,
            reads: 0,
        });
        entry.last_access_nanos = entry.last_access_nanos.max(signal.access_nanos);
        if matches!(signal.kind, EcAccessKind::Read) {
            entry.reads = entry.reads.saturating_add(1);
        }
        true
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove up to `limit` entries to write.
    ///
    /// A flush writes a bounded slice rather than everything it holds, so the
    /// task returns to draining the channel promptly. What is left waits for
    /// the next tick.
    fn take(&mut self, limit: usize) -> Vec<(EcObjectVersionKey, PendingHeat)> {
        let keys: Vec<EcObjectVersionKey> = self.entries.keys().take(limit).cloned().collect();
        keys.into_iter().filter_map(|key| self.entries.remove_entry(&key)).collect()
    }

    /// Put an entry back because its durable write did not happen.
    fn carry(&mut self, object: EcObjectVersionKey, pending: PendingHeat) {
        if self.entries.len() >= self.max_entries {
            return;
        }
        let entry = self.entries.entry(object).or_insert(PendingHeat {
            last_access_nanos: 0,
            reads: 0,
        });
        entry.last_access_nanos = entry.last_access_nanos.max(pending.last_access_nanos);
        entry.reads = entry.reads.saturating_add(pending.reads);
    }
}

/// The half of the tracker's state that the flusher needs.
///
/// It deliberately holds no sender. If the flusher kept the sending half alive,
/// dropping the service would never close the channel, and the task would wait
/// forever on a queue nobody can fill instead of flushing what it holds and
/// exiting.
struct HeatShared {
    config: EcHeatConfig,
    counters: EcHeatCounters,
}

struct HeatTrackerInner {
    shared: Arc<HeatShared>,
    signals: mpsc::Sender<HeatSignal>,
    receiver: Mutex<Option<mpsc::Receiver<HeatSignal>>>,
    flusher_started: AtomicBool,
}

/// Records object accesses without ever blocking the request that caused them.
pub struct EcHeatTracker {
    inner: Option<Arc<HeatTrackerInner>>,
}

impl EcHeatTracker {
    /// Build a tracker for the given settings.
    ///
    /// A disabled configuration, or one with no queue at all, yields a tracker
    /// that allocates nothing and does nothing.
    pub fn new(config: EcHeatConfig) -> Self {
        if !config.enabled || config.queue_capacity == 0 {
            return Self::disabled();
        }
        let (signals, receiver) = mpsc::channel(config.queue_capacity);
        Self {
            inner: Some(Arc::new(HeatTrackerInner {
                shared: Arc::new(HeatShared {
                    config,
                    counters: EcHeatCounters::default(),
                }),
                signals,
                receiver: Mutex::new(Some(receiver)),
                flusher_started: AtomicBool::new(false),
            })),
        }
    }

    /// Build a tracker that ignores every signal.
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    /// Whether this tracker records anything.
    ///
    /// Callers use it to skip building an object key at all when tracking is
    /// off, so a node that does not tier pays nothing on its read path.
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Record a successful read of an object.
    pub fn record_read(&self, object: EcObjectVersionKey) {
        self.record(object, EcAccessKind::Read);
    }

    /// Record a successful write of an object.
    ///
    /// A fresh write counts as use, so that an object is not migrated moments
    /// after a client stored it.
    pub fn record_write(&self, object: EcObjectVersionKey) {
        self.record(object, EcAccessKind::Write);
    }

    fn record(&self, object: EcObjectVersionKey, kind: EcAccessKind) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };
        // Only the current version can migrate, so only the current version is
        // worth a mark. A row for an older version would never be read by the
        // scanner — it looks marks up under `version_id: None` — and nothing
        // would ever collect it, leaving one permanent row per version ever
        // written. See EC2-025 for why versions are out of scope.
        if object.version_id.is_some() {
            return;
        }
        let signal = HeatSignal {
            object,
            access_nanos: unix_time_nanos(),
            kind,
        };
        // try_send, never send: the caller is on the request path, and a slow
        // or stopped flusher must not turn into slow S3 responses. Losing a
        // signal only makes an object look colder than it is.
        match inner.signals.try_send(signal) {
            Ok(()) => {
                inner.shared.counters.signals.fetch_add(1, Ordering::Relaxed);
                ec_metrics::record_heat_signal();
            }
            Err(_) => {
                inner.shared.counters.dropped.fetch_add(1, Ordering::Relaxed);
                ec_metrics::record_heat_signal_dropped();
            }
        }
    }

    /// Start the background flusher.
    ///
    /// Returns `None` when tracking is disabled or the flusher already runs.
    /// The handle follows the contract of the other EE workers, so the caller
    /// can supervise it alongside them.
    pub fn start_flusher(&self, metadata: Arc<EcMetadataStore>) -> Option<JoinHandle<()>> {
        let inner = self.inner.as_ref()?;
        if inner
            .flusher_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        let receiver = inner.receiver.lock().ok()?.take()?;
        let shared = Arc::clone(&inner.shared);
        Some(tokio::spawn(async move {
            run_flusher(receiver, metadata, shared).await;
        }))
    }

    /// Current counter values.
    pub fn counters(&self) -> EcHeatCountersSnapshot {
        self.inner
            .as_ref()
            .map(|inner| inner.shared.counters.snapshot())
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for EcHeatTracker {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EcHeatTracker")
            .field("enabled", &self.is_enabled())
            .finish()
    }
}

/// Last time an object was used, in nanoseconds since the Unix epoch.
///
/// An object with no mark is treated as used when it was created, never as used
/// at the epoch. The difference decides what happens the first time the feature
/// is switched on: without this fallback every object in the cluster would look
/// infinitely cold and the whole store would queue for migration at once.
pub async fn last_access_nanos(
    metadata: &EcMetadataStore,
    object: &EcObjectVersionKey,
    created_at_nanos: u64,
) -> Result<u64, EcMetadataError> {
    Ok(metadata
        .get_object_heat(object)
        .await?
        .map(|record| record.last_access_nanos.max(created_at_nanos))
        .unwrap_or(created_at_nanos))
}

/// Record that an object changed layout.
///
/// The counter and the timestamp behind the pause between two migrations of one
/// object. Both directions write it: without the forward one the pause never
/// engages, and an object the heuristics keep changing their mind about would
/// travel back and forth at the full cost of a decode and three writes each
/// time. Decision [EC2-023].
///
/// Written on the node that carried the migration out. The mark is node-local
/// like every other activity record, and the migration decision already takes
/// the newest one across the pool.
pub async fn record_migration(
    metadata: &EcMetadataStore,
    object: &EcObjectVersionKey,
    now_nanos: u64,
    window_nanos: u64,
) -> Result<(), EcMetadataError> {
    let record = match metadata.get_object_heat(object).await? {
        Some(mut record) => {
            record.migrations = record.migrations.saturating_add(1);
            record.last_migration_nanos = Some(now_nanos);
            record.updated_at = now_nanos;
            record
        }
        // No mark yet: the object was migrated without ever being read on this
        // node, which is the ordinary case for a cold object. The row exists
        // from here on so the pause has something to measure from.
        None => EcObjectHeatRecord {
            object: object.clone(),
            last_access_nanos: now_nanos,
            window: EcHeatWindow::new(window_nanos, now_nanos),
            migrations: 1,
            last_migration_nanos: Some(now_nanos),
            updated_at: now_nanos,
        },
    };
    metadata.put_object_heat(&record).await
}

async fn run_flusher(
    mut receiver: mpsc::Receiver<HeatSignal>,
    metadata: Arc<EcMetadataStore>,
    shared: Arc<HeatShared>,
) {
    let mut batch = HeatBatch::new(shared.config.queue_capacity);
    let mut ticker = tokio::time::interval(shared.config.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The first tick of an interval completes immediately; flushing an empty
    // batch at startup would only add noise to the counters.
    ticker.tick().await;

    loop {
        tokio::select! {
            signal = receiver.recv() => match signal {
                Some(signal) => {
                    if !batch.record(signal) {
                        shared.counters.dropped.fetch_add(1, Ordering::Relaxed);
                        ec_metrics::record_heat_signal_dropped();
                    }
                }
                // Every sender is gone, which means the service is shutting
                // down. Flush what is held so a clean stop does not lose marks.
                None => {
                    flush(&mut batch, &metadata, &shared).await;
                    return;
                }
            },
            _ = ticker.tick() => flush(&mut batch, &metadata, &shared).await,
        }
    }
}

async fn flush(batch: &mut HeatBatch, metadata: &EcMetadataStore, shared: &HeatShared) {
    shared.counters.pending.store(batch.len() as u64, Ordering::Relaxed);
    ec_metrics::set_heat_pending_depth(batch.len());
    if batch.is_empty() {
        return;
    }

    shared.counters.flushes.fetch_add(1, Ordering::Relaxed);
    ec_metrics::record_heat_flush();

    for (object, pending) in batch.take(writes_per_flush(&shared.config)) {
        match write_mark(metadata, &object, pending, &shared.config).await {
            Ok(MarkOutcome::Written) => {
                shared.counters.records_written.fetch_add(1, Ordering::Relaxed);
                ec_metrics::record_heat_record_written();
            }
            // The stored mark is still fresh enough to leave alone, but the
            // reads counted here are not: dropping them would make the counter
            // report a fraction of real traffic, and reverse tiering reads that
            // counter. They wait for the write that does happen.
            Ok(MarkOutcome::Suppressed) => batch.carry(object, pending),
            // A failed mark is not a failed request: the object stays readable
            // and simply looks colder than it is until the next signal lands.
            Err(error) => tracing::warn!(
                bucket = %object.bucket,
                key = %object.key,
                error = %error,
                "failed to write object activity mark"
            ),
        }
    }
}

/// How many durable writes one flush may perform.
///
/// A flush does not poll the channel, and every write costs an fsync. Writing a
/// whole batch would stall the task long enough for the queue behind it to fill.
fn writes_per_flush(config: &EcHeatConfig) -> usize {
    (config.queue_capacity / 8).max(1)
}

/// What happened to one collapsed entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkOutcome {
    /// The mark reached durable metadata.
    Written,
    /// The stored mark was fresh enough; this entry has to wait.
    Suppressed,
}

/// Write one collapsed entry, honouring the resolution window.
async fn write_mark(
    metadata: &EcMetadataStore,
    object: &EcObjectVersionKey,
    pending: PendingHeat,
    config: &EcHeatConfig,
) -> Result<MarkOutcome, EcMetadataError> {
    let resolution_nanos = duration_to_nanos(config.resolution);
    let window_nanos = duration_to_nanos(config.window);

    let record = match metadata.get_object_heat(object).await? {
        Some(mut record) => {
            if pending.last_access_nanos.saturating_sub(record.last_access_nanos) < resolution_nanos
            {
                return Ok(MarkOutcome::Suppressed);
            }
            // A window measured over a width the operator has since changed
            // says nothing about the threshold now in force, so the ring is
            // rebuilt before anything is added to it.
            if record.window.width_nanos() != window_nanos {
                record.window.rescale(window_nanos, pending.last_access_nanos);
            }
            record.window.record(pending.last_access_nanos, pending.reads);
            record.last_access_nanos = pending.last_access_nanos;
            record.updated_at = pending.last_access_nanos;
            record
        }
        None => {
            let mut record = EcObjectHeatRecord {
                object: object.clone(),
                last_access_nanos: pending.last_access_nanos,
                window: EcHeatWindow::new(window_nanos, pending.last_access_nanos),
                migrations: 0,
                last_migration_nanos: None,
                updated_at: pending.last_access_nanos,
            };
            record.window.record(pending.last_access_nanos, pending.reads);
            record
        }
    };

    metadata.put_object_heat(&record).await?;
    Ok(MarkOutcome::Written)
}

fn duration_to_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(key: &str) -> EcObjectVersionKey {
        EcObjectVersionKey::new("bucket", key, None)
    }

    fn config() -> EcHeatConfig {
        EcHeatConfig {
            enabled: true,
            resolution: Duration::from_secs(3_600),
            flush_interval: Duration::from_millis(50),
            queue_capacity: 1_024,
            window: Duration::from_secs(86_400),
        }
    }

    fn signal(key: &str, access_nanos: u64, kind: EcAccessKind) -> HeatSignal {
        HeatSignal {
            object: object(key),
            access_nanos,
            kind,
        }
    }

    async fn store() -> (tempfile::TempDir, Arc<EcMetadataStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = EcMetadataStore::open(&dir.path().join("ec-meta")).unwrap();
        (dir, Arc::new(store))
    }

    /// Ten thousand accesses to one object are one entry with the newest mark.
    ///
    /// This is the property the whole design rests on: without it every read
    /// would eventually reach the metadata store, and the tracker would cost an
    /// fsync per GET.
    #[test]
    fn collapsing_turns_many_signals_into_one_entry() {
        let mut batch = HeatBatch::new(1024);
        for index in 1..=10_000u64 {
            batch.record(signal("hot.bin", index, EcAccessKind::Read));
        }
        // Out-of-order arrival must not move the mark backwards.
        batch.record(signal("hot.bin", 42, EcAccessKind::Read));

        assert_eq!(batch.len(), 1);
        let entries = batch.take(usize::MAX);
        let (_, pending) = &entries[0];
        assert_eq!(pending.last_access_nanos, 10_000);
        assert_eq!(pending.reads, 10_001);
    }

    #[test]
    fn collapsing_keeps_objects_apart_and_counts_reads_only() {
        let mut batch = HeatBatch::new(1024);
        batch.record(signal("a.bin", 10, EcAccessKind::Read));
        batch.record(signal("b.bin", 20, EcAccessKind::Write));
        batch.record(signal("b.bin", 30, EcAccessKind::Read));

        assert_eq!(batch.len(), 2);
        let entries: HashMap<_, _> = batch.take(usize::MAX).into_iter().collect();
        assert_eq!(entries[&object("a.bin")].reads, 1);
        // A write marks the object as used but is not a read: reverse tiering
        // reacts to read frequency, and counting writes would promote objects
        // that are only ever written.
        assert_eq!(entries[&object("b.bin")].reads, 1);
        assert_eq!(entries[&object("b.bin")].last_access_nanos, 30);
    }

    /// The batch is capped, and the cap costs accuracy rather than memory.
    #[test]
    fn a_full_batch_drops_new_objects_and_keeps_updating_known_ones() {
        let mut batch = HeatBatch::new(2);
        assert!(batch.record(signal("a.bin", 10, EcAccessKind::Read)));
        assert!(batch.record(signal("b.bin", 10, EcAccessKind::Read)));
        // A third distinct object does not fit.
        assert!(!batch.record(signal("c.bin", 10, EcAccessKind::Read)));
        // An object already in the batch still collects its reads: refusing
        // those would lose the marks of exactly the objects being read most.
        assert!(batch.record(signal("a.bin", 20, EcAccessKind::Read)));
        assert_eq!(batch.len(), 2);
    }

    /// Reads survive a flush whose durable write the resolution window blocked.
    ///
    /// Without this the counter reports only the reads that happened to land in
    /// a flush that wrote — a small fraction of the traffic — and any promotion
    /// threshold derived from real numbers would never be reached.
    #[test]
    fn carried_reads_accumulate_until_a_write_happens() {
        let mut batch = HeatBatch::new(16);
        batch.record(signal("hot.bin", 100, EcAccessKind::Read));
        let taken = batch.take(usize::MAX);
        assert_eq!(batch.len(), 0);

        let (object, pending) = taken.into_iter().next().unwrap();
        batch.carry(object, pending);
        batch.record(signal("hot.bin", 200, EcAccessKind::Read));

        let entries = batch.take(usize::MAX);
        let (_, pending) = &entries[0];
        assert_eq!(pending.reads, 2, "the carried read is still counted");
        assert_eq!(pending.last_access_nanos, 200);
    }

    /// One flush writes a bounded slice, so the task returns to the channel.
    #[test]
    fn a_flush_writes_a_bounded_number_of_marks() {
        let mut batch = HeatBatch::new(64);
        for index in 0..40u64 {
            batch.record(signal(
                &format!("object-{index}.bin"),
                index + 1,
                EcAccessKind::Read,
            ));
        }
        assert_eq!(batch.take(8).len(), 8);
        assert_eq!(batch.len(), 32, "the rest waits for the next tick");

        assert_eq!(writes_per_flush(&config()), config().queue_capacity / 8);
        assert_eq!(
            writes_per_flush(&EcHeatConfig {
                queue_capacity: 1,
                ..config()
            }),
            1,
            "a flush always makes progress"
        );
    }

    /// A full queue drops signals; it never makes the caller wait.
    #[test]
    fn a_full_queue_drops_signals_instead_of_blocking() {
        let tracker = EcHeatTracker::new(EcHeatConfig {
            queue_capacity: 1,
            ..config()
        });
        // Nothing consumes the channel here, which is the worst case: a stopped
        // or overloaded flusher.
        for _ in 0..64 {
            tracker.record_read(object("hot.bin"));
        }

        let counters = tracker.counters();
        assert_eq!(counters.signals, 1, "only the buffered signal is accepted");
        assert_eq!(counters.dropped, 63);
    }

    /// A mark is only ever kept for the current version.
    ///
    /// The scanner looks marks up under `version_id: None`, so a row for an
    /// older version could never be read — and nothing would collect it.
    #[test]
    fn an_older_version_is_never_marked() {
        let tracker = EcHeatTracker::new(config());
        tracker.record_read(EcObjectVersionKey::new(
            "bucket",
            "hot.bin",
            Some("v-1".to_string()),
        ));
        tracker.record_write(EcObjectVersionKey::new(
            "bucket",
            "hot.bin",
            Some("v-1".to_string()),
        ));
        assert_eq!(tracker.counters().signals, 0);
        assert_eq!(tracker.counters().dropped, 0);

        tracker.record_read(object("hot.bin"));
        assert_eq!(tracker.counters().signals, 1);
    }

    #[test]
    fn a_disabled_tracker_allocates_no_queue_and_records_nothing() {
        for tracker in [
            EcHeatTracker::disabled(),
            EcHeatTracker::new(EcHeatConfig {
                enabled: false,
                ..config()
            }),
            EcHeatTracker::new(EcHeatConfig {
                queue_capacity: 0,
                ..config()
            }),
        ] {
            assert!(!tracker.is_enabled());
            tracker.record_read(object("hot.bin"));
            tracker.record_write(object("hot.bin"));
            assert_eq!(tracker.counters().signals, 0);
            assert_eq!(tracker.counters().dropped, 0);
        }
    }

    /// The resolution window is what keeps a constantly read object cheap.
    ///
    /// This exercises `write_mark` on its own, so the reads of the suppressed
    /// call are simply not written here. In the flusher they are carried to the
    /// next write instead — see `carried_reads_accumulate_until_a_write_happens`.
    #[tokio::test]
    async fn the_resolution_window_suppresses_a_rewrite() {
        let (_dir, metadata) = store().await;
        let config = config();
        let hour = 3_600_000_000_000u64;

        let first = write_mark(
            &metadata,
            &object("hot.bin"),
            PendingHeat {
                last_access_nanos: hour,
                reads: 3,
            },
            &config,
        )
        .await
        .unwrap();
        assert_eq!(
            first,
            MarkOutcome::Written,
            "the first mark is always written"
        );

        // One minute later: the stored mark is still accurate enough, so this
        // must not reach the disk.
        let suppressed = write_mark(
            &metadata,
            &object("hot.bin"),
            PendingHeat {
                last_access_nanos: hour + 60_000_000_000,
                reads: 5,
            },
            &config,
        )
        .await
        .unwrap();
        assert_eq!(suppressed, MarkOutcome::Suppressed);
        let stored = metadata.get_object_heat(&object("hot.bin")).await.unwrap().unwrap();
        assert_eq!(stored.last_access_nanos, hour);
        assert_eq!(stored.window.reads_at(hour), 3);

        // Two hours later the stored mark is stale and is refreshed.
        let refreshed = write_mark(
            &metadata,
            &object("hot.bin"),
            PendingHeat {
                last_access_nanos: hour * 3,
                reads: 5,
            },
            &config,
        )
        .await
        .unwrap();
        assert_eq!(refreshed, MarkOutcome::Written);
        let stored = metadata.get_object_heat(&object("hot.bin")).await.unwrap().unwrap();
        assert_eq!(stored.last_access_nanos, hour * 3);
        assert_eq!(stored.window.reads_at(hour * 3), 8);
    }

    /// The count describes a bounded stretch of time, and decays out of it.
    #[tokio::test]
    async fn the_read_window_decays_instead_of_growing_forever() {
        let (_dir, metadata) = store().await;
        let config = EcHeatConfig {
            resolution: Duration::from_secs(1),
            window: Duration::from_secs(80),
            ..config()
        };
        let second = 1_000_000_000u64;

        for step in 1..=3u64 {
            write_mark(
                &metadata,
                &object("hot.bin"),
                PendingHeat {
                    last_access_nanos: second * step * 2,
                    reads: 2,
                },
                &config,
            )
            .await
            .unwrap();
        }
        let stored = metadata.get_object_heat(&object("hot.bin")).await.unwrap().unwrap();
        assert_eq!(
            stored.window.reads_at(second * 6),
            6,
            "still inside the window"
        );

        // A full window of silence leaves nothing behind, so the count always
        // describes recent traffic rather than the object's whole life.
        assert_eq!(stored.window.reads_at(second * 200), 0);
    }

    /// A change of the configured window rebuilds the ring without losing reads.
    #[tokio::test]
    async fn widening_the_window_keeps_the_reads_already_counted() {
        let (_dir, metadata) = store().await;
        let second = 1_000_000_000u64;
        let narrow = EcHeatConfig {
            resolution: Duration::from_secs(1),
            window: Duration::from_secs(80),
            ..config()
        };
        let wide = EcHeatConfig {
            window: Duration::from_secs(800),
            ..narrow.clone()
        };

        write_mark(
            &metadata,
            &object("hot.bin"),
            PendingHeat {
                last_access_nanos: second * 2,
                reads: 3,
            },
            &narrow,
        )
        .await
        .unwrap();

        write_mark(
            &metadata,
            &object("hot.bin"),
            PendingHeat {
                last_access_nanos: second * 4,
                reads: 2,
            },
            &wide,
        )
        .await
        .unwrap();

        let stored = metadata.get_object_heat(&object("hot.bin")).await.unwrap().unwrap();
        assert_eq!(stored.window.width_nanos(), second * 800);
        assert_eq!(stored.window.reads_at(second * 4), 5);
    }

    /// An object nobody has read counts as used when it was created.
    #[tokio::test]
    async fn a_missing_mark_falls_back_to_the_creation_time() {
        let (_dir, metadata) = store().await;
        let created_at = 5_000u64;

        assert_eq!(
            last_access_nanos(&metadata, &object("cold.bin"), created_at).await.unwrap(),
            created_at
        );

        write_mark(
            &metadata,
            &object("cold.bin"),
            PendingHeat {
                last_access_nanos: 9_000,
                reads: 1,
            },
            &config(),
        )
        .await
        .unwrap();
        assert_eq!(
            last_access_nanos(&metadata, &object("cold.bin"), created_at).await.unwrap(),
            9_000
        );

        // A mark older than the object itself cannot make it look colder than
        // new: an overwrite resets creation time while the old mark survives.
        assert_eq!(
            last_access_nanos(&metadata, &object("cold.bin"), 20_000).await.unwrap(),
            20_000
        );
    }

    /// The signal reaches durable metadata within one flush interval.
    #[tokio::test]
    async fn a_signal_reaches_the_store_within_one_flush_interval() {
        let (_dir, metadata) = store().await;
        let tracker = EcHeatTracker::new(config());
        let handle = tracker.start_flusher(Arc::clone(&metadata)).unwrap();

        tracker.record_read(object("hot.bin"));
        tracker.record_read(object("hot.bin"));
        tokio::time::sleep(Duration::from_millis(400)).await;

        let stored = metadata.get_object_heat(&object("hot.bin")).await.unwrap();
        let stored = stored.expect("the mark must be durable after one flush interval");
        assert!(stored.last_access_nanos > 0);
        assert_eq!(
            stored.window.reads_at(stored.last_access_nanos),
            2,
            "both reads collapsed into one write"
        );

        let counters = tracker.counters();
        assert_eq!(counters.signals, 2);
        assert_eq!(counters.dropped, 0);
        assert_eq!(counters.records_written, 1);

        handle.abort();
    }

    /// A second flusher cannot start, so marks are never written twice.
    #[tokio::test]
    async fn only_one_flusher_starts() {
        let (_dir, metadata) = store().await;
        let tracker = EcHeatTracker::new(config());
        let first = tracker.start_flusher(Arc::clone(&metadata));
        let second = tracker.start_flusher(Arc::clone(&metadata));

        assert!(first.is_some());
        assert!(second.is_none());
        first.unwrap().abort();
    }

    /// Shutting down flushes what the batch still holds.
    #[tokio::test]
    async fn dropping_the_tracker_flushes_the_last_batch() {
        let (_dir, metadata) = store().await;
        let tracker = EcHeatTracker::new(EcHeatConfig {
            // Long enough that only the shutdown path can produce the write.
            flush_interval: Duration::from_secs(3_600),
            ..config()
        });
        let handle = tracker.start_flusher(Arc::clone(&metadata)).unwrap();

        tracker.record_read(object("hot.bin"));
        drop(tracker);
        handle.await.unwrap();

        assert!(metadata.get_object_heat(&object("hot.bin")).await.unwrap().is_some());
    }
}
