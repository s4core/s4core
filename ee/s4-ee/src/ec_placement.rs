// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Placement of new objects across the erasure sets of one pool.
//!
//! Erasure Coding v1 had nothing to decide: a pool was one set, and every
//! object went to it. Once a pool may hold several sets ([EC2-040]), each new
//! object has to pick one, and the pool has to answer honestly when none of
//! them can take it.
//!
//! This module is that single decision point. Strategies are implementations
//! behind it rather than conditionals spread over the write path, so the rest
//! of the runtime asks one question — "which set does this object go to?" —
//! and never learns how the answer was reached.
//!
//! Three rules shape everything here:
//!
//! - **Unknown capacity never blocks a write.** A node reports zeros when it
//!   could not measure its filesystem — a platform without the call, a data
//!   directory that has gone missing — and zeros mean "unknown", never "full".
//!   The free-space strategy then degrades to the even walk instead of refusing
//!   work.
//! - **Fullness is a mark, not a state.** A full set stays `Active` and is
//!   merely passed over; when garbage collection frees space it takes writes
//!   again, with no operator action ([EC2-043]).
//! - **With the feature off, the pool behaves exactly as EC v1.** Every object
//!   goes to the pool's default set, no set is ever excluded, and no capacity
//!   error can be raised. That is what makes the rollback in section 6 of the
//!   conventions real rather than nominal.
//!
//! [EC2-040]: ../../../plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/EC2-040-multi-set-layout.md
//! [EC2-043]: ../../../plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/EC2-043-set-lifecycle.md

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use s4_cluster::{ClusterError, ClusterMembershipQuery, NodeId};
use thiserror::Error;

use crate::erasure_coding::{
    EcConfigError, EcMultiSetConfig, EcPlacementStrategy, EcTopologyHealth, ErasureSet,
    ErasureSetRegistry, ErasureSetState,
};

/// Environment variable holding the used-space ratio at which a set stops
/// taking new objects.
pub const SET_FULL_THRESHOLD_ENV: &str = "S4_EC_SET_FULL_THRESHOLD";

/// Environment variable holding how old a node capacity report may be before
/// the router stops believing it.
pub const CAPACITY_MAX_AGE_ENV: &str = "S4_EC_CAPACITY_MAX_AGE_SECS";

/// Used-space ratio at which a set is considered full.
///
/// Below one on purpose: a set is passed over before its nodes physically run
/// out, so the last objects admitted still fit whole.
pub const DEFAULT_SET_FULL_THRESHOLD: f64 = 0.9;

/// Default freshness window for a node capacity report, in seconds.
///
/// Comfortably wider than the gossip metadata broadcast interval, so an
/// ordinary broadcast delay does not read as missing data.
pub const DEFAULT_CAPACITY_MAX_AGE_SECS: u64 = 300;

// ---------------------------------------------------------------------------
// Capacity
// ---------------------------------------------------------------------------

/// What the cluster last reported about one node's storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcNodeCapacity {
    /// Total bytes of the node's data filesystem.
    pub total_bytes: u64,
    /// Bytes still free on it.
    pub available_bytes: u64,
    /// How long ago the node reported these numbers.
    pub reported_ago: Duration,
}

impl EcNodeCapacity {
    /// Whether the report carries a real measurement.
    ///
    /// A node that has never measured its filesystem broadcasts zeros, and a
    /// zero total is indistinguishable from "no disk", so it counts as unknown
    /// rather than as a full node.
    pub fn is_measured(&self) -> bool {
        self.total_bytes > 0
    }

    /// Whether the report is recent enough to weigh a placement decision on.
    pub fn is_fresh(&self, max_age: Duration) -> bool {
        self.reported_ago <= max_age
    }

    /// Whether the report may be used at all.
    pub fn is_usable(&self, max_age: Duration) -> bool {
        self.is_measured() && self.is_fresh(max_age)
    }

    /// Share of the node's filesystem already in use, `0.0..=1.0`.
    fn used_ratio(&self) -> f64 {
        if self.total_bytes == 0 {
            return 1.0;
        }
        let used = self.total_bytes.saturating_sub(self.available_bytes);
        (used as f64 / self.total_bytes as f64).clamp(0.0, 1.0)
    }
}

/// Where the router reads node capacity from.
///
/// A port rather than a direct gossip call: the strategies are developed and
/// tested against stub capacity data, and the runtime supplies the cluster view
/// only when one has been wired.
pub trait EcCapacitySource: Send + Sync {
    /// Capacity of one node, or `None` when the cluster has nothing to say
    /// about it.
    fn node_capacity(&self, node_id: NodeId) -> Option<EcNodeCapacity>;
}

/// Capacity source for a node with no cluster view.
///
/// Answers "unknown" for every node, which is the state of a single-node
/// deployment and of a cluster node before its membership handle is wired.
/// Placement then falls back to the even walk, so writes keep flowing.
#[derive(Debug, Clone, Copy, Default)]
pub struct EcCapacityUnknown;

impl EcCapacitySource for EcCapacityUnknown {
    fn node_capacity(&self, _node_id: NodeId) -> Option<EcNodeCapacity> {
        None
    }
}

/// Node capacity as the cluster's gossip reports it.
///
/// The router never measures a filesystem itself, on this node or any other: it
/// reads what the nodes have already broadcast, and what this node measured on
/// its own last metadata tick. A node that could not measure reports zeros,
/// which reads as unmeasured here, and the free-space strategy then behaves as
/// the even walk.
pub struct EcClusterCapacity {
    membership: Arc<dyn ClusterMembershipQuery>,
}

impl EcClusterCapacity {
    /// Read capacity through a cluster membership view.
    pub fn new(membership: Arc<dyn ClusterMembershipQuery>) -> Self {
        Self { membership }
    }
}

impl EcCapacitySource for EcClusterCapacity {
    fn node_capacity(&self, node_id: NodeId) -> Option<EcNodeCapacity> {
        let report = self.membership.node_capacity(node_id)?;
        Some(EcNodeCapacity {
            total_bytes: report.capacity.total_bytes,
            available_bytes: report.capacity.available_bytes(),
            reported_ago: report.age,
        })
    }
}

/// Free space of one erasure set, as seen through its tightest node.
///
/// Both figures take the worst node rather than a sum or an average: shards of
/// an object land on every slot of the set, so the node with the least room
/// decides how much the set as a whole can still accept. A sum would promise
/// capacity no shard set could ever use ([EC2-042]).
///
/// [EC2-042]: ../../../plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/EC2-042-placement-strategy.md
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EcSetCapacity {
    /// Free bytes on the tightest node of the set.
    pub available_bytes: u64,
    /// Largest used-space ratio among the set's nodes, `0.0..=1.0`.
    pub used_ratio: f64,
}

impl EcSetCapacity {
    /// Whether the set has crossed the fullness threshold.
    fn is_full(&self, threshold: f64) -> bool {
        self.used_ratio >= threshold
    }
}

/// Capacity of one set, or `None` when any of its nodes is silent, unmeasured
/// or stale.
///
/// All-or-nothing on purpose: a set whose free space is derived from a subset
/// of its nodes would be weighed against sets that reported in full, and the
/// gaps would read as room the set does not have.
fn set_capacity(
    set: &ErasureSet,
    capacity: &dyn EcCapacitySource,
    max_age: Duration,
) -> Option<EcSetCapacity> {
    let mut available_bytes = u64::MAX;
    let mut used_ratio: f64 = 0.0;

    for node_id in set.slot_assignment.distinct_nodes() {
        let node = capacity.node_capacity(node_id)?;
        if !node.is_usable(max_age) {
            return None;
        }
        available_bytes = available_bytes.min(node.available_bytes);
        used_ratio = used_ratio.max(node.used_ratio());
    }

    // An empty set cannot happen — a set is built at the profile's full width —
    // but deriving "infinite room" from an empty loop is not a claim worth
    // making, so say unknown instead.
    if available_bytes == u64::MAX {
        return None;
    }

    Some(EcSetCapacity {
        available_bytes,
        used_ratio,
    })
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// How this pool places new objects.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EcPlacementConfig {
    /// Whether the pool may spread new objects over more than one set.
    ///
    /// With this off the router returns the pool's default set unconditionally,
    /// which is EC v1 behaviour down to the excluded-set rules.
    pub multi_set_enabled: bool,
    /// Strategy used to pick between eligible sets.
    pub strategy: EcPlacementStrategy,
    /// Used-space ratio at which a set stops taking new objects.
    pub full_threshold: f64,
    /// How old a node capacity report may be before it counts as unknown.
    pub capacity_max_age: Duration,
}

impl Default for EcPlacementConfig {
    fn default() -> Self {
        Self {
            multi_set_enabled: false,
            strategy: EcPlacementStrategy::RoundRobin,
            full_threshold: DEFAULT_SET_FULL_THRESHOLD,
            capacity_max_age: Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS),
        }
    }
}

impl EcPlacementConfig {
    /// Read the placement configuration from the process environment.
    pub fn from_env() -> Result<Self, EcConfigError> {
        let multi_set = EcMultiSetConfig::from_env()?;
        Ok(Self {
            multi_set_enabled: multi_set.allows_multiple_sets(),
            strategy: multi_set.placement,
            full_threshold: parse_full_threshold()?,
            capacity_max_age: parse_capacity_max_age()?,
        })
    }
}

fn parse_full_threshold() -> Result<f64, EcConfigError> {
    let Some(raw) = read_env(SET_FULL_THRESHOLD_ENV) else {
        return Ok(DEFAULT_SET_FULL_THRESHOLD);
    };
    let value: f64 = raw.trim().parse().map_err(|_| EcConfigError::Env {
        name: SET_FULL_THRESHOLD_ENV,
        reason: format!("'{raw}' is not a number"),
    })?;
    if !value.is_finite() || value <= 0.0 || value > 1.0 {
        return Err(EcConfigError::Env {
            name: SET_FULL_THRESHOLD_ENV,
            reason: format!("{value} is outside the accepted range (0.0, 1.0]"),
        });
    }
    Ok(value)
}

fn parse_capacity_max_age() -> Result<Duration, EcConfigError> {
    let Some(raw) = read_env(CAPACITY_MAX_AGE_ENV) else {
        return Ok(Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS));
    };
    let secs: u64 = raw.trim().parse().map_err(|_| EcConfigError::Env {
        name: CAPACITY_MAX_AGE_ENV,
        reason: format!("'{raw}' is not a whole number of seconds"),
    })?;
    if secs == 0 {
        return Err(EcConfigError::Env {
            name: CAPACITY_MAX_AGE_ENV,
            reason: "0 would discard every capacity report; use at least 1 second".to_string(),
        });
    }
    Ok(Duration::from_secs(secs))
}

fn read_env(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => Some(raw),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why placement could not name a set.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EcPlacementError {
    /// The pool holds no erasure set at all.
    ///
    /// A configuration fault rather than a capacity one: startup refuses a pool
    /// without a writable set, so reaching this means the registry was built
    /// some other way.
    #[error("EC pool {pool} holds no erasure set")]
    PoolHasNoSets {
        /// Pool the router was asked about.
        pool: String,
    },

    /// Every set of the pool is either closed to writes or full.
    ///
    /// Its own variant rather than a general placement failure: the cluster is
    /// healthy and routing works, so the operator adds a set or frees space
    /// instead of repairing anything ([EC2-043]).
    ///
    /// [EC2-043]: ../../../plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/EC2-043-set-lifecycle.md
    #[error(
        "EC pool {pool} has no erasure set that can take a new object: \
         {total} set(s), {not_writable} not accepting writes, {full} full"
    )]
    CapacityExhausted {
        /// Pool the router was asked about.
        pool: String,
        /// Sets the pool holds.
        total: usize,
        /// Sets whose lifecycle state refuses new objects.
        not_writable: usize,
        /// Active sets passed over because they are full.
        full: usize,
    },

    /// The set that would take the object is held until this node reads its
    /// layout record ([`ErasureSet::layout_pending`]).
    ///
    /// Not the pool running out of room: the set is active and has space, but
    /// this node does not know yet which layout it places shards by. The
    /// object belongs in `set_id` all the same. A write admits its transcode
    /// job there, and the transcode worker encodes it once the record is read
    /// (EC2-088, point 3); tiering tries again on its next cycle.
    #[error(
        "EC erasure set {set_id} of pool {pool} takes new objects on this node once its \
         layout record is read"
    )]
    LayoutPending {
        /// Pool the router was asked about.
        pool: String,
        /// The held set.
        set_id: u64,
    },
}

impl EcPlacementError {
    /// Whether this is the "pool is out of space" answer.
    pub fn is_capacity_exhausted(&self) -> bool {
        matches!(self, Self::CapacityExhausted { .. })
    }

    /// The set the object belongs in once this node reads its layout record,
    /// when that is the only reason no set was chosen.
    pub fn held_set(&self) -> Option<u64> {
        match self {
            Self::LayoutPending { set_id, .. } => Some(*set_id),
            Self::PoolHasNoSets { .. } | Self::CapacityExhausted { .. } => None,
        }
    }
}

impl From<EcPlacementError> for ClusterError {
    /// Carry the distinction all the way to the client.
    ///
    /// A pool out of space becomes `InsufficientCapacity`, which the S3 layer
    /// answers with `507`. Anything a retry could not fix must not arrive as a
    /// generic internal error: the operator has to be able to tell "add
    /// capacity" from "the server broke".
    fn from(error: EcPlacementError) -> Self {
        match error {
            EcPlacementError::PoolHasNoSets { .. } => ClusterError::Config(error.to_string()),
            EcPlacementError::CapacityExhausted { .. } => {
                ClusterError::InsufficientCapacity(error.to_string())
            }
            // Transient, and nothing an operator has to add or free.
            EcPlacementError::LayoutPending { .. } => ClusterError::Placement(error.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// One set the router may still choose, with whatever is known about its room.
struct Candidate<'a> {
    set: &'a ErasureSet,
    capacity: Option<EcSetCapacity>,
}

impl Candidate<'_> {
    /// Whether the set's placement is sound enough to prefer it.
    ///
    /// A degraded set is not excluded outright — see [`EcPlacementRouter::select`].
    fn is_healthy(&self) -> bool {
        self.set.topology_health != EcTopologyHealth::TopologyDegraded
    }
}

/// The router's own memory between decisions.
#[derive(Debug, Default)]
struct RouterState {
    /// Position of the even walk over the eligible sets.
    cursor: usize,
    /// Smooth weighted round-robin credit, in bytes, per set id.
    credits: HashMap<u64, i128>,
}

/// Single point where a new object is assigned an erasure set.
///
/// Strategies live inside it, so callers depend on the question rather than on
/// the answer's shape. Selection happens once per object, before encoding
/// starts, and costs a mutex acquisition and a walk over a handful of sets —
/// nothing next to the encoding it precedes.
///
/// The rotation is this node's own; nodes do not share one. Coordinating it
/// across the pool would need consensus on every write to buy nothing: each
/// node walks the same sets in the same order, so independent rotations still
/// add up to an even spread, and a node that restarts simply starts its own
/// walk again.
#[derive(Debug)]
pub struct EcPlacementRouter {
    config: EcPlacementConfig,
    state: Mutex<RouterState>,
}

impl EcPlacementRouter {
    /// Build a router with the given configuration.
    pub fn new(config: EcPlacementConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RouterState::default()),
        }
    }

    /// Configuration this router applies.
    pub fn config(&self) -> &EcPlacementConfig {
        &self.config
    }

    /// Pick the erasure set a new object goes to.
    ///
    /// The order of decisions matters and is fixed:
    ///
    /// 1. With multi-set placement off, the pool's default set answers every
    ///    time — the EC v1 path, with no exclusion rules applied to it but
    ///    one: a set this node holds until it reads the set's layout record
    ///    ([`ErasureSet::layout_pending`]) is not chosen yet, and the answer is
    ///    [`EcPlacementError::LayoutPending`] naming it.
    /// 2. Sets that refuse new objects by lifecycle state, held sets, and sets
    ///    already full drop out.
    /// 3. Healthy sets are preferred, but a degraded set is used when it is all
    ///    the pool has left. Excluding it outright would stop writes into a
    ///    single-set pool running under a warning topology policy, which EC v1
    ///    accepted.
    /// 4. The configured strategy chooses between what remains.
    ///
    /// When step 2 leaves nothing, a held set with room is named by
    /// [`EcPlacementError::LayoutPending`]: the object belongs there once the
    /// record is read. Otherwise the answer is
    /// [`EcPlacementError::CapacityExhausted`] — never a set that cannot hold
    /// the object.
    pub fn select<'a>(
        &self,
        sets: &'a ErasureSetRegistry,
        capacity: &dyn EcCapacitySource,
    ) -> Result<&'a ErasureSet, EcPlacementError> {
        let pool = sets.pool_id().to_string();
        if sets.is_empty() {
            return Err(EcPlacementError::PoolHasNoSets { pool });
        }

        if !self.config.multi_set_enabled {
            let set = sets
                .default_set()
                .ok_or(EcPlacementError::PoolHasNoSets { pool: pool.clone() })?;
            if set.layout_pending {
                return Err(EcPlacementError::LayoutPending {
                    pool,
                    set_id: set.set_id,
                });
            }
            return Ok(set);
        }

        let writable = sets.active_sets();
        let not_writable = sets.len().saturating_sub(writable.len());

        let mut candidates = Vec::with_capacity(writable.len());
        let mut full = 0usize;
        for set in writable {
            let room = set_capacity(set, capacity, self.config.capacity_max_age);
            if room.is_some_and(|room| room.is_full(self.config.full_threshold)) {
                full = full.saturating_add(1);
                continue;
            }
            candidates.push(Candidate {
                set,
                capacity: room,
            });
        }

        if candidates.is_empty() {
            // Nothing takes the object now; a set held only until this node
            // reads its record will, and an object admitted to it waits for
            // that rather than staying replicated for good.
            let held = sets.sets().into_iter().find(|set| {
                set.state == ErasureSetState::Active
                    && set.layout_pending
                    && !set_capacity(set, capacity, self.config.capacity_max_age)
                        .is_some_and(|room| room.is_full(self.config.full_threshold))
            });
            if let Some(held) = held {
                return Err(EcPlacementError::LayoutPending {
                    pool,
                    set_id: held.set_id,
                });
            }
            return Err(EcPlacementError::CapacityExhausted {
                pool,
                total: sets.len(),
                not_writable,
                full,
            });
        }

        if candidates.iter().any(Candidate::is_healthy) {
            candidates.retain(Candidate::is_healthy);
        }

        // Nothing to decide, and nothing to remember either: leaving the walk
        // untouched keeps a single-set pool from drifting the cursor of a pool
        // that later grows.
        if candidates.len() == 1 {
            return Ok(candidates[0].set);
        }

        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let chosen = match self.config.strategy {
            EcPlacementStrategy::RoundRobin => round_robin(&mut state, &candidates),
            EcPlacementStrategy::FreeSpace => match weights(&candidates) {
                Some(weights) => weighted(&mut state, &candidates, &weights),
                // No usable capacity data, so there is nothing to weigh by.
                // Spreading evenly is the honest answer, and it is the same one
                // the default strategy gives.
                None => round_robin(&mut state, &candidates),
            },
        };
        Ok(candidates[chosen].set)
    }
}

/// Even walk over the candidates, one step per object.
fn round_robin(state: &mut RouterState, candidates: &[Candidate<'_>]) -> usize {
    let index = state.cursor % candidates.len();
    state.cursor = state.cursor.wrapping_add(1);
    index
}

/// Free bytes per candidate, or `None` when the set is not fully measured.
///
/// All-or-nothing again: weighing a measured set against an unmeasured one
/// would silently treat "unknown" as "empty" or as "full", and either reading
/// is a guess presented as a measurement.
fn weights(candidates: &[Candidate<'_>]) -> Option<Vec<i128>> {
    let weights: Vec<i128> = candidates
        .iter()
        .map(|candidate| candidate.capacity.map(|room| i128::from(room.available_bytes)))
        .collect::<Option<Vec<_>>>()?;
    if weights.iter().sum::<i128>() <= 0 {
        return None;
    }
    Some(weights)
}

/// Smooth weighted round-robin over the candidates.
///
/// Each call credits every candidate with its free space, hands the object to
/// the one holding the most credit, and charges that one the total. Over a full
/// cycle each set receives objects in proportion to its room, and the picks
/// interleave instead of arriving in blocks — a set twice as large takes two of
/// every three objects rather than the first two hundred of three hundred.
fn weighted(state: &mut RouterState, candidates: &[Candidate<'_>], weights: &[i128]) -> usize {
    let total: i128 = weights.iter().sum();

    // Sets come and go as they fill up and are emptied; credit for a set that
    // is no longer a candidate must not accumulate while it is away.
    let live: Vec<u64> = candidates.iter().map(|candidate| candidate.set.set_id).collect();
    state.credits.retain(|set_id, _| live.contains(set_id));

    let mut best = 0usize;
    let mut best_credit = i128::MIN;
    for (index, candidate) in candidates.iter().enumerate() {
        let credit = state.credits.entry(candidate.set.set_id).or_insert(0);
        *credit = credit.saturating_add(weights[index]);
        // Strictly greater keeps ties with the earlier candidate, and the
        // registry enumerates in a stable order, so equal weights produce the
        // same sequence on every node and in every run.
        if *credit > best_credit {
            best_credit = *credit;
            best = index;
        }
    }

    if let Some(credit) = state.credits.get_mut(&candidates[best].set.set_id) {
        *credit = credit.saturating_sub(total);
    }
    best
}

/// Free space of one erasure set, as an operator sees it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EcSetCapacityReport {
    /// Set this line is about.
    pub set_id: u64,
    /// Lifecycle state of the set.
    pub state: ErasureSetState,
    /// Whether the set may still receive new objects.
    pub accepts_writes: bool,
    /// Free space, or `None` when a node of the set has not reported usably.
    pub capacity: Option<EcSetCapacity>,
    /// Whether the set has crossed the fullness threshold.
    pub full: bool,
}

/// Free space of a pool and of each of its sets.
#[derive(Debug, Clone, PartialEq)]
pub struct EcPoolCapacityReport {
    /// Sum of the free space of the sets that reported.
    pub available_bytes: u64,
    /// How many sets contributed to that sum.
    pub sets_measured: usize,
    /// How many sets the pool has.
    pub sets_total: usize,
    /// Used-space ratio at which a set stops taking new objects.
    pub full_threshold: f64,
    /// One line per set, in registry order.
    pub sets: Vec<EcSetCapacityReport>,
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// The placement decision for one pool: its sets, its router, its capacity view.
///
/// Held by the EC operations service and shared with the tiering worker, so
/// both admission paths route through the same state. Without the sharing each
/// would keep its own walk and the two would drift apart.
pub struct EcPlacementService {
    sets: Arc<EcErasureSets>,
    router: EcPlacementRouter,
    capacity: OnceLock<Arc<dyn EcCapacitySource>>,
    membership: OnceLock<Arc<dyn ClusterMembershipQuery>>,
}

/// The pool's erasure sets as they are right now.
///
/// A pool used to be settled at startup, so everything that needed the sets took
/// a copy. An operator can now add one to a running pool, and a copy taken at
/// startup would never see it: the new set's objects would be unreadable on
/// every node that had not restarted. This holds the one live roster, and the
/// long-lived workers ask it at the top of each cycle rather than caching it.
///
/// Readers get an `Arc`, so a swap never waits for a reader to finish and a
/// reader never sees the roster change under it mid-operation.
#[derive(Debug)]
pub struct EcErasureSets {
    current: RwLock<Arc<ErasureSetRegistry>>,
}

impl EcErasureSets {
    /// Start from the sets the node's configuration describes.
    pub fn new(sets: ErasureSetRegistry) -> Self {
        Self {
            current: RwLock::new(Arc::new(sets)),
        }
    }

    /// The roster as it is now.
    ///
    /// A poisoned lock is impossible here — nothing panics while it is held —
    /// but the recovery is honest rather than a panic: the last value is
    /// returned, because refusing to answer would stop every read of the pool.
    pub fn current(&self) -> Arc<ErasureSetRegistry> {
        match self.current.read() {
            Ok(guard) => Arc::clone(&guard),
            Err(poisoned) => Arc::clone(&poisoned.into_inner()),
        }
    }

    /// Install a new roster.
    ///
    /// Returns whether the roster actually changed, so a caller refreshing on a
    /// timer can stay quiet when nothing moved.
    pub fn replace(&self, sets: ErasureSetRegistry) -> bool {
        let mut guard = match self.current.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if **guard == sets {
            return false;
        }
        *guard = Arc::new(sets);
        true
    }
}

impl std::fmt::Debug for EcPlacementService {
    /// Hand-written because a capacity source is a live cluster handle, which
    /// has no useful rendering and no business appearing in a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EcPlacementService")
            .field("pool_id", &self.sets.current().pool_id())
            .field("sets", &self.sets.current().len())
            .field("config", self.config())
            .field("capacity_source_wired", &self.capacity.get().is_some())
            .field("membership_wired", &self.membership.get().is_some())
            .finish()
    }
}

impl EcPlacementService {
    /// Build the placement service for a pool.
    pub fn new(sets: ErasureSetRegistry, config: EcPlacementConfig) -> Self {
        Self {
            sets: Arc::new(EcErasureSets::new(sets)),
            router: EcPlacementRouter::new(config),
            capacity: OnceLock::new(),
            membership: OnceLock::new(),
        }
    }

    /// Build the placement service, reading the configuration from the
    /// environment.
    pub fn from_env(sets: ErasureSetRegistry) -> Result<Self, EcConfigError> {
        Ok(Self::new(sets, EcPlacementConfig::from_env()?))
    }

    /// Every erasure set of this pool, as they are now.
    pub fn sets(&self) -> Arc<ErasureSetRegistry> {
        self.sets.current()
    }

    /// The live roster, for a worker that outlives a single cycle.
    pub fn sets_handle(&self) -> Arc<EcErasureSets> {
        Arc::clone(&self.sets)
    }

    /// Install a new roster. Returns whether it changed anything.
    pub fn replace_sets(&self, sets: ErasureSetRegistry) -> bool {
        self.sets.replace(sets)
    }

    /// Configuration this pool places by.
    pub fn config(&self) -> &EcPlacementConfig {
        self.router.config()
    }

    /// Give the router a live view of node capacity.
    ///
    /// Wired after the cluster is up. Until then, and on a node whose cluster
    /// view carries no capacity reports, every set reads as unmeasured and the
    /// free-space strategy behaves as the even walk.
    pub fn configure_capacity_source(&self, source: Arc<dyn EcCapacitySource>) {
        if self.capacity.set(source).is_err() {
            tracing::debug!("EC placement capacity source was already configured");
        }
    }

    /// Give the placement service a live view of cluster membership.
    ///
    /// Wired after the cluster is up. Until then, the rolling-upgrade gate is
    /// inactive so unclustered nodes and unit tests continue to place writes.
    pub fn configure_membership(&self, membership: Arc<dyn ClusterMembershipQuery>) {
        if self.membership.set(membership).is_err() {
            tracing::debug!("EC placement membership was already configured");
        }
    }

    /// Whether every node holding a slot in the given roster confirms EC v2.
    ///
    /// The question is asked of the nodes that hold data this router may place
    /// into, which is why it takes the roster rather than the pool's configured
    /// node list: a node of the pool that holds no slot yet cannot make an
    /// existing set unreadable.
    ///
    /// With no membership view wired this answers `true` and the gate stays
    /// inactive — the opposite direction from the admin gate in `ec_ops`, which
    /// fails closed. Both are deliberate. In production
    /// `configure_cluster_membership` runs at bootstrap before any write path
    /// exists, so an unwired view here means a unit test or a node that is not
    /// clustered, and stopping placement for those would change EC v1 behaviour
    /// for everyone. Refusing to grow a pool is a rare operator action that can
    /// afford to wait for an answer; refusing to place every object is not.
    fn pool_speaks_v2(&self, sets: &ErasureSetRegistry) -> bool {
        match self.membership.get() {
            Some(membership) => membership.pool_nodes_support_v2(&sets.distinct_nodes()),
            None => true,
        }
    }

    /// Free space of every set of the pool, and of the pool as a whole.
    ///
    /// The pool figure is the sum of the per-set figures and is computed here,
    /// from the same numbers, so the aggregate and its parts cannot disagree —
    /// which they would the moment two callers derived them separately
    /// ([EC2-042]).
    ///
    /// A set whose capacity is unknown contributes nothing to the total and is
    /// reported as unknown, rather than as zero: "no room" and "no answer" are
    /// different facts and an operator acts on them differently.
    ///
    /// [EC2-042]: ../../../plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/EC2-042-placement-strategy.md
    pub fn capacity_report(&self) -> EcPoolCapacityReport {
        let sets = self.sets.current();
        let source: &dyn EcCapacitySource = match self.capacity.get() {
            Some(capacity) => capacity.as_ref(),
            None => &EcCapacityUnknown,
        };
        let config = self.config();

        let mut per_set = Vec::with_capacity(sets.len());
        let mut available_bytes = 0u64;
        let mut sets_measured = 0usize;
        for set in sets.sets() {
            let capacity = set_capacity(set, source, config.capacity_max_age);
            if let Some(capacity) = capacity {
                available_bytes = available_bytes.saturating_add(capacity.available_bytes);
                sets_measured = sets_measured.saturating_add(1);
            }
            per_set.push(EcSetCapacityReport {
                set_id: set.set_id,
                state: set.state,
                accepts_writes: set.accepts_writes(),
                capacity,
                full: capacity.is_some_and(|capacity| capacity.is_full(config.full_threshold)),
            });
        }

        EcPoolCapacityReport {
            available_bytes,
            sets_measured,
            sets_total: per_set.len(),
            full_threshold: config.full_threshold,
            sets: per_set,
        }
    }

    /// Pick the erasure set the next object goes to.
    ///
    /// While the pool is mid-upgrade the answer is the inherited set, whatever
    /// the strategy would have said: it is the only set a node running an older
    /// binary can resolve. Everything else is the router's decision.
    ///
    /// Returns the set by value rather than by reference: the roster it came
    /// from can be replaced while the caller is still encoding into the answer,
    /// and a set that has been through a whole transcode is a handful of node
    /// ids to clone.
    pub fn select_write_set(&self) -> Result<ErasureSet, EcPlacementError> {
        let sets = self.sets.current();
        let source: &dyn EcCapacitySource = match self.capacity.get() {
            Some(capacity) => capacity.as_ref(),
            None => &EcCapacityUnknown,
        };

        if !self.pool_speaks_v2(&sets) {
            if let Some(inherited) = sets.inherited_set().filter(|set| set.accepts_writes()) {
                // The pool is mid-upgrade, so the inherited set is the only one
                // every node can read — but it is still a set, and a full set
                // takes no new objects. Skipping that check here would admit a
                // transcode into a set with no room, which fails at the shard
                // write instead of being postponed until room appears.
                let room = set_capacity(inherited, source, self.config().capacity_max_age);
                if room.is_some_and(|room| room.is_full(self.config().full_threshold)) {
                    // The counts below describe the pool truthfully, and on a
                    // pool of several sets they will not add up to "nothing
                    // available" on their own — the missing piece is the
                    // upgrade, which is why it is said out loud here.
                    tracing::warn!(
                        pool = %sets.pool_id(),
                        set_id = inherited.set_id,
                        "EC pool is mid-upgrade, so only its inherited set may take new objects, \
                         and that set is full"
                    );
                    return Err(EcPlacementError::CapacityExhausted {
                        pool: sets.pool_id().to_string(),
                        total: sets.len(),
                        not_writable: sets
                            .sets()
                            .into_iter()
                            .filter(|set| !set.accepts_writes())
                            .count(),
                        full: 1,
                    });
                }
                return Ok(inherited.clone());
            }
            // If the inherited set does not accept writes (an operator
            // deactivated it), fall through to the ordinary router path: an
            // added set is then the only place left, and that state is the
            // operator's own choice.
        }

        let chosen = self.router.select(&sets, source)?;
        Ok(chosen.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use s4_cluster::PoolId;

    use super::*;
    use crate::erasure_coding::{EcProfile, EcTopologyPolicy, FIRST_SET_ID, PLACEMENT_ENV_LOCK};

    const POOL: u32 = 9;
    const GIB: u64 = 1024 * 1024 * 1024;

    /// `count` node ids starting at `start`.
    ///
    /// Sets are built on disjoint node ranges so a test can give one set room
    /// and starve another without the two sharing a node.
    fn nodes_from(start: u8, count: u8) -> Vec<NodeId> {
        (start..start.saturating_add(count))
            .map(|idx| NodeId(u128::from(idx)))
            .collect()
    }

    /// Nodes of the n-th set of the pool, at the profile's full width.
    fn set_nodes(index: u8) -> Vec<NodeId> {
        let width = EcProfile::rs_small().total_shards();
        nodes_from(1u8.saturating_add(index.saturating_mul(width)), width)
    }

    /// An active set with an explicit id over the given nodes.
    fn set_on(set_id: u64, nodes: &[NodeId]) -> ErasureSet {
        ErasureSet::active(
            PoolId::new(POOL),
            set_id,
            EcProfile::rs_small(),
            nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap()
    }

    /// An active inherited set over the given nodes.
    fn inherited_set_on(nodes: &[NodeId]) -> ErasureSet {
        ErasureSet::inherited(
            PoolId::new(POOL),
            EcProfile::rs_small(),
            nodes,
            &HashMap::new(),
            EcTopologyPolicy::Strict,
            1,
            1,
        )
        .unwrap()
    }

    fn registry(sets: Vec<ErasureSet>) -> ErasureSetRegistry {
        ErasureSetRegistry::from_sets(PoolId::new(POOL), sets).unwrap()
    }

    /// Multi-set placement switched on, everything else at its default.
    fn enabled(strategy: EcPlacementStrategy) -> EcPlacementConfig {
        EcPlacementConfig {
            multi_set_enabled: true,
            strategy,
            ..EcPlacementConfig::default()
        }
    }

    /// A set this node holds until it reads its layout record takes no new
    /// object, with multi-set placement on or off: the object stays on its
    /// RF=3 replicas instead of being encoded by a layout the pool may not
    /// have (plan 05, EC2-088).
    #[test]
    fn a_set_held_until_its_record_is_read_takes_no_new_object() {
        let mut held = inherited_set_on(&set_nodes(0));
        held.layout_pending = true;
        let other = set_on(FIRST_SET_ID, &set_nodes(1));

        let held_id = held.set_id;

        // Not chosen, but named: the object belongs there once the record is
        // read, which is not the pool running out of room.
        let off = EcPlacementRouter::new(EcPlacementConfig::default());
        let alone = registry(vec![held.clone()]);
        let error = off.select(&alone, &EcCapacityUnknown).unwrap_err();
        assert_eq!(error.held_set(), Some(held_id), "{error}");
        assert!(!error.is_capacity_exhausted(), "{error}");

        let on = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));
        let error = on.select(&alone, &EcCapacityUnknown).unwrap_err();
        assert_eq!(error.held_set(), Some(held_id), "{error}");
        let both = registry(vec![held.clone(), other.clone()]);
        for _ in 0..4 {
            assert_eq!(
                on.select(&both, &EcCapacityUnknown).unwrap().set_id,
                FIRST_SET_ID
            );
        }

        // The other set full: the held set is the one with room.
        let full_other = StubCapacity::default().measured(&set_nodes(1), 100 * GIB, GIB);
        let error = on.select(&both, &full_other).unwrap_err();
        assert_eq!(error.held_set(), Some(held_id), "{error}");

        // Held and full as well: that is the pool out of room.
        let everything_full = StubCapacity::default()
            .measured(&set_nodes(0), 100 * GIB, GIB)
            .measured(&set_nodes(1), 100 * GIB, GIB);
        assert!(on.select(&both, &everything_full).unwrap_err().is_capacity_exhausted());
    }

    /// Capacity the test states outright, standing in for the cluster's reports.
    #[derive(Default)]
    struct StubCapacity {
        nodes: HashMap<NodeId, EcNodeCapacity>,
    }

    impl StubCapacity {
        /// Report the same figures for every node of one set.
        fn measured(mut self, nodes: &[NodeId], total_bytes: u64, available_bytes: u64) -> Self {
            self.report(
                nodes,
                EcNodeCapacity {
                    total_bytes,
                    available_bytes,
                    reported_ago: Duration::from_secs(1),
                },
            );
            self
        }

        /// Report figures that are real but too old to act on.
        fn stale(mut self, nodes: &[NodeId], total_bytes: u64, available_bytes: u64) -> Self {
            self.report(
                nodes,
                EcNodeCapacity {
                    total_bytes,
                    available_bytes,
                    reported_ago: Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS + 1),
                },
            );
            self
        }

        /// Report what a node that has never measured its filesystem sends.
        fn unmeasured(mut self, nodes: &[NodeId]) -> Self {
            self.report(
                nodes,
                EcNodeCapacity {
                    total_bytes: 0,
                    available_bytes: 0,
                    reported_ago: Duration::from_secs(1),
                },
            );
            self
        }

        fn report(&mut self, nodes: &[NodeId], capacity: EcNodeCapacity) {
            for node_id in nodes {
                self.nodes.insert(*node_id, capacity);
            }
        }
    }

    impl EcCapacitySource for StubCapacity {
        fn node_capacity(&self, node_id: NodeId) -> Option<EcNodeCapacity> {
            self.nodes.get(&node_id).copied()
        }
    }

    /// How many objects each set received over `rounds` placements.
    fn tally(
        router: &EcPlacementRouter,
        sets: &ErasureSetRegistry,
        capacity: &dyn EcCapacitySource,
        rounds: usize,
    ) -> HashMap<u64, usize> {
        let mut counts = HashMap::new();
        for _ in 0..rounds {
            let chosen = router.select(sets, capacity).unwrap();
            *counts.entry(chosen.set_id).or_insert(0) += 1;
        }
        counts
    }

    // -----------------------------------------------------------------------
    // Free-space weighting
    // -----------------------------------------------------------------------

    /// The point of the strategy: room decides share. Three times the free
    /// space means three times the objects, and the picks interleave rather
    /// than arriving in one block — which is why the tally is exact rather than
    /// approximate.
    #[test]
    fn free_space_gives_a_roomier_set_proportionally_more_objects() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).measured(
            &second,
            400 * GIB,
            100 * GIB,
        );
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::FreeSpace));

        let counts = tally(&router, &sets, &capacity, 40);

        assert_eq!(counts.get(&1), Some(&30));
        assert_eq!(counts.get(&2), Some(&10));
    }

    /// Capacity nobody has measured is not a reason to refuse or to guess. The
    /// strategy falls back to the even walk, which is what every pool does today
    /// because no node reports real filesystem figures yet.
    #[test]
    fn free_space_falls_back_to_the_even_walk_without_capacity_data() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().unmeasured(&first).unmeasured(&second);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::FreeSpace));

        let counts = tally(&router, &sets, &capacity, 10);

        assert_eq!(counts.get(&1), Some(&5));
        assert_eq!(counts.get(&2), Some(&5));
    }

    /// Weighing one measured set against one that has gone quiet would read the
    /// silence as room or as fullness, and both are guesses. All or nothing.
    #[test]
    fn free_space_falls_back_to_the_even_walk_when_one_set_is_stale() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).stale(
            &second,
            400 * GIB,
            100 * GIB,
        );
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::FreeSpace));

        let counts = tally(&router, &sets, &capacity, 10);

        assert_eq!(counts.get(&1), Some(&5));
        assert_eq!(counts.get(&2), Some(&5));
    }

    // -----------------------------------------------------------------------
    // Fullness
    // -----------------------------------------------------------------------

    /// Fullness is a mark, not a state: garbage collection frees space and the
    /// set takes writes again, with nothing for an operator to do.
    #[test]
    fn a_full_set_takes_writes_again_once_space_comes_back() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let full = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).measured(
            &second,
            400 * GIB,
            4 * GIB,
        );
        assert_eq!(tally(&router, &sets, &full, 4).get(&1), Some(&4));

        // Same sets, same states — only the free space changed.
        let freed = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).measured(
            &second,
            400 * GIB,
            200 * GIB,
        );
        let counts = tally(&router, &sets, &freed, 4);

        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&2));
    }

    // -----------------------------------------------------------------------
    // Topology health
    // -----------------------------------------------------------------------

    /// A degraded set is passed over while a healthy one can take the object,
    /// and used when it is all the pool has. Excluding it outright would stop
    /// writes into a single-set pool running under a warning topology policy —
    /// which EC v1 accepted, and which this plan must not change.
    #[test]
    fn a_degraded_set_is_used_only_when_no_healthy_set_can_take_writes() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let mut degraded = set_on(2, &second);
        degraded.topology_health = EcTopologyHealth::TopologyDegraded;

        let both = registry(vec![set_on(1, &first), degraded.clone()]);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));
        let counts = tally(&router, &both, &EcCapacityUnknown, 6);
        assert_eq!(counts.get(&1), Some(&6), "the healthy set takes everything");
        assert_eq!(counts.get(&2), None);

        let alone = registry(vec![degraded]);
        let chosen = router.select(&alone, &EcCapacityUnknown).unwrap();
        assert_eq!(
            chosen.set_id, 2,
            "a degraded set still beats refusing the write"
        );
    }

    // -----------------------------------------------------------------------
    // Running out of room
    // -----------------------------------------------------------------------

    /// Its own error, carrying what the operator needs to act: how many sets the
    /// pool has, how many are closed to writes, how many are full. And it
    /// reaches the client as `507`, never as a generic internal error.
    #[test]
    fn every_set_excluded_is_its_own_error_and_reaches_the_client_as_507() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let mut closed = set_on(2, &second);
        closed.state = ErasureSetState::ReadOnly;
        let sets = registry(vec![set_on(1, &first), closed]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 4 * GIB);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let error = router.select(&sets, &capacity).unwrap_err();

        assert_eq!(
            error,
            EcPlacementError::CapacityExhausted {
                pool: PoolId::new(POOL).to_string(),
                total: 2,
                not_writable: 1,
                full: 1,
            }
        );
        assert!(error.is_capacity_exhausted());
        assert!(matches!(
            ClusterError::from(error),
            ClusterError::InsufficientCapacity(_)
        ));
    }

    /// Disabling the multi-set feature preserves strict EC v1 semantics.
    /// Every placement unconditionally chooses the pool's default set.
    #[test]
    fn feature_off_always_places_into_the_pool_default_set() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let router = EcPlacementRouter::new(EcPlacementConfig::default());
        let default_id = sets.default_set().unwrap().set_id;

        for _ in 0..10 {
            let chosen = router.select(&sets, &EcCapacityUnknown).unwrap();
            assert_eq!(chosen.set_id, default_id);
        }
    }

    /// With multi-set placement disabled, fullness thresholds are never evaluated.
    /// Writes must not fail where EC v1 would have accepted them.
    #[test]
    fn feature_off_still_places_into_a_full_default_set() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 4 * GIB);
        let router = EcPlacementRouter::new(EcPlacementConfig::default());
        let default_id = sets.default_set().unwrap().set_id;

        let chosen = router.select(&sets, &capacity).unwrap();
        assert_eq!(chosen.set_id, default_id);
    }

    /// Round-robin distributes writes evenly across all candidate sets.
    /// Over multiple rounds, each eligible set receives an equal share.
    #[test]
    fn round_robin_spreads_objects_evenly_over_the_eligible_sets() {
        let (first, second, third) = (set_nodes(0), set_nodes(1), set_nodes(2));
        let sets = registry(vec![
            set_on(1, &first),
            set_on(2, &second),
            set_on(3, &third),
        ]);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let counts = tally(&router, &sets, &EcCapacityUnknown, 30);

        assert_eq!(counts.get(&1), Some(&10));
        assert_eq!(counts.get(&2), Some(&10));
        assert_eq!(counts.get(&3), Some(&10));
    }

    /// Sets closed to writes by their lifecycle state must be excluded from selection.
    /// ReadOnly and Draining sets never accept new objects.
    #[test]
    fn sets_that_do_not_accept_writes_are_never_chosen() {
        let (first, second, third) = (set_nodes(0), set_nodes(1), set_nodes(2));
        let mut readonly = set_on(2, &second);
        readonly.state = ErasureSetState::ReadOnly;
        let mut draining = set_on(3, &third);
        draining.state = ErasureSetState::Draining;

        let sets = registry(vec![set_on(1, &first), readonly, draining]);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let counts = tally(&router, &sets, &EcCapacityUnknown, 6);

        assert_eq!(counts.get(&1), Some(&6));
        assert_eq!(counts.get(&2), None);
        assert_eq!(counts.get(&3), None);
    }

    /// A set exceeding the full threshold is passed over while another has room.
    /// Incoming writes steer exclusively toward sets that can still hold them.
    #[test]
    fn a_full_set_is_passed_over_while_another_has_room() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).measured(
            &second,
            400 * GIB,
            4 * GIB,
        );
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let counts = tally(&router, &sets, &capacity, 6);

        assert_eq!(counts.get(&1), Some(&6));
        assert_eq!(counts.get(&2), None);
    }

    /// Unmeasured nodes report zeros, indicating absent measurements rather than a full disk.
    /// Clusters without filesystem metrics must keep accepting writes rather than stalling.
    #[test]
    fn unmeasured_capacity_never_marks_a_set_full() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().unmeasured(&first).unmeasured(&second);
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let counts = tally(&router, &sets, &capacity, 6);

        assert_eq!(counts.get(&1), Some(&3));
        assert_eq!(counts.get(&2), Some(&3));
    }

    /// Outdated capacity reports are discarded rather than trusted as current fullness.
    /// Stale data falls back to unknown capacity, keeping the set eligible.
    #[test]
    fn stale_capacity_never_marks_a_set_full() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![set_on(1, &first), set_on(2, &second)]);
        let capacity = StubCapacity::default().measured(&first, 400 * GIB, 300 * GIB).stale(
            &second,
            400 * GIB,
            4 * GIB,
        );
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let counts = tally(&router, &sets, &capacity, 6);

        assert_eq!(counts.get(&1), Some(&3));
        assert_eq!(counts.get(&2), Some(&3));
    }

    /// A pool without erasure sets represents an invalid registry configuration.
    /// It must fail as a configuration error rather than an out-of-capacity error.
    #[test]
    fn a_pool_without_sets_is_a_configuration_error_not_a_capacity_one() {
        let sets = ErasureSetRegistry::new(PoolId::new(POOL));
        let router = EcPlacementRouter::new(enabled(EcPlacementStrategy::RoundRobin));

        let error = router.select(&sets, &EcCapacityUnknown).unwrap_err();

        assert!(matches!(error, EcPlacementError::PoolHasNoSets { .. }));
        assert!(!error.is_capacity_exhausted());
        assert!(matches!(ClusterError::from(error), ClusterError::Config(_)));
    }

    /// Shards land on every node of an erasure set, so capacity is strictly bottlenecked
    /// by the tightest node rather than an aggregate or average of all nodes.
    #[test]
    fn set_free_space_is_the_free_space_of_its_tightest_node() {
        let nodes = set_nodes(0);
        let set = set_on(1, &nodes);
        let mut capacity = StubCapacity::default().measured(&nodes, 400 * GIB, 300 * GIB);
        capacity.report(
            &nodes[0..1],
            EcNodeCapacity {
                total_bytes: 400 * GIB,
                available_bytes: 50 * GIB,
                reported_ago: Duration::from_secs(1),
            },
        );

        let room = set_capacity(
            &set,
            &capacity,
            Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS),
        )
        .unwrap();

        assert_eq!(room.available_bytes, 50 * GIB);
        assert!((room.used_ratio - 0.875).abs() < 1e-9);
    }

    /// When any node in an erasure set fails to report, the set's capacity is unknown.
    /// Estimating capacity from a subset of nodes would overestimate available space.
    #[test]
    fn set_capacity_is_unknown_when_one_node_says_nothing() {
        let nodes = set_nodes(0);
        let set = set_on(1, &nodes);
        let capacity = StubCapacity::default().measured(&nodes[1..], 400 * GIB, 300 * GIB);

        let room = set_capacity(
            &set,
            &capacity,
            Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS),
        );

        assert!(room.is_none());
    }

    /// Default placement parameters ensure backward compatibility and predictable behavior,
    /// keeping multi-set placement disabled until an operator opts in.
    #[test]
    fn placement_config_defaults_match_the_documented_values() {
        let config = EcPlacementConfig::default();

        assert!(!config.multi_set_enabled);
        assert_eq!(config.strategy, EcPlacementStrategy::RoundRobin);
        assert_eq!(config.full_threshold, DEFAULT_SET_FULL_THRESHOLD);
        assert_eq!(
            config.capacity_max_age,
            Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS)
        );
    }

    /// An operator who mistypes a threshold has to be told which setting is
    /// wrong, not left with a pool that silently placed by some other rule.
    ///
    /// The lock is load-bearing: the environment is process-wide, so setting an
    /// invalid value here would otherwise surface as a failure in whichever test
    /// happened to parse the environment at the same moment.
    #[test]
    fn placement_env_overrides_are_parsed_and_validated() {
        let _lock = PLACEMENT_ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::remove_var(SET_FULL_THRESHOLD_ENV);
        std::env::remove_var(CAPACITY_MAX_AGE_ENV);

        assert_eq!(parse_full_threshold().unwrap(), DEFAULT_SET_FULL_THRESHOLD);
        assert_eq!(
            parse_capacity_max_age().unwrap(),
            Duration::from_secs(DEFAULT_CAPACITY_MAX_AGE_SECS)
        );

        std::env::set_var(SET_FULL_THRESHOLD_ENV, "0.75");
        assert_eq!(parse_full_threshold().unwrap(), 0.75);
        std::env::remove_var(SET_FULL_THRESHOLD_ENV);

        std::env::set_var(SET_FULL_THRESHOLD_ENV, "1.5");
        let err = parse_full_threshold().unwrap_err();
        assert!(err.to_string().contains(SET_FULL_THRESHOLD_ENV));
        std::env::remove_var(SET_FULL_THRESHOLD_ENV);

        std::env::set_var(SET_FULL_THRESHOLD_ENV, "many");
        assert!(parse_full_threshold().is_err());
        std::env::remove_var(SET_FULL_THRESHOLD_ENV);

        std::env::set_var(CAPACITY_MAX_AGE_ENV, "30");
        assert_eq!(parse_capacity_max_age().unwrap(), Duration::from_secs(30));
        std::env::remove_var(CAPACITY_MAX_AGE_ENV);

        std::env::set_var(CAPACITY_MAX_AGE_ENV, "0");
        assert!(parse_capacity_max_age().is_err());
        std::env::remove_var(CAPACITY_MAX_AGE_ENV);

        std::env::remove_var(SET_FULL_THRESHOLD_ENV);
        std::env::remove_var(CAPACITY_MAX_AGE_ENV);
    }

    /// While a pool has not confirmed that every node speaks EC v2, new
    /// objects must only be placed in the inherited set that older nodes can
    /// resolve.
    #[test]
    fn a_mixed_pool_places_new_objects_in_the_inherited_set() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![inherited_set_on(&first), set_on(1, &second)]);
        let inherited_id = sets.inherited_set().unwrap().set_id;
        let service = EcPlacementService::new(sets, enabled(EcPlacementStrategy::RoundRobin));
        service.configure_membership(Arc::new(s4_cluster::MockClusterMembership::new(false)));

        for _ in 0..10 {
            let chosen = service.select_write_set().unwrap();
            assert_eq!(chosen.set_id, inherited_id);
        }
    }

    /// Once every node confirms EC v2 support, placement spreads writes across
    /// all active erasure sets according to the configured strategy.
    #[test]
    fn a_uniform_pool_spreads_new_objects_over_its_sets() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![inherited_set_on(&first), set_on(1, &second)]);
        let service = EcPlacementService::new(sets, enabled(EcPlacementStrategy::RoundRobin));
        service.configure_membership(Arc::new(s4_cluster::MockClusterMembership::new(true)));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let chosen = service.select_write_set().unwrap();
            seen.insert(chosen.set_id);
        }
        assert_eq!(seen.len(), 2);
    }

    /// An unwired placement service behaves as it did before rolling-upgrade
    /// checks were introduced, leaving unclustered nodes and unit tests
    /// unaffected.
    #[test]
    fn a_service_without_a_membership_view_places_as_it_did_before() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![inherited_set_on(&first), set_on(1, &second)]);
        let service = EcPlacementService::new(sets, enabled(EcPlacementStrategy::RoundRobin));

        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let chosen = service.select_write_set().unwrap();
            seen.insert(chosen.set_id);
        }
        assert_eq!(seen.len(), 2);
    }

    /// If an operator deactivates the inherited set mid-upgrade, writes must
    /// still succeed by falling through to available active sets rather than
    /// blocking writes entirely.
    #[test]
    fn a_mixed_pool_whose_inherited_set_is_read_only_still_places() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let mut inherited = inherited_set_on(&first);
        inherited.state = ErasureSetState::ReadOnly;
        let additional = set_on(1, &second);
        let sets = registry(vec![inherited, additional]);
        let service = EcPlacementService::new(sets, enabled(EcPlacementStrategy::RoundRobin));
        service.configure_membership(Arc::new(s4_cluster::MockClusterMembership::new(false)));

        let chosen = service.select_write_set().unwrap();
        assert_eq!(chosen.set_id, 1);
        assert!(chosen.accepts_writes());
    }

    /// Mid-upgrade the inherited set is the only candidate, and a full set takes
    /// no new objects even when it is the only one.
    ///
    /// Without this the pool would admit a transcode into a set with no room:
    /// the object would fail at the shard write instead of waiting, replicated,
    /// until space appears.
    #[test]
    fn a_mixed_pool_with_a_full_inherited_set_postpones_the_object() {
        let (first, second) = (set_nodes(0), set_nodes(1));
        let sets = registry(vec![inherited_set_on(&first), set_on(1, &second)]);
        let service = EcPlacementService::new(sets, enabled(EcPlacementStrategy::RoundRobin));
        service.configure_membership(Arc::new(s4_cluster::MockClusterMembership::new(false)));
        // The inherited set is out of room; the second set has plenty, and
        // mid-upgrade that does not help.
        service.configure_capacity_source(Arc::new(
            StubCapacity::default().measured(&first, 100 * GIB, GIB).measured(
                &second,
                100 * GIB,
                90 * GIB,
            ),
        ));

        let error = service.select_write_set().unwrap_err();

        assert!(
            error.is_capacity_exhausted(),
            "a full inherited set must postpone the object, not place it: {error}"
        );
    }
}
