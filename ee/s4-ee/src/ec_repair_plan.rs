// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Which shards a repair reads, and what moving them costs.
//!
//! EC v1 asked every other slot of the chunk for its shard — `n − 1` reads —
//! and let the codec take what it needed (EC2-085, point 2). A repair now
//! decides first. One missing member of an LRC local group is rebuilt by XOR
//! from the rest of its group, `k / local` shards; anything else is decoded
//! from `k` shards, the nearest ones. The decision follows where the losses
//! actually are, and it is taken again, one source further out, whenever a
//! source turns out to be lost too (plan 05, phase 3, decision EC2-089).
//!
//! Pure logic, no I/O: the repair coordinator reads the shards and charges the
//! bandwidth limits; this module says what to read and how each read counts.

use s4_cluster::NodeId;
use s4_codec::{CodecError, EcParity, EcProfileSnapshot, LossTolerance, RecoveryPath};

use crate::erasure_coding::{NodeDistance, PoolTopologyView};

/// How a repair rebuilds its shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairStrategy {
    /// XOR inside the shard's LRC local group, from the group's other
    /// `k / local` members.
    LocalGroup {
        /// The local group, numbered from 0.
        group: u8,
    },
    /// Rebuild the slot from `k` shards: every Reed-Solomon repair, and an LRC
    /// repair the group cannot do alone.
    Global,
}

impl RepairStrategy {
    /// Stable name, used in logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LocalGroup { .. } => "local_group",
            Self::Global => "global",
        }
    }
}

impl std::fmt::Display for RepairStrategy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalGroup { group } => write!(formatter, "local group {group}"),
            Self::Global => formatter.write_str("global decode"),
        }
    }
}

/// What a repair knows about one slot of the chunk while it plans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    /// Not read yet: using it costs a transfer from its node.
    Unread,
    /// Read and verified against the manifest: free to use.
    Fetched,
    /// Unusable: the shard being repaired, or a source that turned out to be
    /// missing, corrupt or out of reach.
    Lost,
}

/// The shards one repair reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairSourcePlan {
    /// How the shard is rebuilt.
    pub strategy: RepairStrategy,
    /// Slots to rebuild it from, nearest first. Exactly `k / local` for
    /// [`RepairStrategy::LocalGroup`] and `k` for [`RepairStrategy::Global`].
    pub sources: Vec<u8>,
}

/// Chooses the shards that repair a slot of one profile.
#[derive(Debug, Clone)]
pub struct RepairPlanner {
    tolerance: LossTolerance,
    k: u8,
    /// Number of LRC local groups; `None` for Reed-Solomon.
    local_groups: Option<u8>,
}

impl RepairPlanner {
    /// A planner for `profile`.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::InvalidProfile`] when the profile is not valid.
    pub fn new(profile: &EcProfileSnapshot) -> Result<Self, CodecError> {
        Ok(Self {
            tolerance: LossTolerance::new(profile)?,
            k: profile.k,
            local_groups: match profile.parity {
                EcParity::Lrc { local, .. } => Some(local),
                EcParity::ReedSolomon { .. } => None,
            },
        })
    }

    /// Number of shard slots in the profile.
    pub fn total_shards(&self) -> usize {
        self.tolerance.total_shards()
    }

    /// Which path rebuilds `slot` once the slots marked in `lost` are gone.
    ///
    /// The planner is asked rather than the codec because it already holds the
    /// profile's [`LossTolerance`], and the answer is needed beside the shards
    /// rather than inside the codec worker. What it is for:
    /// [`RecoveryPath::UnifiedSolver`] marks a shard no EC v1 node would have
    /// rebuilt at all, and the repair worker writes that down instead of
    /// letting the solver save it quietly (decision EC2-103, section 5).
    pub fn repair_path(&self, slot: u8, lost: &[bool]) -> RecoveryPath {
        self.tolerance.repair_path(slot, lost)
    }

    /// The shards to rebuild `slot` from, or `None` when the slots still
    /// usable cannot rebuild it.
    ///
    /// `states[i]` is what the repair knows of slot `i`; `slot` itself counts
    /// as lost, and a slot past the end of `states` as unread. `distance`
    /// tells how far the node holding each slot is.
    ///
    /// The local group comes first whenever every other member of it is
    /// usable: it reads fewer shards than any decode. Otherwise the plan is a
    /// decode from `k` shards: shards already fetched first, since they cost
    /// nothing, then the nearest, taken one by one until the chunk decodes;
    /// then every shard that turns out not to be needed is dropped again,
    /// farthest first.
    pub fn plan(
        &self,
        slot: u8,
        states: &[SlotState],
        distance: impl Fn(u8) -> NodeDistance,
    ) -> Option<RepairSourcePlan> {
        let total = self.total_shards();
        if usize::from(slot) >= total {
            return None;
        }
        let lost = self.lost_slots(slot, states);
        let state_of = |source: u8| states.get(usize::from(source)).copied();
        // Fetched shards first, then by distance; the slot index keeps the
        // order, and therefore the plan, deterministic.
        let cost = |source: u8| {
            let fetched = state_of(source) == Some(SlotState::Fetched);
            (!fetched, distance(source), source)
        };

        if let Some(mut sources) = self.tolerance.local_repair_sources(slot) {
            if sources.iter().all(|source| !lost[usize::from(*source)]) {
                sources.sort_by_key(|source| cost(*source));
                return Some(RepairSourcePlan {
                    strategy: RepairStrategy::LocalGroup {
                        group: self.group_of(slot),
                    },
                    sources,
                });
            }
        }

        let mut candidates = (0..total)
            .filter(|source| !lost[*source])
            .filter_map(|source| u8::try_from(source).ok())
            .collect::<Vec<_>>();
        candidates.sort_by_key(|source| cost(*source));

        let mut chosen = Vec::with_capacity(usize::from(self.k));
        for candidate in candidates {
            chosen.push(candidate);
            if self.repairs_from(slot, &chosen) {
                break;
            }
        }
        if !self.repairs_from(slot, &chosen) {
            return None;
        }
        for index in (0..chosen.len()).rev() {
            let dropped = chosen.remove(index);
            if !self.repairs_from(slot, &chosen) {
                chosen.insert(index, dropped);
            }
        }
        Some(RepairSourcePlan {
            strategy: RepairStrategy::Global,
            sources: chosen,
        })
    }

    /// Why `plan` is the way to rebuild `slot`, given what `states` says.
    pub fn explain_choice(
        &self,
        slot: u8,
        plan: &RepairSourcePlan,
        states: &[SlotState],
    ) -> String {
        match (plan.strategy, self.tolerance.local_repair_sources(slot)) {
            (RepairStrategy::LocalGroup { group }, _) => {
                format!("every other member of local group {group} is usable")
            }
            (RepairStrategy::Global, None) => {
                "the slot has no local path: a global or Reed-Solomon parity slot".to_string()
            }
            (RepairStrategy::Global, Some(members)) => {
                let lost = members
                    .into_iter()
                    .filter(|member| states.get(usize::from(*member)) == Some(&SlotState::Lost))
                    .collect::<Vec<_>>();
                format!(
                    "local group {} has lost slot(s) {lost:?} as well",
                    self.group_of(slot)
                )
            }
        }
    }

    /// Why `slot` cannot be rebuilt from the slots `states` leaves usable, in
    /// words an operator can act on.
    pub fn explain_unrecoverable(&self, slot: u8, states: &[SlotState]) -> String {
        let lost = self.lost_slots(slot, states);
        let lost_slots = (0..lost.len()).filter(|index| lost[*index]).collect::<Vec<_>>();
        let whole_groups = self.whole_groups_lost(&lost);
        let mut detail = format!(
            "{} of {} shard(s) of the chunk are lost (slots {lost_slots:?})",
            lost_slots.len(),
            lost.len()
        );
        if !whole_groups.is_empty() {
            detail.push_str(&format!(
                "; local group(s) {whole_groups:?} lost every member, which no parity rebuilds"
            ));
        }
        detail
    }

    /// Every slot of the chunk, marked lost when `states` says so or when it
    /// is `slot`.
    fn lost_slots(&self, slot: u8, states: &[SlotState]) -> Vec<bool> {
        (0..self.total_shards())
            .map(|index| index == usize::from(slot) || states.get(index) == Some(&SlotState::Lost))
            .collect()
    }

    /// Whether the codec rebuilds `slot` from exactly the slots in `sources`.
    fn repairs_from(&self, slot: u8, sources: &[u8]) -> bool {
        let mut lost = vec![true; self.total_shards()];
        for source in sources {
            lost[usize::from(*source)] = false;
        }
        self.tolerance.can_repair(slot, &lost)
    }

    fn group_of(&self, slot: u8) -> u8 {
        match self.local_groups {
            Some(local) if slot < self.k => slot / (self.k / local),
            Some(_) => slot - self.k,
            None => 0,
        }
    }

    /// Local groups every member of which — data and local parity — is lost.
    fn whole_groups_lost(&self, lost: &[bool]) -> Vec<u8> {
        let Some(local) = self.local_groups else {
            return Vec::new();
        };
        (0..local)
            .filter(|group| {
                let local_parity = self.k + group;
                lost[usize::from(local_parity)]
                    && self
                        .tolerance
                        .local_repair_sources(local_parity)
                        .unwrap_or_default()
                        .iter()
                        .all(|member| lost[usize::from(*member)])
            })
            .collect()
    }
}

/// Which leg of a repair moves the bytes.
///
/// A repair runs on the node that queued it: the source shards travel to that
/// node, and the rebuilt shard travels from it to the slot's owner (EC2-085,
/// point 5). Either leg may cross a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairLeg {
    /// A source shard, read from its node.
    Source,
    /// The rebuilt shard, written to its owner.
    Write,
}

impl RepairLeg {
    /// Both legs, in a fixed order.
    pub const ALL: [Self; 2] = [Self::Source, Self::Write];

    /// Stable name, used as a metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Write => "write",
        }
    }
}

/// Where the bytes of one transfer go, in terms of zones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferScope {
    /// Within this node; nothing crosses the network.
    Local,
    /// Between two nodes of one zone.
    SameZone,
    /// Between two zones.
    CrossZone,
    /// A node without a zone label is involved, so nobody can say.
    UnknownZone,
}

impl TransferScope {
    /// Every scope, in a fixed order.
    pub const ALL: [Self; 4] = [
        Self::Local,
        Self::SameZone,
        Self::CrossZone,
        Self::UnknownZone,
    ];

    /// The scope of bytes moving from `from` to `to`.
    pub fn between(view: &PoolTopologyView, from: NodeId, to: NodeId) -> Self {
        if from == to {
            return Self::Local;
        }
        match view.same_zone(from, to) {
            Some(true) => Self::SameZone,
            Some(false) => Self::CrossZone,
            None => Self::UnknownZone,
        }
    }

    /// Stable name, used as a metric label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::SameZone => "same_zone",
            Self::CrossZone => "cross_zone",
            Self::UnknownZone => "unknown_zone",
        }
    }
}

/// Bytes a repair moves, as its bandwidth limits count them (EC2-085, point 2).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepairTraffic {
    /// Every byte the repair reads or writes: what the node-wide limit counts.
    pub total_bytes: u64,
    /// The bytes that cross the boundary of the owner's zone, on either leg:
    /// what that zone's limit counts.
    pub zone_bytes: u64,
}

impl RepairTraffic {
    /// Count `bytes` moving from `from` to `to` for a repair whose owner is in
    /// `zone`.
    ///
    /// A transfer crosses the zone when exactly one of its ends is inside it.
    /// An end without a zone label counts as outside: the limit is there to
    /// hold traffic down, and an unknown place is not proof that it stays in.
    pub fn add(
        &mut self,
        view: &PoolTopologyView,
        zone: Option<&str>,
        from: NodeId,
        to: NodeId,
        bytes: u64,
    ) {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        let crosses = zone.is_some_and(|zone| {
            let inside = |node: NodeId| view.zone_of(node).is_some_and(|key| key.zone() == zone);
            from != to && inside(from) != inside(to)
        });
        if crosses {
            self.zone_bytes = self.zone_bytes.saturating_add(bytes);
        }
    }

    /// `self` and `other` together.
    pub fn plus(self, other: Self) -> Self {
        Self {
            total_bytes: self.total_bytes.saturating_add(other.total_bytes),
            zone_bytes: self.zone_bytes.saturating_add(other.zone_bytes),
        }
    }

    /// What `self` holds beyond `reserved`, field by field.
    pub fn beyond(self, reserved: Self) -> Self {
        Self {
            total_bytes: self.total_bytes.saturating_sub(reserved.total_bytes),
            zone_bytes: self.zone_bytes.saturating_sub(reserved.zone_bytes),
        }
    }
}

/// Bytes one repair moved, by leg and by scope: what it adds to
/// `s4_ec_repair_bytes_total`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RepairBytes {
    bytes: [[u64; TransferScope::ALL.len()]; RepairLeg::ALL.len()],
}

impl RepairBytes {
    /// Count `bytes` moved on `leg` within `scope`.
    pub fn add(&mut self, leg: RepairLeg, scope: TransferScope, bytes: u64) {
        let cell = &mut self.bytes[leg as usize][scope as usize];
        *cell = cell.saturating_add(bytes);
    }

    /// Bytes moved on `leg` within `scope`.
    pub fn get(&self, leg: RepairLeg, scope: TransferScope) -> u64 {
        self.bytes[leg as usize][scope as usize]
    }

    /// Bytes that crossed a zone boundary, on both legs.
    pub fn cross_zone(&self) -> u64 {
        RepairLeg::ALL.iter().map(|leg| self.get(*leg, TransferScope::CrossZone)).sum()
    }

    /// Bytes moved on `leg`, whatever the scope.
    pub fn on_leg(&self, leg: RepairLeg) -> u64 {
        self.bytes[leg as usize].iter().sum()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::erasure_coding::TopologyLabels;

    fn standard_profiles() -> [EcProfileSnapshot; 6] {
        [
            EcProfileSnapshot::rs_small(),
            EcProfileSnapshot::rs_standard(),
            EcProfileSnapshot::rs_dense(),
            EcProfileSnapshot::lrc_small(),
            EcProfileSnapshot::lrc_standard(),
            EcProfileSnapshot::lrc_dense(),
        ]
    }

    fn lrc_profiles() -> [(EcProfileSnapshot, usize); 3] {
        [
            (EcProfileSnapshot::lrc_small(), 2),
            (EcProfileSnapshot::lrc_standard(), 3),
            (EcProfileSnapshot::lrc_dense(), 5),
        ]
    }

    /// The path a repair takes, as the repair worker asks for it.
    ///
    /// Three answers, and each one means something different to whoever reads
    /// the log: the group did it on its own; `k` shards were read; or nothing
    /// before plan 06 would have rebuilt this shard at all. The last is the
    /// pattern EC2-103 documents — one more data shard of group 0 than global
    /// parity carries, the group's local parity alive — and it is the only one
    /// the solver counter moves on.
    #[test]
    fn the_planner_names_the_path_a_repair_takes() {
        for (profile, per_group) in lrc_profiles() {
            let planner = RepairPlanner::new(&profile).unwrap();
            let total = planner.total_shards();
            let EcParity::Lrc { global, .. } = profile.parity else {
                unreachable!("an LRC profile");
            };
            let lost_of = |slots: &[u8]| {
                (0..total)
                    .map(|slot| slots.contains(&u8::try_from(slot).unwrap()))
                    .collect::<Vec<_>>()
            };

            // Slot 0 alone: the rest of its group rebuilds it.
            assert_eq!(
                planner.repair_path(0, &lost_of(&[0])),
                RecoveryPath::LocalXor,
                "{}",
                profile.name
            );

            // Slot 0 with its group's local parity gone: the group cannot, so
            // the slot is rebuilt from `k` shards — which EC v1 did too.
            assert_eq!(
                planner.repair_path(0, &lost_of(&[0, profile.k])),
                RecoveryPath::GlobalRs,
                "{}",
                profile.name
            );

            // More data shards of the group than global parity carries, its
            // local parity alive: only the unified solver reaches this.
            let beyond_budget = (0..=global).collect::<Vec<_>>();
            assert!(
                usize::from(global) < per_group,
                "{} cannot lose {} data shards in one group",
                profile.name,
                usize::from(global) + 1
            );
            assert_eq!(
                planner.repair_path(0, &lost_of(&beyond_budget)),
                RecoveryPath::UnifiedSolver,
                "{}",
                profile.name
            );

            // A whole local group, local parity included: nothing reaches it,
            // and the planner says so rather than sending a repair that would
            // fail at the codec.
            let whole_group =
                (0..u8::try_from(per_group).unwrap()).chain([profile.k]).collect::<Vec<_>>();
            assert_eq!(
                planner.repair_path(0, &lost_of(&whole_group)),
                RecoveryPath::None,
                "{}",
                profile.name
            );
        }
    }

    fn states_with(total: usize, lost: &[u8], fetched: &[u8]) -> Vec<SlotState> {
        (0..total)
            .map(|slot| {
                let slot = u8::try_from(slot).unwrap();
                if lost.contains(&slot) {
                    SlotState::Lost
                } else if fetched.contains(&slot) {
                    SlotState::Fetched
                } else {
                    SlotState::Unread
                }
            })
            .collect()
    }

    fn everything_remote(_: u8) -> NodeDistance {
        NodeDistance::Remote
    }

    /// One loss in a group reads the rest of the group — `k / local` shards:
    /// 2, 3 and 5 — for every data and local parity slot of every LRC profile,
    /// whatever the distances.
    #[test]
    fn a_single_loss_in_a_group_reads_only_the_rest_of_the_group() {
        for (profile, per_group) in lrc_profiles() {
            let planner = RepairPlanner::new(&profile).unwrap();
            let tolerance = LossTolerance::new(&profile).unwrap();
            let states = states_with(planner.total_shards(), &[], &[]);
            let local_slots = profile.k + 2;
            for slot in 0..local_slots {
                let plan = planner.plan(slot, &states, everything_remote).unwrap();
                let group = if slot < profile.k {
                    slot / u8::try_from(per_group).unwrap()
                } else {
                    slot - profile.k
                };
                assert_eq!(
                    plan.strategy,
                    RepairStrategy::LocalGroup { group },
                    "{} slot {slot}",
                    profile.name
                );
                assert_eq!(
                    plan.sources.len(),
                    per_group,
                    "{} slot {slot}",
                    profile.name
                );
                let mut sources = plan.sources.clone();
                sources.sort_unstable();
                assert_eq!(
                    Some(sources),
                    tolerance.local_repair_sources(slot).map(|mut expected| {
                        expected.sort_unstable();
                        expected
                    }),
                    "{} slot {slot}",
                    profile.name
                );
            }
        }
    }

    /// A global parity slot, and every Reed-Solomon slot, is decoded from
    /// exactly `k` shards.
    #[test]
    fn a_decode_reads_exactly_k_shards() {
        for profile in standard_profiles() {
            let planner = RepairPlanner::new(&profile).unwrap();
            let states = states_with(planner.total_shards(), &[], &[]);
            let first_decoded = match profile.parity {
                EcParity::Lrc { local, .. } => profile.k + local,
                EcParity::ReedSolomon { .. } => 0,
            };
            for slot in first_decoded..profile.total_shards() {
                let plan = planner.plan(slot, &states, everything_remote).unwrap();
                assert_eq!(
                    plan.strategy,
                    RepairStrategy::Global,
                    "{} slot {slot}",
                    profile.name
                );
                assert_eq!(
                    plan.sources.len(),
                    usize::from(profile.k),
                    "{} slot {slot}",
                    profile.name
                );
            }
        }
    }

    /// A second loss in the group sends the repair to the global path at
    /// once, and the plan leaves out both lost shards.
    #[test]
    fn a_second_loss_in_the_group_goes_global_without_the_lost_shards() {
        let profile = EcProfileSnapshot::lrc_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        let states = states_with(planner.total_shards(), &[1], &[]);

        let plan = planner.plan(0, &states, everything_remote).unwrap();

        assert_eq!(plan.strategy, RepairStrategy::Global);
        assert_eq!(plan.sources.len(), 6);
        assert!(!plan.sources.contains(&0) && !plan.sources.contains(&1));
    }

    /// Sources are the nearest ones: with the shards of zone "near" enough to
    /// decode, a Reed-Solomon repair reads nothing from the far zone, and
    /// lists local, rack and zone neighbours in that order.
    #[test]
    fn a_decode_takes_the_nearest_shards_first() {
        let profile = EcProfileSnapshot::rs_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        let states = states_with(planner.total_shards(), &[], &[]);
        let distance = |slot: u8| match slot {
            7 => NodeDistance::Local,
            5 | 6 => NodeDistance::SameRack,
            1..=3 => NodeDistance::SameZone,
            _ => NodeDistance::Remote,
        };

        let plan = planner.plan(0, &states, distance).unwrap();

        assert_eq!(plan.sources, vec![7, 5, 6, 1, 2, 3]);
    }

    /// A shard already fetched costs nothing: a plan made again after a source
    /// was lost reuses what was read, even when an unread shard is nearer.
    #[test]
    fn shards_already_fetched_are_used_before_nearer_unread_ones() {
        let profile = EcProfileSnapshot::rs_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        let states = states_with(planner.total_shards(), &[3], &[7, 8]);
        let distance = |slot: u8| match slot {
            7 | 8 => NodeDistance::Remote,
            _ => NodeDistance::SameRack,
        };

        let plan = planner.plan(0, &states, distance).unwrap();

        assert_eq!(plan.sources, vec![7, 8, 1, 2, 4, 5]);
    }

    /// ...but only what the decode can use. Slot 9 is being rebuilt and the
    /// whole of local group 0 has arrived — data slots 0, 1, 2 and the group's
    /// local parity, slot 6. The local parity is the sum of the three data
    /// shards beside it, so it repeats an equation the decode already holds:
    /// it is dropped rather than counted among the `k`, and a fourth shard is
    /// read instead.
    #[test]
    fn a_fetched_shard_the_decode_cannot_use_is_left_out() {
        let profile = EcProfileSnapshot::lrc_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        let states = states_with(planner.total_shards(), &[], &[0, 1, 2, 6]);

        let plan = planner.plan(9, &states, everything_remote).unwrap();

        assert_eq!(plan.strategy, RepairStrategy::Global);
        assert_eq!(plan.sources[..3], [0, 1, 2]);
        assert!(!plan.sources.contains(&6), "{:?}", plan.sources);
        assert_eq!(plan.sources.len(), 6);
    }

    /// ...and a fetched local parity shard the decode *can* use is kept. Same
    /// group, but slot 0 is being rebuilt and slot 2 turned out to be missing:
    /// with two holes in the group EC v1 had no use for its local parity and
    /// read a shard more. The unified decode takes it as an equation like any
    /// other (EC2-101).
    #[test]
    fn a_fetched_local_parity_shard_the_unified_decode_can_use_is_kept() {
        let profile = EcProfileSnapshot::lrc_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        let states = states_with(planner.total_shards(), &[2], &[1, 6]);

        let plan = planner.plan(0, &states, everything_remote).unwrap();

        assert_eq!(plan.strategy, RepairStrategy::Global);
        assert_eq!(plan.sources[..2], [1, 6]);
        assert_eq!(plan.sources.len(), 6);
    }

    /// A whole local group lost is not rebuilt by any strategy, and the
    /// explanation names the group.
    #[test]
    fn a_whole_group_lost_has_no_plan_and_says_so() {
        let profile = EcProfileSnapshot::lrc_standard();
        let planner = RepairPlanner::new(&profile).unwrap();
        // Group 0: data slots 0-2 and local parity 6.
        let states = states_with(planner.total_shards(), &[1, 2, 6], &[]);

        assert_eq!(planner.plan(0, &states, everything_remote), None);
        let explanation = planner.explain_unrecoverable(0, &states);
        assert!(explanation.contains("4 of 10 shard(s)"), "{explanation}");
        assert!(explanation.contains("local group(s) [0]"), "{explanation}");
    }

    /// Every loss pattern of every standard profile, every lost slot, and
    /// three sets of distances: a plan exists exactly when the codec can
    /// repair, the codec repairs from exactly the planned shards, none of them
    /// is lost, and the count is `k / local` or `k`. For LRC and Reed-Solomon
    /// the sources are also as near as any repairable set of that many
    /// shards — checked against every such set.
    #[test]
    fn the_plan_matches_the_codec_and_an_exhaustive_search_on_every_loss_pattern() {
        let distance_sets: [fn(u8) -> NodeDistance; 3] = [
            |_| NodeDistance::Remote,
            |slot| match slot % 4 {
                0 => NodeDistance::Local,
                1 => NodeDistance::SameRack,
                2 => NodeDistance::SameZone,
                _ => NodeDistance::Remote,
            },
            |slot| {
                if slot >= 5 {
                    NodeDistance::SameZone
                } else {
                    NodeDistance::Remote
                }
            },
        ];
        for profile in standard_profiles() {
            let planner = RepairPlanner::new(&profile).unwrap();
            let tolerance = LossTolerance::new(&profile).unwrap();
            let n = planner.total_shards();
            let k = usize::from(profile.k);
            for mask in 1u32..(1 << n) {
                let lost = (0..n).map(|slot| mask & (1 << slot) != 0).collect::<Vec<_>>();
                let lost_list = (0..n)
                    .filter(|slot| lost[*slot])
                    .map(|slot| u8::try_from(slot).unwrap())
                    .collect::<Vec<_>>();
                let states = states_with(n, &lost_list, &[]);
                for slot in lost_list.iter().copied() {
                    for distance in distance_sets {
                        let plan = planner.plan(slot, &states, distance);
                        assert_eq!(
                            plan.is_some(),
                            tolerance.can_repair(slot, &lost),
                            "{} mask {mask:#b} slot {slot}",
                            profile.name
                        );
                        let Some(plan) = plan else {
                            continue;
                        };
                        assert!(
                            plan.sources.iter().all(|source| !lost[usize::from(*source)]),
                            "{} mask {mask:#b} slot {slot}",
                            profile.name
                        );
                        assert!(planner.repairs_from(slot, &plan.sources));
                        let expected_len = match plan.strategy {
                            RepairStrategy::LocalGroup { .. } => {
                                tolerance.local_repair_sources(slot).unwrap_or_default().len()
                            }
                            RepairStrategy::Global => k,
                        };
                        assert_eq!(plan.sources.len(), expected_len, "{}", profile.name);

                        if plan.strategy == RepairStrategy::Global {
                            let best = nearest_repairable_set(&planner, slot, &lost, k, distance);
                            assert_eq!(
                                farthest_first(&plan.sources, distance),
                                best,
                                "{} mask {mask:#b} slot {slot}: {:?}",
                                profile.name,
                                plan.sources
                            );
                        }
                    }
                }
            }
        }
    }

    /// Distances of `sources`, farthest first: two plans of the same size
    /// compare as "which one reaches less far".
    fn farthest_first(sources: &[u8], distance: fn(u8) -> NodeDistance) -> Vec<NodeDistance> {
        let mut distances = sources.iter().map(|source| distance(*source)).collect::<Vec<_>>();
        distances.sort_unstable_by(|left, right| right.cmp(left));
        distances
    }

    /// The best `size`-shard set that repairs `slot`, by an exhaustive search.
    fn nearest_repairable_set(
        planner: &RepairPlanner,
        slot: u8,
        lost: &[bool],
        size: usize,
        distance: fn(u8) -> NodeDistance,
    ) -> Vec<NodeDistance> {
        let usable = (0..lost.len())
            .filter(|index| !lost[*index])
            .map(|index| u8::try_from(index).unwrap())
            .collect::<Vec<_>>();
        let mut best: Option<Vec<NodeDistance>> = None;
        let mut chosen = Vec::with_capacity(size);
        search(&usable, 0, size, &mut chosen, &mut |set| {
            if planner.repairs_from(slot, set) {
                let candidate = farthest_first(set, distance);
                if best.as_ref().is_none_or(|current| candidate < *current) {
                    best = Some(candidate);
                }
            }
        });
        best.unwrap()
    }

    fn search(
        usable: &[u8],
        from: usize,
        size: usize,
        chosen: &mut Vec<u8>,
        visit: &mut dyn FnMut(&[u8]),
    ) {
        if chosen.len() == size {
            visit(chosen);
            return;
        }
        for index in from..usable.len() {
            chosen.push(usable[index]);
            search(usable, index + 1, size, chosen, visit);
            chosen.pop();
        }
    }

    fn labels(zone: &str, rack: &str) -> TopologyLabels {
        TopologyLabels {
            zone: Some(zone.to_string()),
            rack: Some(rack.to_string()),
            host: Some(format!("{zone}-{rack}")),
            disk_group: Some("d0".to_string()),
        }
    }

    /// Four nodes: 1 and 2 in zone "a", 3 in zone "b", 4 without labels.
    fn two_zone_view(local: u128) -> PoolTopologyView {
        let nodes = (1..=4).map(NodeId).collect::<Vec<_>>();
        let labels_by_node = HashMap::from([
            (NodeId(1), labels("a", "r1")),
            (NodeId(2), labels("a", "r2")),
            (NodeId(3), labels("b", "r1")),
        ]);
        PoolTopologyView::build(NodeId(local), &nodes, &labels_by_node)
    }

    #[test]
    fn a_transfer_is_scoped_by_the_zones_of_its_ends() {
        let view = two_zone_view(1);
        assert_eq!(
            TransferScope::between(&view, NodeId(1), NodeId(1)),
            TransferScope::Local
        );
        assert_eq!(
            TransferScope::between(&view, NodeId(2), NodeId(1)),
            TransferScope::SameZone
        );
        assert_eq!(
            TransferScope::between(&view, NodeId(3), NodeId(1)),
            TransferScope::CrossZone
        );
        assert_eq!(
            TransferScope::between(&view, NodeId(4), NodeId(1)),
            TransferScope::UnknownZone
        );
    }

    /// The zone limit counts only what crosses the owner's zone, on either
    /// leg; the node-wide limit counts everything. A node without labels is
    /// taken as outside the zone.
    #[test]
    fn only_bytes_crossing_the_owners_zone_count_against_it() {
        // The repair runs on node 1, zone "a"; the owner is in zone "a" too.
        let view = two_zone_view(1);
        let mut traffic = RepairTraffic::default();
        traffic.add(&view, Some("a"), NodeId(2), NodeId(1), 10); // source, same zone
        traffic.add(&view, Some("a"), NodeId(1), NodeId(1), 10); // source, local
        traffic.add(&view, Some("a"), NodeId(3), NodeId(1), 10); // source, from zone "b"
        traffic.add(&view, Some("a"), NodeId(4), NodeId(1), 10); // source, unlabeled
        traffic.add(&view, Some("a"), NodeId(1), NodeId(2), 10); // write, same zone
        assert_eq!(
            traffic,
            RepairTraffic {
                total_bytes: 50,
                zone_bytes: 20,
            }
        );

        // The owner in zone "b": a transfer between two nodes of zone "a"
        // does not touch it, the write into "b" does.
        let mut into_b = RepairTraffic::default();
        into_b.add(&view, Some("b"), NodeId(2), NodeId(1), 10);
        into_b.add(&view, Some("b"), NodeId(1), NodeId(3), 10);
        assert_eq!(into_b.zone_bytes, 10);

        // Without an owner zone there is no zone limit to count for.
        let mut unzoned = RepairTraffic::default();
        unzoned.add(&view, None, NodeId(3), NodeId(1), 10);
        assert_eq!(
            unzoned,
            RepairTraffic {
                total_bytes: 10,
                zone_bytes: 0,
            }
        );

        assert_eq!(
            traffic.beyond(RepairTraffic {
                total_bytes: 30,
                zone_bytes: 25,
            }),
            RepairTraffic {
                total_bytes: 20,
                zone_bytes: 0,
            }
        );
    }
}
