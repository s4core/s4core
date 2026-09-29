// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Slot placement of LRC local groups across failure domains.
//!
//! The algorithm of decision EC2-082, for sets laid out by the LRC group rule
//! of EC2-080. A set's nodes are already chosen; this only decides which node
//! takes which slot.
//!
//! **Guarantee.** Every member of a local group — its data shards and its local
//! parity — sits in a different failure domain of the configured level. Losing
//! one domain then takes at most one shard from each group, and each such loss
//! is repaired by XOR inside the group.
//! [`crate::ec_lrc_placement::GroupRuleFeasibility`] says exactly
//! when this is possible: `E = Σ max(0, c_d − l) ≤ g`, where `c_d` is the
//! number of the set's nodes in domain `d`, `l` the number of local groups and
//! `g` the number of global parity slots.
//!
//! **Goals above the guarantee**, compared in this order
//! ([`crate::ec_lrc_placement::LayoutScore`]):
//!
//! 1. more zones whose complete loss the decoder survives;
//! 2. more racks whose complete loss the decoder survives;
//! 3. global parity spread over domains;
//! 4. members of a group gathered in one zone, so a local repair stays there.
//!
//! "Survives" is always asked of the decoder itself, through
//! [`s4_codec::LossTolerance`], never estimated from labels.
//!
//! **Search.** An initial layout built as in the proof of the criterion, then
//! steepest ascent over single swaps of two slots and, when those stall, over
//! two swaps at once. Only a strict improvement is accepted, so the search
//! ends. Everything is sorted explicitly and ties go to the first candidate
//! found, so the result depends on the set of nodes and their labels, never on
//! the order the nodes were passed in.
//!
//! When the criterion does not hold — possible only under
//! `S4_EC_TOPOLOGY_POLICY=warn` (EC2-083) — the same search runs, and the
//! guarantee becomes its first goal: as few group members as possible share a
//! domain with another member of their group.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, HashMap};

use s4_cluster::NodeId;
use s4_codec::LossTolerance;
use thiserror::Error;

use crate::erasure_coding::{
    EcFailureDomainLevel, EcLayoutViolationReason, EcParity, EcProfile, FailureDomainKey,
    TopologyLabels,
};

/// Largest set for which two swaps at once are tried. Every standard LRC
/// profile is within it (7, 10 and 14 slots); without them the search on
/// `lrc_dense` sometimes stops on a layout that survives fewer zones than the
/// EC v1 one (EC2-082, "Comparison with the EC v1 layout").
const PAIR_SWAPS_UP_TO: usize = 16;

/// Accepted moves allowed per slot before the search stops anyway.
///
/// The search ends without it — every accepted move strictly improves a score
/// that takes finitely many values — but on a custom profile with hundreds of
/// slots "finitely many" is not a time an operator can wait for. On the
/// standard profiles the bound is far away: the prototype never needed more
/// than three accepted moves.
const MOVES_PER_SLOT: usize = 4;

/// Why a set cannot be placed by the group rule at all.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LrcPlacementError {
    /// The profile has no local groups to spread.
    #[error("profile '{profile}' is not an LRC profile; only LRC local groups are placed by failure domain")]
    NotLrc {
        /// Profile name.
        profile: String,
    },
    /// The number of nodes does not match the profile's width.
    #[error("profile '{profile}' is {expected} node(s) wide, got {actual}")]
    WrongWidth {
        /// Profile name.
        profile: String,
        /// Slots the profile has.
        expected: usize,
        /// Nodes given.
        actual: usize,
    },
    /// The same node was given twice.
    #[error("node {node_id} is given twice; every slot needs its own node")]
    DuplicateNode {
        /// The repeated node.
        node_id: NodeId,
    },
    /// Some nodes lack labels the rule needs.
    #[error("{} node(s) lack complete topology labels", .nodes.len())]
    IncompleteLabels {
        /// The nodes, and which labels each lacks.
        nodes: Vec<UnlabeledNode>,
    },
    /// The codec refused the profile.
    #[error("profile '{profile}' is not valid: {reason}")]
    InvalidProfile {
        /// Profile name.
        profile: String,
        /// Codec's reason.
        reason: String,
    },
}

/// A node without the complete labels the group rule needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlabeledNode {
    /// The node.
    pub node_id: NodeId,
    /// The labels it lacks: some of `zone`, `rack`, `host`, `disk_group`.
    pub missing: Vec<&'static str>,
}

/// The nodes among `nodes` whose labels are not complete, sorted by id.
///
/// The group rule needs all four labels EC v1 requires for a complete label
/// set (EC2-081, point 5), whatever the level: the rule is only applied to a
/// set whose topology is fully described.
pub fn nodes_without_complete_labels(
    nodes: &[NodeId],
    labels: &HashMap<NodeId, TopologyLabels>,
) -> Vec<UnlabeledNode> {
    let empty = TopologyLabels::default();
    let mut unlabeled = nodes
        .iter()
        .filter_map(|node_id| {
            let label = labels.get(node_id).unwrap_or(&empty);
            let missing = [
                ("zone", label.zone.is_none()),
                ("rack", label.rack.is_none()),
                ("host", label.host.is_none()),
                ("disk_group", label.disk_group.is_none()),
            ]
            .into_iter()
            .filter_map(|(name, absent)| absent.then_some(name))
            .collect::<Vec<_>>();
            (!missing.is_empty()).then_some(UnlabeledNode {
                node_id: *node_id,
                missing,
            })
        })
        .collect::<Vec<_>>();
    unlabeled.sort_unstable_by_key(|node| node.node_id.0);
    unlabeled
}

/// What the satisfiability criterion of EC2-082 says about one set's nodes.
///
/// The guarantee holds for some layout exactly when the excess
/// `E = Σ max(0, c_d − l)` fits the `g` global parity slots: a domain gives at
/// most one node to each of the `l` groups, and its remaining nodes can only
/// take global parity slots. It follows that the set needs at least `s`
/// domains, `s` being the size of a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRuleFeasibility {
    level: EcFailureDomainLevel,
    group_size: usize,
    local_groups: usize,
    global_parity: usize,
    nodes: usize,
    /// Nodes of the set in each domain, domains sorted.
    domains: Vec<(FailureDomainKey, usize)>,
}

impl GroupRuleFeasibility {
    /// Level the members of a group are spread over.
    pub fn level(&self) -> EcFailureDomainLevel {
        self.level
    }

    /// Members of one local group: its data shards plus its local parity.
    pub fn group_size(&self) -> usize {
        self.group_size
    }

    /// Distinct domains the set's nodes are in.
    pub fn domain_count(&self) -> usize {
        self.domains.len()
    }

    /// Nodes of the set in each domain, domains sorted.
    pub fn domains(&self) -> &[(FailureDomainKey, usize)] {
        &self.domains
    }

    /// Nodes that do not fit into the local groups at one per group per
    /// domain: `E` of the criterion.
    pub fn excess(&self) -> usize {
        self.domains
            .iter()
            .map(|(_, count)| count.saturating_sub(self.local_groups))
            .sum()
    }

    /// The fewest group members that must share a domain with another member
    /// of their group in any layout: `max(0, E − g)`. Zero exactly when the
    /// guarantee can be kept.
    pub fn unavoidable_coincidences(&self) -> usize {
        self.excess().saturating_sub(self.global_parity)
    }

    /// Why the guarantee cannot be kept, or `None` when it can.
    ///
    /// Too few domains is named before overloaded domains: with fewer than
    /// `s` domains the excess is always too large as well, but the number of
    /// domains is what the operator has to change.
    pub fn violation(&self) -> Option<EcLayoutViolationReason> {
        if self.domain_count() < self.group_size {
            Some(EcLayoutViolationReason::TooFewDomains)
        } else if self.excess() > self.global_parity {
            Some(EcLayoutViolationReason::DomainOverloaded)
        } else {
            None
        }
    }

    /// The operator-facing explanation of [`Self::violation`], naming what is
    /// needed and what the set has (EC2-083), or `None` when the guarantee can
    /// be kept.
    pub fn explain(&self, profile_name: &str) -> Option<String> {
        let level = self.level;
        match self.violation()? {
            EcLayoutViolationReason::TooFewDomains => Some(format!(
                "profile '{profile_name}' spreads every local group over {} distinct {level}s, but \
                 the {} nodes of this set are in {} {level}(s) ({}): build the set from nodes in \
                 at least {} {level}s",
                self.group_size,
                self.nodes,
                self.domain_count(),
                format_domain_counts(self.domains.iter()),
                self.group_size,
            )),
            EcLayoutViolationReason::DomainOverloaded => {
                let overloaded =
                    self.domains.iter().filter(|(_, count)| *count > self.local_groups);
                Some(format!(
                    "profile '{profile_name}' can place at most {} nodes of one {level} in local \
                     groups (one per group) and has {} global parity slot(s) for the rest; \
                     {level}s {} leave {} node(s) for those {} slot(s): move {} node(s) from an \
                     overloaded {level} to a {level} holding fewer than {} of this set's nodes",
                    self.local_groups,
                    self.global_parity,
                    format_domain_counts(overloaded),
                    self.excess(),
                    self.global_parity,
                    self.unavoidable_coincidences(),
                    self.local_groups,
                ))
            }
            // Labels are checked before a feasibility exists at all.
            EcLayoutViolationReason::IncompleteLabels => None,
        }
    }
}

fn format_domain_counts<'a>(
    domains: impl Iterator<Item = &'a (FailureDomainKey, usize)>,
) -> String {
    domains
        .map(|(domain, count)| format!("{domain}={count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// How good a layout is, compared goal by goal; greater is better.
///
/// The fields are raw counts. The order in which they are compared, and in
/// which direction, is what [`Ord`] implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LayoutScore {
    /// Group members that share a domain of the group level with another
    /// member of their group, summed over groups. Zero when the guarantee
    /// holds. Fewer is better.
    pub coincidences: usize,
    /// Zones of the set whose complete loss the decoder survives.
    pub zones_survived: usize,
    /// Racks of the set whose complete loss the decoder survives. Always every
    /// rack when the guarantee holds at level `rack` or `zone`.
    pub racks_survived: usize,
    /// The largest number of global parity slots in one domain of the group
    /// level. Fewer is better.
    pub max_globals_in_one_domain: usize,
    /// Sum over groups of the squared number of members in each zone. Grows
    /// with every member that joins the zone most of its group is in, which a
    /// count of "groups inside one zone" would not: that count stalls on
    /// layouts no single swap can improve (EC2-082, step 3).
    pub locality: usize,
}

impl LayoutScore {
    fn key(&self) -> (Reverse<usize>, usize, usize, Reverse<usize>, usize) {
        (
            Reverse(self.coincidences),
            self.zones_survived,
            self.racks_survived,
            Reverse(self.max_globals_in_one_domain),
            self.locality,
        )
    }
}

impl Ord for LayoutScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for LayoutScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A layout chosen by [`LrcGroupPlanner::plan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LrcGroupLayout {
    /// Nodes in slot order: the node of slot `i` at index `i`.
    pub slot_order: Vec<NodeId>,
    /// How good the layout is.
    pub score: LayoutScore,
    /// Moves the search accepted after the initial layout.
    pub moves: usize,
}

/// A domain holding more than one member of a local group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedDomain {
    /// The local group.
    pub group: usize,
    /// The domain.
    pub domain: FailureDomainKey,
    /// Members of the group in it.
    pub members: usize,
}

/// A zone whose complete loss leaves a chunk undecodable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FatalZone {
    /// The zone.
    pub zone: FailureDomainKey,
    /// Slots of the set in it.
    pub slots: usize,
}

/// How a given slot order stands against the group rule and the zone goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRuleAssessment {
    /// Where the guarantee is broken: every domain holding two or more
    /// members of one group, by group and then domain.
    pub shared_domains: Vec<SharedDomain>,
    /// Zones whose loss the decoder does not survive, sorted.
    pub fatal_zones: Vec<FatalZone>,
    /// The score of the slot order.
    pub score: LayoutScore,
}

/// Places the nodes of one LRC set by the group rule at one level.
///
/// Built once per set from its nodes and their labels; answers the
/// feasibility question cheaply, and runs the search only when asked, so a
/// set refused under `strict` costs nothing but the criterion.
#[derive(Debug, Clone)]
pub struct LrcGroupPlanner {
    level: EcFailureDomainLevel,
    tolerance: LossTolerance,
    local_groups: usize,
    global_parity: usize,
    group_size: usize,
    /// Local group of each slot; `None` for a global parity slot.
    slot_group: Vec<Option<usize>>,
    /// The set's nodes, sorted by zone, rack, host and then id. Everything
    /// below refers to a node by its index here.
    nodes: Vec<NodeId>,
    zone_of: Vec<usize>,
    rack_of: Vec<usize>,
    domain_of: Vec<usize>,
    /// Nodes of one class are interchangeable for every goal: same rack and
    /// same domain of the group level.
    class_of: Vec<usize>,
    zones: Vec<FailureDomainKey>,
    racks: usize,
    domains: Vec<FailureDomainKey>,
}

impl LrcGroupPlanner {
    /// Prepare to place `nodes` for `profile` with group members spread over
    /// domains of `level`.
    ///
    /// # Errors
    ///
    /// Refuses a profile that is not LRC, a node list that does not match the
    /// profile's width or repeats a node, and nodes without complete labels.
    pub fn new(
        profile: &EcProfile,
        nodes: &[NodeId],
        labels: &HashMap<NodeId, TopologyLabels>,
        level: EcFailureDomainLevel,
    ) -> Result<Self, LrcPlacementError> {
        let EcParity::Lrc { local, global } = profile.parity else {
            return Err(LrcPlacementError::NotLrc {
                profile: profile.name.clone(),
            });
        };
        let tolerance = LossTolerance::new(&profile.codec_snapshot()).map_err(|error| {
            LrcPlacementError::InvalidProfile {
                profile: profile.name.clone(),
                reason: error.to_string(),
            }
        })?;
        let width = usize::from(profile.total_shards());
        if nodes.len() != width {
            return Err(LrcPlacementError::WrongWidth {
                profile: profile.name.clone(),
                expected: width,
                actual: nodes.len(),
            });
        }
        let unlabeled = nodes_without_complete_labels(nodes, labels);
        if !unlabeled.is_empty() {
            return Err(LrcPlacementError::IncompleteLabels { nodes: unlabeled });
        }

        // Where every node sits. Every key exists: the labels were just found
        // complete.
        let mut sites = Vec::with_capacity(width);
        for node_id in nodes {
            let domain_at = |level: EcFailureDomainLevel| {
                labels.get(node_id).and_then(|label| level.domain_of(label)).ok_or_else(|| {
                    LrcPlacementError::IncompleteLabels {
                        nodes: nodes_without_complete_labels(&[*node_id], labels),
                    }
                })
            };
            sites.push(Site {
                node_id: *node_id,
                zone: domain_at(EcFailureDomainLevel::Zone)?,
                rack: domain_at(EcFailureDomainLevel::Rack)?,
                host: domain_at(EcFailureDomainLevel::Host)?,
                domain: domain_at(level)?,
            });
        }
        // Zone, rack, host and then id: the host key names all three labels.
        sites.sort_by(|a, b| (&a.host, a.node_id.0).cmp(&(&b.host, b.node_id.0)));
        for pair in sites.windows(2) {
            if pair[0].node_id == pair[1].node_id {
                return Err(LrcPlacementError::DuplicateNode {
                    node_id: pair[0].node_id,
                });
            }
        }

        let zones = sorted_distinct(sites.iter().map(|site| &site.zone));
        let racks = sorted_distinct(sites.iter().map(|site| &site.rack));
        let domains = sorted_distinct(sites.iter().map(|site| &site.domain));
        let position = |keys: &[FailureDomainKey], key: &FailureDomainKey| {
            keys.binary_search(key).unwrap_or_default()
        };
        let zone_of = sites.iter().map(|site| position(&zones, &site.zone)).collect::<Vec<_>>();
        let rack_of = sites.iter().map(|site| position(&racks, &site.rack)).collect::<Vec<_>>();
        let domain_of =
            sites.iter().map(|site| position(&domains, &site.domain)).collect::<Vec<_>>();
        let mut classes = BTreeMap::new();
        let class_of = rack_of
            .iter()
            .zip(&domain_of)
            .map(|pair| {
                let next = classes.len();
                *classes.entry(pair).or_insert(next)
            })
            .collect();

        let (k, local, global) = (
            usize::from(profile.k),
            usize::from(local),
            usize::from(global),
        );
        let data_per_group = k / local;
        let slot_group = (0..width)
            .map(|slot| {
                if slot < k {
                    Some(slot / data_per_group)
                } else if slot < k + local {
                    Some(slot - k)
                } else {
                    None
                }
            })
            .collect();

        Ok(Self {
            level,
            tolerance,
            local_groups: local,
            global_parity: global,
            group_size: data_per_group + 1,
            slot_group,
            nodes: sites.iter().map(|site| site.node_id).collect(),
            zone_of,
            rack_of,
            domain_of,
            class_of,
            zones,
            racks: racks.len(),
            domains,
        })
    }

    /// What the criterion says about these nodes.
    pub fn feasibility(&self) -> GroupRuleFeasibility {
        let mut counts = vec![0usize; self.domains.len()];
        for domain in &self.domain_of {
            counts[*domain] += 1;
        }
        GroupRuleFeasibility {
            level: self.level,
            group_size: self.group_size,
            local_groups: self.local_groups,
            global_parity: self.global_parity,
            nodes: self.nodes.len(),
            domains: self.domains.iter().cloned().zip(counts).collect(),
        }
    }

    /// Choose the slot order: the initial layout, then the search.
    ///
    /// When the criterion holds, the result keeps the guarantee. When it does
    /// not, the result has the fewest coincidences any layout can have,
    /// [`GroupRuleFeasibility::unavoidable_coincidences`].
    pub fn plan(&self) -> LrcGroupLayout {
        let start = self.initial_layout();
        let (layout, score, moves) = self.improve(start);
        LrcGroupLayout {
            slot_order: layout.iter().map(|node| self.nodes[*node]).collect(),
            score,
            moves,
        }
    }

    /// Judge a slot order chosen elsewhere — a recorded one.
    ///
    /// Returns `None` when `slot_order` is not an ordering of exactly this
    /// planner's nodes.
    pub fn assess(&self, slot_order: &[NodeId]) -> Option<GroupRuleAssessment> {
        let layout = self.layout_of(slot_order)?;
        let group_domains = self.group_counts(&layout, &self.domain_of, self.domains.len());
        let shared_domains = group_domains
            .iter()
            .enumerate()
            .filter(|(_, members)| **members > 1)
            .map(|(cell, members)| SharedDomain {
                group: cell / self.domains.len(),
                domain: self.domains[cell % self.domains.len()].clone(),
                members: *members,
            })
            .collect();
        let mut lost = vec![false; layout.len()];
        let fatal_zones = (0..self.zones.len())
            .filter(|zone| !self.survives(&layout, &self.zone_of, *zone, &mut lost))
            .map(|zone| FatalZone {
                zone: self.zones[zone].clone(),
                slots: layout.iter().filter(|node| self.zone_of[**node] == zone).count(),
            })
            .collect();
        Some(GroupRuleAssessment {
            shared_domains,
            fatal_zones,
            score: self.score(&layout),
        })
    }

    /// Score a slot order, or `None` when it is not an ordering of exactly
    /// this planner's nodes.
    pub fn score_of(&self, slot_order: &[NodeId]) -> Option<LayoutScore> {
        self.layout_of(slot_order).map(|layout| self.score(&layout))
    }

    /// Node indices in slot order, if `slot_order` names each node once.
    fn layout_of(&self, slot_order: &[NodeId]) -> Option<Vec<usize>> {
        if slot_order.len() != self.nodes.len() {
            return None;
        }
        let index = self
            .nodes
            .iter()
            .enumerate()
            .map(|(position, node_id)| (*node_id, position))
            .collect::<HashMap<_, _>>();
        let mut seen = vec![false; self.nodes.len()];
        let mut layout = Vec::with_capacity(slot_order.len());
        for node_id in slot_order {
            let position = *index.get(node_id)?;
            if std::mem::replace(&mut seen[position], true) {
                return None;
            }
            layout.push(position);
        }
        Some(layout)
    }

    /// Members of each group in each domain of one level, as a flat
    /// `group × domain` table.
    fn group_counts(&self, layout: &[usize], domain_of: &[usize], domains: usize) -> Vec<usize> {
        let mut counts = vec![0usize; self.local_groups * domains];
        for (slot, node) in layout.iter().enumerate() {
            if let Some(group) = self.slot_group[slot] {
                counts[group * domains + domain_of[*node]] += 1;
            }
        }
        counts
    }

    /// Whether the decoder survives losing every slot in `domain` of the
    /// level `domain_of` describes. `lost` is scratch space of the layout's
    /// length.
    fn survives(
        &self,
        layout: &[usize],
        domain_of: &[usize],
        domain: usize,
        lost: &mut [bool],
    ) -> bool {
        for (slot, node) in layout.iter().enumerate() {
            lost[slot] = domain_of[*node] == domain;
        }
        self.tolerance.can_decode(lost)
    }

    fn score(&self, layout: &[usize]) -> LayoutScore {
        let coincidences = self
            .group_counts(layout, &self.domain_of, self.domains.len())
            .iter()
            .map(|members| members.saturating_sub(1))
            .sum();
        let locality = self
            .group_counts(layout, &self.zone_of, self.zones.len())
            .iter()
            .map(|members| members * members)
            .sum();

        let mut globals = vec![0usize; self.domains.len()];
        for (slot, node) in layout.iter().enumerate() {
            if self.slot_group[slot].is_none() {
                globals[self.domain_of[*node]] += 1;
            }
        }

        let mut lost = vec![false; layout.len()];
        let zones_survived = (0..self.zones.len())
            .filter(|zone| self.survives(layout, &self.zone_of, *zone, &mut lost))
            .count();
        let racks_survived = (0..self.racks)
            .filter(|rack| self.survives(layout, &self.rack_of, *rack, &mut lost))
            .count();

        LayoutScore {
            coincidences,
            zones_survived,
            racks_survived,
            max_globals_in_one_domain: globals.into_iter().max().unwrap_or(0),
            locality,
        }
    }

    /// The initial layout of EC2-082, step 2, built as in the proof of the
    /// criterion.
    ///
    /// 1. Forced global parity: a domain holding more than `l` nodes gives its
    ///    surplus — the nodes with the highest ids — to the global slots.
    ///    When the surplus is larger than `g`, the most overloaded domains give
    ///    first.
    /// 2. Free global parity: each remaining global slot takes the node that
    ///    is first by: a zone without a global yet, a domain without one, the
    ///    domain with most nodes left, the smallest id.
    /// 3. The remaining nodes, domain by domain, are dealt into the groups in
    ///    turn. Nodes of one domain are consecutive, so while a domain has no
    ///    more than `l` of them they land in different groups.
    /// 4. The local parity of a group is its member in the zone holding most
    ///    of the group: if that zone is lost, the decoder is left with "data
    ///    plus its local parity", which costs one global parity instead of two.
    /// 5. The data slots of a group take its other members in id order.
    fn initial_layout(&self) -> Vec<usize> {
        let mut remaining = vec![Vec::new(); self.domains.len()];
        for (node, domain) in self.domain_of.iter().enumerate() {
            remaining[*domain].push(node);
        }

        let mut globals = Vec::with_capacity(self.global_parity);
        while globals.len() < self.global_parity {
            let fullest = (0..remaining.len())
                .filter(|domain| remaining[*domain].len() > self.local_groups)
                .max_by_key(|domain| (remaining[*domain].len(), Reverse(*domain)));
            let Some(domain) = fullest else {
                break;
            };
            let highest = self.highest_id_position(&remaining[domain]);
            globals.push(remaining[domain].remove(highest));
        }
        while globals.len() < self.global_parity {
            let zones_taken = globals.iter().map(|node| self.zone_of[*node]).collect::<Vec<_>>();
            let domains_taken =
                globals.iter().map(|node| self.domain_of[*node]).collect::<Vec<_>>();
            let best = remaining.iter().flatten().copied().max_by_key(|node| {
                (
                    !zones_taken.contains(&self.zone_of[*node]),
                    !domains_taken.contains(&self.domain_of[*node]),
                    remaining[self.domain_of[*node]].len(),
                    Reverse(self.nodes[*node].0),
                )
            });
            let Some(best) = best else {
                break;
            };
            remaining[self.domain_of[best]].retain(|node| *node != best);
            globals.push(best);
        }

        let mut groups = vec![Vec::with_capacity(self.group_size); self.local_groups];
        for (turn, node) in remaining.into_iter().flatten().enumerate() {
            groups[turn % self.local_groups].push(node);
        }

        let mut data = Vec::new();
        let mut local_parity = Vec::with_capacity(self.local_groups);
        for members in groups {
            let mut per_zone = vec![0usize; self.zones.len()];
            for node in &members {
                per_zone[self.zone_of[*node]] += 1;
            }
            let parity = members
                .iter()
                .copied()
                .max_by_key(|node| (per_zone[self.zone_of[*node]], Reverse(self.nodes[*node].0)));
            let mut rest =
                members.into_iter().filter(|node| Some(*node) != parity).collect::<Vec<_>>();
            rest.sort_unstable_by_key(|node| self.nodes[*node].0);
            data.extend(rest);
            local_parity.extend(parity);
        }
        globals.sort_unstable_by_key(|node| self.nodes[*node].0);

        data.into_iter().chain(local_parity).chain(globals).collect()
    }

    /// Position, within `members`, of the node with the highest id.
    fn highest_id_position(&self, members: &[usize]) -> usize {
        members
            .iter()
            .enumerate()
            .max_by_key(|(_, node)| self.nodes[**node].0)
            .map(|(position, _)| position)
            .unwrap_or_default()
    }

    /// EC2-082, step 3: steepest ascent over single swaps; when they stall,
    /// one step over two swaps at once, then single swaps again.
    ///
    /// Returns the layout, its score and the number of accepted moves.
    fn improve(&self, mut layout: Vec<usize>) -> (Vec<usize>, LayoutScore, usize) {
        let n = layout.len();
        let limit = MOVES_PER_SLOT * n;
        let pairs = (0..n).flat_map(|i| (i + 1..n).map(move |j| (i, j))).collect::<Vec<_>>();
        let mut current = self.score(&layout);
        let mut moves = 0;

        loop {
            // Single swaps until none improves the score.
            while moves < limit {
                let mut best: Option<(LayoutScore, (usize, usize))> = None;
                for &(i, j) in &pairs {
                    if self.interchangeable(&layout, i, j) {
                        continue;
                    }
                    layout.swap(i, j);
                    let score = self.score(&layout);
                    layout.swap(i, j);
                    if score > current && best.is_none_or(|(top, _)| score > top) {
                        best = Some((score, (i, j)));
                    }
                }
                let Some((score, (i, j))) = best else {
                    break;
                };
                layout.swap(i, j);
                current = score;
                moves += 1;
            }

            if n > PAIR_SWAPS_UP_TO || moves >= limit {
                return (layout, current, moves);
            }

            // Two swaps at once. The layout between them may break the
            // guarantee; the result may not, because the score puts the
            // guarantee first. A swap of two interchangeable nodes changes
            // nothing, so a combination containing one is a single swap in
            // disguise — and no single swap improves the score here.
            let mut best: Option<(LayoutScore, usize, usize)> = None;
            for (a, &(i1, j1)) in pairs.iter().enumerate() {
                if self.interchangeable(&layout, i1, j1) {
                    continue;
                }
                layout.swap(i1, j1);
                for (b, &(i2, j2)) in pairs.iter().enumerate().skip(a + 1) {
                    if self.interchangeable(&layout, i2, j2) {
                        continue;
                    }
                    layout.swap(i2, j2);
                    let score = self.score(&layout);
                    layout.swap(i2, j2);
                    if score > current && best.is_none_or(|(top, _, _)| score > top) {
                        best = Some((score, a, b));
                    }
                }
                layout.swap(i1, j1);
            }
            let Some((score, a, b)) = best else {
                return (layout, current, moves);
            };
            layout.swap(pairs[a].0, pairs[a].1);
            layout.swap(pairs[b].0, pairs[b].1);
            current = score;
            moves += 1;
        }
    }

    /// Whether the nodes in slots `i` and `j` are interchangeable, so that
    /// swapping them changes no goal.
    fn interchangeable(&self, layout: &[usize], i: usize, j: usize) -> bool {
        self.class_of[layout[i]] == self.class_of[layout[j]]
    }
}

/// Where one node of the set sits, at every level placement looks at.
struct Site {
    node_id: NodeId,
    zone: FailureDomainKey,
    rack: FailureDomainKey,
    host: FailureDomainKey,
    /// The domain at the level the group rule spreads members over.
    domain: FailureDomainKey,
}

fn sorted_distinct<'a>(keys: impl Iterator<Item = &'a FailureDomainKey>) -> Vec<FailureDomainKey> {
    let mut keys = keys.cloned().collect::<Vec<_>>();
    keys.sort_unstable();
    keys.dedup();
    keys
}

#[cfg(test)]
mod tests {
    //! The topology matrix of EC2-082 in Rust (`EC2-ZONE-02-007`).
    //!
    //! `lrc_small` and `lrc_standard` are checked against brute force over
    //! every layout that differs in anything a goal can see; `lrc_dense` is too
    //! large for that and is checked against upper bounds known exactly. The
    //! topologies are those of the prototype in
    //! `EC2-082-placement-prototype/check_matrix.py`, plus the other two
    //! levels.

    use super::*;
    use crate::erasure_coding::{EcTopologyPolicy, ShardSlotAssignment};

    /// (zone, rack, nodes) of one rack.
    type Spec<'a> = &'a [(&'a str, &'a str, usize)];

    /// A topology owned by a test case.
    type OwnedSpec = Vec<(&'static str, &'static str, usize)>;

    /// Nodes numbered from 1, rack by rack, one host each.
    fn topology(spec: Spec<'_>) -> (Vec<NodeId>, HashMap<NodeId, TopologyLabels>) {
        let mut nodes = Vec::new();
        let mut labels = HashMap::new();
        for (zone, rack, count) in spec {
            for _ in 0..*count {
                let node_id = NodeId(nodes.len() as u128 + 1);
                labels.insert(
                    node_id,
                    TopologyLabels {
                        zone: Some((*zone).to_string()),
                        rack: Some((*rack).to_string()),
                        host: Some(format!("h{}", node_id.0)),
                        disk_group: Some("d0".to_string()),
                    },
                );
                nodes.push(node_id);
            }
        }
        (nodes, labels)
    }

    fn planner(
        profile: &EcProfile,
        spec: Spec<'_>,
        level: EcFailureDomainLevel,
    ) -> LrcGroupPlanner {
        let (nodes, labels) = topology(spec);
        LrcGroupPlanner::new(profile, &nodes, &labels, level).unwrap()
    }

    /// The best score over every layout, by brute force.
    ///
    /// Nodes of one class are interchangeable for every goal, and so are the
    /// data slots of one group and the global parity slots among themselves:
    /// the decoder counts what a group lost, not which of its data slots. So
    /// only layouts whose classes do not decrease inside those blocks are
    /// visited — every other layout scores the same as one of them.
    fn brute_force_best(planner: &LrcGroupPlanner) -> LayoutScore {
        let n = planner.nodes.len();
        let classes = planner.class_of.iter().max().map_or(0, |max| max + 1);
        let mut members = vec![Vec::new(); classes];
        for (node, class) in planner.class_of.iter().enumerate() {
            members[*class].push(node);
        }
        // The slot before `slot` in the same interchangeable block, if any.
        let data_per_group = planner.group_size - 1;
        let data = planner.local_groups * data_per_group;
        let first_global = data + planner.local_groups;
        let previous = (0..n)
            .map(|slot| {
                let same_block = if slot < data {
                    slot % data_per_group != 0
                } else {
                    slot > first_global
                };
                same_block.then(|| slot - 1)
            })
            .collect::<Vec<_>>();

        struct Search<'a> {
            planner: &'a LrcGroupPlanner,
            members: Vec<Vec<usize>>,
            left: Vec<usize>,
            previous: Vec<Option<usize>>,
            sequence: Vec<usize>,
            best: Option<LayoutScore>,
        }
        impl Search<'_> {
            fn run(&mut self, slot: usize) {
                if slot == self.sequence.len() {
                    let mut taken = vec![0usize; self.members.len()];
                    let layout = self
                        .sequence
                        .iter()
                        .map(|class| {
                            taken[*class] += 1;
                            self.members[*class][taken[*class] - 1]
                        })
                        .collect::<Vec<_>>();
                    let score = self.planner.score(&layout);
                    if self.best.is_none_or(|best| score > best) {
                        self.best = Some(score);
                    }
                    return;
                }
                let lowest = self.previous[slot].map_or(0, |before| self.sequence[before]);
                for class in lowest..self.left.len() {
                    if self.left[class] > 0 {
                        self.left[class] -= 1;
                        self.sequence[slot] = class;
                        self.run(slot + 1);
                        self.left[class] += 1;
                    }
                }
            }
        }

        let mut search = Search {
            planner,
            left: members.iter().map(Vec::len).collect(),
            members,
            previous,
            sequence: vec![0; n],
            best: None,
        };
        search.run(0);
        search.best.expect("a set has at least one layout")
    }

    /// Whether some layout keeps the guarantee, found by trying every way to
    /// split each domain between the groups and the global slots — a search
    /// that knows nothing of the criterion it checks.
    fn feasible_by_search(
        local: usize,
        global: usize,
        group_size: usize,
        counts: &[usize],
    ) -> bool {
        fn search(
            counts: &[usize],
            fill: &mut Vec<usize>,
            globals: usize,
            local: usize,
            global: usize,
            size: usize,
        ) -> bool {
            let Some((&count, rest)) = counts.split_first() else {
                return fill.iter().all(|members| *members == size) && globals == global;
            };
            // The domain gives one node to each group in `mask`, the rest to
            // global slots.
            for mask in 0u32..(1 << local) {
                let to_groups = mask.count_ones() as usize;
                if to_groups > count || globals + (count - to_groups) > global {
                    continue;
                }
                let groups =
                    (0..local).filter(|group| mask & (1 << group) != 0).collect::<Vec<_>>();
                if groups.iter().any(|group| fill[*group] == size) {
                    continue;
                }
                for group in &groups {
                    fill[*group] += 1;
                }
                let found = search(rest, fill, globals + count - to_groups, local, global, size);
                for group in &groups {
                    fill[*group] -= 1;
                }
                if found {
                    return true;
                }
            }
            false
        }
        search(counts, &mut vec![0; local], 0, local, global, group_size)
    }

    /// Every way to split `n` nodes into racks, largest rack first.
    fn partitions(n: usize, largest: usize) -> Vec<Vec<usize>> {
        if n == 0 {
            return vec![Vec::new()];
        }
        let mut all = Vec::new();
        for first in (1..=n.min(largest)).rev() {
            for mut rest in partitions(n - first, first) {
                rest.insert(0, first);
                all.push(rest);
            }
        }
        all
    }

    fn lrc_profiles() -> [EcProfile; 3] {
        [
            EcProfile::lrc_small(),
            EcProfile::lrc_standard(),
            EcProfile::lrc_dense(),
        ]
    }

    /// The numbers decision EC2-080 is built on, fixed by a test rather than by
    /// text: a group is 3, 4 and 6 nodes, so a set needs at least that many
    /// racks. For `lrc_standard` in particular, a group of four needs four
    /// racks — three never do, however the nodes are split.
    #[test]
    fn a_group_is_three_four_and_six_nodes_and_needs_as_many_racks() {
        for (profile, group_size) in lrc_profiles().into_iter().zip([3, 4, 6]) {
            let n = usize::from(profile.total_shards());
            let splits = partitions(n, n);
            let fewest_racks = splits
                .iter()
                .filter(|split| {
                    let spec = split
                        .iter()
                        .enumerate()
                        .map(|(rack, count)| ("z", RACKS[rack], *count))
                        .collect::<Vec<_>>();
                    planner(&profile, &spec, EcFailureDomainLevel::Rack)
                        .feasibility()
                        .violation()
                        .is_none()
                })
                .map(Vec::len)
                .min()
                .unwrap();
            let one = planner(&profile, &[("z", "a", n)], EcFailureDomainLevel::Rack);
            assert_eq!(
                one.feasibility().group_size(),
                group_size,
                "{}",
                profile.name
            );
            assert_eq!(fewest_racks, group_size, "{}", profile.name);
        }

        let standard = EcProfile::lrc_standard();
        for split in partitions(10, 10).into_iter().filter(|split| split.len() == 3) {
            let spec = split
                .iter()
                .enumerate()
                .map(|(rack, count)| ("z", RACKS[rack], *count))
                .collect::<Vec<_>>();
            assert_eq!(
                planner(&standard, &spec, EcFailureDomainLevel::Rack).feasibility().violation(),
                Some(EcLayoutViolationReason::TooFewDomains),
                "lrc_standard on racks {split:?}"
            );
        }
    }

    const RACKS: [&str; 14] = [
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n",
    ];

    /// The criterion `E ≤ g` against an independent search, on every way to
    /// split a set's nodes into racks: 15, 42 and 135 splits.
    #[test]
    fn the_criterion_agrees_with_search_on_every_split_into_racks() {
        for (profile, expected_splits) in lrc_profiles().into_iter().zip([15, 42, 135]) {
            let EcParity::Lrc { local, global } = profile.parity else {
                unreachable!("an LRC profile");
            };
            let n = usize::from(profile.total_shards());
            let splits = partitions(n, n);
            assert_eq!(splits.len(), expected_splits, "{}", profile.name);
            for split in splits {
                let spec = split
                    .iter()
                    .enumerate()
                    .map(|(rack, count)| ("z", RACKS[rack], *count))
                    .collect::<Vec<_>>();
                let feasibility =
                    planner(&profile, &spec, EcFailureDomainLevel::Rack).feasibility();
                let by_search = feasible_by_search(
                    usize::from(local),
                    usize::from(global),
                    feasibility.group_size(),
                    &split,
                );
                assert_eq!(
                    feasibility.violation().is_none(),
                    by_search,
                    "{} on racks {split:?}",
                    profile.name
                );
            }
        }
    }

    /// The initial layout alone is the proof of the criterion made concrete:
    /// wherever `E ≤ g` it keeps the guarantee before any search, and
    /// elsewhere it breaks it exactly as often as it must. That is what makes
    /// the guarantee independent of how well the search does, so it is
    /// checked on its own — on every split into racks of all three profiles,
    /// and on random topologies over zones.
    #[test]
    fn the_initial_layout_alone_keeps_the_guarantee_whenever_the_criterion_holds() {
        let check = |profile: &EcProfile, spec: &[(&str, &str, usize)]| {
            let planner = planner(profile, spec, EcFailureDomainLevel::Rack);
            let start = planner.initial_layout();
            let score = planner.score(&start);
            assert_eq!(
                score.coincidences,
                planner.feasibility().unavoidable_coincidences(),
                "{} {spec:?}",
                profile.name
            );
            let mut sorted = start.clone();
            sorted.sort_unstable();
            assert_eq!(sorted, (0..start.len()).collect::<Vec<_>>());
        };
        for profile in lrc_profiles() {
            let n = usize::from(profile.total_shards());
            for split in partitions(n, n) {
                let spec = split
                    .iter()
                    .enumerate()
                    .map(|(rack, count)| ("z", RACKS[rack], *count))
                    .collect::<Vec<_>>();
                check(&profile, &spec);
            }
        }
        let mut random = SplitMix(11);
        for profile in lrc_profiles() {
            let n = usize::from(profile.total_shards());
            for _ in 0..200 {
                check(&profile, &random_spec(&mut random, n));
            }
        }
    }

    /// One case of the matrix: the topology, the level, whether the criterion
    /// must hold, and — for the prototype's own topologies — the goals it
    /// reached: (zones survived, most global parity in one rack, locality).
    ///
    /// Two of these moved with plan 06: on three zones, `lrc_standard` now
    /// reaches locality 16 while still surviving every zone, where the
    /// two-stage decoder forced a choice between the two. The prototype was
    /// re-run with the unified decoder and agrees.
    struct Case {
        title: &'static str,
        spec: Vec<(&'static str, &'static str, usize)>,
        level: EcFailureDomainLevel,
        satisfiable: bool,
        prototype: Option<(usize, usize, usize)>,
    }

    fn case(
        title: &'static str,
        spec: &[(&'static str, &'static str, usize)],
        level: EcFailureDomainLevel,
        satisfiable: bool,
        prototype: Option<(usize, usize, usize)>,
    ) -> Case {
        Case {
            title,
            spec: spec.to_vec(),
            level,
            satisfiable,
            prototype,
        }
    }

    fn small_matrix() -> Vec<Case> {
        use EcFailureDomainLevel::{Host, Rack, Zone};
        vec![
            case(
                "1 zone, racks 3/2/2",
                &[("z", "a", 3), ("z", "b", 2), ("z", "c", 2)],
                Rack,
                true,
                Some((0, 1, 18)),
            ),
            case(
                "1 zone, 7 racks",
                &[
                    ("z", "a", 1),
                    ("z", "b", 1),
                    ("z", "c", 1),
                    ("z", "d", 1),
                    ("z", "e", 1),
                    ("z", "f", 1),
                    ("z", "g", 1),
                ],
                Rack,
                true,
                Some((0, 1, 18)),
            ),
            case(
                "2 zones 4/3",
                &[("x", "a", 2), ("x", "b", 2), ("y", "a", 2), ("y", "b", 1)],
                Rack,
                true,
                Some((1, 1, 10)),
            ),
            case(
                "3 zones 3/2/2",
                &[
                    ("x", "a", 2),
                    ("x", "b", 1),
                    ("y", "a", 1),
                    ("y", "b", 1),
                    ("w", "a", 1),
                    ("w", "b", 1),
                ],
                Rack,
                true,
                Some((3, 1, 10)),
            ),
            case(
                "1 zone, 2 racks",
                &[("z", "a", 4), ("z", "b", 3)],
                Rack,
                false,
                None,
            ),
            case(
                "zone level, 3 zones 3/2/2",
                &[("x", "a", 2), ("x", "b", 1), ("y", "a", 2), ("w", "a", 2)],
                Zone,
                true,
                None,
            ),
            case(
                "zone level, 2 zones",
                &[("x", "a", 4), ("y", "a", 3)],
                Zone,
                false,
                None,
            ),
            case("host level, 1 rack", &[("z", "a", 7)], Host, true, None),
        ]
    }

    fn standard_matrix() -> Vec<Case> {
        use EcFailureDomainLevel::{Rack, Zone};
        vec![
            case(
                "1 zone, racks 3/3/2/2",
                &[("z", "a", 3), ("z", "b", 3), ("z", "c", 2), ("z", "d", 2)],
                Rack,
                true,
                Some((0, 1, 32)),
            ),
            case(
                "1 zone, 5 racks x 2",
                &[
                    ("z", "a", 2),
                    ("z", "b", 2),
                    ("z", "c", 2),
                    ("z", "d", 2),
                    ("z", "e", 2),
                ],
                Rack,
                true,
                Some((0, 1, 32)),
            ),
            case(
                "2 zones x 4 racks",
                &[
                    ("x", "a", 2),
                    ("x", "b", 1),
                    ("x", "c", 1),
                    ("x", "d", 1),
                    ("y", "a", 2),
                    ("y", "b", 1),
                    ("y", "c", 1),
                    ("y", "d", 1),
                ],
                Rack,
                true,
                Some((0, 1, 32)),
            ),
            case(
                "3 zones x 2 racks, 4/4/2",
                &[
                    ("za", "r1", 2),
                    ("za", "r2", 2),
                    ("zb", "r1", 2),
                    ("zb", "r2", 2),
                    ("zc", "r1", 1),
                    ("zc", "r2", 1),
                ],
                Rack,
                true,
                Some((3, 1, 16)),
            ),
            case(
                "3 zones 4/3/3, 2 racks each",
                &[
                    ("x", "a", 2),
                    ("x", "b", 2),
                    ("y", "a", 2),
                    ("y", "b", 1),
                    ("w", "a", 2),
                    ("w", "b", 1),
                ],
                Rack,
                true,
                Some((3, 1, 16)),
            ),
            case(
                "1 zone, 3 racks",
                &[("z", "a", 4), ("z", "b", 3), ("z", "c", 3)],
                Rack,
                false,
                None,
            ),
            case(
                "1 zone, racks 5/2/2/1",
                &[("z", "a", 5), ("z", "b", 2), ("z", "c", 2), ("z", "d", 1)],
                Rack,
                false,
                None,
            ),
            case(
                "zone level, 4 zones 3/3/2/2",
                &[("w", "a", 3), ("x", "a", 3), ("y", "a", 2), ("z", "a", 2)],
                Zone,
                true,
                None,
            ),
        ]
    }

    /// Checks one case against brute force and the criterion, and returns the
    /// layout the planner chose.
    fn check_against_brute_force(profile: &EcProfile, case: &Case) -> LrcGroupLayout {
        let planner = planner(profile, &case.spec, case.level);
        let feasibility = planner.feasibility();
        let layout = planner.plan();
        let best = brute_force_best(&planner);
        let label = format!("{} {}", profile.name, case.title);

        assert_eq!(
            feasibility.violation().is_none(),
            case.satisfiable,
            "{label}"
        );
        // The best layout keeps the guarantee exactly when the criterion says
        // it can — the criterion is exact.
        assert_eq!(best.coincidences == 0, case.satisfiable, "{label}");
        assert_eq!(
            best.coincidences,
            feasibility.unavoidable_coincidences(),
            "{label}"
        );
        assert_eq!(layout.score, best, "{label}: the search missed the optimum");
        if let Some((zones, globals, locality)) = case.prototype {
            let got = (
                layout.score.zones_survived,
                layout.score.max_globals_in_one_domain,
                layout.score.locality,
            );
            assert_eq!(
                got,
                (zones, globals, locality),
                "{label}: the prototype reached another"
            );
        }
        assert_eq!(
            planner.score_of(&layout.slot_order),
            Some(layout.score),
            "{label}"
        );
        layout
    }

    #[test]
    fn the_matrix_of_lrc_small_reaches_the_brute_force_optimum() {
        let profile = EcProfile::lrc_small();
        for case in small_matrix() {
            check_against_brute_force(&profile, &case);
        }
    }

    #[test]
    fn the_matrix_of_lrc_standard_reaches_the_brute_force_optimum() {
        let profile = EcProfile::lrc_standard();
        for case in standard_matrix() {
            check_against_brute_force(&profile, &case);
        }
    }

    /// The three-zone topology of EC2-080: the EC v1 layout survives the loss
    /// of any zone while breaking the rack guarantee; the rule keeps the
    /// guarantee and still survives any zone. Putting locality first would
    /// survive only one or two.
    #[test]
    fn three_zones_of_four_four_and_two_survive_any_zone_with_the_guarantee() {
        let profile = EcProfile::lrc_standard();
        let spec = [
            ("za", "r1", 2),
            ("za", "r2", 2),
            ("zb", "r1", 2),
            ("zb", "r2", 2),
            ("zc", "r1", 1),
            ("zc", "r2", 1),
        ];
        let planner = planner(&profile, &spec, EcFailureDomainLevel::Rack);
        let layout = planner.plan();

        assert_eq!(layout.score.coincidences, 0);
        assert_eq!(layout.score.zones_survived, 3);
        let (nodes, labels) = topology(&spec);
        let v1 = ShardSlotAssignment::build(&profile, &nodes, &labels).unwrap();
        let v1_score = planner.score_of(&v1.slot_order()).unwrap();
        assert_eq!(v1_score.zones_survived, 3);
        assert!(v1_score.coincidences > 0, "{v1_score:?}");
    }

    /// `lrc_dense` has over 150 million distinct layouts even on six racks,
    /// so it is checked against bounds known exactly: zones a layout can
    /// survive, one global parity per domain, and both groups inside one zone
    /// (locality 2 · 6² = 72).
    #[test]
    fn the_matrix_of_lrc_dense_reaches_the_known_bounds() {
        let profile = EcProfile::lrc_dense();
        let racks = |zone: &'static str, counts: &[usize]| {
            counts
                .iter()
                .enumerate()
                .map(|(rack, count)| (zone, RACKS[rack], *count))
                .collect::<Vec<_>>()
        };
        // (title, topology, zones survived, locality); `None` where no bound
        // is known.
        let cases: Vec<(&str, OwnedSpec, usize, Option<usize>)> = vec![
            (
                "1 zone, racks 3/3/2/2/2/2",
                racks("z", &[3, 3, 2, 2, 2, 2]),
                0,
                Some(72),
            ),
            ("1 zone, 7 racks x 2", racks("z", &[2; 7]), 0, Some(72)),
            (
                "2 zones x 6 racks",
                [
                    racks("x", &[2, 1, 1, 1, 1, 1]),
                    racks("y", &[2, 1, 1, 1, 1, 1]),
                ]
                .concat(),
                0,
                Some(72),
            ),
            (
                "4 zones 4/4/3/3, 2 racks each",
                [
                    racks("w", &[2, 2]),
                    racks("x", &[2, 2]),
                    racks("y", &[2, 1]),
                    racks("q", &[2, 1]),
                ]
                .concat(),
                4,
                None,
            ),
            // A zone of five shards is never survivable — the decoder survives
            // at most four losses — so only the zone of four can count.
            (
                "3 zones 5/5/4, 3 racks each",
                [
                    racks("x", &[2, 2, 1]),
                    racks("y", &[2, 2, 1]),
                    racks("w", &[2, 1, 1]),
                ]
                .concat(),
                1,
                None,
            ),
        ];
        for (title, spec, zones, locality) in cases {
            let planner = planner(&profile, &spec, EcFailureDomainLevel::Rack);
            assert!(planner.feasibility().violation().is_none(), "{title}");
            let layout = planner.plan();
            assert_eq!(layout.score.coincidences, 0, "{title}");
            assert_eq!(layout.score.zones_survived, zones, "{title}");
            assert_eq!(layout.score.racks_survived, planner.racks, "{title}");
            assert_eq!(layout.score.max_globals_in_one_domain, 1, "{title}");
            if let Some(locality) = locality {
                assert_eq!(layout.score.locality, locality, "{title}");
            }
        }

        let five_racks = racks("z", &[3, 3, 3, 3, 2]);
        assert_eq!(
            planner(&profile, &five_racks, EcFailureDomainLevel::Rack)
                .feasibility()
                .violation(),
            Some(EcLayoutViolationReason::TooFewDomains)
        );
    }

    /// The layout is a function of the nodes and their labels: the same set
    /// given in any order is laid out the same way.
    #[test]
    fn the_layout_does_not_depend_on_the_order_of_the_nodes() {
        for (profile, cases) in [
            (EcProfile::lrc_small(), small_matrix()),
            (EcProfile::lrc_standard(), standard_matrix()),
        ] {
            for case in cases {
                let (nodes, labels) = topology(&case.spec);
                let plan = |order: &[NodeId]| {
                    LrcGroupPlanner::new(&profile, order, &labels, case.level).unwrap().plan()
                };
                let forward = plan(&nodes);
                let mut reversed = nodes.clone();
                reversed.reverse();
                let mut rotated = nodes.clone();
                rotated.rotate_left(3);
                let mut interleaved = nodes.iter().step_by(2).copied().collect::<Vec<_>>();
                interleaved.extend(nodes.iter().skip(1).step_by(2));
                for order in [reversed, rotated, interleaved] {
                    assert_eq!(plan(&order), forward, "{} {}", profile.name, case.title);
                }
            }
        }
    }

    /// The four topologies of EC2-080 where the EC v1 layout puts two members
    /// of a group into one rack, although a layout without that exists. The
    /// rule finds it.
    #[test]
    fn the_rule_keeps_groups_apart_where_the_ec_v1_layout_does_not() {
        let cases: [(EcProfile, OwnedSpec); 4] = [
            (
                EcProfile::lrc_standard(),
                vec![("z", "a", 3), ("z", "b", 3), ("z", "c", 2), ("z", "d", 2)],
            ),
            (
                EcProfile::lrc_standard(),
                vec![
                    ("z", "a", 2),
                    ("z", "b", 2),
                    ("z", "c", 2),
                    ("z", "d", 2),
                    ("z", "e", 2),
                ],
            ),
            (
                EcProfile::lrc_small(),
                vec![("z", "a", 3), ("z", "b", 2), ("z", "c", 2)],
            ),
            (
                EcProfile::lrc_dense(),
                vec![
                    ("z", "a", 3),
                    ("z", "b", 3),
                    ("z", "c", 2),
                    ("z", "d", 2),
                    ("z", "e", 2),
                    ("z", "f", 2),
                ],
            ),
        ];
        for (profile, spec) in cases {
            let (nodes, labels) = topology(&spec);
            let planner =
                LrcGroupPlanner::new(&profile, &nodes, &labels, EcFailureDomainLevel::Rack)
                    .unwrap();
            let v1 = ShardSlotAssignment::build(&profile, &nodes, &labels).unwrap();
            let v1_assessment = planner.assess(&v1.slot_order()).unwrap();
            assert!(!v1_assessment.shared_domains.is_empty(), "{}", profile.name);

            let layout = planner.plan();
            let assessment = planner.assess(&layout.slot_order).unwrap();
            assert!(
                assessment.shared_domains.is_empty(),
                "{}: {assessment:?}",
                profile.name
            );
        }
    }

    /// Global parity is spread: on one zone of five racks the two global
    /// parity shards of `lrc_standard` are in different racks, and on three
    /// zones in different zones (`EC2-ZONE-02-003`).
    #[test]
    fn global_parity_is_spread_over_domains_and_zones() {
        let profile = EcProfile::lrc_standard();
        let spec = [
            ("z", "a", 2),
            ("z", "b", 2),
            ("z", "c", 2),
            ("z", "d", 2),
            ("z", "e", 2),
        ];
        let (nodes, labels) = topology(&spec);
        let layout = LrcGroupPlanner::new(&profile, &nodes, &labels, EcFailureDomainLevel::Rack)
            .unwrap()
            .plan();
        let rack_of = |node: &NodeId| labels[node].rack.clone().unwrap();
        let globals = &layout.slot_order[8..];
        assert_ne!(rack_of(&globals[0]), rack_of(&globals[1]));

        let spec = [
            ("x", "a", 2),
            ("x", "b", 2),
            ("y", "a", 2),
            ("y", "b", 1),
            ("w", "a", 2),
            ("w", "b", 1),
        ];
        let (nodes, labels) = topology(&spec);
        let layout = LrcGroupPlanner::new(&profile, &nodes, &labels, EcFailureDomainLevel::Rack)
            .unwrap()
            .plan();
        let zone_of = |node: &NodeId| labels[node].zone.clone().unwrap();
        let globals = &layout.slot_order[8..];
        assert_ne!(zone_of(&globals[0]), zone_of(&globals[1]));
        assert_eq!(layout.score.max_globals_in_one_domain, 1);
    }

    /// A small deterministic generator, so random topologies are the same on
    /// every run.
    struct SplitMix(u64);

    impl SplitMix {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^ (z >> 31)
        }

        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }
    }

    const ZONES: [&str; 4] = ["z0", "z1", "z2", "z3"];

    /// A random topology of `n` nodes: one to four zones, one to four racks in
    /// each, as the prototype draws them.
    fn random_spec(random: &mut SplitMix, n: usize) -> Vec<(&'static str, &'static str, usize)> {
        let zones = 1 + random.below(4);
        let domains = (0..zones)
            .flat_map(|zone| {
                let racks = 1 + random.below(4);
                (0..racks).map(move |rack| (ZONES[zone], RACKS[rack]))
            })
            .collect::<Vec<_>>();
        let mut counts = vec![0usize; domains.len()];
        for _ in 0..n {
            counts[random.below(domains.len())] += 1;
        }
        domains
            .into_iter()
            .zip(counts)
            .filter(|(_, count)| *count > 0)
            .map(|((zone, rack), count)| (zone, rack, count))
            .collect()
    }

    /// Random topologies against brute force, satisfiable and not: the
    /// criterion is exact, the search reaches the optimum, and where the
    /// guarantee cannot be kept it breaks it no more than any layout must.
    #[test]
    fn random_topologies_reach_the_brute_force_optimum() {
        let mut random = SplitMix(20_260_911);
        for (profile, runs) in [
            (EcProfile::lrc_small(), 120),
            (EcProfile::lrc_standard(), 30),
        ] {
            let n = usize::from(profile.total_shards());
            let mut unsatisfiable = 0;
            for _ in 0..runs {
                let spec = random_spec(&mut random, n);
                let satisfiable = {
                    let planner = planner(&profile, &spec, EcFailureDomainLevel::Rack);
                    planner.feasibility().violation().is_none()
                };
                unsatisfiable += usize::from(!satisfiable);
                let case = Case {
                    title: "random",
                    spec,
                    level: EcFailureDomainLevel::Rack,
                    satisfiable,
                    prototype: None,
                };
                check_against_brute_force(&profile, &case);
            }
            // The draw covers both outcomes, or it would prove half as much.
            assert!(
                unsatisfiable > 0 && unsatisfiable < runs,
                "{}",
                profile.name
            );
        }
    }

    /// On random topologies where the guarantee can be kept, the rule never
    /// survives fewer zones than the EC v1 layout of the same nodes
    /// (EC2-080, "what we optimise"), on all three profiles.
    #[test]
    fn the_rule_never_survives_fewer_zones_than_the_ec_v1_layout() {
        let mut random = SplitMix(7);
        for (profile, runs) in [
            (EcProfile::lrc_small(), 60),
            (EcProfile::lrc_standard(), 40),
            (EcProfile::lrc_dense(), 15),
        ] {
            let n = usize::from(profile.total_shards());
            let mut done = 0;
            while done < runs {
                let spec = random_spec(&mut random, n);
                let (nodes, labels) = topology(&spec);
                let planner =
                    LrcGroupPlanner::new(&profile, &nodes, &labels, EcFailureDomainLevel::Rack)
                        .unwrap();
                if planner.feasibility().violation().is_some() {
                    continue;
                }
                done += 1;
                let layout = planner.plan();
                let v1 = ShardSlotAssignment::build(&profile, &nodes, &labels).unwrap();
                let v1_score = planner.score_of(&v1.slot_order()).unwrap();
                assert_eq!(layout.score.coincidences, 0, "{} {spec:?}", profile.name);
                assert!(
                    layout.score.zones_survived >= v1_score.zones_survived,
                    "{} {spec:?}: {:?} against EC v1 {:?}",
                    profile.name,
                    layout.score,
                    v1_score
                );
                assert!(layout.moves <= MOVES_PER_SLOT * n);
            }
        }
    }

    /// Under `warn` with too few racks the layout is as close to the rule as
    /// the nodes allow, and the explanation names the numbers EC2-083 asks
    /// for.
    #[test]
    fn an_unsatisfiable_set_is_explained_with_its_numbers() {
        let profile = EcProfile::lrc_standard();
        let too_few = planner(
            &profile,
            &[("z1", "a", 4), ("z1", "b", 3), ("z1", "c", 3)],
            EcFailureDomainLevel::Rack,
        );
        let text = too_few.feasibility().explain(&profile.name).unwrap();
        assert!(text.contains("over 4 distinct racks"), "{text}");
        assert!(
            text.contains("in 3 rack(s) (z1/a=4, z1/b=3, z1/c=3)"),
            "{text}"
        );
        assert!(text.contains("at least 4 racks"), "{text}");

        let overloaded = planner(
            &profile,
            &[
                ("z1", "a", 5),
                ("z1", "b", 2),
                ("z1", "c", 2),
                ("z1", "d", 1),
            ],
            EcFailureDomainLevel::Rack,
        );
        let feasibility = overloaded.feasibility();
        assert_eq!(
            feasibility.violation(),
            Some(EcLayoutViolationReason::DomainOverloaded)
        );
        assert_eq!(feasibility.excess(), 3);
        assert_eq!(feasibility.unavoidable_coincidences(), 1);
        let text = feasibility.explain(&profile.name).unwrap();
        assert!(
            text.contains("racks z1/a=5 leave 3 node(s) for those 2 slot(s)"),
            "{text}"
        );
        assert!(text.contains("move 1 node(s)"), "{text}");
        assert_eq!(overloaded.plan().score.coincidences, 1);

        let fine = planner(
            &profile,
            &[
                ("z1", "a", 2),
                ("z1", "b", 2),
                ("z1", "c", 2),
                ("z1", "d", 2),
                ("z1", "e", 2),
            ],
            EcFailureDomainLevel::Rack,
        );
        assert_eq!(fine.feasibility().explain(&profile.name), None);
    }

    /// A rack named the same in two zones is two racks: the rule spreads a
    /// group over them as it would over any two racks (EC2-081, point 1).
    #[test]
    fn racks_named_alike_in_two_zones_are_two_racks() {
        let profile = EcProfile::lrc_standard();
        let spec = [
            ("x", "r1", 2),
            ("x", "r2", 3),
            ("y", "r1", 2),
            ("y", "r2", 3),
        ];
        let planner = planner(&profile, &spec, EcFailureDomainLevel::Rack);
        assert_eq!(planner.feasibility().domain_count(), 4);
        assert!(planner.feasibility().violation().is_none());
        assert_eq!(planner.plan().score.coincidences, 0);
    }

    #[test]
    fn the_planner_refuses_what_it_cannot_place() {
        let (nodes, labels) = topology(&[("z", "a", 10)]);
        let level = EcFailureDomainLevel::Rack;

        let rs = LrcGroupPlanner::new(&EcProfile::rs_standard(), &nodes[..9], &labels, level);
        assert!(matches!(rs, Err(LrcPlacementError::NotLrc { .. })));

        let short = LrcGroupPlanner::new(&EcProfile::lrc_standard(), &nodes[..9], &labels, level);
        assert!(matches!(
            short,
            Err(LrcPlacementError::WrongWidth {
                expected: 10,
                actual: 9,
                ..
            })
        ));

        let mut repeated = nodes.clone();
        repeated[9] = repeated[0];
        let twice = LrcGroupPlanner::new(&EcProfile::lrc_standard(), &repeated, &labels, level);
        assert!(matches!(
            twice,
            Err(LrcPlacementError::DuplicateNode { .. })
        ));

        let mut partial = labels.clone();
        partial.get_mut(&nodes[3]).unwrap().rack = None;
        partial.remove(&nodes[7]);
        let unlabeled = LrcGroupPlanner::new(&EcProfile::lrc_standard(), &nodes, &partial, level);
        let Err(LrcPlacementError::IncompleteLabels { nodes: missing }) = unlabeled else {
            panic!("incomplete labels must be refused: {unlabeled:?}");
        };
        assert_eq!(
            missing,
            vec![
                UnlabeledNode {
                    node_id: nodes[3],
                    missing: vec!["rack"]
                },
                UnlabeledNode {
                    node_id: nodes[7],
                    missing: vec!["zone", "rack", "host", "disk_group"],
                },
            ]
        );
    }

    /// A slot order that is not an ordering of exactly the planner's nodes is
    /// not judged.
    #[test]
    fn only_an_ordering_of_the_sets_own_nodes_is_assessed() {
        let profile = EcProfile::lrc_small();
        let planner = planner(
            &profile,
            &[("z", "a", 3), ("z", "b", 2), ("z", "c", 2)],
            EcFailureDomainLevel::Rack,
        );
        let order = planner.plan().slot_order;
        assert!(planner.assess(&order).is_some());
        assert!(planner.assess(&order[1..]).is_none());
        let mut repeated = order.clone();
        repeated[1] = repeated[0];
        assert!(planner.assess(&repeated).is_none());
        let mut foreign = order.clone();
        foreign[0] = NodeId(9_999);
        assert!(planner.score_of(&foreign).is_none());
        let _ = EcTopologyPolicy::Strict;
    }
}
