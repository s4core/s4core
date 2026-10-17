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

//! The model test of the lease of IAM changes (plan
//! CLUSTER-IAM-REPLICATION.md, task CIR-03-4).
//!
//! N nodes, each with the real [`LeaseLedger`], an HLC whose wall clock is
//! off by its own skew, and a store of versions ordered by HLC as every path
//! of the pool orders them. Changes come to random nodes at random times:
//! creating a user of one of a few names, changing a user, deleting one. A
//! change takes the lease from the nodes its node reaches, reads from them,
//! checks its precondition, sends its write to every node and later counts
//! the acknowledgements that came back: a majority confirms it. Between any
//! two steps of a change time moves, other changes run, nodes are cut off
//! and come back, restart (keeping only what their ledger keeps), messages
//! are held for up to three leases — a holder paused in the middle of a
//! write — and anti-entropy copies the newest version between two nodes.
//!
//! After the history every node is brought to the newest version of every
//! key, and the test checks:
//!
//! - **Nothing confirmed is lost:** every confirmed change of a user is in
//!   the history of the version every node ends with — every later change of
//!   that user read it, directly or through others.
//! - **The history of each user is linear:** of two confirmed changes of one
//!   user, the later read the earlier.
//! - **A name is one user:** no two confirmed creations of one name are both
//!   alive at the end, and the name resolves — by the directory's rule, the
//!   later epoch first — to the confirmed one.
//!
//! The seed of a failing history is printed. `S4_IAM_MODEL_HISTORIES` sets
//! how many histories run (2 000 by default; the plan's criterion is 10 000),
//! `S4_IAM_MODEL_SEED` runs one history. Without fencing, and with fencing
//! but no HLC window, the same model finds a counterexample; what fencing a
//! read adds is shown by a timeline of its own, which random histories
//! rarely hit.

use std::collections::{BTreeMap, BTreeSet};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use s4_core::storage::{VersionKind, VersionRank};

use super::ledger::{Fenced, Grant, LeaseFence, LeaseLedger, LedgerRecord};
use crate::hlc::HlcTimestamp;
use crate::identity::NodeId;

const LEASE_MS: u64 = 5_000;

/// What the model leaves out, to show that each part of fencing matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutation {
    /// Fencing as it is.
    None,
    /// Replicas take every write and every read: no fencing at all.
    NoFencing,
    /// Writes carry no HLC window: the epoch is checked, the order is the clocks'.
    NoWindow,
}
const MARGIN_MS: u64 = 500;
const NAMES: [&str; 3] = ["ann", "bob", "cid"];

/// What a user record holds.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Value {
    name: String,
    created_epoch: u64,
    created_at: u64,
}

/// One version of a key: the write that made it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Version {
    hlc: HlcTimestamp,
    /// `None` for a tombstone.
    value: Option<Value>,
    /// The write that made it.
    write: usize,
}

impl Version {
    fn rank(&self) -> VersionRank {
        let kind = if self.value.is_some() {
            VersionKind::Live
        } else {
            VersionKind::Tombstone
        };
        VersionRank::new(self.hlc.as_tuple(), kind, &self.write.to_string())
    }
}

/// Every write any change sent.
#[derive(Debug, Clone)]
struct Write {
    key: String,
    version: Version,
    /// The write whose version the change read; `None` for a creation.
    based_on: Option<usize>,
    confirmed: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct Clock {
    wall: u64,
    logical: u32,
}

struct Node {
    ledger: LeaseLedger,
    store: BTreeMap<String, Version>,
    clock: Clock,
    skew_ms: i64,
    up: bool,
    /// The change this node holds its turn for: one change of a node at a
    /// time, as `IamLeaseHolder` takes them.
    turn: Option<usize>,
    /// The last epoch this node asked for, never asked for again.
    last_epoch: u64,
    /// Cut off from every other node until then.
    isolated_until: u64,
}

impl Node {
    fn hlc_now(&mut self, id: usize, now_ms: u64) -> HlcTimestamp {
        let wall = now_ms.saturating_add_signed(self.skew_ms);
        if wall > self.clock.wall {
            self.clock = Clock { wall, logical: 0 };
        } else {
            self.clock.logical += 1;
        }
        HlcTimestamp::from_record(self.clock.wall, self.clock.logical, id as u128 + 1)
    }

    fn observe(&mut self, hlc: HlcTimestamp) {
        if hlc.wall_time > self.clock.wall
            || (hlc.wall_time == self.clock.wall && hlc.logical >= self.clock.logical)
        {
            self.clock = Clock {
                wall: hlc.wall_time,
                logical: hlc.logical + 1,
            };
        }
    }

    /// Last writer wins: a repair copy, or a write the node admitted.
    fn apply(&mut self, key: &str, version: &Version) {
        match self.store.get(key) {
            Some(held) if held.rank() >= version.rank() => {}
            _ => {
                self.store.insert(key.to_string(), version.clone());
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Kind {
    Create(String),
    Update(String),
    Delete(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    Acquire,
    Read,
    Collect,
    Release,
}

struct Change {
    holder: usize,
    kind: Kind,
    step: Step,
    fence: Option<LeaseFence>,
    granted: Vec<usize>,
    valid_until: u64,
    read: BTreeMap<String, Version>,
    write: Option<usize>,
    acks: BTreeSet<usize>,
}

/// A write on its way to a node.
struct Message {
    deliver_at: u64,
    to: usize,
    write: usize,
    fence: LeaseFence,
    change: usize,
}

struct Model {
    rng: StdRng,
    mutation: Mutation,
    now: u64,
    nodes: Vec<Node>,
    changes: Vec<Change>,
    writes: Vec<Write>,
    messages: Vec<Message>,
    next_user: usize,
    /// What happened, printed with a violation of a single history.
    trace: Vec<String>,
}

impl Model {
    fn new(seed: u64, mutation: Mutation) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let count = if rng.random_bool(0.5) { 3 } else { 5 };
        let nodes = (0..count)
            .map(|_| Node {
                ledger: LeaseLedger::default(),
                store: BTreeMap::new(),
                clock: Clock::default(),
                skew_ms: rng.random_range(-800..=800),
                up: true,
                isolated_until: 0,
                turn: None,
                last_epoch: 0,
            })
            .collect();
        Self {
            rng,
            mutation,
            now: 1_000_000,
            nodes,
            changes: Vec::new(),
            writes: Vec::new(),
            messages: Vec::new(),
            next_user: 0,
            trace: Vec::new(),
        }
    }

    fn note(&mut self, event: String) {
        let line = format!("{:>9} {event}", self.now);
        self.trace.push(line);
    }

    fn majority(&self) -> usize {
        self.nodes.len() / 2 + 1
    }

    fn reaches(&self, from: usize, to: usize) -> bool {
        let up = self.nodes[from].up && self.nodes[to].up;
        up && (from == to
            || (self.nodes[from].isolated_until <= self.now
                && self.nodes[to].isolated_until <= self.now))
    }

    fn start_change(&mut self) {
        let holder = self.rng.random_range(0..self.nodes.len());
        if !self.nodes[holder].up {
            return;
        }
        let known: Vec<String> = self
            .writes
            .iter()
            .map(|write| write.key.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let kind = match self.rng.random_range(0..3) {
            0 if !known.is_empty() => {
                Kind::Update(known[self.rng.random_range(0..known.len())].clone())
            }
            1 if !known.is_empty() => {
                Kind::Delete(known[self.rng.random_range(0..known.len())].clone())
            }
            _ => Kind::Create(NAMES[self.rng.random_range(0..NAMES.len())].to_string()),
        };
        self.changes.push(Change {
            holder,
            kind,
            step: Step::Acquire,
            fence: None,
            granted: Vec::new(),
            valid_until: 0,
            read: BTreeMap::new(),
            write: None,
            acks: BTreeSet::new(),
        });
    }

    /// Runs the next step of change `c`; `false` once it is over.
    fn advance(&mut self, c: usize) -> bool {
        let holder = self.changes[c].holder;
        if !self.nodes[holder].up {
            return false;
        }
        match self.changes[c].step {
            Step::Acquire => self.acquire(c),
            Step::Read => self.read_and_write(c),
            Step::Collect => {
                let confirmed = self.changes[c].acks.len() >= self.majority();
                if let Some(write) = self.changes[c].write {
                    self.writes[write].confirmed = confirmed;
                    let acks = self.changes[c].acks.clone();
                    self.note(format!(
                        "change {c}: write {write} acks {acks:?} confirmed {confirmed}"
                    ));
                }
                self.changes[c].step = Step::Release;
                true
            }
            Step::Release => {
                self.release(c);
                if self.nodes[holder].turn == Some(c) {
                    self.nodes[holder].turn = None;
                }
                false
            }
        }
    }

    fn acquire(&mut self, c: usize) -> bool {
        let holder = self.changes[c].holder;
        match self.nodes[holder].turn {
            Some(other) if other != c => return true,
            _ => self.nodes[holder].turn = Some(c),
        }
        let epoch = self.nodes[holder].ledger.max_epoch().max(self.nodes[holder].last_epoch) + 1;
        self.nodes[holder].last_epoch = epoch;
        let hlc = self.nodes[holder].hlc_now(holder, self.now);
        let ceiling = HlcTimestamp::from_record(hlc.wall_time + LEASE_MS, 0, 0);
        let mut granted = Vec::new();
        let mut floor = HlcTimestamp::zero();
        for n in 0..self.nodes.len() {
            if !self.reaches(holder, n) {
                continue;
            }
            // The request reaches each node a little later than the holder
            // sent it, so each grant runs out a little later than the
            // holder's lease: a node's clock, not the holder's, ends it.
            let arrived = if n == holder {
                self.now
            } else {
                self.now + self.rng.random_range(0..1_500)
            };
            if let (Grant::Granted { floor: theirs }, _) = self.nodes[n].ledger.acquire(
                NodeId(holder as u128 + 1),
                epoch,
                ceiling,
                LEASE_MS,
                arrived,
            ) {
                granted.push(n);
                floor = floor.max(theirs);
            }
        }
        let fence = LeaseFence {
            epoch,
            holder: NodeId(holder as u128 + 1),
            floor,
            ceiling,
        };
        if self.mutation != Mutation::NoWindow {
            self.nodes[holder].observe(floor);
        }
        let ok = granted.len() >= self.majority()
            && self.nodes[holder].hlc_now(holder, self.now) < ceiling;
        self.note(format!(
            "change {c} at node {holder}: epoch {epoch} granted by {granted:?}, floor {floor}, ceiling {ceiling}, ok {ok}"
        ));
        self.changes[c].granted = granted;
        self.changes[c].fence = Some(fence);
        if !ok {
            self.changes[c].step = Step::Release;
            return true;
        }
        self.changes[c].valid_until = self.now + LEASE_MS - MARGIN_MS;
        self.changes[c].step = Step::Read;
        true
    }

    fn read_and_write(&mut self, c: usize) -> bool {
        let holder = self.changes[c].holder;
        let Some(fence) = self.changes[c].fence else {
            return false;
        };
        self.changes[c].step = Step::Release;
        if self.now >= self.changes[c].valid_until {
            return true;
        }
        // Read from every node reached, each taking the epoch first.
        let mut answered = Vec::new();
        let mut read: BTreeMap<String, Version> = BTreeMap::new();
        for n in 0..self.nodes.len() {
            if !self.reaches(holder, n) {
                continue;
            }
            let fenced_reads = self.mutation != Mutation::NoFencing;
            if fenced_reads && self.nodes[n].ledger.admit_read(Some(&fence)).is_err() {
                continue;
            }
            answered.push(n);
            for (key, version) in &self.nodes[n].store {
                match read.get(key) {
                    Some(held) if held.rank() >= version.rank() => {}
                    _ => {
                        read.insert(key.clone(), version.clone());
                    }
                }
            }
        }
        if answered.len() < self.majority() {
            return true;
        }
        // What fewer than a majority hold is written again under this lease
        // before the change acts on it, or the change stops.
        let unsettled: Vec<(String, Version)> = read
            .iter()
            .filter(|(key, newest)| {
                let holders = answered
                    .iter()
                    .filter(|&&n| {
                        self.nodes[n]
                            .store
                            .get(*key)
                            .is_some_and(|held| held.rank() == newest.rank())
                    })
                    .count();
                holders < self.majority()
            })
            .map(|(key, version)| (key.clone(), version.clone()))
            .collect();
        for (key, newest) in unsettled {
            let hlc = self.nodes[holder].hlc_now(holder, self.now);
            if !fence.admits_hlc(hlc) {
                return true;
            }
            let write = self.writes.len();
            let version = Version {
                hlc,
                value: newest.value.clone(),
                write,
            };
            self.writes.push(Write {
                key: key.clone(),
                version: version.clone(),
                based_on: Some(newest.write),
                confirmed: false,
            });
            let mut acks = 0;
            for n in 0..self.nodes.len() {
                if self.reaches(holder, n)
                    && self.nodes[n].ledger.admit(Some(&fence), hlc, self.now).is_ok()
                {
                    self.nodes[n].apply(&key, &version);
                    acks += 1;
                }
            }
            let confirmed = acks >= self.majority();
            self.writes[write].confirmed = confirmed;
            self.note(format!(
                "change {c}: write {write} settles {key} at {hlc} after write {}, confirmed {confirmed}",
                newest.write
            ));
            if !confirmed {
                return true;
            }
            read.insert(key, version);
        }
        let live = |key: &str| read.get(key).filter(|version| version.value.is_some());
        let (key, value, based_on) = match self.changes[c].kind.clone() {
            Kind::Create(name) => {
                if read
                    .values()
                    .any(|version| version.value.as_ref().is_some_and(|value| value.name == name))
                {
                    return true;
                }
                self.next_user += 1;
                let value = Value {
                    name,
                    created_epoch: fence.epoch,
                    created_at: self.now,
                };
                (format!("u{:04}", self.next_user), Some(value), None)
            }
            Kind::Update(key) => {
                let Some(current) = live(&key).cloned() else {
                    return true;
                };
                (key, current.value.clone(), Some(current.write))
            }
            Kind::Delete(key) => {
                let Some(current) = live(&key).cloned() else {
                    return true;
                };
                (key, None, Some(current.write))
            }
        };
        let hlc = self.nodes[holder].hlc_now(holder, self.now);
        if self.mutation != Mutation::NoWindow && !fence.admits_hlc(hlc) {
            return true;
        }
        let write = self.writes.len();
        self.writes.push(Write {
            key,
            version: Version { hlc, value, write },
            based_on,
            confirmed: false,
        });
        let w = &self.writes[write];
        let line = format!(
            "change {c} at node {holder}: epoch {} read {} nodes, write {write} {} {:?} at {} based on {:?}",
            fence.epoch,
            answered.len(),
            w.key,
            w.version.value,
            w.version.hlc,
            w.based_on
        );
        self.note(line);
        self.changes[c].read = read;
        self.changes[c].write = Some(write);
        // A holder paused between stamping the write and sending it holds
        // every copy back alike.
        let paused = if self.rng.random_bool(0.2) {
            self.rng.random_range(0..3 * LEASE_MS)
        } else {
            0
        };
        for to in 0..self.nodes.len() {
            // Most copies land at once; some are held on their way, for up to
            // three leases.
            let delay = if self.rng.random_bool(0.7) {
                paused
            } else {
                paused + self.rng.random_range(0..3 * LEASE_MS)
            };
            self.messages.push(Message {
                deliver_at: self.now + delay,
                to,
                write,
                fence,
                change: c,
            });
        }
        self.changes[c].step = Step::Collect;
        self.deliver();
        true
    }

    fn release(&mut self, c: usize) {
        let holder = self.changes[c].holder;
        let Some(fence) = self.changes[c].fence else {
            return;
        };
        // Some holders never release: their grants run out.
        if self.rng.random_bool(0.2) {
            return;
        }
        let closing = self.nodes[holder].hlc_now(holder, self.now).min(fence.ceiling);
        self.note(format!(
            "change {c}: epoch {} released at {closing}",
            fence.epoch
        ));
        for n in self.changes[c].granted.clone() {
            if self.reaches(holder, n) {
                self.nodes[n].ledger.release(fence.holder, fence.epoch, closing);
            }
        }
    }

    /// Delivers every message due, to a node that is up and reached.
    fn deliver(&mut self) {
        let now = self.now;
        let (due, held): (Vec<Message>, Vec<Message>) = std::mem::take(&mut self.messages)
            .into_iter()
            .partition(|m| m.deliver_at <= now);
        self.messages = held;
        for message in due {
            let holder = self.changes[message.change].holder;
            if !self.reaches(holder, message.to) && message.to != holder {
                continue;
            }
            if !self.nodes[message.to].up {
                continue;
            }
            let write = &self.writes[message.write];
            let (key, version) = (write.key.clone(), write.version.clone());
            // Without the window a write is checked inside a window it always fits.
            let fence = match self.mutation {
                Mutation::NoWindow => LeaseFence {
                    floor: HlcTimestamp::zero(),
                    ceiling: HlcTimestamp::from_record(u64::MAX, 0, 0),
                    ..message.fence
                },
                _ => message.fence,
            };
            let admission = if self.mutation == Mutation::NoFencing {
                Ok(false)
            } else {
                self.nodes[message.to].ledger.admit(Some(&fence), version.hlc, now)
            };
            self.note(format!(
                "write {} to node {}: {admission:?}",
                message.write, message.to
            ));
            let admitted = admission.is_ok();
            if admitted {
                self.nodes[message.to].apply(&key, &version);
                if self.changes[message.change].step == Step::Collect
                    && self.reaches(message.to, holder)
                {
                    self.changes[message.change].acks.insert(message.to);
                }
            }
        }
    }

    fn anti_entropy(&mut self, a: usize, b: usize) {
        let from_a: Vec<(String, Version)> =
            self.nodes[a].store.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let from_b: Vec<(String, Version)> =
            self.nodes[b].store.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (key, version) in from_a {
            self.nodes[b].apply(&key, &version);
        }
        for (key, version) in from_b {
            self.nodes[a].apply(&key, &version);
        }
    }

    fn run(&mut self, steps: usize) {
        let mut active: Vec<usize> = Vec::new();
        for _ in 0..steps {
            self.now += self.rng.random_range(0..700);
            match self.rng.random_range(0..100) {
                0..=19 => {
                    self.start_change();
                    active.push(self.changes.len() - 1);
                }
                20..=64 if !active.is_empty() => {
                    let i = self.rng.random_range(0..active.len());
                    if !self.advance(active[i]) {
                        active.swap_remove(i);
                    }
                }
                65..=79 => {
                    let n = self.rng.random_range(0..self.nodes.len());
                    self.nodes[n].isolated_until =
                        self.now + self.rng.random_range(0..2 * LEASE_MS);
                    let until = self.nodes[n].isolated_until;
                    self.note(format!("node {n} cut off until {until}"));
                }
                80..=83 => {
                    // A restart: the ledger keeps only what it keeps on disk.
                    let n = self.rng.random_range(0..self.nodes.len());
                    let record: LedgerRecord = self.nodes[n].ledger.record();
                    self.nodes[n].ledger = LeaseLedger::from_record(record);
                    self.note(format!("node {n} restarted"));
                }
                84..=93 => {
                    let a = self.rng.random_range(0..self.nodes.len());
                    let b = self.rng.random_range(0..self.nodes.len());
                    if a != b && self.reaches(a, b) {
                        self.anti_entropy(a, b);
                    }
                }
                _ => {}
            }
            self.deliver();
        }
        // The end: every message lands or is lost, every node is reached and
        // brought to the newest version of every key.
        self.now += 10 * LEASE_MS;
        for node in &mut self.nodes {
            node.isolated_until = 0;
        }
        // A change waiting for its node's turn goes on once the change
        // holding it is over.
        while !active.is_empty() {
            active.retain(|&c| self.advance(c));
        }
        self.deliver();
        for a in 0..self.nodes.len() {
            for b in 0..self.nodes.len() {
                if a != b {
                    self.anti_entropy(a, b);
                }
            }
        }
    }

    /// Whether `ancestor` is in the history of write `of`.
    fn descends(&self, of: usize, ancestor: usize) -> bool {
        let mut at = Some(of);
        while let Some(write) = at {
            if write == ancestor {
                return true;
            }
            at = self.writes[write].based_on;
        }
        false
    }

    /// What is wrong with the history, if anything.
    fn violation(&self) -> Option<String> {
        let end = &self.nodes[0].store;
        if self.nodes.iter().any(|node| &node.store != end) {
            return Some("the nodes did not converge".into());
        }
        let mut by_key: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (i, write) in self.writes.iter().enumerate() {
            if write.confirmed {
                by_key.entry(write.key.as_str()).or_default().push(i);
            }
        }
        for (key, mut confirmed) in by_key {
            let Some(last) = end.get(key) else {
                return Some(format!("{key}: confirmed, and on no node at the end"));
            };
            for &write in &confirmed {
                if !self.descends(last.write, write) {
                    return Some(format!(
                        "{key}: confirmed write {write} lost; the end is write {} ({:?})",
                        last.write, self.writes[last.write]
                    ));
                }
            }
            confirmed.sort_by_key(|&w| self.writes[w].version.rank());
            for pair in confirmed.windows(2) {
                if !self.descends(pair[1], pair[0]) {
                    return Some(format!(
                        "{key}: confirmed write {} did not read confirmed write {}",
                        pair[1], pair[0]
                    ));
                }
            }
        }
        for name in NAMES {
            let alive: Vec<(&String, &Version)> = end
                .iter()
                .filter(|(_, v)| v.value.as_ref().is_some_and(|value| value.name == name))
                .collect();
            let confirmed: Vec<&(&String, &Version)> = alive
                .iter()
                .filter(|(key, _)| {
                    self.writes
                        .iter()
                        .any(|w| &w.key == *key && w.based_on.is_none() && w.confirmed)
                })
                .collect();
            if confirmed.len() > 1 {
                return Some(format!("{name}: {} confirmed users alive", confirmed.len()));
            }
            // The directory's rule: the later epoch, then the earlier creation, then the key.
            let answers = alive.iter().min_by_key(|(key, v)| {
                let value = v.value.as_ref().map(|value| (value.created_epoch, value.created_at));
                let (epoch, at) = value.unwrap_or_default();
                (std::cmp::Reverse(epoch), at, (*key).clone())
            });
            if let (Some(&&(confirmed_key, _)), Some((answer_key, _))) =
                (confirmed.first(), answers)
            {
                if confirmed_key != *answer_key {
                    return Some(format!(
                        "{name}: answered by {answer_key}, not by the confirmed {confirmed_key}"
                    ));
                }
            }
        }
        None
    }
}

fn histories() -> u64 {
    std::env::var("S4_IAM_MODEL_HISTORIES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000)
}

/// One history of `seed`: what is wrong with it, and how many changes were
/// confirmed.
fn history(seed: u64, mutation: Mutation) -> (Option<String>, usize) {
    let mut model = Model::new(seed, mutation);
    model.run(400);
    let confirmed = model.writes.iter().filter(|w| w.confirmed).count();
    let violation = model.violation();
    if violation.is_some() && std::env::var("S4_IAM_MODEL_SEED").is_ok() {
        eprintln!("{} nodes\n{}", model.nodes.len(), model.trace.join("\n"));
    }
    (violation, confirmed)
}

#[test]
fn random_histories_lose_nothing_confirmed_and_keep_names_unique() {
    let seeds: Vec<u64> = match std::env::var("S4_IAM_MODEL_SEED").ok().and_then(|s| s.parse().ok())
    {
        Some(seed) => vec![seed],
        None => {
            let base: u64 = rand::rng().random();
            // A fixed run first, then random ones.
            (0..histories())
                .map(|i| {
                    if i < histories() / 2 {
                        i
                    } else {
                        base.wrapping_add(i)
                    }
                })
                .collect()
        }
    };
    let mut confirmed = 0;
    for seed in seeds {
        let (violation, count) = history(seed, Mutation::None);
        if let Some(violation) = violation {
            panic!("seed {seed} (S4_IAM_MODEL_SEED={seed}): {violation}");
        }
        confirmed += count;
    }
    assert!(
        confirmed > 0,
        "no history confirmed a change: the model tests nothing"
    );
}

/// The first history of the fixed seeds `mutation` breaks, if any does.
fn counterexample(mutation: Mutation) -> Option<(u64, String)> {
    (0..2_000u64).find_map(|seed| history(seed, mutation).0.map(|v| (seed, v)))
}

/// Histories that found a defect once, kept green: two changes of one node
/// sharing an epoch, and a delete only one node held freeing a name a later
/// change brought back.
#[test]
fn the_counterexamples_found_stay_fixed() {
    for seed in [7_241_580_682_314_574_484, 2_503_380_748_822_901_893] {
        if let (Some(violation), _) = history(seed, Mutation::None) {
            panic!("seed {seed}: {violation}");
        }
    }
}

#[test]
fn without_fencing_the_model_finds_a_counterexample() {
    for mutation in [Mutation::NoFencing, Mutation::NoWindow] {
        let found = counterexample(mutation);
        eprintln!("{mutation:?}: {found:?}");
        assert!(
            found.is_some(),
            "the model found nothing wrong with {mutation:?}: it cannot tell that part works"
        );
    }
}

/// Fencing a read is what keeps a change of an earlier lease from being
/// confirmed after a later holder read past it. Three nodes, a timeline the
/// random histories rarely hit:
///
/// - 0 ms: node A takes epoch 5 from A and B at once; C gets the request
///   late, at 1 400 ms, so its grant runs to 6 400 ms.
/// - 4 400 ms: A writes the change; its own copy lands, B's and C's are held.
/// - 5 100 ms: B takes epoch 6 from A and B, whose grants ran out; C is busy.
/// - 5 150 ms: A is cut off; B reads from B and C, and misses A's change.
/// - 6 000 ms: A's held copy reaches C, inside C's grant of epoch 5.
///
/// Admitted there, it is acknowledged by A and C, a majority: confirmed,
/// though B's change, written after its read, never saw it. The read taught
/// C epoch 6, so C refuses it.
#[test]
fn a_read_under_the_lease_fences_a_late_write_the_reader_missed() {
    let (a, b) = (NodeId(1), NodeId(2));
    let fence_5 = LeaseFence {
        epoch: 5,
        holder: a,
        floor: HlcTimestamp::zero(),
        ceiling: HlcTimestamp::from_record(5_000, 0, 0),
    };
    let fence_6 = LeaseFence {
        epoch: 6,
        holder: b,
        floor: fence_5.ceiling,
        ceiling: HlcTimestamp::from_record(10_100, 0, 0),
    };
    let write_5 = HlcTimestamp::from_record(4_400, 0, 1);

    for read_fencing in [true, false] {
        let mut ledgers = [
            LeaseLedger::default(),
            LeaseLedger::default(),
            LeaseLedger::default(),
        ];
        let [node_a, node_b, node_c] = &mut ledgers;
        assert!(matches!(
            node_a.acquire(a, 5, fence_5.ceiling, LEASE_MS, 0).0,
            Grant::Granted { .. }
        ));
        assert!(matches!(
            node_b.acquire(a, 5, fence_5.ceiling, LEASE_MS, 0).0,
            Grant::Granted { .. }
        ));
        assert!(matches!(
            node_c.acquire(a, 5, fence_5.ceiling, LEASE_MS, 1_400).0,
            Grant::Granted { .. }
        ));
        assert!(node_a.admit(Some(&fence_5), write_5, 4_400).is_ok());

        assert!(matches!(
            node_a.acquire(b, 6, fence_6.ceiling, LEASE_MS, 5_100).0,
            Grant::Granted { .. }
        ));
        assert!(matches!(
            node_b.acquire(b, 6, fence_6.ceiling, LEASE_MS, 5_100).0,
            Grant::Granted { .. }
        ));
        assert!(matches!(
            node_c.acquire(b, 6, fence_6.ceiling, LEASE_MS, 5_100).0,
            Grant::Busy { .. }
        ));
        if read_fencing {
            assert_eq!(node_b.admit_read(Some(&fence_6)), Ok(false));
            assert_eq!(node_c.admit_read(Some(&fence_6)), Ok(true));
        }

        let late = node_c.admit(Some(&fence_5), write_5, 6_000);
        if read_fencing {
            assert!(matches!(
                late,
                Err(Fenced::EpochBehind {
                    epoch: 5,
                    max_epoch: 6
                })
            ));
        } else {
            assert!(
                late.is_ok(),
                "without fencing reads the late copy lands: A and C confirm it"
            );
        }
    }
}
