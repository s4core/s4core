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

//! Pool generation persistence, validation and readiness contracts.

use s4_cluster::pool_generation::{
    self, GenerationAnnouncement, GenerationPeer, PoolGeneration, PoolGenerationRecord,
};
use s4_cluster::{NodeId, PoolId};
use s4_core::storage::{IndexDb, IndexOptions};

fn nodes(count: u128) -> Vec<NodeId> {
    (1..=count).map(NodeId).collect()
}

fn stable() -> PoolGenerationRecord {
    PoolGenerationRecord::initial(PoolId(0), nodes(3)).unwrap()
}

#[test]
fn append_prepares_the_next_epoch_without_changing_current_owners() {
    let original = stable();
    let growing = original.configure(nodes(6)).unwrap();
    assert_eq!(
        growing.current,
        PoolGeneration {
            epoch: 1,
            nodes: nodes(3)
        }
    );
    assert_eq!(growing.proposed.as_ref().unwrap().epoch, 2);
    assert_eq!(growing.proposed.as_ref().unwrap().nodes, nodes(6));
    assert_eq!(growing.configure(nodes(6)).unwrap(), growing);
    assert!(growing.configure(nodes(9)).unwrap_err().to_string().contains("already"));
    assert_eq!(original.configure(nodes(3)).unwrap(), original);
}

#[test]
fn reordering_removal_replacement_and_duplicate_identities_are_refused() {
    for invalid in [
        vec![NodeId(2), NodeId(1), NodeId(3)],
        nodes(2),
        vec![NodeId(1), NodeId(2), NodeId(4)],
        vec![NodeId(1), NodeId(2), NodeId(3), NodeId(3)],
    ] {
        assert!(stable().configure(invalid).is_err());
    }
    assert!(PoolGenerationRecord::initial(PoolId(0), vec![]).is_err());
}

#[test]
fn an_unstarted_proposal_can_be_withdrawn_but_a_moving_pool_cannot_go_back() {
    let preparing = stable().configure(nodes(6)).unwrap();
    assert_eq!(preparing.configure(nodes(3)).unwrap(), stable());
    let moving = preparing.begin().unwrap();
    assert!(moving.configure(nodes(3)).is_err());
    assert!(moving.configure(nodes(9)).is_err());
}

#[test]
fn generation_announcement_reconstructs_only_the_exact_ordered_prefix() {
    let growing = stable().configure(nodes(6)).unwrap();
    let announcement = GenerationAnnouncement::of(&growing, false);
    assert_eq!(announcement.resolve(PoolId(0), &nodes(6)).unwrap(), growing);
    let mut reordered = nodes(6);
    reordered.swap(0, 1);
    assert!(announcement.resolve(PoolId(0), &reordered).is_err());
    assert!(announcement.resolve(PoolId(1), &nodes(6)).is_err());
    assert!(announcement.resolve(PoolId(0), &nodes(2)).is_err());
}

#[test]
fn the_gate_requires_every_new_member_protocol_and_exact_generation() {
    let growing = stable().configure(nodes(6)).unwrap();
    let same = GenerationAnnouncement::of(&growing, false);
    let ready = |_| {
        Some(GenerationPeer {
            alive: true,
            protocol: s4_cluster::protocol::POOL_GENERATION_MIN_PROTOCOL_VERSION,
            announcement: Some(same),
            metadata_settled: true,
        })
    };
    assert!(growing.gate(ready).is_ok());
    for missing in nodes(6) {
        assert!(growing.gate(|id| if id == missing { None } else { ready(id) }).is_err());
        assert!(growing
            .gate(|id| if id == missing {
                Some(GenerationPeer {
                    protocol: 8,
                    ..ready(id).unwrap()
                })
            } else {
                ready(id)
            })
            .is_err());
        assert!(growing
            .gate(|id| if id == missing {
                Some(GenerationPeer {
                    alive: false,
                    ..ready(id).unwrap()
                })
            } else {
                ready(id)
            })
            .is_err());
        assert!(growing
            .gate(|id| if id == missing {
                Some(GenerationPeer {
                    announcement: Some(GenerationAnnouncement::of(&stable(), false)),
                    ..ready(id).unwrap()
                })
            } else {
                ready(id)
            })
            .is_err());
        assert!(growing
            .gate(|id| if id == missing {
                Some(GenerationPeer {
                    metadata_settled: false,
                    ..ready(id).unwrap()
                })
            } else {
                ready(id)
            })
            .is_err());
    }
    let tentative = GenerationAnnouncement::of(&growing, true);
    assert!(growing
        .gate(|id| Some(GenerationPeer {
            announcement: Some(tentative),
            ..ready(id).unwrap()
        }))
        .is_err());
}

#[tokio::test]
async fn local_generation_is_durable_and_survives_single_mode() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index");
    let options = IndexOptions {
        version_digests: true,
    };
    let growing = stable().configure(nodes(6)).unwrap();
    {
        let index = IndexDb::open(&path, options).unwrap();
        pool_generation::store(&index, &stable()).await.unwrap();
        pool_generation::store(&index, &growing).await.unwrap();
    }
    drop(IndexDb::open(&path, IndexOptions::default()).unwrap());
    let index = IndexDb::open(&path, options).unwrap();
    assert_eq!(pool_generation::load(&index).await.unwrap(), Some(growing));
}

#[tokio::test]
async fn durable_moving_state_cannot_be_replaced_with_a_preparation_or_other_roster() {
    let dir = tempfile::tempdir().unwrap();
    let index = IndexDb::open(
        dir.path(),
        IndexOptions {
            version_digests: true,
        },
    )
    .unwrap();
    let preparing = stable().configure(nodes(6)).unwrap();
    let moving = preparing.begin().unwrap();
    pool_generation::store(&index, &moving).await.unwrap();
    assert!(pool_generation::store(&index, &preparing).await.is_err());
    assert!(pool_generation::store(&index, &stable()).await.is_err());
    assert!(
        pool_generation::store(&index, &stable().configure(nodes(9)).unwrap())
            .await
            .is_err()
    );
    assert_eq!(pool_generation::load(&index).await.unwrap(), Some(moving));
}

#[test]
fn corrupt_records_and_epoch_overflow_are_refused() {
    assert!(PoolGenerationRecord::decode(b"not a generation").is_err());
    let mut record = stable();
    record.current.epoch = u64::MAX;
    assert!(record.configure(nodes(6)).is_err());
    record.current.epoch = 0;
    assert!(record.validate().is_err());
}

/// Only a moving pool settles, on exactly its successor; a settled pool is
/// stable on the grown roster, takes it from `S4_POOL_NODES` as it is, and
/// can grow again.
#[test]
fn a_moving_pool_settles_on_its_successor_and_can_grow_again() {
    let preparing = stable().configure(nodes(6)).unwrap();
    assert!(stable().finish().is_err());
    assert!(preparing.finish().is_err());
    let moving = preparing.begin().unwrap();
    let settled = moving.finish().unwrap();
    assert_eq!(
        settled.current,
        PoolGeneration {
            epoch: 2,
            nodes: nodes(6)
        }
    );
    assert!(settled.proposed.is_none() && !settled.moving);
    assert!(moving.finishes_into(&settled));
    assert!(!preparing.finishes_into(&settled));
    assert!(!moving.finishes_into(&stable()));
    assert_eq!(settled.configure(nodes(6)).unwrap(), settled);
    assert!(settled.configure(nodes(3)).is_err());
    let again = settled.configure(nodes(12)).unwrap();
    assert_eq!(again.proposed.unwrap().epoch, 3);
}

/// The durable record goes from moving to its own settling, and from no
/// other record: neither to another roster nor back from settled.
#[tokio::test]
async fn the_durable_record_settles_only_into_its_own_successor() {
    let dir = tempfile::tempdir().unwrap();
    let index = IndexDb::open(
        dir.path(),
        IndexOptions {
            version_digests: true,
        },
    )
    .unwrap();
    let moving = stable().configure(nodes(6)).unwrap().begin().unwrap();
    pool_generation::store(&index, &moving).await.unwrap();
    let other = PoolGenerationRecord::initial(PoolId(0), nodes(9)).unwrap();
    assert!(pool_generation::store(&index, &other).await.is_err());
    let settled = moving.finish().unwrap();
    pool_generation::store(&index, &settled).await.unwrap();
    assert_eq!(
        pool_generation::load(&index).await.unwrap(),
        Some(settled.clone())
    );
    assert!(pool_generation::store(&index, &moving).await.is_err());
    assert!(pool_generation::store(&index, &stable()).await.is_err());
}

/// The cut-over record of a move only grows, stays sorted, and is told
/// apart from the record of another move.
#[test]
fn the_cut_over_record_merges_and_names_its_move() {
    use pool_generation::CutOverRecord;
    let moving = stable().configure(nodes(6)).unwrap().begin().unwrap();
    let mut record = CutOverRecord::of(&moving).unwrap();
    assert!(CutOverRecord::of(&stable()).is_err());
    assert!(record.merge([30, 10]));
    assert!(record.merge([20, 10]));
    assert!(!record.merge([30]));
    assert_eq!(record.ranges, vec![10, 20, 30]);
    assert_eq!(
        CutOverRecord::decode(&record.encode().unwrap()).unwrap(),
        record
    );
    assert!(record.belongs_to(&moving));
    let later = moving.finish().unwrap().configure(nodes(9)).unwrap();
    assert!(!record.belongs_to(&later));
    assert_ne!(
        pool_generation::cut_over_key(PoolId(0), 2),
        pool_generation::object_key(PoolId(0))
    );
    assert!(CutOverRecord::decode(b"{}").is_err());
}
