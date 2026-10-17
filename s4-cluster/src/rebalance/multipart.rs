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

//! Handing an open multipart upload to replicas that lack it.
//!
//! An upload begun before the pool grew lives on the old owners of its key
//! only, while its later parts and its completion need the new owners too
//! (the joint quorum). A node holding the upload hands it over as it holds
//! it: the session where the replica has none, then every part the replica
//! lacks, read back from this node's disk against its CRC32 and SHA-256 and
//! sent as the coordinator sends a part. The replica computes the checksums
//! of what it received and reports them in the part's record; a record that
//! differs from this node's is a copy damaged on its way, and the upload is
//! handed over again later.
//!
//! A replica's part is replaced only by a newer one of the same number: a
//! part a client uploaded again through the joint quorum may be newer on the
//! replica than here. An upload that is completing, completed or aborted is
//! not handed over; one aborted or completed while it was being handed over
//! is aborted again on the replicas that received its session from here, so
//! that none is left behind.

use std::sync::Arc;
use std::time::Duration;

use s4_core::types::composite::{
    MultipartPartRecord, MultipartUploadSession, MultipartUploadState,
};

use crate::replica::MultipartReplicaStorage;
use crate::rpc::{proto, NodeClient};
use crate::{ClusterError, NodeId, PlacementRouter};

/// Bytes of a part sent per message.
const CHUNK: usize = 1024 * 1024;

/// What became of handing one upload over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadHandedOver {
    /// Every replica asked holds the session and this node's parts.
    InStep {
        /// Parts sent.
        parts: u64,
        /// Bytes of the parts sent.
        bytes: u64,
    },
    /// The upload is no longer open here: there is nothing to hand over.
    Gone,
    /// A replica could not be brought to the upload now.
    Failed(String),
    /// A part arrived with other checksums than it left with.
    Damaged(String),
}

impl UploadHandedOver {
    /// The code `PoolMove` answers a hand-over with.
    pub fn outcome(&self) -> (u32, String) {
        match self {
            Self::InStep { .. } => (1, String::new()),
            Self::Gone => (2, String::new()),
            Self::Failed(why) => (3, why.clone()),
            Self::Damaged(why) => (4, why.clone()),
        }
    }

    /// The outcome a `PoolMove` answer names, `why` for the failures.
    pub fn from_outcome(code: u32, why: String) -> Self {
        match code {
            1 => Self::InStep { parts: 0, bytes: 0 },
            2 => Self::Gone,
            4 => Self::Damaged(why),
            0 => Self::Failed("the replica asked does not hand uploads over".into()),
            _ => Self::Failed(why),
        }
    }
}

/// Where an upload stands on the replicas of its key — in a growing pool,
/// of both generations.
///
/// A completion or an abort removes the session from the replicas it
/// reaches; one that missed it keeps the session open, stale, until the
/// upload's age expires it. So the upload lives only while a write quorum of
/// some group of the key's replicas holds it open or completing: every part
/// and the completion need that quorum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UploadStanding {
    /// The upload lives.
    Open {
        /// Replicas holding it open.
        holders: Vec<NodeId>,
        /// Replicas that answered with no session of it.
        lacking: Vec<NodeId>,
    },
    /// Fewer replicas than a write quorum of any group hold it, even with
    /// those that did not answer: it was completed or aborted.
    Closed {
        /// Replicas that still hold it open: the completion or the abort
        /// missed them.
        stale: Vec<NodeId>,
    },
    /// Replicas that did not answer decide it.
    Unknown(String),
}

/// Hands open uploads of this node to the replicas of their keys.
pub struct MultipartTransfer {
    storage: Arc<dyn MultipartReplicaStorage>,
    rpc_client: Arc<NodeClient>,
    router: Arc<PlacementRouter>,
    local: NodeId,
    topology_epoch: u64,
    call_timeout: Duration,
}

impl MultipartTransfer {
    /// Hands over the uploads of `storage`, node `local`'s, to the replicas
    /// `router` places their keys on.
    pub fn new(
        storage: Arc<dyn MultipartReplicaStorage>,
        rpc_client: Arc<NodeClient>,
        router: Arc<PlacementRouter>,
        local: NodeId,
        topology_epoch: u64,
    ) -> Self {
        Self {
            storage,
            rpc_client,
            router,
            local,
            topology_epoch,
            call_timeout: Duration::from_secs(60),
        }
    }

    /// This node's sessions, a page at a time.
    pub(crate) async fn sessions_after(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MultipartUploadSession>, ClusterError> {
        self.storage
            .list_multipart_sessions_after(after, limit)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }

    /// Removes this node's own copy of the upload `upload_id`, its session
    /// and parts, without touching any other replica: a former owner's copy
    /// once the owners hold it.
    pub(crate) async fn abort_local(&self, upload_id: &str) -> Result<(), ClusterError> {
        self.storage
            .abort_multipart_native(upload_id)
            .await
            .map_err(|e| ClusterError::Storage(e.to_string()))
    }

    /// The session of `upload_id` on this node, if it holds one.
    pub async fn local_session(&self, upload_id: &str) -> Option<MultipartUploadSession> {
        self.storage.get_multipart_session(upload_id).await.ok()
    }

    /// Where the upload `upload_id` of `bucket/key` stands on the replicas
    /// of its key, this node among them.
    pub async fn standing(&self, bucket: &str, key: &str, upload_id: &str) -> UploadStanding {
        let decision = match self.router.route(bucket, key) {
            Ok(decision) => decision,
            Err(e) => return UploadStanding::Unknown(e.to_string()),
        };
        let mut open = Vec::new();
        let mut alive = Vec::new();
        let mut lacking = Vec::new();
        let mut unanswered = Vec::new();
        for replica in decision.replicas.iter().copied() {
            match self.state_on(replica, bucket, key, upload_id).await {
                Ok(Some(MultipartUploadState::Open)) => {
                    open.push(replica);
                    alive.push(replica);
                }
                Ok(Some(MultipartUploadState::Completing)) => alive.push(replica),
                Ok(None) => lacking.push(replica),
                Err(e) => unanswered.push((replica, e.to_string())),
            }
        }
        let mut undecided = None;
        for requirement in decision.write_rule.requirements() {
            let holding = requirement.nodes.iter().filter(|node| alive.contains(node)).count();
            let maybe = requirement
                .nodes
                .iter()
                .filter(|node| unanswered.iter().any(|(replica, _)| replica == *node))
                .count();
            if holding >= usize::from(requirement.count) {
                return UploadStanding::Open {
                    holders: open,
                    lacking,
                };
            }
            if holding + maybe >= usize::from(requirement.count) {
                undecided = unanswered.first().cloned();
            }
        }
        match undecided {
            Some((replica, why)) => UploadStanding::Unknown(format!("replica {replica}: {why}")),
            None => UploadStanding::Closed { stale: open },
        }
    }

    /// Brings the replicas of `bucket/key` among `targets` — every one when
    /// `None` — that hold no session of the upload `upload_id` to it, while
    /// it lives: handed over from here when this node holds it open, else
    /// asked of a replica that does ([`Self::ask_to_hand_over`]).
    pub async fn bring_to(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        targets: Option<&[NodeId]>,
    ) -> UploadHandedOver {
        let (holders, lacking) = match self.standing(bucket, key, upload_id).await {
            UploadStanding::Open { holders, lacking } => (holders, lacking),
            UploadStanding::Closed { .. } => return UploadHandedOver::Gone,
            UploadStanding::Unknown(why) => return UploadHandedOver::Failed(why),
        };
        let wanted: Vec<NodeId> = lacking
            .into_iter()
            .filter(|node| targets.is_none_or(|targets| targets.contains(node)))
            .collect();
        if wanted.is_empty() {
            return UploadHandedOver::InStep { parts: 0, bytes: 0 };
        }
        if holders.contains(&self.local) {
            if let Some(session) = self.local_session(upload_id).await {
                return self.hand_over_live(&session, Some(&wanted)).await;
            }
        }
        let mut outcome = UploadHandedOver::Failed("no replica holds the upload open".into());
        for holder in holders.into_iter().filter(|holder| *holder != self.local) {
            outcome = self.ask_to_hand_over(holder, bucket, key, upload_id, &wanted).await;
            if matches!(
                outcome,
                UploadHandedOver::InStep { .. } | UploadHandedOver::Gone
            ) {
                return outcome;
            }
        }
        outcome
    }

    /// Asks `holder` to hand the upload over to `targets` (`PoolMove`).
    pub async fn ask_to_hand_over(
        &self,
        holder: NodeId,
        bucket: &str,
        key: &str,
        upload_id: &str,
        targets: &[NodeId],
    ) -> UploadHandedOver {
        let pool_id = match self.router.route(bucket, key) {
            Ok(decision) => decision.pool_id,
            Err(e) => return UploadHandedOver::Failed(e.to_string()),
        };
        let request = proto::PoolMoveRequest {
            pool_id: pool_id.as_u32(),
            hand_over_upload: Some(proto::UploadHandOver {
                bucket: bucket.to_string(),
                key: key.to_string(),
                upload_id: upload_id.to_string(),
                targets: targets.iter().map(|node| node.to_bytes().to_vec()).collect(),
            }),
            ..proto::PoolMoveRequest::default()
        };
        match self.call(self.rpc_client.send_pool_move(holder, request)).await {
            Ok(response) => UploadHandedOver::from_outcome(
                response.upload_outcome,
                format!("replica {holder}: {}", response.upload_error),
            ),
            Err(e) => UploadHandedOver::Failed(format!("replica {holder}: {e}")),
        }
    }

    /// Answers a `hand_over_upload` asked of this node: the upload as this
    /// node holds it, to `request.targets`.
    pub async fn answer_hand_over(&self, request: &proto::UploadHandOver) -> UploadHandedOver {
        let targets: Vec<NodeId> = match request
            .targets
            .iter()
            .map(|bytes| NodeId::from_bytes(bytes))
            .collect::<Result<_, _>>()
        {
            Ok(targets) => targets,
            Err(e) => return UploadHandedOver::Failed(format!("a target is not a node: {e}")),
        };
        match self.local_session(&request.upload_id).await {
            Some(session) if session.bucket == request.bucket && session.key == request.key => {
                self.hand_over(&session, Some(&targets)).await
            }
            _ => UploadHandedOver::Failed("this node holds no session of the upload".into()),
        }
    }

    /// Aborts the stale session of `upload_id` on `target`, which a
    /// completion or an abort missed: one that is handed over would bring a
    /// closed upload back.
    pub async fn abort_stale(
        &self,
        target: NodeId,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ClusterError> {
        let placement_generation = self.router.route(bucket, key)?.placement_generation;
        if target == self.local {
            return self.abort_local(upload_id).await;
        }
        let response = self
            .call(self.rpc_client.send_multipart_abort(
                target,
                proto::AbortMultipartUploadReplicaRequest {
                    upload_id: upload_id.to_string(),
                    coordinator_id: self.local.to_bytes().to_vec(),
                    operation_id: operation_id(),
                    topology_epoch: self.topology_epoch,
                    placement_generation,
                },
            ))
            .await?;
        if response.success {
            Ok(())
        } else {
            Err(ClusterError::Rpc(response.error))
        }
    }

    /// Hands `session` over to `targets`, or to every other replica of its
    /// key when `None`.
    ///
    /// Only an upload that lives is handed over ([`Self::standing`]): a
    /// session a completion or an abort missed here is stale, and handing it
    /// over would bring a closed upload back on the replicas.
    pub async fn hand_over(
        &self,
        session: &MultipartUploadSession,
        targets: Option<&[NodeId]>,
    ) -> UploadHandedOver {
        if session.state != MultipartUploadState::Open {
            return UploadHandedOver::Gone;
        }
        match self.standing(&session.bucket, &session.key, &session.upload_id).await {
            UploadStanding::Open { .. } => {}
            UploadStanding::Closed { .. } => return UploadHandedOver::Gone,
            UploadStanding::Unknown(why) => return UploadHandedOver::Failed(why),
        }
        self.hand_over_live(session, targets).await
    }

    /// [`Self::hand_over`] of an upload known to live.
    async fn hand_over_live(
        &self,
        session: &MultipartUploadSession,
        targets: Option<&[NodeId]>,
    ) -> UploadHandedOver {
        if session.state != MultipartUploadState::Open {
            return UploadHandedOver::Gone;
        }
        let decision = match self.router.route(&session.bucket, &session.key) {
            Ok(decision) => decision,
            Err(e) => return UploadHandedOver::Failed(e.to_string()),
        };
        let targets: Vec<NodeId> = match targets {
            Some(targets) => targets.to_vec(),
            None => decision.replicas.clone(),
        }
        .into_iter()
        .filter(|node| *node != self.local)
        .collect();
        let parts = match self.storage.list_multipart_parts(&session.upload_id).await {
            Ok(parts) => parts,
            Err(e) => return UploadHandedOver::Failed(e.to_string()),
        };
        let mut created = Vec::new();
        let mut sent = (0u64, 0u64);
        let mut outcome = None;
        for target in targets {
            match self
                .bring(
                    target,
                    session,
                    &parts,
                    decision.placement_generation,
                    &mut created,
                )
                .await
            {
                Ok((count, bytes)) => {
                    sent.0 += count;
                    sent.1 += bytes;
                }
                Err(failure) => {
                    outcome.get_or_insert(failure);
                }
            }
        }
        // Aborted or completed meanwhile: a session this node gave a replica
        // must not outlive it there.
        let still_open = self
            .local_session(&session.upload_id)
            .await
            .is_some_and(|now| now.state == MultipartUploadState::Open);
        if !still_open {
            for target in created {
                let _ = self.abort(target, &session.upload_id, decision.placement_generation).await;
            }
            return UploadHandedOver::Gone;
        }
        outcome.unwrap_or(UploadHandedOver::InStep {
            parts: sent.0,
            bytes: sent.1,
        })
    }

    /// Brings `target` to the session and parts this node holds: the parts
    /// sent and their bytes.
    async fn bring(
        &self,
        target: NodeId,
        session: &MultipartUploadSession,
        parts: &[MultipartPartRecord],
        placement_generation: u64,
        created: &mut Vec<NodeId>,
    ) -> Result<(u64, u64), UploadHandedOver> {
        let failed = |why: String| UploadHandedOver::Failed(format!("replica {target}: {why}"));
        let held = match self
            .inspect(target, &session.bucket, &session.key, &session.upload_id)
            .await
            .map_err(|e| failed(e.to_string()))?
        {
            Some((MultipartUploadState::Open, held)) => held,
            // Completing or done there: its completion is the object's path.
            Some(_) => return Ok((0, 0)),
            None => {
                self.create(target, session, placement_generation)
                    .await
                    .map_err(|e| failed(e.to_string()))?;
                created.push(target);
                Vec::new()
            }
        };
        let mut sent = (0, 0);
        for part in parts {
            let theirs = held.iter().find(|p| p.part_number == part.part_number);
            let behind = match theirs {
                None => true,
                Some(theirs) => {
                    theirs.physical_hash != part.physical_hash
                        && theirs.created_at < part.created_at
                }
            };
            if !behind {
                continue;
            }
            let (record, data) = self
                .storage
                .read_multipart_part(&session.upload_id, part.part_number)
                .await
                .map_err(|e| {
                    UploadHandedOver::Failed(format!(
                        "part {} could not be read here: {e}",
                        part.part_number
                    ))
                })?;
            let bytes = data.len() as u64;
            let received = self
                .send_part(
                    target,
                    &session.upload_id,
                    &record,
                    data,
                    placement_generation,
                )
                .await
                .map_err(|e| failed(e.to_string()))?;
            if received.physical_hash != record.physical_hash
                || received.crc32 != record.crc32
                || received.size != record.size
                || received.etag_md5_hex != record.etag_md5_hex
            {
                return Err(UploadHandedOver::Damaged(format!(
                    "{}: part {} of upload {} arrived at {target} with other checksums than it left with",
                    crate::replica::CHECKSUM_MISMATCH,
                    part.part_number,
                    session.upload_id
                )));
            }
            sent.0 += 1;
            sent.1 += bytes;
        }
        Ok(sent)
    }

    /// The state and parts of the upload on `target`, `None` when it holds
    /// no session of it.
    async fn inspect(
        &self,
        target: NodeId,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<(MultipartUploadState, Vec<MultipartPartRecord>)>, ClusterError> {
        let request = proto::InspectMultipartUploadReplicaRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            coordinator_id: self.local.to_bytes().to_vec(),
            operation_id: operation_id(),
            topology_epoch: self.topology_epoch,
        };
        let response = self.call(self.rpc_client.send_multipart_inspect(target, request)).await?;
        if !response.success {
            if response.error == "NoSuchUpload" {
                return Ok(None);
            }
            return Err(ClusterError::Rpc(if response.epoch_mismatch {
                "on another topology epoch".into()
            } else {
                response.error
            }));
        }
        let held: MultipartUploadSession = bincode::deserialize(&response.session)
            .map_err(|e| ClusterError::Codec(e.to_string()))?;
        let parts = response
            .part_records
            .iter()
            .map(|bytes| {
                bincode::deserialize(bytes).map_err(|e| ClusterError::Codec(e.to_string()))
            })
            .collect::<Result<Vec<MultipartPartRecord>, _>>()?;
        Ok(Some((held.state, parts)))
    }

    /// The state of the upload on `replica`, this node's from its storage;
    /// `None` when it holds no session of it.
    async fn state_on(
        &self,
        replica: NodeId,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Option<MultipartUploadState>, ClusterError> {
        if replica == self.local {
            return Ok(self
                .local_session(upload_id)
                .await
                .filter(|session| session.bucket == bucket && session.key == key)
                .map(|session| session.state));
        }
        Ok(self.inspect(replica, bucket, key, upload_id).await?.map(|(state, _)| state))
    }

    async fn create(
        &self,
        target: NodeId,
        session: &MultipartUploadSession,
        placement_generation: u64,
    ) -> Result<(), ClusterError> {
        let request = proto::CreateMultipartUploadReplicaRequest {
            upload_id: session.upload_id.clone(),
            bucket: session.bucket.clone(),
            key: session.key.clone(),
            content_type: session.content_type.clone(),
            metadata: session.metadata.clone(),
            coordinator_id: self.local.to_bytes().to_vec(),
            operation_id: operation_id(),
            topology_epoch: self.topology_epoch,
            placement_generation,
        };
        let response = self.call(self.rpc_client.send_multipart_create(target, request)).await?;
        if response.success {
            Ok(())
        } else {
            Err(ClusterError::Rpc(response.error))
        }
    }

    async fn send_part(
        &self,
        target: NodeId,
        upload_id: &str,
        record: &MultipartPartRecord,
        data: Vec<u8>,
        placement_generation: u64,
    ) -> Result<MultipartPartRecord, ClusterError> {
        let mut chunks = vec![proto::UploadMultipartPartReplicaChunk {
            payload: Some(proto::upload_multipart_part_replica_chunk::Payload::Header(
                proto::UploadMultipartPartReplicaHeader {
                    upload_id: upload_id.to_string(),
                    part_number: record.part_number,
                    coordinator_id: self.local.to_bytes().to_vec(),
                    operation_id: operation_id(),
                    topology_epoch: self.topology_epoch,
                    total_size: data.len() as u64,
                    placement_generation,
                },
            )),
        }];
        chunks.extend(
            data.chunks(CHUNK).map(|piece| proto::UploadMultipartPartReplicaChunk {
                payload: Some(proto::upload_multipart_part_replica_chunk::Payload::Data(
                    piece.to_vec(),
                )),
            }),
        );
        let response = self
            .call(
                self.rpc_client
                    .send_multipart_upload_part_stream(target, tokio_stream::iter(chunks)),
            )
            .await?;
        if !response.success {
            return Err(ClusterError::Rpc(response.error));
        }
        bincode::deserialize(&response.part_record).map_err(|e| ClusterError::Codec(e.to_string()))
    }

    async fn abort(
        &self,
        target: NodeId,
        upload_id: &str,
        placement_generation: u64,
    ) -> Result<(), ClusterError> {
        let request = proto::AbortMultipartUploadReplicaRequest {
            upload_id: upload_id.to_string(),
            coordinator_id: self.local.to_bytes().to_vec(),
            operation_id: operation_id(),
            topology_epoch: self.topology_epoch,
            placement_generation,
        };
        self.call(self.rpc_client.send_multipart_abort(target, request))
            .await
            .map(|_| ())
    }

    async fn call<T>(
        &self,
        call: impl std::future::Future<Output = Result<T, ClusterError>>,
    ) -> Result<T, ClusterError> {
        tokio::time::timeout(self.call_timeout, call)
            .await
            .map_err(|_| ClusterError::WriteTimeout(self.call_timeout))?
    }
}

fn operation_id() -> Vec<u8> {
    uuid::Uuid::new_v4().as_u128().to_be_bytes().to_vec()
}
