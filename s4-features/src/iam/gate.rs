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

//! Whether this node checks IAM credentials against its own copy of IAM.
//!
//! A cluster node checks passwords, tokens and signatures against its own
//! copy, without asking another node (plan CLUSTER-IAM-REPLICATION.md,
//! decision 4). Until a majority of its pool has agreed with that copy since
//! the node started, or once no majority has for longer than the staleness
//! bound, it does not: the copy may lack a deleted user, a removed key or a
//! lowered role. Such a request is refused as unavailable — to be retried
//! through another node — rather than as a wrong credential. Root and the S3
//! credentials of the environment are no IAM credentials and pass.

/// Why IAM credentials are not checked on this node for now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotServing {
    /// No majority of the pool has agreed with this node's copy since it
    /// started.
    Starting,
    /// No majority of the pool has agreed with it for longer than the
    /// staleness bound.
    Stale,
    /// The secret of the S3 credentials is sealed with a key this node does
    /// not have: the nodes are configured with different IAM encryption keys.
    SecretKeyUnknown,
}

impl std::fmt::Display for NotServing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Starting => write!(
                f,
                "this node is still confirming its copy of IAM with its pool"
            ),
            Self::Stale => write!(
                f,
                "this node has not confirmed its copy of IAM with its pool for longer than \
                 S4_IAM_MAX_STALENESS_SECS"
            ),
            Self::SecretKeyUnknown => write!(
                f,
                "this node does not have the key the secret of these S3 credentials is \
                 encrypted with: set the same S4_IAM_ENCRYPTION_KEY on every node"
            ),
        }
    }
}

/// Opens and closes the checking of IAM credentials on this node.
pub trait CredentialGate: Send + Sync {
    /// Whether IAM credentials may be checked now.
    ///
    /// # Errors
    ///
    /// [`NotServing`] while they may not.
    fn admit(&self) -> Result<(), NotServing>;
}

/// Opens IAM changes once no node of the pool keeps IAM of its own that the
/// pool has not taken over (plan CLUSTER-IAM-REPLICATION.md, decision 8).
pub trait AdoptionGate: Send + Sync {
    /// Whether IAM may be changed through the pool now.
    fn is_open(&self) -> bool;
}
