// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Auto-tiering: moving objects between RF=3 and erasure coding.
//!
//! Activity tracking records what is used ([`heat`]), the policy engine decides
//! what that means ([`policy`]), and two workers carry the decision out: one
//! moves cold objects into erasure coding ([`worker`]) and one brings hot ones
//! back ([`promote`]). Both read the same activity records, and the second is
//! also the operator's way to undo the first. Decisions live next to the plan,
//! in `plans/S4-FEDERATION/ALGORITHM-S4-FEDERATION/ERASURE-CODING/EC-v2/adr/`.

pub mod heat;
pub mod policy;
pub mod promote;
pub mod worker;
