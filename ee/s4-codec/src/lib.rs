// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Runtime-independent erasure coding math for S4 Enterprise Edition.
//!
//! This crate is deliberately synchronous and has no dependency on Tokio,
//! gRPC, fjall, or S4 storage internals. Async callers must run codec work
//! through an external CPU limiter before using a blocking executor.

pub mod cpu;
mod engine;
mod gf;
mod loss;
mod lrc;
mod lrc_solver;
pub mod reference;
mod rs;
pub mod test_hooks;
mod types;

pub use engine::{
    select_engine, CodecEngineKind, CodecEngineSelection, CodecEngineSetting, EngineAvailability,
    CODEC_ENGINE_ENV,
};
pub use loss::LossTolerance;
pub use lrc::LrcCodec;
pub use rs::ReedSolomonCodec;
pub use types::{
    build_codec, CodecEngine, CodecError, EcCodecKind, EcParity, EcProfileSnapshot, EncodedChunk,
    FailureMatrix, FailureMatrixEntry, RecoveryPath, Shard, ShardRole, DEFAULT_CHUNK_SIZE,
    DEFAULT_MIN_OBJECT_SIZE,
};
