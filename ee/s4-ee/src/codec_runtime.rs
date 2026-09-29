// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Runtime controls for CPU-heavy erasure coding work.
//!
//! The codec crate is synchronous by design. Async callers use this module to
//! acquire an EC-specific permit and then run the CPU job on `spawn_blocking`.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use s4_codec::CodecError;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Default target CPU budget for EC codec work.
pub const DEFAULT_EC_CPU_BUDGET_PERCENT: u8 = 60;
/// Default maximum encoded input bytes allowed in flight.
pub const DEFAULT_EC_MAX_INFLIGHT_ENCODE_BYTES: u64 = 256 * 1_024 * 1_024;

/// EC-specific CPU scheduler configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EcCodecCpuConfig {
    /// Hard cap for concurrent codec jobs.
    pub workers: NonZeroUsize,
    /// Target maximum CPU budget percentage.
    pub cpu_budget_percent: u8,
    /// Hard cap for bytes currently being encoded.
    pub max_inflight_encode_bytes: u64,
}

impl EcCodecCpuConfig {
    /// Parse `S4_EC_CPU_*` scheduler settings from the environment.
    pub fn from_env() -> Result<Self, EcCpuError> {
        let cpu_budget_percent =
            parse_optional_u8("S4_EC_CPU_BUDGET_PERCENT")?.unwrap_or(DEFAULT_EC_CPU_BUDGET_PERCENT);
        if cpu_budget_percent == 0 || cpu_budget_percent > 100 {
            return Err(EcCpuError::InvalidConfig {
                name: "S4_EC_CPU_BUDGET_PERCENT",
                reason: "must be between 1 and 100".to_string(),
            });
        }

        let available = std::thread::available_parallelism().map(NonZeroUsize::get).unwrap_or(1);
        let default_workers = (available * usize::from(cpu_budget_percent)).div_ceil(100).max(1);
        let workers = parse_optional_usize("S4_EC_CPU_WORKERS")?.unwrap_or(default_workers);
        let workers = NonZeroUsize::new(workers).ok_or_else(|| EcCpuError::InvalidConfig {
            name: "S4_EC_CPU_WORKERS",
            reason: "must be greater than 0".to_string(),
        })?;

        let max_inflight_encode_bytes = parse_optional_u64("S4_EC_MAX_INFLIGHT_ENCODE_BYTES")?
            .unwrap_or(DEFAULT_EC_MAX_INFLIGHT_ENCODE_BYTES);
        if max_inflight_encode_bytes == 0 {
            return Err(EcCpuError::InvalidConfig {
                name: "S4_EC_MAX_INFLIGHT_ENCODE_BYTES",
                reason: "must be greater than 0".to_string(),
            });
        }

        Ok(Self {
            workers,
            cpu_budget_percent,
            max_inflight_encode_bytes,
        })
    }
}

impl Default for EcCodecCpuConfig {
    fn default() -> Self {
        let workers = NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN);
        Self {
            workers,
            cpu_budget_percent: DEFAULT_EC_CPU_BUDGET_PERCENT,
            max_inflight_encode_bytes: DEFAULT_EC_MAX_INFLIGHT_ENCODE_BYTES,
        }
    }
}

/// EC-specific CPU limiter used before `spawn_blocking`.
#[derive(Debug, Clone)]
pub struct EcCodecCpuLimiter {
    config: EcCodecCpuConfig,
    semaphore: Arc<Semaphore>,
    inflight_encode_bytes: Arc<AtomicU64>,
}

impl EcCodecCpuLimiter {
    /// Build a limiter from explicit configuration.
    pub fn new(config: EcCodecCpuConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.workers.get())),
            inflight_encode_bytes: Arc::new(AtomicU64::new(0)),
            config,
        }
    }

    /// Build a limiter from environment variables.
    pub fn from_env() -> Result<Self, EcCpuError> {
        Ok(Self::new(EcCodecCpuConfig::from_env()?))
    }

    /// Current scheduler configuration.
    pub fn config(&self) -> &EcCodecCpuConfig {
        &self.config
    }

    /// Current encoded input bytes in flight.
    pub fn inflight_encode_bytes(&self) -> u64 {
        self.inflight_encode_bytes.load(Ordering::Relaxed)
    }

    /// Run an encode job after acquiring EC-specific worker and byte permits.
    pub async fn run_encode<F, R>(&self, input_bytes: u64, job: F) -> Result<R, EcCpuError>
    where
        F: FnOnce() -> Result<R, CodecError> + Send + 'static,
        R: Send + 'static,
    {
        let permit = self.acquire_worker().await?;
        let bytes = self.reserve_encode_bytes(input_bytes)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _bytes = bytes;
            job()
        })
        .await
        .map_err(|e| EcCpuError::Join {
            reason: e.to_string(),
        })?
        .map_err(EcCpuError::Codec)
    }

    /// Run a decode or repair job after acquiring an EC-specific worker permit.
    pub async fn run_decode_or_repair<F, R>(&self, job: F) -> Result<R, EcCpuError>
    where
        F: FnOnce() -> Result<R, CodecError> + Send + 'static,
        R: Send + 'static,
    {
        let permit = self.acquire_worker().await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            job()
        })
        .await
        .map_err(|e| EcCpuError::Join {
            reason: e.to_string(),
        })?
        .map_err(EcCpuError::Codec)
    }

    async fn acquire_worker(&self) -> Result<OwnedSemaphorePermit, EcCpuError> {
        self.semaphore.clone().acquire_owned().await.map_err(|_| EcCpuError::Closed)
    }

    fn reserve_encode_bytes(&self, bytes: u64) -> Result<EncodeBytePermit, EcCpuError> {
        loop {
            let current = self.inflight_encode_bytes.load(Ordering::Acquire);
            let Some(next) = current.checked_add(bytes) else {
                return Err(EcCpuError::Backpressure {
                    reason: "inflight encode bytes overflowed u64".to_string(),
                });
            };
            if next > self.config.max_inflight_encode_bytes {
                return Err(EcCpuError::Backpressure {
                    reason: format!(
                        "encoding {bytes} byte(s) would exceed EC encode budget {}",
                        self.config.max_inflight_encode_bytes
                    ),
                });
            }
            if self
                .inflight_encode_bytes
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(EncodeBytePermit {
                    bytes,
                    counter: Arc::clone(&self.inflight_encode_bytes),
                });
            }
        }
    }
}

struct EncodeBytePermit {
    bytes: u64,
    counter: Arc<AtomicU64>,
}

impl Drop for EncodeBytePermit {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

/// EC CPU scheduling errors.
#[derive(Debug, Error)]
pub enum EcCpuError {
    /// Invalid environment configuration.
    #[error("invalid {name}: {reason}")]
    InvalidConfig {
        /// Environment variable name.
        name: &'static str,
        /// Human-readable validation failure.
        reason: String,
    },

    /// Limiter is closed.
    #[error("EC CPU limiter is closed")]
    Closed,

    /// EC work was rejected by a limiter.
    #[error("EC CPU backpressure: {reason}")]
    Backpressure {
        /// Human-readable rejection reason.
        reason: String,
    },

    /// Codec job returned an error.
    #[error(transparent)]
    Codec(#[from] CodecError),

    /// Blocking task failed to join.
    #[error("EC codec worker failed: {reason}")]
    Join {
        /// Join failure reason.
        reason: String,
    },
}

fn parse_optional_u8(name: &'static str) -> Result<Option<u8>, EcCpuError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u8>().map(Some).map_err(|e| EcCpuError::InvalidConfig {
            name,
            reason: e.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn parse_optional_usize(name: &'static str) -> Result<Option<usize>, EcCpuError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<usize>().map(Some).map_err(|e| EcCpuError::InvalidConfig {
            name,
            reason: e.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

fn parse_optional_u64(name: &'static str) -> Result<Option<u64>, EcCpuError> {
    match std::env::var(name) {
        Ok(raw) => raw.parse::<u64>().map(Some).map_err(|e| EcCpuError::InvalidConfig {
            name,
            reason: e.to_string(),
        }),
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use s4_codec::{CodecEngine, EcProfileSnapshot, ReedSolomonCodec};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn encode_jobs_release_byte_budget() {
        let limiter = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 128,
        });
        let codec = ReedSolomonCodec::new(EcProfileSnapshot::rs_small()).unwrap();
        let encoded =
            limiter.run_encode(64, move || codec.encode_chunk(b"hello", 1)).await.unwrap();
        assert_eq!(encoded.original_size, 5);
        assert_eq!(limiter.inflight_encode_bytes(), 0);
    }

    #[tokio::test]
    async fn encode_budget_rejects_oversized_job() {
        let limiter = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 8,
        });
        let err = limiter
            .run_encode(9, || {
                let codec = ReedSolomonCodec::new(EcProfileSnapshot::rs_small())?;
                codec.encode_chunk(b"hello", 1)
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("backpressure"));
    }

    #[tokio::test]
    async fn worker_permit_limits_concurrency() {
        let limiter = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024,
        });
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let first = {
            let limiter = limiter.clone();
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);
            tokio::spawn(async move {
                limiter
                    .run_decode_or_repair(move || {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(now, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(30));
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, CodecError>(())
                    })
                    .await
            })
        };
        let second = {
            let limiter = limiter.clone();
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);
            tokio::spawn(async move {
                limiter
                    .run_decode_or_repair(move || {
                        let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                        max_seen.fetch_max(now, Ordering::SeqCst);
                        active.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, CodecError>(())
                    })
                    .await
            })
        };

        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn encode_byte_permit_survives_cancelled_awaiter() {
        let limiter = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(2).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 64,
        });
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let handle = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                limiter
                    .run_encode(64, move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, CodecError>(())
                    })
                    .await
            })
        };

        started_rx.await.unwrap();
        handle.abort();
        tokio::task::yield_now().await;

        assert_eq!(limiter.inflight_encode_bytes(), 64);
        let err = limiter.run_encode(1, || Ok::<_, CodecError>(())).await.unwrap_err();
        assert!(err.to_string().contains("backpressure"));

        release_tx.send(()).unwrap();
        wait_for_inflight_bytes(&limiter, 0).await;
    }

    #[tokio::test]
    async fn worker_permit_survives_cancelled_awaiter() {
        let limiter = EcCodecCpuLimiter::new(EcCodecCpuConfig {
            workers: NonZeroUsize::new(1).unwrap(),
            cpu_budget_percent: 60,
            max_inflight_encode_bytes: 1024,
        });
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let handle = {
            let limiter = limiter.clone();
            tokio::spawn(async move {
                limiter
                    .run_decode_or_repair(move || {
                        let _ = started_tx.send(());
                        let _ = release_rx.recv();
                        Ok::<_, CodecError>(())
                    })
                    .await
            })
        };

        started_rx.await.unwrap();
        handle.abort();
        tokio::task::yield_now().await;

        let blocked = tokio::time::timeout(
            Duration::from_millis(30),
            limiter.run_decode_or_repair(|| Ok::<_, CodecError>(())),
        )
        .await;
        assert!(
            blocked.is_err(),
            "worker permit was released before blocking job exited"
        );

        release_tx.send(()).unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            limiter.run_decode_or_repair(|| Ok::<_, CodecError>(())),
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn wait_for_inflight_bytes(limiter: &EcCodecCpuLimiter, expected: u64) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while limiter.inflight_encode_bytes() != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
