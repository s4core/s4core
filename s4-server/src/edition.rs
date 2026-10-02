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

//! CE/EE edition resolution — the **only** module with `#[cfg(feature)]` gates.
//!
//! At startup this module determines whether the server is running in
//! Community Edition or Enterprise Edition mode, and produces:
//!
//! - [`ClusterLimits`] — quantitative caps (pools, nodes).
//! - [`ClusterServices`] — trait-object DI container for behavioral features.
//!
//! All downstream code receives these through dependency injection and never
//! checks the edition directly.

use s4_cluster::{ClusterLimits, ClusterServices, S4_VERSION};
use tracing::info;

/// What this start decided about the edition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEdition {
    /// The limits the node runs under.
    pub limits: ClusterLimits,
    /// When the Enterprise licence the node runs under expires, in seconds
    /// since the Unix epoch; `None` when it runs without one. An expired
    /// licence still counts: the node keeps running as Enterprise, degraded.
    pub license_expires_at: Option<i64>,
}

/// Human-readable edition label for logs and /health endpoint.
pub fn edition_label() -> &'static str {
    #[cfg(feature = "enterprise")]
    {
        "Enterprise Edition"
    }
    #[cfg(not(feature = "enterprise"))]
    {
        "Community Edition"
    }
}

/// Resolve cluster limits based on the active edition and license.
///
/// - **CE build** (`cargo build`): Always returns `ClusterLimits::community()`.
/// - **EE build** (`cargo build --features enterprise`): Loads the license key
///   and extracts limits from it. Falls back to CE limits on any error.
pub fn resolve_cluster_limits() -> ClusterLimits {
    resolve_edition().limits
}

/// Resolve the edition: the limits of [`resolve_cluster_limits`], and the
/// expiry of the licence they come from.
pub fn resolve_edition() -> ResolvedEdition {
    #[cfg(feature = "enterprise")]
    {
        resolve_enterprise_edition()
    }
    #[cfg(not(feature = "enterprise"))]
    {
        info!("Running in Community Edition mode (1 pool, 3 nodes)");
        ResolvedEdition {
            limits: ClusterLimits::community(),
            license_expires_at: None,
        }
    }
}

/// Publishes `s4_build_info` and, under an Enterprise licence,
/// `s4_license_expiry_timestamp_seconds`.
///
/// The edition in `s4_build_info` is the one the node runs as, not the one it
/// was built as: an Enterprise build without a licence runs, and reports, as
/// `ce`. Called once the Prometheus recorder is installed — recorded before
/// that, both would go nowhere.
pub fn record_edition_metrics(edition: &ResolvedEdition) {
    let label = if edition.limits.is_community() {
        "ce"
    } else {
        "ee"
    };
    metrics::gauge!("s4_build_info", "version" => S4_VERSION, "edition" => label).set(1.0);
    if let Some(expires_at) = edition.license_expires_at {
        metrics::gauge!("s4_license_expiry_timestamp_seconds").set(expires_at as f64);
    }
}

/// Build the cluster services DI container for the active edition.
///
/// - **CE build**: All services are Noop implementations.
/// - **EE build**: EE implementations are injected where the license allows.
///   Features not covered by the license fall back to Noop.
pub fn build_cluster_services(_limits: &ClusterLimits) -> ClusterServices {
    #[cfg(feature = "enterprise")]
    {
        build_enterprise_services(_limits)
    }
    #[cfg(not(feature = "enterprise"))]
    {
        ClusterServices::community()
    }
}

// ---------------------------------------------------------------------------
// Enterprise-only helpers (compiled out in CE builds)
// ---------------------------------------------------------------------------

#[cfg(feature = "enterprise")]
fn resolve_enterprise_edition() -> ResolvedEdition {
    match s4_ee::load_license() {
        Ok(license) => {
            let license_expires_at = Some(license.expires_at.timestamp());
            let limits = if license.is_expired() {
                tracing::warn!(
                    customer = %license.customer,
                    expired_at = %license.expires_at,
                    "Enterprise license expired — running in degraded mode \
                     (existing topology preserved, new EE operations blocked)"
                );
                // ADR-F14: expired license preserves existing topology
                // but returns CE limits for new operations.
                limits_from_capabilities(&license.capabilities)
            } else {
                info!(
                    customer = %license.customer,
                    expires_at = %license.expires_at,
                    "Enterprise license validated"
                );
                limits_from_capabilities(&license.capabilities)
            };
            ResolvedEdition {
                limits,
                license_expires_at,
            }
        }
        Err(s4_ee::LicenseError::NotFound) => {
            tracing::warn!(
                "No Enterprise license key provided. Set S4_LICENSE_KEY or S4_LICENSE_FILE \
                 to activate Enterprise Edition. Running in Community Edition mode \
                 (1 pool, max 3 nodes)."
            );
            ResolvedEdition {
                limits: ClusterLimits::community(),
                license_expires_at: None,
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "Enterprise license validation failed — falling back to Community Edition mode"
            );
            ResolvedEdition {
                limits: ClusterLimits::community(),
                license_expires_at: None,
            }
        }
    }
}

#[cfg(feature = "enterprise")]
fn limits_from_capabilities(caps: &s4_ee::Capabilities) -> ClusterLimits {
    ClusterLimits {
        is_enterprise: true,
        max_pools: caps.max_pools,
        max_nodes_per_pool: caps.max_nodes_per_pool,
        allow_deep_scrubber: caps.deep_scrubber,
        allow_rolling_upgrade: caps.rolling_upgrades,
        allow_auto_node_replace: caps.deep_scrubber, // tied to deep scrubber for now
        allow_external_auth: caps.ldap || caps.saml,
        allow_webdav: caps.webdav,
        allow_audit_log: caps.audit_log,
        allow_erasure_coding: caps.erasure_coding,
    }
}

#[cfg(feature = "enterprise")]
fn build_enterprise_services(limits: &ClusterLimits) -> ClusterServices {
    // For now, all EE implementations are stubs (Phase 9/10 will provide real ones).
    // The DI wiring is in place: when `s4-ee` adds real implementations,
    // they plug in here with no changes to domain code.
    let _ = limits;
    info!("Enterprise services initialized (implementations pending Phase 9/10)");
    ClusterServices::community()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The exposition `/metrics` would serve after [`record_edition_metrics`].
#[cfg(test)]
fn render(edition: &ResolvedEdition) -> String {
    let recorder = s4_api::prometheus::builder().expect("bucket bounds").build_recorder();
    metrics::with_local_recorder(&recorder, || record_edition_metrics(edition));
    recorder.handle().render()
}

#[cfg(test)]
#[cfg(not(feature = "enterprise"))]
mod tests {
    use super::*;

    #[test]
    fn ce_edition_label() {
        assert_eq!(edition_label(), "Community Edition");
    }

    #[test]
    fn ce_resolve_limits_returns_community() {
        let limits = resolve_cluster_limits();
        assert_eq!(limits, ClusterLimits::community());
    }

    #[test]
    fn ce_reports_its_build_and_no_licence() {
        let edition = resolve_edition();
        assert_eq!(edition.license_expires_at, None);

        let exposition = render(&edition);
        assert!(
            exposition.contains(&format!(
                "s4_build_info{{version=\"{S4_VERSION}\",edition=\"ce\"}} 1\n"
            )),
            "{exposition}"
        );
        assert!(!exposition.contains("s4_license_expiry_timestamp_seconds"));
    }

    #[test]
    fn ce_build_services_returns_community() {
        let limits = ClusterLimits::community();
        let services = build_cluster_services(&limits);
        assert!(!services.deep_scrubber.is_running());
    }
}

#[cfg(test)]
#[cfg(feature = "enterprise")]
mod tests_ee {
    use super::*;
    use std::sync::Mutex;

    /// Serializes tests that manipulate license env vars. `set_var`/
    /// `remove_var` affect the whole process, so parallel tests race.
    ///
    /// NOTE: this Mutex only serializes tests **inside this module**. If any
    /// code under test spawns a background thread that calls `std::env::var`,
    /// it can still race with `set_var`/`remove_var` here — POSIX `setenv` is
    /// not thread-safe. `resolve_cluster_limits` is synchronous and reads
    /// env vars on the same thread that holds the guard, so this is
    /// currently a non-issue. Revisit if the license path ever spawns tasks.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Clear every `S4_LICENSE_*` variable. Defensive against future vars
    /// (e.g. a hypothetical `S4_LICENSE_SERVER`) — we collect keys first so
    /// we don't mutate the environment while iterating over it.
    fn clear_env() {
        let keys: Vec<String> = std::env::vars()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with("S4_LICENSE"))
            .collect();
        for key in keys {
            std::env::remove_var(key);
        }
    }

    /// RAII guard that serializes license-env-var access across tests and
    /// clears those vars both on acquire and on drop.
    ///
    /// Clearing on drop is the load-bearing property: when a test panics
    /// between `set_var` and the next `clear_env()`, the old cleanup left
    /// the env dirty *and* the `ENV_LOCK` poisoned. `EnvGuard` solves both:
    ///
    /// - `Drop` always runs, even during an unwinding panic, so env vars
    ///   can't leak into the next test.
    /// - `acquire` recovers from a poisoned lock via `into_inner`, so one
    ///   failing test doesn't cascade into every other test in the module.
    struct EnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn acquire() -> Self {
            // Recover from poison: some earlier test panicked, but that's
            // not a reason to fail every subsequent test. `into_inner`
            // returns the inner guard and lets us continue.
            let lock = ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            // Start every test from a clean slate — even if a previous
            // test's Drop ran, another test in a different module could
            // theoretically set S4_LICENSE_* between runs.
            clear_env();
            Self { _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            clear_env();
        }
    }

    #[test]
    fn ee_edition_label() {
        assert_eq!(edition_label(), "Enterprise Edition");
    }

    #[test]
    fn ee_resolve_limits_without_license_falls_back_to_ce() {
        let _guard = EnvGuard::acquire();
        let limits = resolve_cluster_limits();
        assert_eq!(limits, ClusterLimits::community());
        assert!(limits.is_community());
    }

    #[test]
    fn ee_resolve_limits_with_dev_license_is_unlimited() {
        let _guard = EnvGuard::acquire();
        std::env::set_var("S4_LICENSE_DEV", "1");

        let limits = resolve_cluster_limits();
        assert!(limits.is_enterprise, "dev license must set is_enterprise");
        assert!(!limits.is_community());
        assert_eq!(limits.max_pools, u32::MAX);
        assert_eq!(limits.max_nodes_per_pool, u32::MAX);
        assert!(limits.allow_deep_scrubber);
        assert!(limits.allow_rolling_upgrade);
        assert!(limits.allow_auto_node_replace);
        assert!(limits.allow_external_auth);
        assert!(limits.allow_webdav);
        assert!(limits.allow_audit_log);
        assert!(limits.allow_erasure_coding);
        // _guard drops here → clear_env() runs automatically.
    }

    /// A node under a licence reports when it expires, and reports itself as
    /// `ee`; the same build without one reports `ce` and no expiry.
    #[test]
    fn ee_reports_the_expiry_of_the_licence_it_runs_under() {
        let _guard = EnvGuard::acquire();
        std::env::set_var("S4_LICENSE_DEV", "1");

        let edition = resolve_edition();
        let expires_at = edition.license_expires_at.expect("a dev licence has an expiry");
        let in_a_year = (chrono::Utc::now() + chrono::Duration::days(365)).timestamp();
        assert!(
            (in_a_year - expires_at).abs() < 5,
            "{expires_at} vs {in_a_year}"
        );

        let exposition = render(&edition);
        assert!(
            exposition.contains(&format!(
                "s4_license_expiry_timestamp_seconds {expires_at}\n"
            )),
            "{exposition}"
        );
        assert!(exposition.contains(&format!(
            "s4_build_info{{version=\"{S4_VERSION}\",edition=\"ee\"}} 1\n"
        )));

        std::env::remove_var("S4_LICENSE_DEV");
        let without = resolve_edition();
        assert_eq!(without.license_expires_at, None);
        let exposition = render(&without);
        assert!(!exposition.contains("s4_license_expiry_timestamp_seconds"));
        assert!(exposition.contains("edition=\"ce\""));
    }

    #[test]
    fn ee_resolve_limits_with_invalid_license_key_falls_back_to_ce() {
        let _guard = EnvGuard::acquire();
        std::env::set_var("S4_LICENSE_KEY", "not-a-real-license");

        let limits = resolve_cluster_limits();
        assert_eq!(
            limits,
            ClusterLimits::community(),
            "invalid license must fall back to CE"
        );
        // _guard drops here → clear_env() runs automatically.
    }

    #[test]
    fn ee_build_services_does_not_panic() {
        let _guard = EnvGuard::acquire();
        std::env::set_var("S4_LICENSE_DEV", "1");

        let limits = resolve_cluster_limits();
        let services = build_cluster_services(&limits);
        // Smoke test: services are constructed; the EE path wires real
        // implementations in Phase 9/10, for now it returns the CE stubs.
        assert!(!services.deep_scrubber.is_running());
        // _guard drops here → clear_env() runs automatically.
    }

    #[test]
    fn ee_dev_license_enforces_cluster_config_correctly() {
        use s4_cluster::config::{ClusterConfig, PoolConfig};
        use s4_cluster::identity::{NodeAddr, PoolId};
        use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

        // NOTE: this test holds the `ENV_LOCK` for the duration of config
        // building and two validate_with_limits calls — noticeably longer
        // than the other tests. Tests here are few and fast, so we accept
        // the reduced parallelism; if that changes, move the `ClusterConfig`
        // construction outside the guarded section.
        let _guard = EnvGuard::acquire();
        std::env::set_var("S4_LICENSE_DEV", "1");

        let limits = resolve_cluster_limits();

        let test_addr = |port: u16| {
            let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port));
            NodeAddr {
                grpc_addr: addr,
                http_addr: addr,
                gossip_addr: addr,
            }
        };
        let pool = PoolConfig {
            pool_id: PoolId(1),
            name: "pool-big".into(),
            nodes: (0..10).map(|i| test_addr(9000 + i)).collect(),
            replication_factor: None,
            write_quorum: None,
            read_quorum: None,
            storage_class: s4_core::types::StorageClass::Standard,
        };
        let config = ClusterConfig {
            pools: vec![pool],
            ..Default::default()
        };

        // 10 nodes in one pool — rejected by CE, accepted under dev license.
        assert!(
            config.validate_with_limits(&limits).is_ok(),
            "dev license must allow 10-node pools"
        );
        assert!(
            config.validate_with_limits(&ClusterLimits::community()).is_err(),
            "CE must reject 10-node pools"
        );
        // _guard drops here → clear_env() runs automatically.
    }
}
