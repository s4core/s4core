// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Deterministic Enterprise-only EC test hooks.
//!
//! These hooks are intentionally inert unless an explicit test-hook environment
//! flag is present. Production operation must not depend on them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use s4_cluster::ClusterError;
use serde::{Deserialize, Serialize};

use crate::ec_metadata::EcObjectVersionKey;

/// Mode accepted by the reconstruction-failure hook.
pub const FAIL_RECONSTRUCTION_AFTER_SHARDS: &str = "fail_reconstruction_after_shards";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FailureRule {
    bucket: String,
    key: String,
    version_id: Option<String>,
    mode: String,
}

static FAILURE_RULES: OnceLock<Mutex<Vec<FailureRule>>> = OnceLock::new();

/// Returns true when EE test hooks are explicitly enabled for this process.
pub fn hooks_enabled() -> bool {
    env_truthy("S4_EC_TEST_HOOKS_ENABLED")
        || std::env::var("S4_EC_TEST_FAIL_RECONSTRUCTION_PATH").is_ok()
}

/// Arm a one-shot reconstruction failure for a specific object.
pub fn arm_fail_reconstruction_after_shards(
    bucket: String,
    key: String,
    version_id: Option<String>,
    mode: String,
) -> Result<(), ClusterError> {
    if !hooks_enabled() {
        return Err(ClusterError::Config(
            "EC test hooks are disabled; set S4_EC_TEST_HOOKS_ENABLED=true or a specific S4_EC_TEST_* path"
                .to_string(),
        ));
    }
    if mode != FAIL_RECONSTRUCTION_AFTER_SHARDS {
        return Err(ClusterError::Config(format!(
            "unsupported EC test hook mode '{mode}'"
        )));
    }
    if bucket.trim().is_empty() || key.is_empty() {
        return Err(ClusterError::Config(
            "EC test hook bucket and key must be non-empty".to_string(),
        ));
    }

    let rules = FAILURE_RULES.get_or_init(|| Mutex::new(Vec::new()));
    let mut guard = rules
        .lock()
        .map_err(|_| ClusterError::Config("EC test hook state is poisoned".to_string()))?;
    guard.push(FailureRule {
        bucket,
        key,
        version_id,
        mode,
    });
    Ok(())
}

/// Arm or disarm the foreign generator matrix on the accelerated engine.
///
/// This makes the node encode new shards with a matrix that is deliberately not
/// S4's, so the mixed-cluster script can prove that its byte-equality check is
/// able to fail. Objects written while it is armed are unreadable to the rest of
/// the cluster; existing objects are untouched, because decode and repair keep
/// the stored matrix.
///
/// The gate lives in `s4_codec::test_hooks`, which is the only definition of
/// "hooks are enabled" for this hook. Adding a second check here would let a
/// caller pass the gate this module advertises and be refused by an invisible
/// one behind it.
pub fn set_foreign_matrix(enabled: bool) -> Result<(), ClusterError> {
    s4_codec::test_hooks::set_foreign_matrix(enabled)
        .map_err(|error| ClusterError::Config(error.to_string()))
}

/// Whether the foreign generator matrix is currently armed.
pub fn foreign_matrix_armed() -> bool {
    s4_codec::test_hooks::foreign_matrix_armed()
}

/// Consume a matching reconstruction-failure rule, if one is armed.
pub(crate) fn take_fail_reconstruction_after_shards(object: &EcObjectVersionKey) -> bool {
    if !hooks_enabled() {
        return false;
    }

    if let Some(rules) = FAILURE_RULES.get() {
        if let Ok(mut guard) = rules.lock() {
            if let Some(index) = guard.iter().position(|rule| {
                rule.mode == FAIL_RECONSTRUCTION_AFTER_SHARDS
                    && rule.bucket == object.bucket
                    && rule.key == object.key
                    && rule.version_id == object.version_id
            }) {
                guard.remove(index);
                return true;
            }
        }
    }

    let Ok(key) = std::env::var("S4_EC_TEST_FAIL_RECONSTRUCTION_OBJECT_KEY") else {
        return false;
    };
    key == object.key
}

fn env_truthy(name: &'static str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Transcode pause
// ---------------------------------------------------------------------------

/// Mode accepted by the transcode pause hook.
pub const PAUSE_ONCE: &str = "pause_once";

/// How long a paused transcode waits before it gives up and continues.
///
/// A test that forgets to release would otherwise hold a transcode lease for
/// the life of the process. Far longer than any scenario needs, short enough
/// that a forgotten pause is a delay rather than a wedged worker.
const PAUSE_MAX_WAIT: Duration = Duration::from_secs(600);

/// How often a paused transcode looks to see whether it may go on.
const PAUSE_POLL: Duration = Duration::from_millis(100);

/// One armed pause: an object, the state to stop at, and what the stop saw.
#[derive(Debug, Clone)]
struct PauseRule {
    bucket: String,
    key: String,
    version_id: Option<String>,
    state: String,
    /// Set by the transcode that actually stopped here.
    holding: bool,
    operation_id: Option<String>,
    node_id: Option<String>,
    /// Set by `release`; the held transcode goes on and the rule is dropped.
    released: bool,
}

static PAUSE_RULES: OnceLock<Mutex<Vec<PauseRule>>> = OnceLock::new();

/// Whether any pause is armed at all.
///
/// Read once per state transition, so a build with no pause armed — every
/// production build — pays one relaxed atomic load and nothing else.
static PAUSE_ARMED: AtomicBool = AtomicBool::new(false);

fn pause_rules() -> &'static Mutex<Vec<PauseRule>> {
    PAUSE_RULES.get_or_init(|| Mutex::new(Vec::new()))
}

fn lock_pause_rules() -> Result<std::sync::MutexGuard<'static, Vec<PauseRule>>, ClusterError> {
    pause_rules()
        .lock()
        .map_err(|_| ClusterError::Config("EC transcode pause state is poisoned".to_string()))
}

fn matches_rule(rule: &PauseRule, object: &EcObjectVersionKey, state: &str) -> bool {
    rule.bucket == object.bucket
        && rule.key == object.key
        && rule.state == state
        && match (&rule.version_id, &object.version_id) {
            (None, _) => true,
            (Some(wanted), Some(actual)) => wanted == actual,
            (Some(_), None) => false,
        }
}

/// Arm a one-shot pause for `bucket`/`key` at `state`.
///
/// # Errors
///
/// Returns [`ClusterError::Config`] when hooks are disabled, the mode is not
/// [`PAUSE_ONCE`], or the pause state is poisoned.
pub fn arm_transcode_pause(
    bucket: String,
    key: String,
    version_id: Option<String>,
    state: String,
    mode: &str,
) -> Result<(), ClusterError> {
    if !hooks_enabled() {
        return Err(ClusterError::Config(
            "EC test hooks are disabled; set S4_EC_TEST_HOOKS_ENABLED=true".to_string(),
        ));
    }
    if mode != PAUSE_ONCE {
        return Err(ClusterError::Config(format!(
            "unsupported EC transcode pause mode '{mode}'"
        )));
    }

    let mut rules = lock_pause_rules()?;
    rules.retain(|rule| {
        !(rule.bucket == bucket
            && rule.key == key
            && rule.version_id == version_id
            && rule.state == state)
    });
    rules.push(PauseRule {
        bucket,
        key,
        version_id,
        state,
        holding: false,
        operation_id: None,
        node_id: None,
        released: false,
    });
    PAUSE_ARMED.store(true, Ordering::Relaxed);
    Ok(())
}

/// What one armed pause looks like from outside.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EcTranscodePauseStatus {
    /// Whether a pause is armed for this object and state right now.
    ///
    /// Reported from the rule rather than from the action the caller sent, so
    /// `status` on a pause nobody armed answers `false` instead of echoing the
    /// question back.
    pub armed: bool,
    /// Whether a transcode is being held at that state at this moment.
    pub paused: bool,
    /// The operation being held, once one has stopped here.
    pub operation_id: Option<String>,
    /// The node holding it.
    pub node_id: Option<String>,
}

/// What the armed pause has seen so far: whether it is armed at all, whether a
/// transcode is held at that state, and which operation and node hold it.
///
/// # Errors
///
/// Returns [`ClusterError::Config`] when the pause state is poisoned.
pub fn transcode_pause_status(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    state: &str,
) -> Result<EcTranscodePauseStatus, ClusterError> {
    let rules = lock_pause_rules()?;
    let found = rules.iter().find(|rule| {
        rule.bucket == bucket
            && rule.key == key
            && rule.state == state
            && rule.version_id.as_deref() == version_id
    });
    Ok(match found {
        Some(rule) => EcTranscodePauseStatus {
            armed: !rule.released,
            paused: rule.holding && !rule.released,
            operation_id: rule.operation_id.clone(),
            node_id: rule.node_id.clone(),
        },
        None => EcTranscodePauseStatus::default(),
    })
}

/// Let a held transcode go on, and forget the rule.
///
/// Releasing a pause nobody reached is not an error: a scenario may release on
/// its way out of a phase it never entered.
///
/// # Errors
///
/// Returns [`ClusterError::Config`] when the pause state is poisoned.
pub fn release_transcode_pause(
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    state: &str,
) -> Result<bool, ClusterError> {
    let mut rules = lock_pause_rules()?;
    let mut released = false;
    for rule in rules.iter_mut() {
        if rule.bucket == bucket
            && rule.key == key
            && rule.state == state
            && rule.version_id.as_deref() == version_id
        {
            rule.released = true;
            released = true;
        }
    }
    if rules.iter().all(|rule| rule.released) {
        PAUSE_ARMED.store(false, Ordering::Relaxed);
    }
    Ok(released)
}

/// Hold the transcode of `object` at `state` while a pause is armed for it.
///
/// Returns as soon as nothing is armed, which is the only path a production
/// build ever takes.
pub async fn pause_if_armed(
    object: &EcObjectVersionKey,
    state: &str,
    operation_id: &str,
    node_id: &str,
) {
    if !PAUSE_ARMED.load(Ordering::Relaxed) {
        return;
    }

    // Claim the rule: from here the status endpoint can say who is held.
    {
        let Ok(mut rules) = pause_rules().lock() else {
            return;
        };
        let Some(rule) = rules
            .iter_mut()
            .find(|rule| !rule.released && matches_rule(rule, object, state))
        else {
            return;
        };
        rule.holding = true;
        rule.operation_id = Some(operation_id.to_string());
        rule.node_id = Some(node_id.to_string());
    }

    tracing::warn!(
        bucket = %object.bucket,
        key = %object.key,
        state,
        operation_id,
        "EC transcode held by the pause test hook"
    );

    let deadline = Instant::now() + PAUSE_MAX_WAIT;
    loop {
        {
            let Ok(rules) = pause_rules().lock() else {
                return;
            };
            let still_held =
                rules.iter().any(|rule| !rule.released && matches_rule(rule, object, state));
            if !still_held {
                break;
            }
        }
        if Instant::now() >= deadline {
            tracing::warn!(
                bucket = %object.bucket,
                key = %object.key,
                state,
                "EC transcode pause hook was never released; going on"
            );
            break;
        }
        tokio::time::sleep(PAUSE_POLL).await;
    }

    let Ok(mut rules) = pause_rules().lock() else {
        return;
    };
    rules.retain(|rule| !matches_rule(rule, object, state));
    if rules.is_empty() {
        PAUSE_ARMED.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests share a process and this flag is global, so the ones that need it
    /// on take it in turn.
    static HOOK_FLAG: Mutex<()> = Mutex::new(());

    /// EC2-FIX-04-002: the status answers from the rule, not from the question.
    ///
    /// `armed` used to be derived from the action the caller sent, so a status
    /// call on a pause nobody had armed answered `armed: true` — a scenario
    /// waiting for a state to be caught could not tell a hook that was set from
    /// one that was never accepted.
    #[test]
    fn the_pause_status_answers_from_the_rule() {
        let _flag = HOOK_FLAG.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        std::env::set_var("S4_EC_TEST_HOOKS_ENABLED", "true");

        let (bucket, key, state) = ("photos", "status-probe.bin", "ec_encoding");
        let nothing = transcode_pause_status(bucket, key, None, state).unwrap();
        assert_eq!(nothing, EcTranscodePauseStatus::default());

        arm_transcode_pause(
            bucket.to_string(),
            key.to_string(),
            None,
            state.to_string(),
            PAUSE_ONCE,
        )
        .unwrap();
        let armed = transcode_pause_status(bucket, key, None, state).unwrap();
        assert!(armed.armed, "the rule is there");
        assert!(!armed.paused, "no transcode has reached that state");
        assert_eq!(armed.operation_id, None);

        assert!(release_transcode_pause(bucket, key, None, state).unwrap());
        let released = transcode_pause_status(bucket, key, None, state).unwrap();
        assert!(!released.armed, "a released pause is not armed");
        assert!(!released.paused);

        std::env::remove_var("S4_EC_TEST_HOOKS_ENABLED");
    }

    /// Releasing a pause nobody reached is not an error: a scenario may release
    /// on its way out of a phase it never entered.
    #[test]
    fn releasing_a_pause_that_was_never_armed_is_not_an_error() {
        let released =
            release_transcode_pause("photos", "never-armed.bin", None, "ec_encoding").unwrap();
        assert!(!released);
    }
}
