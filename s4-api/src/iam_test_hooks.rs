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

//! A deterministic test hook of IAM changes in a cluster: the holder of the
//! lease that wakes up after its lease passed on (scenario 62, group I).
//!
//! Fencing exists for a holder paused between the last check of its lease
//! and its write — a long GC pause, a stopped process — that then writes as
//! if nothing happened. From outside a process that moment cannot be hit
//! reliably, so the hook makes the holder pause exactly there: after the
//! change checked, and if needed renewed, its lease, before it writes the
//! user record it names, for as long as it is told, without checking again.
//!
//! The hook is inert unless `S4_IAM_TEST_HOOKS_ENABLED` is set to a true
//! value; production operation never depends on it.
//!
//! - `S4_IAM_TEST_STALL_USERNAME` — the user whose record a change of this
//!   node stalls before writing.
//! - `S4_IAM_TEST_STALL_MS` — for how long.

use std::sync::OnceLock;
use std::time::Duration;

use s4_features::iam::USER_KEY_PREFIX;

/// Which change stalls, and for how long.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StallRule {
    username: String,
    pause: Duration,
}

/// The rule the environment arms; `None` unless hooks are enabled and both
/// variables are set.
fn rule_from(
    enabled: Option<&str>,
    username: Option<&str>,
    millis: Option<&str>,
) -> Option<StallRule> {
    let enabled = enabled.is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    });
    let username = username.filter(|name| !name.is_empty())?;
    let pause = Duration::from_millis(millis?.parse().ok()?);
    (enabled && !pause.is_zero()).then(|| StallRule {
        username: username.to_string(),
        pause,
    })
}

fn rule() -> Option<&'static StallRule> {
    static RULE: OnceLock<Option<StallRule>> = OnceLock::new();
    RULE.get_or_init(|| {
        let var = |name: &str| std::env::var(name).ok();
        let rule = rule_from(
            var("S4_IAM_TEST_HOOKS_ENABLED").as_deref(),
            var("S4_IAM_TEST_STALL_USERNAME").as_deref(),
            var("S4_IAM_TEST_STALL_MS").as_deref(),
        );
        if let Some(rule) = &rule {
            tracing::warn!(
                username = %rule.username,
                pause_ms = rule.pause.as_millis() as u64,
                "IAM test hook armed: a change of this user stalls before its write"
            );
        }
        rule
    })
    .as_ref()
}

/// Whether a write of `key` holding `data` is the one `rule` stalls.
fn stalls(rule: &StallRule, key: &str, data: &[u8]) -> bool {
    key.starts_with(USER_KEY_PREFIX)
        && serde_json::from_slice::<serde_json::Value>(data)
            .ok()
            .and_then(|record| record.get("username")?.as_str().map(str::to_string))
            .is_some_and(|username| username == rule.username)
}

/// Pauses before the write of `key` when the armed rule names its user.
pub(crate) async fn stall_before_write(key: &str, data: &[u8]) {
    let Some(rule) = rule() else {
        return;
    };
    if stalls(rule, key, data) {
        tracing::warn!(
            username = %rule.username,
            pause_ms = rule.pause.as_millis() as u64,
            "IAM test hook: the holder of the lease stalls before its write"
        );
        tokio::time::sleep(rule.pause).await;
        tracing::warn!(username = %rule.username, "IAM test hook: the holder of the lease writes");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inert_unless_enabled_and_named() {
        assert_eq!(rule_from(None, Some("ivan"), Some("8000")), None);
        assert_eq!(rule_from(Some("false"), Some("ivan"), Some("8000")), None);
        assert_eq!(rule_from(Some("true"), None, Some("8000")), None);
        assert_eq!(rule_from(Some("true"), Some("ivan"), None), None);
        assert_eq!(rule_from(Some("true"), Some("ivan"), Some("0")), None);
        assert_eq!(rule_from(Some("true"), Some("ivan"), Some("soon")), None);
        assert_eq!(
            rule_from(Some("TRUE"), Some("ivan"), Some("8000")),
            Some(StallRule {
                username: "ivan".into(),
                pause: Duration::from_millis(8000)
            })
        );
    }

    #[test]
    fn only_the_record_of_the_named_user_stalls() {
        let rule = StallRule {
            username: "ivan".into(),
            pause: Duration::from_millis(1),
        };
        let key = format!("{USER_KEY_PREFIX}0001");
        assert!(stalls(
            &rule,
            &key,
            br#"{"username":"ivan","role":"Writer"}"#
        ));
        assert!(!stalls(&rule, &key, br#"{"username":"ivy"}"#));
        assert!(!stalls(
            &rule,
            "__s4_iam_v2_audit_1_2_3",
            br#"{"username":"ivan"}"#
        ));
        assert!(!stalls(&rule, &key, b"not json"));
    }
}
