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

//! Secrets from the environment: set directly, or read from the file that
//! `<NAME>_FILE` points to.
//!
//! The `_FILE` form is the Docker and Kubernetes secrets convention. The
//! orchestrator mounts every secret as a file (`/run/secrets/<name>`), so the
//! value never appears in `docker inspect`, in the process environment or in a
//! compose file kept in git.
//!
//! The rules are the same for every secret:
//!
//! - `NAME` and `NAME_FILE` both set is refused: it is never a guess which one won.
//!   An empty `NAME` does not count, so a compose template such as
//!   `S4_ROOT_PASSWORD=${S4_ROOT_PASSWORD:-}` can sit next to a `_FILE` variant.
//! - A `NAME_FILE` that cannot be read, is larger than [`MAX_SECRET_FILE_LEN`],
//!   is not UTF-8 text or is empty is refused, instead of silently falling back
//!   to a default. An empty `NAME_FILE` counts as unset.
//! - Both forms are cleaned up the same way: a leading UTF-8 byte order mark and
//!   trailing line breaks are stripped, so `echo secret > file`, a mounted
//!   Kubernetes secret and the same secret passed as a variable give one value.
//! - An empty `NAME` on its own is returned as an empty value. What empty means
//!   is up to the setting; see `SecurityConfig::from_env`.

use std::ffi::OsString;
use std::fmt;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use s4_features::iam::jwt::{generate_secret, MIN_SECRET_LEN};

use crate::config::ServerMode;

/// Suffix that turns a variable into the path of a file holding its value.
pub const FILE_SUFFIX: &str = "_FILE";

/// Largest `NAME_FILE` accepted, in bytes. A secret takes a few hundred bytes at
/// most; the cap turns a wrong path such as `/dev/urandom` into an error instead
/// of an endless read.
pub const MAX_SECRET_FILE_LEN: usize = 64 * 1024;

/// Why a secret could not be resolved.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// Both `NAME` and `NAME_FILE` are set.
    #[error("{name} and {name}_FILE are both set; set only one of them")]
    Conflict {
        /// Variable name without the `_FILE` suffix.
        name: String,
    },

    /// The variable's value is not valid UTF-8.
    #[error("{name} is not valid UTF-8")]
    NotUnicode {
        /// Variable name.
        name: String,
    },

    /// The file named by `NAME_FILE` could not be read.
    #[error("cannot read {name}_FILE {}", .path.display())]
    Read {
        /// Variable name without the `_FILE` suffix.
        name: String,
        /// Path taken from `NAME_FILE`.
        path: PathBuf,
        /// Underlying I/O failure.
        #[source]
        source: io::Error,
    },

    /// The file named by `NAME_FILE` is larger than [`MAX_SECRET_FILE_LEN`].
    #[error(
        "{name}_FILE {} is larger than {MAX_SECRET_FILE_LEN} bytes; it must hold only the secret",
        .path.display()
    )]
    TooLarge {
        /// Variable name without the `_FILE` suffix.
        name: String,
        /// Path taken from `NAME_FILE`.
        path: PathBuf,
    },

    /// The file named by `NAME_FILE` is not UTF-8 text.
    #[error("{name}_FILE {} is not valid UTF-8 text", .path.display())]
    FileNotUnicode {
        /// Variable name without the `_FILE` suffix.
        name: String,
        /// Path taken from `NAME_FILE`.
        path: PathBuf,
    },

    /// The file named by `NAME_FILE` holds nothing but line breaks.
    #[error("{name}_FILE {} is empty", .path.display())]
    Empty {
        /// Variable name without the `_FILE` suffix.
        name: String,
        /// Path taken from `NAME_FILE`.
        path: PathBuf,
    },

    /// The configured JWT secret is too short to resist an offline guess.
    #[error(
        "S4_JWT_SECRET is {len} bytes long; it must be at least {MIN_SECRET_LEN} bytes. \
         Generate one with `openssl rand -hex 32`"
    )]
    WeakJwtSecret {
        /// Length of the configured secret, in bytes.
        len: usize,
    },

    /// A multi-node deployment with IAM has no shared JWT secret.
    #[error(
        "S4_JWT_SECRET (or S4_JWT_SECRET_FILE) must be set in {mode} mode when {reason}: \
         every node has to sign and verify admin tokens with the same secret"
    )]
    JwtSecretRequired {
        /// Operating mode that needs the shared secret (`cluster` or `gateway`).
        mode: &'static str,
        /// What shows that IAM is in use.
        reason: &'static str,
    },
}

/// Reads the secret `name` from the process environment, directly or through
/// `name_FILE`. Returns `None` when neither is set.
pub fn secret_from_env(name: &str) -> Result<Option<String>, SecretError> {
    resolve_secret(
        name,
        |key| std::env::var_os(key),
        |path| read_at_most(path, MAX_SECRET_FILE_LEN + 1),
    )
}

/// Reads up to `limit` bytes of `path`, so an endless file ends the read.
fn read_at_most(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?.take(limit as u64).read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// Strips what editors and `echo` add around a secret: a UTF-8 byte order mark
/// and trailing line breaks.
fn normalize(text: &str) -> &str {
    text.strip_prefix('\u{feff}').unwrap_or(text).trim_end_matches(['\r', '\n'])
}

/// Applies the rules in the module docs through the given lookups, so they can
/// be tested without touching the process environment.
fn resolve_secret(
    name: &str,
    var: impl Fn(&str) -> Option<OsString>,
    read: impl Fn(&Path) -> io::Result<Vec<u8>>,
) -> Result<Option<String>, SecretError> {
    let direct = var(name)
        .map(|value| {
            value.into_string().map_err(|_| SecretError::NotUnicode {
                name: name.to_string(),
            })
        })
        .transpose()?
        .map(|value| normalize(&value).to_string());
    let file = var(&format!("{name}{FILE_SUFFIX}")).filter(|path| !path.is_empty());

    let Some(path) = file else {
        return Ok(direct);
    };
    if direct.is_some_and(|value| !value.is_empty()) {
        return Err(SecretError::Conflict {
            name: name.to_string(),
        });
    }

    let path = PathBuf::from(path);
    let bytes = read(&path).map_err(|source| SecretError::Read {
        name: name.to_string(),
        path: path.clone(),
        source,
    })?;
    if bytes.len() > MAX_SECRET_FILE_LEN {
        return Err(SecretError::TooLarge {
            name: name.to_string(),
            path,
        });
    }
    let text = String::from_utf8(bytes).map_err(|_| SecretError::FileNotUnicode {
        name: name.to_string(),
        path: path.clone(),
    })?;
    let value = normalize(&text);
    if value.is_empty() {
        return Err(SecretError::Empty {
            name: name.to_string(),
            path,
        });
    }
    Ok(Some(value.to_string()))
}

/// The secret that signs and verifies admin JWTs, and where it came from.
#[derive(Clone, PartialEq, Eq)]
pub enum JwtSecret {
    /// Set by the operator through `S4_JWT_SECRET` or `S4_JWT_SECRET_FILE`.
    Configured(String),
    /// Generated at startup because none was set. It lives as long as the
    /// process, so admin sessions end on restart.
    Generated(String),
}

impl JwtSecret {
    /// The secret itself.
    pub fn value(&self) -> &str {
        match self {
            Self::Configured(secret) | Self::Generated(secret) => secret,
        }
    }
}

impl fmt::Debug for JwtSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let origin = match self {
            Self::Configured(_) => "Configured",
            Self::Generated(_) => "Generated",
        };
        write!(f, "JwtSecret::{origin}(<redacted>)")
    }
}

/// Decides which secret signs admin JWTs.
///
/// A configured secret must be at least [`MIN_SECRET_LEN`] bytes: every user
/// with a token holds a message signed with it, and a short secret can be
/// guessed offline from that token and then used to sign a SuperUser one.
///
/// Without a configured secret, a single node generates a random one. A cluster
/// or gateway node cannot: every node would generate its own, and a token
/// issued by one node would be rejected by the rest. So when IAM is bootstrapped
/// there (`root_password_set`), the start is refused with an error that names
/// the missing variable. Without IAM there is no one to log in, and a generated
/// secret is enough; [`check_iam_users`] covers users left from an earlier start.
pub fn resolve_jwt_secret(
    configured: Option<&str>,
    mode: ServerMode,
    root_password_set: bool,
) -> Result<JwtSecret, SecretError> {
    if let Some(secret) = configured {
        if secret.len() < MIN_SECRET_LEN {
            return Err(SecretError::WeakJwtSecret { len: secret.len() });
        }
        return Ok(JwtSecret::Configured(secret.to_string()));
    }
    match multi_node_mode(mode) {
        Some(mode) if root_password_set => Err(SecretError::JwtSecretRequired {
            mode,
            reason: "S4_ROOT_PASSWORD is set",
        }),
        _ => Ok(JwtSecret::Generated(generate_secret())),
    }
}

/// Refuses a generated secret on a cluster or gateway node whose IAM already
/// has users.
///
/// [`resolve_jwt_secret`] sees only `S4_ROOT_PASSWORD`, which may have been
/// removed after the first start while the users created since remain and can
/// still log in.
pub fn check_iam_users(
    secret: &JwtSecret,
    mode: ServerMode,
    iam_users: usize,
) -> Result<(), SecretError> {
    match (secret, multi_node_mode(mode)) {
        (JwtSecret::Generated(_), Some(mode)) if iam_users > 0 => {
            Err(SecretError::JwtSecretRequired {
                mode,
                reason: "IAM already has users",
            })
        }
        _ => Ok(()),
    }
}

/// Name of a mode in which several nodes must share one JWT secret.
fn multi_node_mode(mode: ServerMode) -> Option<&'static str> {
    match mode {
        ServerMode::Single => None,
        ServerMode::Cluster => Some("cluster"),
        ServerMode::Gateway => Some("gateway"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::Write;

    /// A configured secret long enough to be accepted.
    const STRONG: &str = "configured-secret-0123456789abcdef";

    /// Resolves `name` against a fake environment and a fake file system.
    fn resolve(
        name: &str,
        env: &[(&str, &str)],
        files: &[(&str, &[u8])],
    ) -> Result<Option<String>, SecretError> {
        let env: HashMap<String, OsString> =
            env.iter().map(|(k, v)| (k.to_string(), OsString::from(v))).collect();
        let files: HashMap<PathBuf, Vec<u8>> =
            files.iter().map(|(p, c)| (PathBuf::from(p), c.to_vec())).collect();
        resolve_secret(
            name,
            |key| env.get(key).cloned(),
            |path| {
                files
                    .get(path)
                    .cloned()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
            },
        )
    }

    #[test]
    fn unset_secret_is_none() {
        assert!(resolve("S4_ROOT_PASSWORD", &[], &[]).unwrap().is_none());
    }

    #[test]
    fn direct_value_keeps_spaces_and_loses_trailing_line_breaks() {
        let value = resolve(
            "S4_ROOT_PASSWORD",
            &[("S4_ROOT_PASSWORD", " p@ss \r\n")],
            &[],
        )
        .unwrap();
        assert_eq!(value.as_deref(), Some(" p@ss "));
    }

    #[test]
    fn file_value_is_read_and_trailing_line_breaks_are_stripped() {
        for content in [&b"s3cret"[..], b"s3cret\n", b"s3cret\r\n", b"s3cret\n\n"] {
            let value = resolve(
                "S4_ROOT_PASSWORD",
                &[("S4_ROOT_PASSWORD_FILE", "/run/secrets/s4_pass")],
                &[("/run/secrets/s4_pass", content)],
            )
            .unwrap();
            assert_eq!(value.as_deref(), Some("s3cret"), "content {content:?}");
        }
    }

    #[test]
    fn file_value_keeps_inner_and_leading_whitespace() {
        let value = resolve(
            "S4_JWT_SECRET",
            &[("S4_JWT_SECRET_FILE", "/s")],
            &[("/s", b"  two words \n")],
        )
        .unwrap();
        assert_eq!(value.as_deref(), Some("  two words "));
    }

    #[test]
    fn byte_order_mark_is_stripped_from_both_forms() {
        let from_file = resolve(
            "S4_JWT_SECRET",
            &[("S4_JWT_SECRET_FILE", "/s")],
            &[("/s", "\u{feff}secret\r\n".as_bytes())],
        )
        .unwrap();
        let direct = resolve(
            "S4_JWT_SECRET",
            &[("S4_JWT_SECRET", "\u{feff}secret\n")],
            &[],
        )
        .unwrap();

        assert_eq!(from_file.as_deref(), Some("secret"));
        assert_eq!(direct, from_file);
    }

    #[test]
    fn both_set_is_refused() {
        let err = resolve(
            "S4_ACCESS_KEY_ID",
            &[
                ("S4_ACCESS_KEY_ID", "direct"),
                ("S4_ACCESS_KEY_ID_FILE", "/s"),
            ],
            &[("/s", b"from-file")],
        )
        .unwrap_err();
        assert!(matches!(err, SecretError::Conflict { .. }), "{err}");
        assert_eq!(
            err.to_string(),
            "S4_ACCESS_KEY_ID and S4_ACCESS_KEY_ID_FILE are both set; set only one of them"
        );
    }

    #[test]
    fn empty_direct_value_gives_way_to_the_file() {
        let value = resolve(
            "S4_ROOT_PASSWORD",
            &[("S4_ROOT_PASSWORD", ""), ("S4_ROOT_PASSWORD_FILE", "/s")],
            &[("/s", b"from-file")],
        )
        .unwrap();
        assert_eq!(value.as_deref(), Some("from-file"));
    }

    #[test]
    fn empty_direct_value_alone_is_an_empty_value() {
        for env in [
            &[("S4_ACCESS_KEY_ID", "")][..],
            &[("S4_ACCESS_KEY_ID", ""), ("S4_ACCESS_KEY_ID_FILE", "")],
        ] {
            let value = resolve("S4_ACCESS_KEY_ID", env, &[]).unwrap();
            assert_eq!(value.as_deref(), Some(""), "env {env:?}");
        }
    }

    #[test]
    fn empty_file_variable_counts_as_unset() {
        let value = resolve("S4_ROOT_PASSWORD", &[("S4_ROOT_PASSWORD_FILE", "")], &[]).unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn missing_file_is_refused_and_named() {
        let err = resolve(
            "S4_SECRET_ACCESS_KEY",
            &[("S4_SECRET_ACCESS_KEY_FILE", "/run/secrets/missing")],
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, SecretError::Read { .. }), "{err}");
        let message = err.to_string();
        assert!(message.contains("S4_SECRET_ACCESS_KEY_FILE"), "{message}");
        assert!(message.contains("/run/secrets/missing"), "{message}");
    }

    #[test]
    fn empty_file_is_refused() {
        for content in [&b""[..], b"\n", b"\r\n", "\u{feff}\n".as_bytes()] {
            let err = resolve(
                "S4_ROOT_PASSWORD",
                &[("S4_ROOT_PASSWORD_FILE", "/s")],
                &[("/s", content)],
            )
            .unwrap_err();
            assert!(matches!(err, SecretError::Empty { .. }), "{err}");
        }
    }

    #[test]
    fn binary_file_is_refused() {
        let err = resolve(
            "S4_JWT_SECRET",
            &[("S4_JWT_SECRET_FILE", "/s")],
            &[("/s", &[0xff, 0xfe, 0x00])],
        )
        .unwrap_err();
        assert!(matches!(err, SecretError::FileNotUnicode { .. }), "{err}");
    }

    #[test]
    fn file_larger_than_the_cap_is_refused() {
        let at_cap = vec![b'a'; MAX_SECRET_FILE_LEN];
        let over_cap = vec![b'a'; MAX_SECRET_FILE_LEN + 1];
        let env = [("S4_JWT_SECRET_FILE", "/s")];

        assert!(resolve("S4_JWT_SECRET", &env, &[("/s", &at_cap)]).is_ok());
        let err = resolve("S4_JWT_SECRET", &env, &[("/s", &over_cap)]).unwrap_err();
        assert!(matches!(err, SecretError::TooLarge { .. }), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn endless_file_is_refused_instead_of_read_forever() {
        let err = resolve_secret(
            "S4_JWT_SECRET",
            |key| (key == "S4_JWT_SECRET_FILE").then(|| OsString::from("/dev/zero")),
            |path| read_at_most(path, MAX_SECRET_FILE_LEN + 1),
        )
        .unwrap_err();
        assert!(matches!(err, SecretError::TooLarge { .. }), "{err}");
        assert!(err.to_string().contains("/dev/zero"), "{err}");
    }

    #[test]
    fn real_file_on_disk_is_read() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "disk-secret").unwrap();
        let path = file.path().to_str().unwrap().to_string();

        let value = resolve_secret(
            "S4_JWT_SECRET",
            |key| (key == "S4_JWT_SECRET_FILE").then(|| OsString::from(&path)),
            |p| read_at_most(p, MAX_SECRET_FILE_LEN + 1),
        )
        .unwrap();
        assert_eq!(value.as_deref(), Some("disk-secret"));
    }

    #[test]
    fn configured_jwt_secret_wins_in_every_mode() {
        for mode in [ServerMode::Single, ServerMode::Cluster, ServerMode::Gateway] {
            let secret = resolve_jwt_secret(Some(STRONG), mode, true).unwrap();
            assert_eq!(secret, JwtSecret::Configured(STRONG.to_string()));
        }
    }

    #[test]
    fn configured_jwt_secret_shorter_than_the_minimum_is_refused() {
        let short = "a".repeat(MIN_SECRET_LEN - 1);
        let enough = "a".repeat(MIN_SECRET_LEN);

        for mode in [ServerMode::Single, ServerMode::Cluster, ServerMode::Gateway] {
            let err = resolve_jwt_secret(Some(&short), mode, false).unwrap_err();
            assert!(
                matches!(err, SecretError::WeakJwtSecret { len } if len == MIN_SECRET_LEN - 1),
                "{err}"
            );
            assert!(resolve_jwt_secret(Some(&enough), mode, false).is_ok());
        }
        assert_eq!(
            resolve_jwt_secret(Some("short"), ServerMode::Single, false)
                .unwrap_err()
                .to_string(),
            "S4_JWT_SECRET is 5 bytes long; it must be at least 32 bytes. \
             Generate one with `openssl rand -hex 32`"
        );
    }

    #[test]
    fn single_node_without_secret_generates_a_random_one() {
        let first = resolve_jwt_secret(None, ServerMode::Single, true).unwrap();
        let second = resolve_jwt_secret(None, ServerMode::Single, true).unwrap();

        assert!(matches!(first, JwtSecret::Generated(_)));
        assert!(first.value().len() >= MIN_SECRET_LEN);
        assert_ne!(first.value(), second.value());
    }

    #[test]
    fn multi_node_with_iam_and_without_secret_is_refused() {
        for (mode, name) in [
            (ServerMode::Cluster, "cluster"),
            (ServerMode::Gateway, "gateway"),
        ] {
            let err = resolve_jwt_secret(None, mode, true).unwrap_err();
            assert!(
                matches!(err, SecretError::JwtSecretRequired { mode, .. } if mode == name),
                "{err}"
            );
            assert!(err.to_string().contains("S4_JWT_SECRET"));
            assert!(err.to_string().contains("when S4_ROOT_PASSWORD is set"));
        }
    }

    #[test]
    fn multi_node_without_iam_generates_a_secret() {
        let secret = resolve_jwt_secret(None, ServerMode::Cluster, false).unwrap();
        assert!(matches!(secret, JwtSecret::Generated(_)));
    }

    #[test]
    fn multi_node_with_iam_users_and_a_generated_secret_is_refused() {
        let generated = resolve_jwt_secret(None, ServerMode::Cluster, false).unwrap();

        for mode in [ServerMode::Cluster, ServerMode::Gateway] {
            let err = check_iam_users(&generated, mode, 1).unwrap_err();
            assert!(
                matches!(err, SecretError::JwtSecretRequired { .. }),
                "{err}"
            );
            assert!(
                err.to_string().contains("when IAM already has users"),
                "{err}"
            );
            assert!(check_iam_users(&generated, mode, 0).is_ok());
        }
    }

    #[test]
    fn iam_users_need_no_shared_secret_on_a_single_node_or_with_a_configured_one() {
        let generated = resolve_jwt_secret(None, ServerMode::Single, false).unwrap();
        let configured = JwtSecret::Configured(STRONG.to_string());

        assert!(check_iam_users(&generated, ServerMode::Single, 5).is_ok());
        assert!(check_iam_users(&configured, ServerMode::Cluster, 5).is_ok());
        assert!(check_iam_users(&configured, ServerMode::Gateway, 5).is_ok());
    }

    #[test]
    fn debug_output_never_shows_the_secret() {
        let secret = JwtSecret::Configured("super-secret-value".to_string());
        let shown = format!("{secret:?}");
        assert!(!shown.contains("super-secret-value"), "{shown}");
        assert_eq!(shown, "JwtSecret::Configured(<redacted>)");
    }
}
