// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Black-box integration tests for the `license-gen` CLI binary.
//!
//! These tests spawn the compiled binary and assert on its stdout/stderr/exit
//! code. They are gated behind the `gen` feature:
//!
//! ```text
//! cargo test -p s4-ee-license --features gen --test license_gen_cli
//! ```

#![cfg(feature = "gen")]

use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde_json::Value;
use std::path::PathBuf;
use std::process::{Command, Output};

/// Path to the `license-gen` binary produced by this crate's build.
/// Cargo exposes this automatically via `CARGO_BIN_EXE_<bin-name>`.
fn license_gen_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_license-gen"))
}

fn run(args: &[&str]) -> Output {
    Command::new(license_gen_bin())
        .args(args)
        .output()
        .expect("failed to spawn license-gen")
}

/// Parse a license key string into `(json_bytes, signature)`.
fn split_license(license: &str) -> (Vec<u8>, Signature) {
    let engine = base64::engine::general_purpose::STANDARD;
    let dot = license.rfind('.').expect("license must contain '.'");
    let payload_b64 = &license[..dot];
    let sig_b64 = &license[dot + 1..];
    let json_bytes = engine.decode(payload_b64).expect("payload b64");
    let sig_bytes = engine.decode(sig_b64).expect("signature b64");
    let signature = Signature::from_slice(&sig_bytes).expect("valid signature bytes");
    (json_bytes, signature)
}

// ---------------------------------------------------------------------------
// --help / --generate-key / error paths
// ---------------------------------------------------------------------------

#[test]
fn help_prints_usage_and_exits_zero() {
    let output = run(&["--help"]);
    assert!(output.status.success(), "--help should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("S4Core License Generator"));
    assert!(stdout.contains("--private-key-file"));
    assert!(stdout.contains("--generate-key"));
    assert!(stdout.contains("--output"));
}

#[test]
fn short_help_flag_also_works() {
    let output = run(&["-h"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("S4Core License Generator"));
}

#[test]
fn no_args_prints_usage_and_exits_nonzero() {
    let output = run(&[]);
    assert!(!output.status.success());
}

#[test]
fn missing_private_key_exits_nonzero() {
    let output = run(&["--customer", "Acme"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--private-key") || stderr.contains("private"),
        "stderr should mention the missing key: {stderr}"
    );
}

#[test]
fn both_private_key_sources_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let key_file = tmp.path().join("seed.txt");
    std::fs::write(&key_file, "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=").unwrap();

    let output = run(&[
        "--private-key",
        "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=",
        "--private-key-file",
        key_file.to_str().unwrap(),
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not both") || stderr.contains("either"));
}

#[test]
fn invalid_base64_private_key_fails() {
    let output = run(&["--private-key", "this-is-not-base64!!!"]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.to_lowercase().contains("base64"));
}

#[test]
fn wrong_length_private_key_fails() {
    // 16 bytes base64-encoded — too short.
    let output = run(&["--private-key", "AAAAAAAAAAAAAAAAAAAAAA=="]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("32 bytes"));
}

#[test]
fn negative_days_rejected() {
    let output = run(&[
        "--private-key",
        "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=",
        "--days",
        "-5",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--days"),
        "error should mention --days: {stderr}"
    );
}

#[test]
fn empty_customer_rejected() {
    let output = run(&[
        "--private-key",
        "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=",
        "--customer",
        "   ",
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--customer"));
}

// ---------------------------------------------------------------------------
// --generate-key
// ---------------------------------------------------------------------------

#[test]
fn generate_key_produces_valid_keypair() {
    let output = run(&["--generate-key"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("PUBLIC_KEY_BYTES"));
    assert!(stdout.contains("seed = "));

    // Parse the seed and verify it decodes to 32 bytes.
    let seed_line = stdout.lines().find(|l| l.starts_with("seed = ")).expect("seed line");
    let seed_b64 = seed_line.trim_start_matches("seed = ").trim();
    let engine = base64::engine::general_purpose::STANDARD;
    let seed = engine.decode(seed_b64).expect("seed is base64");
    assert_eq!(seed.len(), 32, "seed must be 32 bytes");

    // Parse the public key bytes from the generated const block.
    // Locate the opening bracket of the array literal after `= [`.
    let array_start_tag = "PUBLIC_KEY_BYTES: [u8; 32] = [";
    let array_start = stdout.find(array_start_tag).unwrap() + array_start_tag.len();
    let hex_section_end = stdout[array_start..].find("];").unwrap();
    let hex_section = &stdout[array_start..array_start + hex_section_end];

    // Parse each comma-separated byte loudly: an unexpected format is a bug,
    // not something to silently skip. `filter(|s| !s.is_empty())` only drops
    // the empty tail after the trailing comma of the last byte.
    let pk_bytes: Vec<u8> = hex_section
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| {
            let hex = s.strip_prefix("0x").unwrap_or_else(|| {
                panic!("expected 0x-prefixed hex byte, got '{s}' in section:\n{hex_section}")
            });
            u8::from_str_radix(hex, 16).unwrap_or_else(|e| panic!("invalid hex byte '{s}': {e}"))
        })
        .collect();
    assert_eq!(pk_bytes.len(), 32, "public key must be 32 bytes");

    // Derive the public key from the seed and verify it matches.
    let mut seed_arr = [0u8; 32];
    seed_arr.copy_from_slice(&seed);
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&seed_arr);
    let derived_pk = signing_key.verifying_key().to_bytes();
    assert_eq!(
        derived_pk.as_slice(),
        pk_bytes.as_slice(),
        "public key must match the private seed"
    );
}

// ---------------------------------------------------------------------------
// Signing: round-trip with ed25519-dalek verification
// ---------------------------------------------------------------------------

/// A fixed test seed shared with `license.rs` unit tests.
const TEST_SEED_B64: &str = "EmsxsGpkANqm7m8Mhvh1sAIB3Cd+O7ABKZIgO8WRL+Y=";

fn test_verifying_key() -> VerifyingKey {
    let engine = base64::engine::general_purpose::STANDARD;
    let seed_bytes = engine.decode(TEST_SEED_B64).unwrap();
    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    ed25519_dalek::SigningKey::from_bytes(&seed).verifying_key()
}

#[test]
fn sign_and_verify_round_trip_inline_key() {
    let output = run(&[
        "--private-key",
        TEST_SEED_B64,
        "--customer",
        "Integration Corp",
        "--customer-id",
        "int-001",
        "--days",
        "90",
        "--max-pools",
        "7",
        "--max-nodes",
        "21",
    ]);
    assert!(output.status.success(), "signing should succeed");

    let license = String::from_utf8_lossy(&output.stdout);
    let license = license.trim();
    assert!(!license.is_empty());

    // Inline --private-key must emit a WARNING and a human-readable summary
    // block on stderr. The license key itself stays on stdout for piping.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("WARNING"),
        "inline --private-key should emit a warning: {stderr}"
    );
    assert!(
        stderr.contains("Customer:"),
        "summary should include 'Customer:' line: {stderr}"
    );
    assert!(
        stderr.contains("Integration Corp"),
        "summary should include the customer name: {stderr}"
    );
    assert!(
        stderr.contains("Customer ID:"),
        "summary should include 'Customer ID:' line: {stderr}"
    );
    assert!(
        stderr.contains("int-001"),
        "summary should include the customer id: {stderr}"
    );
    assert!(
        stderr.contains("Issued:"),
        "summary should include 'Issued:' line: {stderr}"
    );
    assert!(
        stderr.contains("Expires:"),
        "summary should include 'Expires:' line: {stderr}"
    );
    assert!(
        stderr.contains("Max pools:"),
        "summary should include 'Max pools:' line: {stderr}"
    );
    assert!(
        stderr.contains("Max nodes:"),
        "summary should include 'Max nodes:' line: {stderr}"
    );

    // Verify signature against the known public key.
    let (json_bytes, signature) = split_license(license);
    test_verifying_key()
        .verify(&json_bytes, &signature)
        .expect("signature must verify with the matching public key");

    // Inspect payload fields.
    let payload: Value = serde_json::from_slice(&json_bytes).unwrap();
    assert_eq!(payload["customer"], "Integration Corp");
    assert_eq!(payload["customer_id"], "int-001");
    assert_eq!(payload["capabilities"]["max_pools"], 7);
    assert_eq!(payload["capabilities"]["max_nodes_per_pool"], 21);
    assert_eq!(payload["capabilities"]["deep_scrubber"], true);
    assert_eq!(payload["capabilities"]["rolling_upgrades"], true);
    assert_eq!(payload["capabilities"]["ldap"], true);
    assert_eq!(payload["capabilities"]["saml"], true);
    assert_eq!(payload["capabilities"]["webdav"], true);
    assert_eq!(payload["capabilities"]["audit_log"], true);
}

#[test]
fn sign_reads_private_key_from_file() {
    let tmp = tempfile::tempdir().unwrap();
    let key_file = tmp.path().join("seed.txt");
    // Trailing newline deliberately — should be trimmed.
    std::fs::write(&key_file, format!("{TEST_SEED_B64}\n")).unwrap();

    let output = run(&[
        "--private-key-file",
        key_file.to_str().unwrap(),
        "--customer",
        "File Corp",
        "--days",
        "30",
    ]);
    assert!(output.status.success(), "signing from file should succeed");

    // File-based key must NOT emit the inline-key WARNING.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("WARNING"),
        "file-based key should not emit an inline-key warning: {stderr}"
    );

    let license = String::from_utf8_lossy(&output.stdout);
    let (json_bytes, signature) = split_license(license.trim());
    test_verifying_key().verify(&json_bytes, &signature).unwrap();

    let payload: Value = serde_json::from_slice(&json_bytes).unwrap();
    assert_eq!(payload["customer"], "File Corp");
}

#[test]
fn output_flag_writes_license_to_file() {
    let tmp = tempfile::tempdir().unwrap();
    let out_file = tmp.path().join("acme.license");

    let output = run(&[
        "--private-key",
        TEST_SEED_B64,
        "--customer",
        "Output Corp",
        "--output",
        out_file.to_str().unwrap(),
    ]);
    assert!(output.status.success());

    // Stdout should be empty (license went to file).
    assert!(output.stdout.is_empty());

    // File should contain a valid license.
    let contents = std::fs::read_to_string(&out_file).unwrap();
    let license = contents.trim();
    let (json_bytes, signature) = split_license(license);
    test_verifying_key().verify(&json_bytes, &signature).unwrap();
}

#[test]
fn no_flags_disable_capabilities() {
    let output = run(&[
        "--private-key",
        TEST_SEED_B64,
        "--customer",
        "Minimal Corp",
        "--no-ldap",
        "--no-saml",
        "--no-webdav",
        "--no-audit-log",
    ]);
    assert!(output.status.success());

    let license = String::from_utf8_lossy(&output.stdout);
    let (json_bytes, _signature) = split_license(license.trim());
    let payload: Value = serde_json::from_slice(&json_bytes).unwrap();

    // Explicitly disabled:
    assert_eq!(payload["capabilities"]["ldap"], false);
    assert_eq!(payload["capabilities"]["saml"], false);
    assert_eq!(payload["capabilities"]["webdav"], false);
    assert_eq!(payload["capabilities"]["audit_log"], false);
    // Still enabled by default:
    assert_eq!(payload["capabilities"]["deep_scrubber"], true);
    assert_eq!(payload["capabilities"]["rolling_upgrades"], true);
}

#[test]
fn defaults_produce_unlimited_caps() {
    let output = run(&["--private-key", TEST_SEED_B64, "--customer", "Default Corp"]);
    assert!(output.status.success());

    let license = String::from_utf8_lossy(&output.stdout);
    let (json_bytes, _sig) = split_license(license.trim());
    let payload: Value = serde_json::from_slice(&json_bytes).unwrap();

    assert_eq!(payload["capabilities"]["max_pools"], u32::MAX);
    assert_eq!(payload["capabilities"]["max_nodes_per_pool"], u32::MAX);
}

// ---------------------------------------------------------------------------
// Argument parser — defensive error paths
// ---------------------------------------------------------------------------

#[test]
fn flag_without_value_is_rejected() {
    // `--customer` is the last arg, with no value following it. Previously
    // this silently fell back to the default "Development" customer.
    let output = run(&["--private-key", TEST_SEED_B64, "--customer"]);
    assert!(
        !output.status.success(),
        "--customer without a value must not succeed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--customer"),
        "error should mention --customer: {stderr}"
    );
    assert!(
        stderr.contains("requires a value"),
        "error should say 'requires a value': {stderr}"
    );
}

#[test]
fn flag_value_cannot_start_with_double_dash() {
    // Typical mistake: forgetting to pass a value to --customer, so the
    // next flag (--no-ldap) gets eaten as the customer name. The parser
    // must catch this instead of silently producing a license for a
    // customer literally named "--no-ldap".
    let output = run(&["--private-key", TEST_SEED_B64, "--customer", "--no-ldap"]);
    assert!(
        !output.status.success(),
        "--customer --no-ldap must not succeed"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("--customer"),
        "error should mention --customer: {stderr}"
    );
    assert!(
        stderr.contains("--no-ldap"),
        "error should echo the offending value: {stderr}"
    );
}

#[test]
fn output_flag_warns_when_overwriting_existing_file() {
    let tmp = tempfile::tempdir().unwrap();
    let out_file = tmp.path().join("existing.license");
    std::fs::write(&out_file, "old contents").unwrap();

    let output = run(&[
        "--private-key",
        TEST_SEED_B64,
        "--customer",
        "Overwrite Corp",
        "--output",
        out_file.to_str().unwrap(),
    ]);
    assert!(output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("overwriting existing file"),
        "stderr should mention overwriting: {stderr}"
    );

    // File should now contain a fresh license, not the old contents.
    let contents = std::fs::read_to_string(&out_file).unwrap();
    let license = contents.trim();
    assert_ne!(license, "old contents");
    let (json_bytes, signature) = split_license(license);
    test_verifying_key().verify(&json_bytes, &signature).unwrap();
}
