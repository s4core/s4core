// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! License generation tool for S4Core Enterprise Edition.
//!
//! Signs license payloads with an Ed25519 private key. The private key can be
//! supplied either directly on the command line (`--private-key`) or — safer —
//! loaded from a file (`--private-key-file`).
//!
//! The tool can also bootstrap a fresh keypair with `--generate-key`.
//!
//! # Usage
//!
//! ```bash
//! # Build the tool (requires `gen` feature):
//! cargo build -p s4-ee-license --features gen --bin license-gen
//!
//! # Generate a new production keypair (one-time setup):
//! license-gen --generate-key
//!
//! # Sign a license (preferred: read private key from a file):
//! license-gen \
//!   --private-key-file ee/private_key.txt \
//!   --customer "Acme Corp" \
//!   --customer-id "acme-001" \
//!   --days 365 \
//!   --max-pools 100 \
//!   --max-nodes 50 \
//!   --output acme.license
//!
//! # Output format: base64(json).base64(signature)
//! ```

use base64::Engine;
use chrono::{Duration, Utc};
use ed25519_dalek::{Signer, SigningKey};
use s4_ee_license::Capabilities;
use serde::Serialize;
use std::process;

const HELP: &str = "\
S4Core License Generator

Usage:
  license-gen --generate-key
  license-gen (--private-key <B64> | --private-key-file <PATH>) [options]
  license-gen --help

Modes:
  --generate-key                 Generate a fresh Ed25519 keypair and exit

Private key (one of):
  --private-key <BASE64>         Ed25519 private seed, 32 bytes base64
                                 (WARNING: visible in shell history and ps)
  --private-key-file <PATH>      Read the seed from a file (preferred)

License fields (all optional, sensible defaults):
  --customer <NAME>              Customer display name         [Development]
  --customer-id <ID>             Customer identifier           [dev-000]
  --days <N>                     Duration in days              [365]
  --max-pools <N>                Max server pools              [unlimited]
  --max-nodes <N>                Max nodes per pool            [unlimited]

Disable specific capabilities (default: all enabled):
  --no-deep-scrubber             Disable deep SHA-256 scrubber
  --no-rolling-upgrades          Disable rolling upgrades
  --no-ldap                      Disable LDAP auth
  --no-saml                      Disable SAML/OIDC auth
  --no-webdav                    Disable WebDAV
  --no-audit-log                 Disable audit log

Output:
  --output <PATH>                Write the license key to a file (default: stdout)
  --help                         Show this help

Exit codes:
  0  success
  1  invalid arguments or signing failure
";

/// Sentinel value used in the CLI to mean "no limit" for count-based
/// capabilities. Matches `u32::MAX` because `Capabilities` stores limits
/// as `u32` and the library treats `u32::MAX` as unlimited.
const UNLIMITED: u32 = u32::MAX;

/// License payload (mirrors `License` but owned for serialization).
#[derive(Serialize)]
struct LicensePayload {
    customer: String,
    customer_id: String,
    issued_at: String,
    expires_at: String,
    capabilities: Capabilities,
}

#[cold]
fn fail(msg: impl AsRef<str>) -> ! {
    eprintln!("Error: {}", msg.as_ref());
    eprintln!();
    eprintln!("Run `license-gen --help` for usage.");
    process::exit(1);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // --help is checked first so it works with any number of args.
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{HELP}");
        process::exit(0);
    }

    // --generate-key is a standalone mode.
    if args.iter().any(|a| a == "--generate-key") {
        generate_keypair();
        process::exit(0);
    }

    // Normal signing mode requires at least one private key source.
    if args.len() < 2 {
        eprintln!("{HELP}");
        process::exit(1);
    }

    // `get_arg` fails loudly if a flag is present but has no value, or if its
    // "value" is actually another `--flag` (typical mistake: forgetting to
    // pass a value so the next flag gets eaten). This also protects `has_flag`
    // below: if someone writes `--customer --no-ldap`, we error out here
    // instead of silently setting customer="--no-ldap" and also treating
    // `--no-ldap` as a boolean flag.
    let get_arg = |name: &str| -> Option<String> {
        let pos = args.iter().position(|a| a == name)?;
        match args.get(pos + 1) {
            None => fail(format!("{name} requires a value")),
            Some(value) if value.starts_with("--") => fail(format!(
                "{name} requires a value, got flag '{value}' \
                 (did you forget to pass a value?)"
            )),
            Some(value) => Some(value.clone()),
        }
    };
    let has_flag = |name: &str| -> bool { args.iter().any(|a| a == name) };

    // Private key: either inline, or from file.
    let private_key_b64 = match (get_arg("--private-key"), get_arg("--private-key-file")) {
        (Some(_), Some(_)) => {
            fail("use either --private-key or --private-key-file, not both");
        }
        (Some(key), None) => {
            eprintln!(
                "WARNING: --private-key was passed on the command line. \
                 The key may leak via shell history or process listings. \
                 Prefer --private-key-file."
            );
            key
        }
        (None, Some(path)) => std::fs::read_to_string(&path)
            .unwrap_or_else(|e| fail(format!("cannot read --private-key-file '{path}': {e}")))
            .trim()
            .to_owned(),
        (None, None) => {
            fail("either --private-key or --private-key-file is required");
        }
    };

    let customer = get_arg("--customer").unwrap_or_else(|| "Development".into());
    if customer.trim().is_empty() {
        fail("--customer must not be empty");
    }
    let customer_id = get_arg("--customer-id").unwrap_or_else(|| "dev-000".into());
    if customer_id.trim().is_empty() {
        fail("--customer-id must not be empty");
    }

    let days: i64 = match get_arg("--days") {
        Some(s) => s
            .parse()
            .unwrap_or_else(|_| fail(format!("--days must be a positive integer, got '{s}'"))),
        None => 365,
    };
    if days <= 0 {
        fail("--days must be greater than zero");
    }

    let max_pools: u32 = match get_arg("--max-pools") {
        Some(s) => s.parse().unwrap_or_else(|_| {
            fail(format!(
                "--max-pools must be a non-negative integer, got '{s}'"
            ))
        }),
        None => UNLIMITED,
    };

    let max_nodes: u32 = match get_arg("--max-nodes") {
        Some(s) => s.parse().unwrap_or_else(|_| {
            fail(format!(
                "--max-nodes must be a non-negative integer, got '{s}'"
            ))
        }),
        None => UNLIMITED,
    };

    // Boolean capabilities (default: all enabled; --no-* disables).
    let deep_scrubber = !has_flag("--no-deep-scrubber");
    let rolling_upgrades = !has_flag("--no-rolling-upgrades");
    let ldap = !has_flag("--no-ldap");
    let saml = !has_flag("--no-saml");
    let webdav = !has_flag("--no-webdav");
    let audit_log = !has_flag("--no-audit-log");

    // Decode private key seed.
    let engine = base64::engine::general_purpose::STANDARD;
    let seed_bytes = engine
        .decode(&private_key_b64)
        .unwrap_or_else(|e| fail(format!("invalid base64 in private key: {e}")));

    if seed_bytes.len() != 32 {
        fail(format!(
            "private key seed must be 32 bytes, got {}",
            seed_bytes.len()
        ));
    }

    let mut seed = [0u8; 32];
    seed.copy_from_slice(&seed_bytes);
    let signing_key = SigningKey::from_bytes(&seed);

    // Build payload.
    let now = Utc::now();
    let payload = LicensePayload {
        customer,
        customer_id,
        issued_at: now.to_rfc3339(),
        expires_at: (now + Duration::days(days)).to_rfc3339(),
        capabilities: Capabilities {
            max_pools,
            max_nodes_per_pool: max_nodes,
            deep_scrubber,
            rolling_upgrades,
            ldap,
            saml,
            webdav,
            audit_log,
        },
    };

    let json = serde_json::to_string(&payload)
        .unwrap_or_else(|e| fail(format!("failed to serialize payload: {e}")));

    // Sign.
    let json_bytes = json.as_bytes();
    let signature = signing_key.sign(json_bytes);

    // Encode: base64(json).base64(signature).
    let payload_b64 = engine.encode(json_bytes);
    let sig_b64 = engine.encode(signature.to_bytes());
    let license_key = format!("{payload_b64}.{sig_b64}");

    // Summary → stderr (so stdout stays clean for piping).
    eprintln!("Customer:    {}", payload.customer);
    eprintln!("Customer ID: {}", payload.customer_id);
    eprintln!("Issued:      {}", payload.issued_at);
    eprintln!("Expires:     {}", payload.expires_at);
    eprintln!(
        "Max pools:   {}",
        fmt_unlimited(payload.capabilities.max_pools)
    );
    eprintln!(
        "Max nodes:   {}",
        fmt_unlimited(payload.capabilities.max_nodes_per_pool)
    );
    eprintln!();

    // Output to file or stdout.
    match get_arg("--output") {
        Some(path) => {
            // Re-issuing a license for the same customer is a valid workflow,
            // so we overwrite intentionally — but surface a notice so it's
            // obvious in operator logs.
            if std::path::Path::new(&path).exists() {
                eprintln!("Note: overwriting existing file '{path}'");
            }
            std::fs::write(&path, format!("{license_key}\n"))
                .unwrap_or_else(|e| fail(format!("failed to write --output '{path}': {e}")));
            eprintln!("License key written to {path}");
        }
        None => {
            println!("{license_key}");
        }
    }
}

fn fmt_unlimited(value: u32) -> String {
    if value == UNLIMITED {
        "unlimited".into()
    } else {
        value.to_string()
    }
}

/// Bootstrap a fresh Ed25519 keypair and print it to stdout.
///
/// This is the same format expected by `license.rs` (`PUBLIC_KEY_BYTES`) and
/// the `--private-key` / `--private-key-file` inputs.
fn generate_keypair() {
    use rand::rngs::OsRng;

    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();

    let engine = base64::engine::general_purpose::STANDARD;
    let seed_b64 = engine.encode(signing_key.to_bytes());

    println!("# S4Core Ed25519 keypair");
    println!("# Generated: {}", Utc::now().to_rfc3339());
    println!("#");
    println!("# SECURITY: store the private seed in a secrets manager or offline.");
    println!("# Anyone with the seed can forge licenses for this public key.");
    println!();
    println!("# Public key (embed in ee/s4-ee-license/src/license.rs as PUBLIC_KEY_BYTES):");
    print!("const PUBLIC_KEY_BYTES: [u8; 32] = [");
    for (i, b) in verifying_key.to_bytes().iter().enumerate() {
        if i % 16 == 0 {
            print!("\n    ");
        } else {
            print!(" ");
        }
        print!("0x{b:02x},");
    }
    println!("\n];");
    println!();
    println!("# Private seed (save to ee/private_key.txt, gitignored):");
    println!("seed = {seed_b64}");
}
