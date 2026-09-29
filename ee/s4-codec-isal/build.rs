// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! Builds the vendored ISA-L erasure coding module and links it statically.
//!
//! Only x86_64 is built. On every other architecture this script does nothing
//! and exits successfully, so `cargo build` keeps working there and the codec
//! falls back to the pure-Rust engine. See decisions EC2-003 and EC2-004.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Lowest nasm release that can assemble the ISA-L sources we vendor.
const MIN_NASM_VERSION: (u32, u32) = (2, 14);

fn main() {
    let vendor = PathBuf::from(env("CARGO_MANIFEST_DIR")).join("vendor/isa-l");
    println!("cargo:rerun-if-changed={}", vendor.display());
    println!("cargo:rerun-if-changed=build.rs");

    let target_arch = env("CARGO_CFG_TARGET_ARCH");
    if target_arch != "x86_64" {
        // Not an error. The crate compiles to an empty library and the caller
        // uses the pure-Rust codec instead.
        println!(
            "cargo:warning=ISA-L is only built for x86_64, target is {target_arch}; \
             the crate will expose no accelerated codec"
        );
        return;
    }

    let asm_objects = assemble(&vendor);
    compile_c_and_link(&vendor, &asm_objects);
}

/// Reads a build-time environment variable that cargo always sets.
fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("cargo must set {name}"))
}

/// Assembles every vendored `.asm` file and returns the object files.
///
/// nasm is invoked directly rather than through a helper crate. The command is
/// short, and keeping it here means the exact flags used to build the shipped
/// binary are visible in one place instead of hidden behind a dependency.
fn assemble(vendor: &Path) -> Vec<PathBuf> {
    check_nasm_version();

    let out_dir = PathBuf::from(env("OUT_DIR"));
    let asm_dir = vendor.join("erasure_code");
    let include_dir = vendor.join("include");

    let mut objects = Vec::new();
    let entries = std::fs::read_dir(&asm_dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", asm_dir.display()));

    let mut sources: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "asm"))
        .collect();
    // Directory order is not stable across filesystems. Sorting keeps the build
    // reproducible, which matters because the output must be bit-identical.
    sources.sort();

    assert!(
        !sources.is_empty(),
        "no .asm sources found in {}; the vendored copy looks incomplete",
        asm_dir.display()
    );

    for source in sources {
        let stem = source
            .file_stem()
            .expect("asm file must have a name")
            .to_string_lossy()
            .into_owned();
        let object = out_dir.join(format!("{stem}.o"));

        let status = Command::new("nasm")
            .arg("-f")
            .arg("elf64")
            .arg("-I")
            .arg(format!("{}/", include_dir.display()))
            .arg("-I")
            .arg(format!("{}/", asm_dir.display()))
            .arg("-o")
            .arg(&object)
            .arg(&source)
            .status()
            .unwrap_or_else(|e| panic!("failed to run nasm: {e}"));

        assert!(status.success(), "nasm failed on {}", source.display());
        objects.push(object);
    }

    objects
}

/// Fails early and clearly when nasm is missing or too old.
///
/// Without this the build breaks somewhere in the middle of assembling with a
/// message that does not say what to install.
fn check_nasm_version() {
    let output = Command::new("nasm").arg("-v").output().unwrap_or_else(|e| {
        panic!(
            "nasm is required to build ISA-L but could not be run: {e}\n\
             Install nasm {}.{} or newer, or build without the `isal` feature.",
            MIN_NASM_VERSION.0, MIN_NASM_VERSION.1
        )
    });

    let banner = String::from_utf8_lossy(&output.stdout);
    let version = parse_nasm_version(&banner).unwrap_or_else(|| {
        panic!("cannot read a version number from nasm output: {banner:?}");
    });

    assert!(
        version >= MIN_NASM_VERSION,
        "nasm {}.{} is too old for the vendored ISA-L sources, need {}.{} or newer",
        version.0,
        version.1,
        MIN_NASM_VERSION.0,
        MIN_NASM_VERSION.1
    );
}

/// Extracts `(major, minor)` from a banner like `NASM version 2.15.05`.
fn parse_nasm_version(banner: &str) -> Option<(u32, u32)> {
    let version = banner.split_whitespace().nth(2)?;
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some((major, minor))
}

/// Compiles the C part of ISA-L and archives it together with the assembly.
fn compile_c_and_link(vendor: &Path, asm_objects: &[PathBuf]) {
    let mut build = cc::Build::new();
    build
        .include(vendor.join("include"))
        .include(vendor.join("erasure_code"))
        .file(vendor.join("erasure_code/ec_base.c"))
        .file(vendor.join("erasure_code/ec_highlevel_func.c"))
        // Third-party code we do not modify, so its warnings are noise for us.
        .warnings(false)
        .opt_level(2);

    for object in asm_objects {
        build.object(object);
    }

    build.compile("isal_ec");
}
