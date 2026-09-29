// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! ISA-L accelerated erasure coding backend for S4 Enterprise Edition.
//!
//! Intel ISA-L computes Reed-Solomon parity with SIMD instructions, which is
//! several times faster than the portable Rust engine in `s4-codec`. This crate
//! wraps it.
//!
//! # The rule that shapes everything here
//!
//! An accelerated shard must be byte-for-byte identical to the shard the
//! portable engine would produce. Speed is the only thing that may differ.
//!
//! This is not a style preference. During a rolling upgrade some nodes encode
//! with one engine and some with the other. If the bytes differed, the scrubber
//! would compare a shard against its recorded hash, call a healthy shard
//! corrupt, and repair would overwrite good data with a "fix". Degraded reads
//! would then return rebuilt garbage that still passes its own checksum.
//!
//! Byte equality holds because both engines use the same field, with reduction
//! polynomial `0x1d`, and because this crate feeds ISA-L the generator matrix
//! that `s4-codec` builds. ISA-L's own matrix generators, `gf_gen_rs_matrix` and
//! `gf_gen_cauchy1_matrix`, produce different coefficients and are therefore
//! never used. See decision EC2-002.
//!
//! # Layout
//!
//! All `unsafe` lives in [`ffi`], which is the only module that touches C.
//! Nothing unsafe appears in this crate's public API.
//!
//! # Platform support
//!
//! Built for x86_64 only. On every other architecture the modules below are
//! compiled out, so the crate exposes nothing and callers use the portable
//! engine instead. The gate is not cosmetic: `build.rs` assembles no ISA-L
//! objects off x86_64, so code that called into C would fail to link. See
//! decision EC2-004.

#[cfg(target_arch = "x86_64")]
pub mod ffi;
#[cfg(target_arch = "x86_64")]
mod rs;

#[cfg(target_arch = "x86_64")]
pub use rs::IsalReedSolomonCodec;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    /// ISA-L functions that would build a different matrix than S4 does.
    const FORBIDDEN: [&str; 2] = ["gf_gen_rs_matrix", "gf_gen_cauchy1_matrix"];

    /// The crate must neither declare nor call ISA-L's own matrix generators.
    ///
    /// Their coefficients differ from the ones `s4-codec` builds, so a shard
    /// encoded with them would not match the rest of the cluster. Decision
    /// EC2-002 forbids them; this test is what enforces the ban, rather than a
    /// reviewer noticing.
    ///
    /// The whole `src` tree is walked at test time rather than a fixed list of
    /// files, so a module added later is covered without anyone remembering to
    /// add it here. The only file skipped is this one, which necessarily
    /// contains the names being searched for.
    #[test]
    fn no_isal_matrix_generator_is_declared_or_called() {
        let mut scanned = 0usize;

        for path in rust_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src")) {
            if path.file_name() == Path::new(file!()).file_name() {
                continue;
            }
            let source = std::fs::read_to_string(&path).expect("source file is readable");
            scanned += 1;

            for (index, line) in source.lines().enumerate() {
                // Prose may name them; code may not.
                let code = line.split("//").next().unwrap_or_default();
                for forbidden in FORBIDDEN {
                    assert!(
                        !code.contains(forbidden),
                        "{}:{} uses {forbidden}, which decision EC2-002 forbids",
                        path.display(),
                        index + 1
                    );
                }
            }
        }

        // A silently empty walk would turn this test into a no-op.
        assert!(scanned > 0, "no source files were scanned");
    }

    /// Every `.rs` file under `root`, including nested modules.
    fn rust_sources(root: &Path) -> Vec<PathBuf> {
        let mut pending = vec![root.to_path_buf()];
        let mut files = Vec::new();

        while let Some(directory) = pending.pop() {
            let entries = std::fs::read_dir(&directory)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()));
            for entry in entries {
                let path = entry.expect("readable directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }

        files
    }
}
