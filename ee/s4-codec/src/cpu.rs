// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! What the processor this node runs on can offer the codec.
//!
//! ISA-L picks its own implementation inside `ec_encode_data` — SSE, AVX, AVX2,
//! or AVX-512 with GFNI — so S4 does not choose between them. It only has to
//! decide whether using ISA-L at all is worth it, and below AVX2 it is not: the
//! remaining paths are close enough to the portable engine that the FFI is not
//! paying for itself.
//!
//! Detection never panics and never refuses to answer. On a processor without
//! the instructions, and on every architecture that is not x86_64, the answer
//! is simply "no" and the caller falls back. See decision EC2-004.

/// Whether this CPU has the instructions that make ISA-L worthwhile.
///
/// Always false off x86_64.
#[must_use]
pub fn supports_isal_acceleration() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        // `is_x86_feature_detected!` reads CPUID once and caches the answer in
        // the standard library, so this stays cheap when called repeatedly.
        std::arch::is_x86_feature_detected!("avx2")
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the test is that asking is always safe.
    ///
    /// The answer depends on the machine, so it cannot be asserted. What can be
    /// asserted is that detection returns rather than panicking, and that it is
    /// stable across calls — a codec that changed engines mid-run would be far
    /// worse than one that is merely slow.
    #[test]
    fn detection_is_answerable_and_stable() {
        let first = supports_isal_acceleration();
        assert_eq!(first, supports_isal_acceleration());

        #[cfg(not(target_arch = "x86_64"))]
        assert!(!first, "no non-x86_64 target can offer ISA-L");
    }
}
