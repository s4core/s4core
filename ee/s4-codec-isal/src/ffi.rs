// Copyright 2026 S4Core Team
//
// Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

//! The only module in the workspace that calls into C.
//!
//! Everything unsafe lives here. The rest of the crate, and every caller outside
//! it, works with the safe functions below and never sees a raw pointer.
//!
//! Each wrapper checks what C cannot check for itself: buffer lengths, matching
//! shard counts, and values that must fit into a C `int`. A violation returns a
//! [`CodecError`] instead of causing undefined behaviour.
//!
//! # The read-only inputs
//!
//! ISA-L declares its input buffers as `unsigned char *` even though it only
//! reads them: the coding matrix in `ec_init_tables`, and the tables and source
//! buffers in `ec_encode_data`. This was checked against the vendored sources —
//! `ec_init_tables_base` walks the matrix with `*a++` and writes only to
//! `g_tbls`, and the encode paths write only to `coding`.
//!
//! Both wrappers therefore cast a shared slice to `*mut` rather than copying
//! it, and both do so for the same stated reason. This matters more than the
//! saved copy: several threads may hand the same cached tables to C at once, so
//! the read-only contract is what makes that sound, and it is written down in
//! one place instead of being reasoned out twice.
//!
//! See decision EC2-001 for why the boundary is drawn here.

use std::ffi::{c_int, c_uchar};

use s4_codec::CodecError;

/// Table space ISA-L needs for one (output row, input source) pair, in bytes.
///
/// Fixed by the library: `ec_init_tables` writes 32 bytes per pair and the
/// encode functions read the same layout back.
const TABLE_BYTES_PER_PAIR: usize = 32;

/// Signature every ISA-L table builder shares.
type InitTablesFn =
    unsafe extern "C" fn(k: c_int, rows: c_int, a: *mut c_uchar, gftbls: *mut c_uchar);

/// Signature every ISA-L encode implementation shares.
type EncodeDataFn = unsafe extern "C" fn(
    len: c_int,
    k: c_int,
    rows: c_int,
    gftbls: *mut c_uchar,
    data: *mut *mut c_uchar,
    coding: *mut *mut c_uchar,
);

/// Declarations of the ISA-L functions we use.
///
/// These are written by hand rather than generated. Their signatures are
/// trivial, and a reviewer can check each line against
/// `vendor/isa-l/include/erasure_code.h` and `erasure_code/ec_multibinary.asm`
/// directly. Generating thousands of lines nobody reads would work against the
/// point of keeping this boundary small and auditable. See decision EC2-008.
///
/// Production needs three of them: `gf_mul`, `ec_init_tables` and
/// `ec_encode_data`. The rest are the individual implementations that
/// `ec_encode_data` dispatches to, declared so [`IsalVariant`] can reach each
/// one directly; see that type for why.
mod sys {
    use std::ffi::{c_int, c_uchar};

    extern "C" {
        /// `unsigned char gf_mul(unsigned char a, unsigned char b)`
        pub(super) fn gf_mul(a: c_uchar, b: c_uchar) -> c_uchar;

        /// `void ec_init_tables(int k, int rows, unsigned char *a, unsigned char *gftbls)`
        ///
        /// Reads `rows * k` coefficients from `a` and writes `32 * k * rows`
        /// bytes to `gftbls`. `a` is declared mutable in C but only read.
        pub(super) fn ec_init_tables(k: c_int, rows: c_int, a: *mut c_uchar, gftbls: *mut c_uchar);

        /// Table builder behind every non-GFNI implementation.
        pub(super) fn ec_init_tables_base(
            k: c_int,
            rows: c_int,
            a: *mut c_uchar,
            gftbls: *mut c_uchar,
        );

        /// Table builder behind both GFNI implementations.
        ///
        /// Same buffer size, different contents: GFNI multiplies through an
        /// 8x8 bit matrix instead of a lookup table. Tables and encode function
        /// therefore have to come from the same family.
        pub(super) fn ec_init_tables_gfni(
            k: c_int,
            rows: c_int,
            a: *mut c_uchar,
            gftbls: *mut c_uchar,
        );

        /// `void ec_encode_data(int len, int k, int rows, unsigned char *gftbls,
        ///                      unsigned char **data, unsigned char **coding)`
        ///
        /// Reads `len` bytes from each of the `k` `data` buffers and writes
        /// `len` bytes to each of the `rows` `coding` buffers. The `data`
        /// buffers are declared mutable in C but only read.
        ///
        /// Any `len` is accepted: the library falls back to its portable
        /// implementation below 16 bytes, so callers do not need a minimum.
        pub(super) fn ec_encode_data(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// Portable C implementation. Runs on any CPU.
        pub(super) fn ec_encode_data_base(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// SSE4.2 implementation.
        pub(super) fn ec_encode_data_sse(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// AVX implementation.
        pub(super) fn ec_encode_data_avx(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// AVX2 implementation.
        pub(super) fn ec_encode_data_avx2(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// AVX2 implementation using GFNI. Needs GFNI tables.
        pub(super) fn ec_encode_data_avx2_gfni(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// AVX-512 implementation.
        pub(super) fn ec_encode_data_avx512(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );

        /// AVX-512 implementation using GFNI. Needs GFNI tables.
        pub(super) fn ec_encode_data_avx512_gfni(
            len: c_int,
            k: c_int,
            rows: c_int,
            gftbls: *mut c_uchar,
            data: *mut *mut c_uchar,
            coding: *mut *mut c_uchar,
        );
    }
}

/// One ISA-L implementation of the encode step.
///
/// Production always uses [`IsalVariant::Dispatched`], which is ISA-L's own
/// `ec_encode_data`: it inspects CPUID once and jumps to one of the named
/// implementations below. That is the whole point of the library, and nothing
/// in the codec chooses an implementation by hand.
///
/// The named ones exist for a single reason. A differential test that runs on
/// one machine only ever exercises the implementation that machine's CPUID
/// selected, so byte equality would be proven for that one and merely assumed
/// for the other six. With the variants reachable directly, the test can walk
/// every implementation this CPU is able to execute and compare each against
/// the portable engine. See task `EC2-ISA-04-003`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsalVariant {
    /// Whatever ISA-L's dispatcher picks for this CPU. The production path.
    Dispatched,
    /// Portable C, no SIMD.
    Base,
    /// SSE4.2.
    Sse,
    /// AVX.
    Avx,
    /// AVX2.
    Avx2,
    /// AVX2 with GFNI.
    Avx2Gfni,
    /// AVX-512.
    Avx512,
    /// AVX-512 with GFNI.
    Avx512Gfni,
}

impl IsalVariant {
    /// Every implementation the vendored library carries.
    pub const ALL: [Self; 8] = [
        Self::Dispatched,
        Self::Base,
        Self::Sse,
        Self::Avx,
        Self::Avx2,
        Self::Avx2Gfni,
        Self::Avx512,
        Self::Avx512Gfni,
    ];

    /// Name used in log lines and test failures.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Dispatched => "dispatched",
            Self::Base => "base",
            Self::Sse => "sse",
            Self::Avx => "avx",
            Self::Avx2 => "avx2",
            Self::Avx2Gfni => "avx2_gfni",
            Self::Avx512 => "avx512",
            Self::Avx512Gfni => "avx512_gfni",
        }
    }

    /// True when this CPU can execute this implementation.
    ///
    /// Running one it cannot is not a slow path but an illegal instruction, so
    /// this is what keeps [`encode_data_with`] safe to call with any variant.
    ///
    /// The conditions mirror `mbin_dispatch_init8` in
    /// `vendor/isa-l/include/multibinary.asm` — the same CPUID bits the library
    /// itself tests, expressed through `std::arch`, which also accounts for the
    /// operating system having enabled the wider register files.
    #[must_use]
    pub fn is_supported(self) -> bool {
        // The AVX-512 group ISA-L requires as one unit ("G1").
        let avx512_g1 = is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512vl")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512cd")
            && is_x86_feature_detected!("avx512dq");
        // The GFNI group, likewise required as a unit ("G2").
        let gfni_group = is_x86_feature_detected!("gfni")
            && is_x86_feature_detected!("vaes")
            && is_x86_feature_detected!("vpclmulqdq");

        match self {
            Self::Dispatched | Self::Base => true,
            Self::Sse => is_x86_feature_detected!("sse4.2"),
            Self::Avx => is_x86_feature_detected!("avx"),
            Self::Avx2 => is_x86_feature_detected!("avx2"),
            Self::Avx2Gfni => is_x86_feature_detected!("avx2") && gfni_group,
            Self::Avx512 => avx512_g1,
            Self::Avx512Gfni => {
                avx512_g1
                    && gfni_group
                    && is_x86_feature_detected!("avx512vbmi2")
                    && is_x86_feature_detected!("avx512vnni")
                    && is_x86_feature_detected!("avx512bitalg")
                    && is_x86_feature_detected!("avx512vpopcntdq")
            }
        }
    }

    /// Table builder that produces tables this implementation can read.
    fn init_tables_fn(self) -> InitTablesFn {
        match self {
            Self::Dispatched => sys::ec_init_tables,
            Self::Avx2Gfni | Self::Avx512Gfni => sys::ec_init_tables_gfni,
            Self::Base | Self::Sse | Self::Avx | Self::Avx2 | Self::Avx512 => {
                sys::ec_init_tables_base
            }
        }
    }

    /// The encode entry point itself.
    fn encode_fn(self) -> EncodeDataFn {
        match self {
            Self::Dispatched => sys::ec_encode_data,
            Self::Base => sys::ec_encode_data_base,
            Self::Sse => sys::ec_encode_data_sse,
            Self::Avx => sys::ec_encode_data_avx,
            Self::Avx2 => sys::ec_encode_data_avx2,
            Self::Avx2Gfni => sys::ec_encode_data_avx2_gfni,
            Self::Avx512 => sys::ec_encode_data_avx512,
            Self::Avx512Gfni => sys::ec_encode_data_avx512_gfni,
        }
    }

    /// Refuses a variant this CPU cannot run.
    fn ensure_supported(self) -> Result<(), CodecError> {
        if self.is_supported() {
            return Ok(());
        }
        Err(CodecError::InvalidInput {
            reason: format!(
                "ISA-L implementation '{}' is not supported by this CPU",
                self.name()
            ),
        })
    }
}

/// Multiplies two elements of GF(2^8) using ISA-L.
///
/// Exists so the field can be compared against
/// [`s4_codec::reference::gf_mul`]. Production code multiplies through the
/// encode functions, which are far faster than calling this per byte.
#[must_use]
pub fn gf_mul(a: u8, b: u8) -> u8 {
    // SAFETY: a pure function over two bytes. It reads no memory, has no
    // preconditions, and cannot fail.
    unsafe { sys::gf_mul(a, b) }
}

/// Builds the multiplication tables ISA-L uses to encode.
///
/// `matrix` holds the coding coefficients row by row, `rows * k` bytes in total.
/// The returned tables are passed to [`encode_data`] unchanged.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when the dimensions are zero, do not fit
/// into a C `int`, or do not match the length of `matrix`.
pub fn init_tables(k: usize, rows: usize, matrix: &[u8]) -> Result<Vec<u8>, CodecError> {
    init_tables_for(IsalVariant::Dispatched, k, rows, matrix)
}

/// Builds the multiplication tables of one specific ISA-L implementation.
///
/// The tables must be passed to [`encode_data_with`] under the same variant:
/// the GFNI implementations read a different table layout than the others.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when this CPU cannot run the variant,
/// or when the dimensions are zero, do not fit into a C `int`, or do not match
/// the length of `matrix`.
pub fn init_tables_for(
    variant: IsalVariant,
    k: usize,
    rows: usize,
    matrix: &[u8],
) -> Result<Vec<u8>, CodecError> {
    variant.ensure_supported()?;
    let k_c = to_c_int(k, "k")?;
    let rows_c = to_c_int(rows, "rows")?;

    let expected = rows.checked_mul(k).ok_or_else(|| CodecError::InvalidInput {
        reason: "coding matrix dimensions overflow".to_string(),
    })?;
    if matrix.len() != expected {
        return Err(CodecError::InvalidInput {
            reason: format!(
                "coding matrix must hold {expected} coefficient(s) for {rows}x{k}, got {}",
                matrix.len()
            ),
        });
    }

    let table_len =
        expected
            .checked_mul(TABLE_BYTES_PER_PAIR)
            .ok_or_else(|| CodecError::InvalidInput {
                reason: "table size overflows".to_string(),
            })?;
    let mut tables = vec![0u8; table_len];

    // SAFETY: the variant is supported by this CPU, checked above. `k` and
    // `rows` are positive and fit in c_int. `matrix` holds exactly `rows * k`
    // bytes, which is what ISA-L reads, and it is only read:
    // `ec_init_tables_base` walks it with `*a++` and writes solely to `g_tbls`
    // (see the read-only note in this module's documentation), and the GFNI
    // builder does the same. `tables` holds exactly `32 * k * rows` bytes,
    // which is what ISA-L writes, and is exclusively owned here. Both buffers
    // outlive the call.
    unsafe {
        (variant.init_tables_fn())(k_c, rows_c, matrix.as_ptr().cast_mut(), tables.as_mut_ptr());
    }

    Ok(tables)
}

/// Computes parity buffers from data buffers.
///
/// `tables` comes from [`init_tables`]. Every buffer in `data` and `parity` must
/// be the same length; that length is what gets encoded.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when the buffer counts do not match the
/// tables, when buffer lengths differ, or when a size does not fit into a C
/// `int`.
pub fn encode_data(
    tables: &[u8],
    data: &[&[u8]],
    parity: &mut [Vec<u8>],
) -> Result<(), CodecError> {
    encode_data_with(IsalVariant::Dispatched, tables, data, parity)
}

/// Computes parity buffers with one specific ISA-L implementation.
///
/// `tables` must come from [`init_tables_for`] under the same variant.
///
/// # Errors
///
/// Returns [`CodecError::InvalidInput`] when this CPU cannot run the variant,
/// when the buffer counts do not match the tables, when buffer lengths differ,
/// or when a size does not fit into a C `int`.
pub fn encode_data_with(
    variant: IsalVariant,
    tables: &[u8],
    data: &[&[u8]],
    parity: &mut [Vec<u8>],
) -> Result<(), CodecError> {
    variant.ensure_supported()?;
    let k = data.len();
    let rows = parity.len();
    let k_c = to_c_int(k, "data shard count")?;
    let rows_c = to_c_int(rows, "parity shard count")?;

    let expected_tables = k
        .checked_mul(rows)
        .and_then(|pairs| pairs.checked_mul(TABLE_BYTES_PER_PAIR))
        .ok_or_else(|| CodecError::InvalidInput {
            reason: "table size overflows".to_string(),
        })?;
    if tables.len() != expected_tables {
        return Err(CodecError::InvalidInput {
            reason: format!(
                "tables must be {expected_tables} byte(s) for {rows}x{k}, got {}",
                tables.len()
            ),
        });
    }

    let len = data[0].len();
    if data.iter().any(|buffer| buffer.len() != len) {
        return Err(CodecError::InvalidInput {
            reason: "all data buffers must have the same length".to_string(),
        });
    }
    if parity.iter().any(|buffer| buffer.len() != len) {
        return Err(CodecError::InvalidInput {
            reason: "parity buffers must have the same length as data buffers".to_string(),
        });
    }
    let len_c = to_c_int(len, "buffer length")?;

    // ISA-L wants arrays of pointers. The data pointers are cast to `*mut`
    // because that is what the C signature says, but the library only reads
    // through them, so no aliasing rule is broken.
    let mut data_ptrs: Vec<*mut c_uchar> =
        data.iter().map(|buffer| buffer.as_ptr().cast_mut()).collect();
    let mut parity_ptrs: Vec<*mut c_uchar> =
        parity.iter_mut().map(|buffer| buffer.as_mut_ptr()).collect();

    // SAFETY: the variant is supported by this CPU, checked above, so its
    // instructions are legal here. All counts are positive and fit in c_int.
    // `tables` has the exact size ISA-L expects for this k and rows, and like
    // the data buffers it is only read (see the read-only note in this module's
    // documentation), which is why the shared slice is cast rather than copied.
    // `data_ptrs` holds k valid pointers and `parity_ptrs` holds rows valid
    // pointers, each to a buffer of `len` bytes, checked above. The parity
    // buffers are exclusively borrowed for the call, so the writes cannot alias
    // the reads.
    unsafe {
        (variant.encode_fn())(
            len_c,
            k_c,
            rows_c,
            tables.as_ptr().cast_mut(),
            data_ptrs.as_mut_ptr(),
            parity_ptrs.as_mut_ptr(),
        );
    }

    Ok(())
}

/// Converts a count to a C `int`, rejecting zero and anything too large.
fn to_c_int(value: usize, what: &str) -> Result<c_int, CodecError> {
    if value == 0 {
        return Err(CodecError::InvalidInput {
            reason: format!("{what} must be greater than 0"),
        });
    }
    c_int::try_from(value).map_err(|_| CodecError::InvalidInput {
        reason: format!("{what} of {value} does not fit in a C int"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The field must match S4 exactly, for every possible input.
    ///
    /// This is checked over all 65536 pairs rather than a sample. Everything the
    /// accelerated codec produces is built from this operation, so a single
    /// disagreeing pair would mean silently different parity bytes, and a mixed
    /// cluster would then treat healthy shards as corrupt.
    #[test]
    fn gf_multiplication_matches_s4_over_the_whole_field() {
        for a in 0..=u8::MAX {
            for b in 0..=u8::MAX {
                assert_eq!(
                    gf_mul(a, b),
                    s4_codec::reference::gf_mul(a, b),
                    "ISA-L and S4 disagree on {a} * {b} in GF(2^8)"
                );
            }
        }
    }

    #[test]
    fn init_tables_reports_dimension_mismatch() {
        // 2 rows over 3 sources needs 6 coefficients, not 5.
        let error = init_tables(3, 2, &[1, 2, 3, 4, 5]).unwrap_err();
        assert!(matches!(error, CodecError::InvalidInput { .. }));
    }

    #[test]
    fn init_tables_rejects_zero_dimensions() {
        assert!(init_tables(0, 2, &[]).is_err());
        assert!(init_tables(2, 0, &[]).is_err());
    }

    #[test]
    fn init_tables_returns_the_size_isal_documents() {
        let tables = init_tables(4, 2, &[1; 8]).expect("valid dimensions");
        assert_eq!(tables.len(), 32 * 4 * 2);
    }

    #[test]
    fn encode_data_rejects_mismatched_buffer_lengths() {
        let tables = init_tables(2, 1, &[1, 1]).expect("valid dimensions");
        let short = [0u8; 4];
        let long = [0u8; 8];
        let mut parity = vec![vec![0u8; 4]];

        let error = encode_data(&tables, &[&short, &long], &mut parity).unwrap_err();
        assert!(matches!(error, CodecError::InvalidInput { .. }));
    }

    #[test]
    fn encode_data_rejects_tables_of_the_wrong_size() {
        let data = [0u8; 4];
        let mut parity = vec![vec![0u8; 4]];
        let error = encode_data(&[0; 8], &[&data, &data], &mut parity).unwrap_err();
        assert!(matches!(error, CodecError::InvalidInput { .. }));
    }

    /// A single XOR row is the simplest encoding with a known answer.
    #[test]
    fn encode_data_computes_a_known_xor_row() {
        // One parity row over two sources, both coefficients 1, so the parity is
        // the XOR of the two inputs.
        let tables = init_tables(2, 1, &[1, 1]).expect("valid dimensions");
        let left: Vec<u8> = (0..64).collect();
        let right: Vec<u8> = (0..64).map(|byte| byte ^ 0xA5).collect();
        let mut parity = vec![vec![0u8; 64]];

        encode_data(&tables, &[&left, &right], &mut parity).expect("encode must succeed");

        let expected: Vec<u8> = left.iter().zip(&right).map(|(a, b)| a ^ b).collect();
        assert_eq!(parity[0], expected);
    }

    /// Buffers shorter than ISA-L's SIMD block must still encode correctly.
    ///
    /// The assembly paths bail out below 16 bytes and the library falls back to
    /// its portable code. This test pins that behaviour down so a future version
    /// bump cannot break small inputs unnoticed.
    #[test]
    fn encode_data_handles_buffers_shorter_than_one_simd_block() {
        let tables = init_tables(2, 1, &[1, 1]).expect("valid dimensions");
        for len in 1..16usize {
            let left: Vec<u8> = (0..len as u8).collect();
            let right: Vec<u8> = (0..len as u8).map(|byte| byte ^ 0x3C).collect();
            let mut parity = vec![vec![0u8; len]];

            encode_data(&tables, &[&left, &right], &mut parity).expect("encode must succeed");

            let expected: Vec<u8> = left.iter().zip(&right).map(|(a, b)| a ^ b).collect();
            assert_eq!(parity[0], expected, "length {len} encoded incorrectly");
        }
    }
}
