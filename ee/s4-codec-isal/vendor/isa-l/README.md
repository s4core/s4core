# Vendored ISA-L (erasure coding module only)

Third-party source, copied unmodified. Do not edit these files.

| | |
|---|---|
| Project | Intel Intelligent Storage Acceleration Library (ISA-L) |
| Upstream | https://github.com/intel/isa-l |
| Version | **v2.32.1** |
| Copied on | 2026-09-04 |
| License | BSD-3-Clause, see `LICENSE` in this directory |

## What was copied and why only this much

ISA-L also ships CRC, compression and cryptography modules. None of them are
used by S4, so they are not here: everything vendored has to be audited and kept
up to date, and copying unused code only grows that burden.

| Path | Contents |
|---|---|
| `erasure_code/ec_base.c`, `ec_base.h` | Portable C implementation and the field arithmetic |
| `erasure_code/ec_highlevel_func.c` | Dispatch to the SIMD implementations |
| `erasure_code/*.asm` | 72 x86_64 assembly kernels (SSE, AVX, AVX2, AVX-512, GFNI) |
| `erasure_code/gf_vect_gfni.inc` | Assembly macros used by the GFNI kernels |
| `include/erasure_code.h`, `gf_vect_mul.h`, `isal_api.h` | Public C headers |
| `include/reg_sizes.asm`, `multibinary.asm`, `memcpy.asm` | Assembly includes |
| `include/unaligned.h` | Helper header used by the C sources |

The assembly list is exactly the `lsrc_x86_64` set from upstream
`erasure_code/Makefile.am`, so it matches what a normal ISA-L build would link.

## How it is built

`../../build.rs` assembles the `.asm` files with `nasm` and compiles the C files
with the `cc` crate, then archives both into one static library. Nothing here is
built through autotools or CMake.

Building requires **nasm 2.14.01 or newer** on x86_64. On other architectures
nothing in this directory is compiled at all.

## How to update

Updating is a deliberate change, not a routine dependency bump. Both engines
must keep producing identical bytes, so:

1. Copy the same file set from the new upstream tag.
2. Update the version and date in this file.
3. Run `cargo test -p s4-codec-isal`, which checks the field over all 65536
   input pairs.
4. Run the differential test against the portable codec on all six profiles.
5. Run `scripts/41-ec-isal-mixed-cluster-test.sh`.

If any of those disagree, the new version changes results and must not be
merged as-is.

## License notes

BSD-3-Clause requires the copyright notice and license text to be kept. That is
why `LICENSE` sits next to the sources and the Intel headers in each file are
left alone.

The project's `add_license.sh` only rewrites `*.rs` files, so the C, assembly and
header files here are untouched by it today. If that script is ever widened to
other file types, this directory has to be excluded.
