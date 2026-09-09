//! Seeded pseudorandom permutation over `[0, N)`.
//!
//! Maps a registration index to a queue position as a bijection derived on
//! read from a single stored seed, rather than storing the shuffled mapping.
//! The construction is a 4-round balanced Feistel network over the smallest
//! power-of-four domain `>= N`, with `HMAC-SHA256(seed, round || x)` as the
//! round function and cycle-walking to restrict the output to `[0, N)` — the
//! small-domain format-preserving-encryption construction of NIST SP 800-38G
//! (FF1), <https://doi.org/10.6028/NIST.SP.800-38G>.
//!
//! # Wire encoding
//!
//! The exact byte encoding is fixed so a third party can recompute every
//! position from `(seed, i, N)`; any deviation produces different positions.
//! All multi-byte integers are big-endian.
//!
//! - `b = ceil(bit_length(N - 1) / 2)`; `domain = 2^(2b)`; `mask = 2^b - 1`.
//!   Both halves are exactly `b` bits. `N = 1` is a degenerate identity.
//! - Exactly 4 rounds, `round in {0, 1, 2, 3}` ascending.
//! - HMAC key is the 256-bit seed, used verbatim — never part of the message.
//! - Message `r || x` is 5 bytes: `r` the round as 1 byte (`0x00`–`0x03`),
//!   `x` the right half as a 4-byte big-endian `u32`.
//! - `F(r, x) = be_u32(HMAC-SHA256(seed, r || x)[0..4]) & mask`.
//! - One round is `L, R = R, L XOR F(round, R)`; recombine `(L << b) | R`.
//! - Cycle-walk: re-apply `enc` until the result is `< N`.

mod assembly;

pub use assembly::{Assignment, SHARDS, SealError, SealedOffsets, shard_for};

use aws_lc_rs::hmac;

/// The 256-bit seed used as the HMAC-SHA256 key.
///
/// A newtype over the raw key bytes so a seed is never confused with an
/// arbitrary byte slice at a call site.
#[derive(Clone, Copy)]
pub struct Seed(pub [u8; 32]);

/// Number of Feistel rounds; fixed at 4 by the wire encoding.
const ROUNDS: u8 = 4;

/// Computes the queue position for a registration index under a seeded
/// permutation over `[0, N)`.
///
/// `i` is the global registration index; `n` is the cohort size. Returns a
/// position in `[0, N)`.
///
/// The permutation is only defined over `[0, N)`, so `i >= n` is returned
/// unchanged rather than evaluated — the caller passes such an index only when
/// it should fall back to a non-permuted position, never to permute it.
#[must_use]
pub fn prp(seed: &Seed, i: u64, n: u64) -> u64 {
    // Degenerate cohorts: the permutation over 0 or 1 elements is the identity,
    // and no rounds run (b would be 0). Also handles the out-of-domain i >= n.
    if n <= 1 || i >= n {
        return i;
    }

    let b = half_width(n);
    let mask = (1u64 << b) - 1;
    let key = hmac::Key::new(hmac::HMAC_SHA256, &seed.0);

    // Cycle-walk: enc is a bijection over [0, domain); re-apply until it lands
    // inside [0, N). Expected iterations = domain / N, bounded by 4.
    let mut v = i;
    loop {
        v = enc(&key, v, b, mask);
        if v < n {
            return v;
        }
    }
}

/// Half-width `b = ceil(bit_length(N - 1) / 2)`, so `domain = 2^(2b)` is the
/// smallest power of four `>= N`. `n >= 2` here, so `N - 1 >= 1` and `b >= 1`.
fn half_width(n: u64) -> u32 {
    let bits = 64 - (n - 1).leading_zeros(); // bit_length(N - 1)
    bits.div_ceil(2)
}

/// One full 4-round Feistel encryption over the `2b`-bit domain.
fn enc(key: &hmac::Key, v: u64, b: u32, mask: u64) -> u64 {
    let mut left = v >> b;
    let mut right = v & mask;
    let mut round = 0u8;
    while round < ROUNDS {
        let next_right = left ^ round_function(key, round, right, mask);
        left = right;
        right = next_right;
        round += 1;
    }
    (left << b) | right
}

/// `F(r, x) = be_u32(HMAC-SHA256(seed, r || x)[0..4]) & mask`.
///
/// The message is exactly 5 bytes: the round as one byte, then the right half
/// as a 4-byte big-endian `u32` (`x < 2^b <= 2^32`).
fn round_function(key: &hmac::Key, round: u8, x: u64, mask: u64) -> u64 {
    // x = R < 2^b <= 2^32, so its value lives in the low 4 bytes of the
    // 8-byte big-endian form; take those directly rather than a lossy cast.
    let mut msg = [0u8; 5];
    msg[0] = round;
    msg[1..5].copy_from_slice(&x.to_be_bytes()[4..8]);
    let tag = hmac::sign(key, &msg);
    let bytes = tag.as_ref();
    let word = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    u64::from(word) & mask
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn seed_from(byte: u8) -> Seed {
        Seed([byte; 32])
    }

    /// Collect every image of `[0, n)` under `prp` for a given seed.
    fn images(seed: &Seed, n: u64) -> Vec<u64> {
        (0..n).map(|i| prp(seed, i, n)).collect()
    }

    #[test]
    fn degenerate_domains_are_identity() {
        let seed = seed_from(0x11);
        assert_eq!(prp(&seed, 0, 0), 0);
        assert_eq!(prp(&seed, 0, 1), 0);
    }

    #[test]
    fn out_of_domain_index_returns_unchanged() {
        // An index at or beyond the cohort size is outside the permutation
        // domain and must be returned unchanged, never evaluated.
        let seed = seed_from(0x22);
        assert_eq!(prp(&seed, 100, 50), 100);
        assert_eq!(prp(&seed, 50, 50), 50);
    }

    #[test]
    fn every_output_is_in_domain() {
        let seed = seed_from(0x33);
        for n in [2u64, 3, 7, 16, 17, 1000, 1024, 1025] {
            for i in 0..n {
                let p = prp(&seed, i, n);
                assert!(p < n, "prp(_, {i}, {n}) = {p} escaped [0, {n})");
            }
        }
    }

    #[test]
    fn bijective_over_small_domains() {
        let seed = seed_from(0x44);
        for n in [2u64, 3, 5, 8, 15, 16, 100, 256, 999, 1000] {
            let mut seen = images(&seed, n);
            seen.sort_unstable();
            let expected: Vec<u64> = (0..n).collect();
            assert_eq!(seen, expected, "not a bijection over [0, {n})");
        }
    }

    #[test]
    fn deterministic_across_calls() {
        // Same (seed, i, n) always yields the same position.
        let seed = seed_from(0x55);
        for i in 0..500u64 {
            assert_eq!(prp(&seed, i, 500), prp(&seed, i, 500));
        }
    }

    #[test]
    fn distinct_seeds_generally_differ() {
        // Two different seeds should not produce the identical permutation
        // (they could by chance on a tiny domain, so use a large one).
        let a = images(&seed_from(0x66), 1000);
        let b = images(&seed_from(0x67), 1000);
        assert_ne!(a, b);
    }

    #[test]
    fn seed_scrambles_the_identity() {
        // The seed determines the mapping: the permutation is not the identity,
        // so a position cannot be known without the seed.
        let seed = seed_from(0x77);
        let images: Vec<u64> = images(&seed, 10_000);
        assert!(
            images
                .iter()
                .enumerate()
                .any(|(i, &p)| u64::try_from(i).is_ok_and(|i| i != p)),
            "permutation is the identity — seed is not scrambling"
        );
    }

    proptest! {
        /// Bijectivity over the full domain for arbitrary seed and cohort size:
        /// the sorted image set equals [0, N) exactly — no collisions, no gaps.
        #[test]
        fn prop_bijective(seed_byte: u8, n in 1u64..2000) {
            let seed = seed_from(seed_byte);
            let mut seen = images(&seed, n);
            seen.sort_unstable();
            let expected: Vec<u64> = (0..n).collect();
            prop_assert_eq!(seen, expected);
        }

        /// Every position stays inside [0, N) for any seed, index, and cohort.
        #[test]
        fn prop_in_domain(seed_byte: u8, n in 1u64..100_000, i in 0u64..100_000) {
            prop_assume!(i < n);
            let seed = seed_from(seed_byte);
            let p = prp(&seed, i, n);
            prop_assert!(p < n);
        }

        /// Determinism: recomputation yields the identical position.
        #[test]
        fn prop_deterministic(seed_byte: u8, n in 1u64..100_000, i in 0u64..100_000) {
            prop_assume!(i < n);
            let seed = seed_from(seed_byte);
            prop_assert_eq!(prp(&seed, i, n), prp(&seed, i, n));
        }

        /// Uniformity by chi-square: over a large cohort split into 10 deciles,
        /// the position distribution does not deviate from uniform beyond the
        /// p=0.001 critical value for 9 degrees of freedom (27.88). Guards the
        /// round function's quality, distinct from bijectivity.
        #[test]
        fn prop_uniform_by_decile(seed_byte: u8) {
            let seed = seed_from(seed_byte);
            let n = 10_000u64;
            let bucket = n / 10;
            let mut deciles = [0u32; 10];
            for i in 0..n {
                let p = prp(&seed, i, n);
                deciles[usize::try_from(p / bucket).unwrap_or(9)] += 1;
            }
            let expected = f64::from(u32::try_from(bucket).unwrap_or(u32::MAX));
            let chi_sq: f64 = deciles
                .iter()
                .map(|&observed| {
                    let d = f64::from(observed) - expected;
                    d * d / expected
                })
                .sum();
            prop_assert!(
                chi_sq < 27.88,
                "chi-square {chi_sq} exceeds the p=0.001 df=9 critical value"
            );
        }
    }
}
