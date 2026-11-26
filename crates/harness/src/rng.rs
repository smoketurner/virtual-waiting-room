//! A small seeded generator, so a run can be repeated exactly.
//!
//! Visitors draw their arrival time, their jitter and whether they are a
//! no-show from here rather than from the operating system, which is what
//! makes `--seed` reproduce a run's request pattern. The waiting room's own
//! randomness (the permutation seed, the possession secrets, the shard draws)
//! is not replaced: those come from the binaries under test.

/// `SplitMix64`: tiny, fast, and good enough for scheduling noise.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// An independent stream for one visitor, derived from the run's seed.
    #[must_use]
    pub fn fork(&mut self) -> Self {
        Self(self.next_u64())
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn fill(&mut self, bytes: &mut [u8]) {
        for chunk in bytes.chunks_mut(8) {
            let word = self.next_u64().to_le_bytes();
            for (b, w) in chunk.iter_mut().zip(word) {
                *b = w;
            }
        }
    }
}
