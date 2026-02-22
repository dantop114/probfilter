//! Standard Bloom filter using Kirsch-Mitzenmacher double hashing.

use crate::{
    traits::{FilterInsert, PointFilter},
    util::{bits::BitVec, hash},
};

/// A classic Bloom filter backed by a single flat bit array.
///
/// Sized automatically from expected item count and target false positive rate.
/// Uses SipHash-128 with double hashing for probe generation.
pub struct StandardBloomFilter {
    bitmap: BitVec,
    /// Total number of bits in the filter.
    m: usize,
    /// Number of hash probes per lookup.
    k: u32,
}

impl StandardBloomFilter {
    pub fn new(items: usize, fp_rate: f64) -> Self {
        let m = Self::bitmap_size(items, fp_rate);
        let k = Self::k(fp_rate);

        let bitmap = BitVec::new(m);

        StandardBloomFilter { bitmap, m, k }
    }

    /// Optimal bit count
    fn bitmap_size(items: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }

    /// Optimal probe count
    fn k(fp_rate: f64) -> u32 {
        let k = ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32;
        k.clamp(1, 30)
    }
}

impl FilterInsert for StandardBloomFilter {
    fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = hash::hash_key(key);

        for k_i in 0..self.k {
            let index = hash::probe_position(h1, h2, k_i, self.m as u64) as usize;
            self.bitmap.set(index);
        }
    }
}

impl PointFilter for StandardBloomFilter {
    fn may_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = hash::hash_key(key);

        for k_i in 0..self.k {
            let index = hash::probe_position(h1, h2, k_i, self.m as u64) as usize;

            if !self.bitmap.get(index).unwrap() {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::StandardBloomFilter;
    use crate::traits::{FilterInsert, PointFilter};
    use rand::Rng;

    /// Generate `count` random 8-byte keys starting from the given RNG.
    fn random_keys(rng: &mut impl Rng, count: usize) -> Vec<[u8; 8]> {
        (0..count).map(|_| rng.random::<[u8; 8]>()).collect()
    }

    #[test]
    fn empty_filter_returns_false() {
        let filter = StandardBloomFilter::new(1_000, 0.01);
        for i in 0u64..1_000 {
            assert!(
                !filter.may_contain(&i.to_le_bytes()),
                "empty filter must not report membership"
            );
        }
    }

    #[test]
    fn no_false_negatives() {
        let n = 10_000;
        let mut filter = StandardBloomFilter::new(n, 0.01);
        let mut rng = rand::rng();
        let keys = random_keys(&mut rng, n);

        for key in &keys {
            filter.insert(key);
        }

        for key in &keys {
            assert!(
                filter.may_contain(key),
                "inserted key must always be found (no false negatives)"
            );
        }
    }

    #[test]
    fn fpr_within_expected_bounds() {
        let n = 10_000;
        let target_fpr = 0.01;
        let mut filter = StandardBloomFilter::new(n, target_fpr);
        let mut rng = rand::rng();

        // Insert n members.
        let members = random_keys(&mut rng, n);
        for key in &members {
            filter.insert(key);
        }

        // Query 100K non-members.
        let num_queries = 100_000;
        let non_members = random_keys(&mut rng, num_queries);
        let false_positives = non_members
            .iter()
            .filter(|key| filter.may_contain(*key))
            .count();

        let measured_fpr = false_positives as f64 / num_queries as f64;

        // Allow up to 2x the target FPR.
        assert!(
            measured_fpr < target_fpr * 2.0,
            "measured FPR {measured_fpr:.4} exceeds 2x target {target_fpr}"
        );
    }
}
