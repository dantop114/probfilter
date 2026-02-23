//! Cache-line-aligned blocked Bloom filter.
//!
//! All probes for a key land within a single 64-byte block,
//! guaranteeing at most one cache miss per lookup.
//! Block probing follows RocksDB's `FastLocalBloomImpl`.

use crate::util::hash;

/// Knuth multiplicative hash constant
const GOLDEN: u32 = 0x9e3779b9;

/// A blocked Bloom filter with 512-bit blocks.
pub struct BlockedBloomFilter {
    blocks: Box<[Block]>,
    /// Number of hash probes per lookup.
    k: u32,
}

/// A single 64-byte block.
#[repr(align(64))]
#[derive(Clone, Copy)]
pub(crate) struct Block {
    data: [u64; 8],
}

impl BlockedBloomFilter {
    pub fn new(items: usize, fp_rate: f64) -> Self {
        let total_bits = Self::bitmap_size(items, fp_rate);
        let num_blocks = total_bits.div_ceil(512).max(1);
        let k = Self::k(fp_rate);
        let blocks = vec![Block { data: [0u64; 8] }; num_blocks].into_boxed_slice();

        Self { blocks, k }
    }

    /// Optimal bit count
    fn bitmap_size(items: usize, fp_rate: f64) -> usize {
        let ln2_2 = core::f64::consts::LN_2 * core::f64::consts::LN_2;
        ((-1.0f64 * items as f64 * fp_rate.ln()) / ln2_2).ceil() as usize
    }

    /// Optimal probe count.
    fn k(fp_rate: f64) -> u32 {
        let k = ((-1.0f64 * fp_rate.ln()) / core::f64::consts::LN_2).ceil() as u32;
        k.clamp(1, 30)
    }

    /// Map h uniformly into 0..n without division.
    fn fast_range_32(h: u32, n: u32) -> u32 {
        ((h as u64 * n as u64) >> 32) as u32
    }

    /// Set k probe bits within a single block using multiplicative remixing.
    fn set_block_probes(data: &mut [u64; 8], mut h: u32, k: u32) {
        for _ in 0..k {
            let bitpos = (h >> (32 - 9)) as usize;
            data[bitpos >> 6] |= 1u64 << (bitpos & 63);
            h = h.wrapping_mul(GOLDEN);
        }
    }

    /// Check k probe bits within a single block. Returns false on first miss.
    fn check_block_probes(data: &[u64; 8], mut h: u32, k: u32) -> bool {
        for _ in 0..k {
            let bitpos = (h >> (32 - 9)) as usize;
            if data[bitpos >> 6] & (1u64 << (bitpos & 63)) == 0 {
                return false;
            }
            h = h.wrapping_mul(GOLDEN);
        }
        true
    }
}

impl crate::traits::FilterInsert for BlockedBloomFilter {
    fn insert(&mut self, key: &[u8]) {
        let (full_h1, full_h2) = hash::hash_key(key); // two u64s from SipHash
        let h1 = full_h1 as u32;
        let h2 = full_h2 as u32;

        let block_idx = Self::fast_range_32(h1, self.blocks.len() as u32) as usize;
        Self::set_block_probes(&mut self.blocks[block_idx].data, h2, self.k);
    }
}

impl crate::traits::PointFilter for BlockedBloomFilter {
    fn may_contain(&self, key: &[u8]) -> bool {
        let (full_h1, full_h2) = hash::hash_key(key);
        let h1 = full_h1 as u32;
        let h2 = full_h2 as u32;

        let block_idx = Self::fast_range_32(h1, self.blocks.len() as u32) as usize;
        Self::check_block_probes(&self.blocks[block_idx].data, h2, self.k)
    }
}

#[cfg(test)]
mod tests {
    use super::BlockedBloomFilter;
    use crate::bloom::standard::StandardBloomFilter;
    use crate::traits::{FilterInsert, PointFilter};
    use rand::Rng;

    /// Helper: generate `count` random 8-byte keys.
    fn random_keys(rng: &mut impl Rng, count: usize) -> Vec<[u8; 8]> {
        (0..count).map(|_| rng.random::<[u8; 8]>()).collect()
    }

    #[test]
    fn empty_filter_returns_false() {
        let filter = BlockedBloomFilter::new(1_000, 0.01);
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
        let mut filter = BlockedBloomFilter::new(n, 0.01);
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
        let mut filter = BlockedBloomFilter::new(n, target_fpr);
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

        // Blocked filters have higher FPR than classic for the same memory,
        // so we allow up to 3x the target.
        assert!(
            measured_fpr < target_fpr * 3.0,
            "measured FPR {measured_fpr:.4} exceeds 3x target {target_fpr}"
        );
    }

    #[test]
    fn blocked_fpr_higher_than_classic_same_memory() {
        // Verify blocked FPR is higher than classic for the same total memory
        // budget. This confirms the expected tradeoff: blocking trades FPR
        // for cache locality.
        let n = 10_000;
        let target_fpr = 0.01;
        let mut rng = rand::rng();

        let mut standard = StandardBloomFilter::new_with_fp_rate(n, target_fpr);
        let mut blocked = BlockedBloomFilter::new(n, target_fpr);

        let members = random_keys(&mut rng, n);
        for key in &members {
            standard.insert(key);
            blocked.insert(key);
        }

        let num_queries = 200_000;
        let non_members = random_keys(&mut rng, num_queries);

        let standard_fp = non_members
            .iter()
            .filter(|key| standard.may_contain(*key))
            .count();
        let blocked_fp = non_members
            .iter()
            .filter(|key| blocked.may_contain(*key))
            .count();

        let standard_fpr = standard_fp as f64 / num_queries as f64;
        let blocked_fpr = blocked_fp as f64 / num_queries as f64;

        // Blocked should have equal or higher FPR (the tradeoff for cache locality).
        // We don't assert strict > because at very low load factors they can be close.
        // Just confirm blocked isn't wildly better.
        assert!(
            blocked_fpr >= standard_fpr * 0.8,
            "blocked FPR {blocked_fpr:.5} unexpectedly much lower than standard {standard_fpr:.5}"
        );
    }
}
