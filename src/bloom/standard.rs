//! Standard Bloom filter using Kirsch-Mitzenmacher double hashing.

use crate::{
    traits::{FilterInsert, PointFilter},
    util::{bits::BitVec, hash},
};

/// A classic Bloom filter backed by a single flat bit array.
///
/// Sized from expected item count and one of:
/// - target false positive rate
/// - bits-per-key.
/// Uses SipHash-128 with double hashing for probing.
pub struct StandardBloomFilter {
    bitmap: BitVec,
    /// Total number of bits in the filter.
    m: usize,
    /// Number of hash probes per lookup.
    k: u32,
}

impl StandardBloomFilter {
    /// Compute (m, k) from expected bits per key
    ///
    /// The optimal probe count for a standard Bloom filter is:
    ///     k = (m/n) * ln(2)
    /// where m/n is the bits-per-key ratio. This minimizes the FPR by
    /// keeping the filter exactly half full (maximum entropy per bit).
    ///
    /// Total bits is simply m = n × bits_per_key.
    fn params_from_bpk(num_keys: usize, bits_per_key: f64) -> (usize, u32) {
        assert!(
            bits_per_key > 0.0,
            "bits per key must be positive, got {bits_per_key}"
        );
        assert!(
            bits_per_key.is_finite(),
            "bits per key must be finite, got {bits_per_key}"
        );
        assert!(num_keys > 0, "number of keys must be positive");

        let m = (num_keys as f64 * bits_per_key).ceil() as usize;
        let k = (bits_per_key * core::f64::consts::LN_2).round() as u32;

        // m must be at least 1.
        // k must be at least 1 (at least one probe) and is capped at
        // 30 because there's diminishing returns beyond this value at
        // the cost of CPU cycles.
        (m.max(1), k.clamp(1, 30))
    }

    /// Compute (m, k) from expected false positive rate.
    ///
    /// k = ceil(log2(1/fp_rate))
    /// bits_per_key = log2(1/fp_rate) / ln(2)
    fn params_from_fpr(num_keys: usize, fp_rate: f64) -> (usize, u32) {
        assert!(fp_rate > 0.0, "fp_rate must be positive, got {fp_rate}");
        assert!(
            fp_rate < 1.0,
            "fp_rate must be less than 1.0, got {fp_rate}"
        );
        assert!(num_keys > 0, "num keys can't be zero");

        let log2_inv_fp = fp_rate.recip().log2();
        let k = log2_inv_fp.ceil() as u32;
        let bits_per_key = log2_inv_fp / core::f64::consts::LN_2;
        let m = (num_keys as f64 * bits_per_key).ceil() as usize;

        (m.max(1), k.clamp(1, 30))
    }

    /// Create an empty filter sized for `num_keys` with
    /// `bits_per_key`.
    pub fn new_with_bits_per_key(num_keys: usize, bits_per_key: f64) -> Self {
        let (m, k) = Self::params_from_bpk(num_keys, bits_per_key);
        Self {
            bitmap: BitVec::new(m),
            m,
            k,
        }
    }

    /// Create an empty filter sized for `num_keys` items and
    /// `fp_rate` false positive rate.
    pub fn new_with_fp_rate(num_keys: usize, fp_rate: f64) -> Self {
        let (m, k) = Self::params_from_fpr(num_keys, fp_rate);
        StandardBloomFilter {
            bitmap: BitVec::new(m),
            m,
            k,
        }
    }

    /// Build a filter from a slice of keys and the given `bits_per_key`.
    /// Uses `new_with_bits_per_key` to build the empty filter to populate.
    pub fn from_keys_bpk(keys: &[impl AsRef<[u8]>], bits_per_key: f64) -> Self {
        let mut filter = Self::new_with_bits_per_key(keys.len(), bits_per_key);
        for key in keys {
            filter.insert(key.as_ref());
        }
        filter
    }

    /// Build a filter from a slice of keys and the given `fp_rate`.
    /// Uses `new_with_fp_rate` to build the empty filter to populate.
    pub fn from_keys_fpr(keys: &[impl AsRef<[u8]>], fp_rate: f64) -> Self {
        let mut filter = Self::new_with_fp_rate(keys.len(), fp_rate);
        for key in keys {
            filter.insert(key.as_ref());
        }
        filter
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
        let filter = StandardBloomFilter::new_with_fp_rate(1_000, 0.01);
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
        let mut filter = StandardBloomFilter::new_with_fp_rate(n, 0.01);
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
        let mut filter = StandardBloomFilter::new_with_fp_rate(n, target_fpr);
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
