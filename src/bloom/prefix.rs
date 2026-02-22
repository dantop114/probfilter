//! Prefix Bloom filter using a fixed-length prefix extractor.

use crate::{
    bloom::blocked::BlockedBloomFilter,
    traits::{FilterInsert, PointFilter},
};

/// A Bloom filter that operates on key prefixes rather than full keys.
pub struct PrefixBloomFilter {
    inner: BlockedBloomFilter,
    prefix_len: usize,
}

impl PrefixBloomFilter {
    pub fn new(items: usize, fp_rate: f64, prefix_len: usize) -> Self {
        Self {
            inner: BlockedBloomFilter::new(items, fp_rate),
            prefix_len,
        }
    }

    /// Extract key's prefix based on `prefix_len`.
    fn extract_prefix<'a>(&self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len.min(key.len())]
    }
}

impl FilterInsert for PrefixBloomFilter {
    /// Inserts prefix of `key`.
    fn insert(&mut self, key: &[u8]) {
        self.inner.insert(self.extract_prefix(key));
    }
}

impl PointFilter for PrefixBloomFilter {
    /// Looks for prefix of `key`.
    fn may_contain(&self, key: &[u8]) -> bool {
        self.inner.may_contain(self.extract_prefix(key))
    }
}

#[cfg(test)]
mod tests {
    use super::PrefixBloomFilter;
    use crate::traits::{FilterInsert, PointFilter};
    use rand::Rng;

    fn random_keys(rng: &mut impl Rng, count: usize, len: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|_| (0..len).map(|_| rng.random::<u8>()).collect())
            .collect()
    }

    #[test]
    fn empty_filter_returns_false() {
        let filter = PrefixBloomFilter::new(1_000, 0.01, 4);
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
        let prefix_len = 4;
        let mut filter = PrefixBloomFilter::new(n, 0.01, prefix_len);
        let mut rng = rand::rng();
        let keys = random_keys(&mut rng, n, 8);

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
        let prefix_len = 4;
        let mut filter = PrefixBloomFilter::new(n, target_fpr, prefix_len);
        let mut rng = rand::rng();

        // Insert n members with unique prefixes (8-byte keys, 4-byte prefix).
        let members = random_keys(&mut rng, n, 8);
        for key in &members {
            filter.insert(key);
        }

        // Query 100K non-members.
        let num_queries = 100_000;
        let non_members = random_keys(&mut rng, num_queries, 8);
        let false_positives = non_members
            .iter()
            .filter(|key| filter.may_contain(key.as_slice()))
            .count();

        let measured_fpr = false_positives as f64 / num_queries as f64;

        // Inherited from blocked filter - allow up to 3x target.
        assert!(
            measured_fpr < target_fpr * 3.0,
            "measured FPR {measured_fpr:.4} exceeds 3x target {target_fpr}"
        );
    }

    #[test]
    fn keys_sharing_prefix_both_match() {
        // Keys sharing a prefix are indistinguishable - this is the defining
        // property of a prefix filter.
        let prefix_len = 5;
        let mut filter = PrefixBloomFilter::new(100, 0.01, prefix_len);

        filter.insert(b"hello_world");
        assert!(
            filter.may_contain(b"hello_other"),
            "key sharing the same prefix must match"
        );
        assert!(
            filter.may_contain(b"hello"),
            "query equal to the prefix itself must match"
        );
    }

    #[test]
    fn different_prefixes_are_independent() {
        // Keys with different prefixes should be independent (modulo FPR).
        let prefix_len = 4;
        let mut filter = PrefixBloomFilter::new(100, 0.001, prefix_len);

        filter.insert(b"aaaa_something");

        // A single non-matching query could be a false positive,
        // so check several different prefixes.
        let misses: usize = [
            &b"bbbb_something"[..],
            b"cccc_something",
            b"dddd_something",
            b"eeee_something",
            b"ffff_something",
        ]
        .iter()
        .filter(|k| !filter.may_contain(k))
        .count();

        assert!(
            misses >= 4,
            "most keys with different prefixes should not match"
        );
    }

    #[test]
    fn key_shorter_than_prefix_len() {
        // When the key is shorter than prefix_len, `extract_prefix` returns
        // the whole key. Two keys that share the short key as a prefix should
        // still be treated equivalently.
        let prefix_len = 8;
        let mut filter = PrefixBloomFilter::new(100, 0.01, prefix_len);

        // Insert a 3-byte key — prefix extracted is the full key b"ab\x00".
        filter.insert(b"ab");
        assert!(
            filter.may_contain(b"ab"),
            "short key must be found after insert"
        );

        // A longer key starting with "ab" but with prefix_len=8 extracts
        // "abcdef\x01\x02", which differs from "ab", so should generally
        // not match. Check several to account for possible false positives.
        let misses: usize = [
            &b"abcdef\x01\x02"[..],
            b"ab\x00\x00\x00\x00\x00\x01",
            b"abzzzzzz",
            b"ab123456",
        ]
        .iter()
        .filter(|k| !filter.may_contain(k))
        .count();

        assert!(
            misses >= 3,
            "longer keys with different 8-byte prefixes should not match a 2-byte insert"
        );
    }

    #[test]
    fn short_key_matches_longer_key_with_same_prefix() {
        // With prefix_len=2, a short key and a longer key sharing that prefix
        // should be treated as the same.
        let prefix_len = 2;
        let mut filter = PrefixBloomFilter::new(100, 0.01, prefix_len);

        filter.insert(b"ab");
        assert!(
            filter.may_contain(b"abcdef"),
            "longer key with same prefix must match"
        );
        assert!(
            filter.may_contain(b"ab\x00\x00\x00"),
            "padded key with same prefix must match"
        );
    }

    #[test]
    fn prefix_len_zero_everything_matches() {
        // prefix_len=0 means every key maps to the empty slice.
        // After one insert, every query returns true.
        let mut filter = PrefixBloomFilter::new(100, 0.01, 0);

        // Before any insert, should be false.
        assert!(!filter.may_contain(b"anything"));

        // One insert makes everything match.
        filter.insert(b"x");

        assert!(filter.may_contain(b"anything"));
        assert!(filter.may_contain(b""));
        assert!(filter.may_contain(b"completely different"));
        assert!(filter.may_contain(&[0u8; 100]));
    }

    #[test]
    fn prefix_len_longer_than_keys() {
        // When prefix_len exceeds all key lengths, the filter effectively
        // operates on full keys -- no prefix collapsing happens.
        let prefix_len = 100;
        let mut filter = PrefixBloomFilter::new(100, 0.01, prefix_len);

        filter.insert(b"short");
        assert!(filter.may_contain(b"short"));

        // "shorter" shares the first 5 bytes but is a different full key,
        // and since prefix_len=100 > both key lengths, full keys are compared.
        // Should generally not match (unless false positive).
        let misses: usize = [&b"shorter"[..], b"shor", b"SHORT", b"other"]
            .iter()
            .filter(|k| !filter.may_contain(k))
            .count();

        assert!(
            misses >= 3,
            "with prefix_len > key length, different keys should not match"
        );
    }

    #[test]
    fn single_byte_prefix_discrimination() {
        // With prefix_len=1, keys are discriminated only by their first byte.
        let mut filter = PrefixBloomFilter::new(1_000, 0.01, 1);

        filter.insert(b"\x42rest_of_key");

        // Same first byte, different suffix — must match.
        assert!(filter.may_contain(b"\x42_totally_different"));

        // Different first byte — should generally not match.
        let misses: usize = (0u8..=255)
            .filter(|&b| b != 0x42)
            .filter(|b| !filter.may_contain(&[*b, b'x', b'y', b'z']))
            .count();

        // With only 1 inserted prefix out of 256 possible, almost all
        // other first-byte values should miss.
        assert!(
            misses >= 250,
            "most other first-byte values should not match, got {misses} misses"
        );
    }

    #[test]
    fn empty_key() {
        // Empty key should work without panicking.
        let mut filter = PrefixBloomFilter::new(100, 0.01, 4);

        // Insert empty key — prefix extracted is b"".
        filter.insert(b"");
        assert!(
            filter.may_contain(b""),
            "empty key must be found after insert"
        );

        // Another empty key with different prefix_len.
        let mut filter2 = PrefixBloomFilter::new(100, 0.01, 0);
        filter2.insert(b"");
        assert!(filter2.may_contain(b""));
    }
}
