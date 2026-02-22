//! Hashing utilities for filter probe generation.

use siphasher::sip128::{Hasher128, SipHasher13};
use std::hash::Hasher;

/// Hash a key and return two independent 64-bit hashes.
pub fn hash_key(key: &[u8]) -> (u64, u64) {
    let mut hasher = SipHasher13::new();
    hasher.write(key);
    let hash = hasher.finish128();
    (hash.h1, hash.h2)
}

/// Compute the i-th probe position from two hashes, modulo m.
///
/// Uses double hashing: g(i) = (h1 + i * h2) mod m.
pub fn probe_position(h1: u64, h2: u64, i: u32, m: u64) -> u64 {
    let h2 = h2 | 1;
    h1.wrapping_add((i as u64).wrapping_mul(h2)) % m
}
