//! Fixed-size bit vector backed by a `Box<[u64]>`.

/// A fixed-size, densely packed bit array.
///
/// Used as the backing store for standard Bloom filters.
/// Supports get, set, clear, popcount, and rank queries.
pub struct BitVec {
    len: usize,
    words: Box<[u64]>,
}

impl BitVec {
    /// Allocates a zeroed bit array holding at least `num_bits` bits.
    /// The actual backing storage rounds up to the next full u64.
    pub fn new(num_bits: usize) -> Self {
        let num_words = num_bits.div_ceil(64);
        let words = vec![0u64; num_words].into_boxed_slice();
        Self {
            words,
            len: num_bits,
        }
    }

    /// Number of valid bits.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the bit at `index`.
    pub fn get(&self, index: usize) -> Option<bool> {
        if index >= self.len {
            return None;
        }

        Some(self.words[index >> 6] & (1 << (index & 63)) != 0)
    }

    /// Set the bit at `index` to 1. Panics if out of bounds.
    pub fn set(&mut self, index: usize) {
        assert!(
            index < self.len,
            "bit index {index} out of bounds (len {})",
            self.len
        );

        self.words[index >> 6] |= 1 << (index & 63);
    }

    /// Set the bit at `index` to 0. Panics if out of bounds.
    pub fn clear(&mut self, index: usize) {
        assert!(
            index < self.len,
            "bit index {index} out of bounds (len {})",
            self.len
        );

        self.words[index >> 6] &= !(1 << (index & 63))
    }

    /// Count of all set bits (popcount).
    pub fn count_ones(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Count of all set bits before `index`. (rank1)
    pub fn count_ones_before(&self, index: usize) -> usize {
        assert!(
            index <= self.len,
            "bit index {index} out of bounds (len {})",
            self.len
        );

        let word = index >> 6;
        let bit = index & 63;

        let full_words: usize = self.words[..word]
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum();

        if bit == 0 {
            full_words
        } else {
            let mask = (1u64 << bit) - 1;
            full_words + (self.words[word] & mask).count_ones() as usize
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitVec;

    #[test]
    fn new_all_zeros() {
        let bv = BitVec::new(128);
        for i in 0..128 {
            assert_eq!(bv.get(i), Some(false));
        }
        assert_eq!(bv.count_ones(), 0);
    }

    #[test]
    fn set_get_clear_round_trip() {
        let mut bv = BitVec::new(200);
        for &i in &[0, 63, 64, 65, 127, 128, 199] {
            bv.set(i);
            assert_eq!(bv.get(i), Some(true));
        }
        assert_eq!(bv.count_ones(), 7);

        bv.clear(64);
        assert_eq!(bv.get(64), Some(false));
        assert_eq!(bv.get(63), Some(true));
        assert_eq!(bv.get(65), Some(true));
        assert_eq!(bv.count_ones(), 6);
    }

    #[test]
    fn out_of_bounds() {
        let bv = BitVec::new(65);
        assert_eq!(bv.get(65), None);
        assert_eq!(bv.get(100), None);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn set_out_of_bounds_panics() {
        let mut bv = BitVec::new(64);
        bv.set(64);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn clear_out_of_bounds_panics() {
        let mut bv = BitVec::new(64);
        bv.clear(64);
    }
}
