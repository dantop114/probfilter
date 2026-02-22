/// A filter that answers point membership queries.
pub trait PointFilter {
    /// Returns true if the key might be in the set.
    fn may_contain(&self, key: &[u8]) -> bool;
}

/// A filter that supports adding keys after construction.
pub trait FilterInsert {
    /// Insert a key into the filter.
    fn insert(&mut self, key: &[u8]);
}
