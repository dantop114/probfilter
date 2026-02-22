//! Probabilistic filter data structures in Rust.
//!
//! Implements point filters (Bloom filters) with plans for
//! range filters (prefix Bloom, Diva filters).

pub mod bloom;
pub mod traits;
pub mod util;
