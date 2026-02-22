# probfilter

> [!WARNING]
> Work in progress.

Probabilistic filters data structures in Rust.

## What's inside

- **Standard Bloom filter** - classic flat bit array with double hashing
- **Blocked Bloom filter** - 512-bit blocks, multiplicative remix probing (follows RocksDB's `FastLocalBloomImpl`)
- **Register-blocked Bloom filter** — 64-bit word-sized blocks with mask-based insert/query
- **Prefix Bloom filter** - fixed-length prefix extractor over a blocked filter

## Planned

- [ ] Diva range filter
- [ ] Serialization

## Usage

```rust
use probfilter::bloom::standard::StandardBloomFilter;
use probfilter::traits::{PointFilter, FilterInsert};

let mut filter = StandardBloomFilter::new(10_000, 0.01);
filter.insert(b"hello");
assert!(filter.may_contain(b"hello"));
```


## Tests

```sh
cargo test
```

## Benchmarks

```sh
cargo bench --bench bloom
```

Filter to a specific group:

```sh
cargo bench --bench bloom -- bloom_lookup
cargo bench --bench bloom -- bloom_insert
```

Results are saved to `target/criterion/` with HTML reports.

## References

- [Modern Bloom Filters: 22x Faster](https://save-buffer.github.io/bloom_filter.html) - the article that kicked this off, covering register-blocked and patterned filters with SIMD
- [RocksDB Bloom Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter) - `FastLocalBloomImpl` design and cache-line blocking
- [Diva: Dynamic Range Filter for Var-Length Keys and Queries](https://www.vldb.org/pvldb/vol18/p3923-eslami.pdf) (PVLDB 2025) - sampling trie with infix compression
- [Bloom Filters (Arpit Bhayani)](https://arpitbhayani.me/blogs/bloom-filters/) - FPR derivations, optimal sizing formulas, and the Kirsch-Mitzenmacher double hashing trick


## License
MIT