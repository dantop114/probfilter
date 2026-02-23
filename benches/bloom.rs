//! Benchmarks comparing standard vs blocked Bloom filter performance.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use probfilter::bloom::blocked::BlockedBloomFilter;
use probfilter::bloom::standard::StandardBloomFilter;
use probfilter::traits::{FilterInsert, PointFilter};
use rand::Rng;

/// Pre-build a filter and a large set of random query keys, then benchmark lookups.
fn bench_lookups(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_lookup");

    for &n in &[10_000, 100_000, 1_000_000] {
        let fp_rate = 0.01;
        let mut rng = rand::rng();

        let members: Vec<[u8; 8]> = (0..n).map(|_| rng.random::<[u8; 8]>()).collect();

        let num_queries = n.max(100_000);
        let queries: Vec<[u8; 8]> = (0..num_queries).map(|_| rng.random::<[u8; 8]>()).collect();

        // Standard filter.
        let mut standard = StandardBloomFilter::new_with_fp_rate(n, fp_rate);
        for key in &members {
            standard.insert(key);
        }

        // Blocked filter.
        let mut blocked = BlockedBloomFilter::new(n, fp_rate);
        for key in &members {
            blocked.insert(key);
        }

        // Randomised index order so sequential prefetching can't help.
        let indices: Vec<usize> = {
            use rand::seq::SliceRandom;
            let mut v: Vec<usize> = (0..num_queries).collect();
            v.shuffle(&mut rng);
            v
        };

        group.bench_with_input(BenchmarkId::new("standard", n), &n, |b, _| {
            let mut i = 0;
            b.iter(|| {
                let key = &queries[indices[i % num_queries]];
                let result = standard.may_contain(key);
                i += 1;
                result
            });
        });

        group.bench_with_input(BenchmarkId::new("blocked", n), &n, |b, _| {
            let mut i = 0;
            b.iter(|| {
                let key = &queries[indices[i % num_queries]];
                let result = blocked.may_contain(key);
                i += 1;
                result
            });
        });
    }

    group.finish();
}

fn bench_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("bloom_insert");
    let n = 100_000;
    let fp_rate = 0.01;
    let mut rng = rand::rng();
    let keys: Vec<[u8; 8]> = (0..n).map(|_| rng.random::<[u8; 8]>()).collect();

    group.bench_function("standard", |b| {
        b.iter_with_setup(
            || StandardBloomFilter::new_with_fp_rate(n, fp_rate),
            |mut filter| {
                for key in &keys {
                    filter.insert(key);
                }
            },
        );
    });

    group.bench_function("blocked", |b| {
        b.iter_with_setup(
            || BlockedBloomFilter::new(n, fp_rate),
            |mut filter| {
                for key in &keys {
                    filter.insert(key);
                }
            },
        );
    });

    group.finish();
}

criterion_group!(benches, bench_lookups, bench_inserts);
criterion_main!(benches);
