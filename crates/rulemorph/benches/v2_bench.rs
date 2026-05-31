mod common;

use common::{V2_COLLECTION_RULES, elements, parse_rule, v2_collection_input};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rulemorph::transform;

fn bench_v2_collection(c: &mut Criterion) {
    let record_count = 500usize;
    let rule = parse_rule(V2_COLLECTION_RULES);
    let mut group = c.benchmark_group("transform/v2/collection");
    group.throughput(elements(record_count));

    for item_count in [4usize, 16, 64] {
        let input = v2_collection_input(record_count, item_count);
        group.bench_with_input(
            BenchmarkId::new("items_per_record", item_count),
            &item_count,
            |b, _| {
                b.iter(|| {
                    let output =
                        transform(&rule, black_box(&input), None).expect("transform failed");
                    black_box(output);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_v2_collection);
criterion_main!(benches);
