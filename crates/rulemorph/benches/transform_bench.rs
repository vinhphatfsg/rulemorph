mod common;

use common::{
    LOOKUP_RULES, SIMPLE_RULES, elements, extended_input, lookup_context, lookup_input, parse_rule,
    simple_input,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rulemorph::{parse_rule_file, transform};

const EXTENDED_RULES: &str = include_str!("../tests/fixtures/t13_expr_extended/rules.yaml");

fn bench_simple_transform(c: &mut Criterion) {
    let record_count = 5_000usize;
    let rule = parse_rule(SIMPLE_RULES);
    let input = simple_input(record_count);

    let mut group = c.benchmark_group("transform/batch/simple");
    group.throughput(elements(record_count));
    group.bench_function("records_5k", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), None).expect("transform failed");
            black_box(output);
        })
    });
    group.finish();
}

fn bench_lookup_transform(c: &mut Criterion) {
    let record_count = 5_000usize;
    let user_count = 100usize;
    let tag_count = 100usize;
    let rule = parse_rule(LOOKUP_RULES);
    let input = lookup_input(record_count, user_count, tag_count);
    let context = lookup_context(user_count, tag_count);

    let mut group = c.benchmark_group("transform/batch/lookup");
    group.throughput(elements(record_count));
    group.bench_function("records_5k_context_100", |b| {
        b.iter(|| {
            let output =
                transform(&rule, black_box(&input), Some(&context)).expect("transform failed");
            black_box(output);
        })
    });
    group.finish();
}

fn bench_extended_transform_with_rule_parse(c: &mut Criterion) {
    let record_count = 5_000usize;
    let input = extended_input(record_count);

    let mut group = c.benchmark_group("transform/batch/extended");
    group.throughput(elements(record_count));
    group.bench_function("cold_parse_records_5k", |b| {
        b.iter(|| {
            let rule = parse_rule_file(EXTENDED_RULES).expect("failed to parse rules");
            let output = transform(&rule, black_box(&input), None).expect("transform failed");
            black_box(output);
        })
    });

    let rule = parse_rule_file(EXTENDED_RULES).expect("failed to parse rules");
    group.bench_function("hot_records_5k", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), None).expect("transform failed");
            black_box(output);
        })
    });
    group.finish();
}

fn bench_lookup_scale(c: &mut Criterion) {
    let record_count = 250usize;
    let rule = parse_rule(LOOKUP_RULES);
    let mut group = c.benchmark_group("transform/batch/lookup_scale");
    group.throughput(elements(record_count));

    for context_size in [10usize, 100, 1_000] {
        let input = lookup_input(record_count, context_size, context_size);
        let context = lookup_context(context_size, context_size);
        group.bench_with_input(
            BenchmarkId::new("context_size", context_size),
            &context_size,
            |b, _| {
                b.iter(|| {
                    let output = transform(&rule, black_box(&input), Some(&context))
                        .expect("transform failed");
                    black_box(output);
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_simple_transform,
    bench_lookup_transform,
    bench_extended_transform_with_rule_parse,
    bench_lookup_scale
);
criterion_main!(benches);
