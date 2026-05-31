mod common;

use common::{SIMPLE_RULES, elements, parse_rule, simple_input};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rulemorph::{InputData, TransformTraceOptions, transform, transform_input_with_trace};

fn bench_trace_overhead(c: &mut Criterion) {
    let record_count = 1_000usize;
    let rule = parse_rule(SIMPLE_RULES);
    let input = simple_input(record_count);
    let metadata_only = TransformTraceOptions::metadata_only();
    let raw = TransformTraceOptions::raw();

    let mut group = c.benchmark_group("trace/simple");
    group.throughput(elements(record_count));

    group.bench_function("trace_off", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), None).expect("transform failed");
            black_box(output);
        })
    });

    group.bench_function("trace_metadata_only", |b| {
        b.iter(|| {
            let result = transform_input_with_trace(
                &rule,
                InputData::Text(black_box(&input)),
                None,
                &metadata_only,
            )
            .expect("trace transform failed");
            let event_count: usize = result
                .trace
                .records
                .iter()
                .map(|record| record.events.len())
                .sum();
            black_box(result.output);
            black_box(event_count);
        })
    });

    group.bench_function("trace_raw", |b| {
        b.iter(|| {
            let result =
                transform_input_with_trace(&rule, InputData::Text(black_box(&input)), None, &raw)
                    .expect("trace transform failed");
            let event_count: usize = result
                .trace
                .records
                .iter()
                .map(|record| record.events.len())
                .sum();
            black_box(result.output);
            black_box(event_count);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_trace_overhead);
criterion_main!(benches);
