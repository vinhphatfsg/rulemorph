mod common;

use common::{SIMPLE_RULES, csv_input, drain_stream, elements, parse_rule, simple_input};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rulemorph::{transform, transform_record, transform_stream};
use serde_json::Value as JsonValue;

const CSV_RULES: &str = r#"
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "input.id"
  - target: "name"
    source: "input.name"
"#;

fn bench_exec_modes(c: &mut Criterion) {
    let record_count = 10_000usize;
    let rule = parse_rule(SIMPLE_RULES);
    let input = simple_input(record_count);
    let records: Vec<JsonValue> =
        serde_json::from_str(&input).expect("benchmark json should parse");

    let mut end_to_end = c.benchmark_group("transform/end_to_end/json");
    end_to_end.throughput(elements(record_count));

    end_to_end.bench_function("batch_transform", |b| {
        b.iter(|| {
            let output = transform(&rule, black_box(&input), None).expect("transform failed");
            black_box(output);
        })
    });

    end_to_end.bench_function("stream_drain", |b| {
        b.iter(|| {
            let stream = transform_stream(&rule, black_box(&input), None).expect("stream failed");
            let count = drain_stream(stream);
            black_box(count);
        })
    });
    end_to_end.finish();

    let mut evaluator = c.benchmark_group("transform/evaluator/json");
    evaluator.throughput(elements(record_count));

    evaluator.bench_function("record_loop", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for record in &records {
                let output = transform_record(&rule, black_box(record), None)
                    .expect("record transform failed");
                if output.is_some() {
                    count += 1;
                }
            }
            black_box(count);
        })
    });

    evaluator.finish();

    let csv_rule = parse_rule(CSV_RULES);
    let csv_data = csv_input(record_count);
    let mut csv_stream = c.benchmark_group("transform/stream/csv");
    csv_stream.throughput(elements(record_count));
    csv_stream.bench_function("records_10k", |b| {
        b.iter(|| {
            let stream =
                transform_stream(&csv_rule, black_box(&csv_data), None).expect("stream failed");
            let count = drain_stream(stream);
            black_box(count);
        })
    });
    csv_stream.finish();
}

criterion_group!(benches, bench_exec_modes);
criterion_main!(benches);
