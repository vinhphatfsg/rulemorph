mod common;

use common::{SIMPLE_RULES, bytes, csv_input, parse_rule, simple_input};
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rulemorph::{InputData, normalize_records};

const CSV_RULES: &str = r#"
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "input.id"
"#;

fn bench_normalize_json(c: &mut Criterion) {
    let record_count = 10_000usize;
    let rule = parse_rule(SIMPLE_RULES);
    let input = simple_input(record_count);
    let mut group = c.benchmark_group("normalize/json");
    group.throughput(bytes(&input));
    group.bench_function("records_10k", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for record in normalize_records(&rule, InputData::Text(black_box(&input)))
                .expect("json should normalize")
            {
                let record = record.expect("json record should normalize");
                black_box(record);
                count += 1;
            }
            black_box(count);
        })
    });
    group.finish();
}

fn bench_normalize_csv(c: &mut Criterion) {
    let record_count = 10_000usize;
    let rule = parse_rule(CSV_RULES);
    let input = csv_input(record_count);
    let mut group = c.benchmark_group("normalize/csv");
    group.throughput(bytes(&input));
    group.bench_function("records_10k", |b| {
        b.iter(|| {
            let mut count = 0usize;
            for record in normalize_records(&rule, InputData::Text(black_box(&input)))
                .expect("csv should normalize")
            {
                let record = record.expect("csv record should normalize");
                black_box(record);
                count += 1;
            }
            black_box(count);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_normalize_json, bench_normalize_csv);
criterion_main!(benches);
