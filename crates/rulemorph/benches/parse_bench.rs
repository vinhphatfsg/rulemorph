mod common;

use common::{LOOKUP_RULES, SIMPLE_RULES};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use rulemorph::parse_rule_file;

const EXTENDED_RULES: &str = include_str!("../tests/fixtures/t13_expr_extended/rules.yaml");

fn bench_parse_rules(c: &mut Criterion) {
    let mut cached = c.benchmark_group("parse/rule_file_cached");
    for (name, source) in [
        ("simple", SIMPLE_RULES),
        ("lookup", LOOKUP_RULES),
        ("extended", EXTENDED_RULES),
    ] {
        cached.bench_function(name, |b| {
            b.iter(|| {
                let rule = parse_rule_file(black_box(source)).expect("rule should parse");
                black_box(rule);
            })
        });
    }
    cached.finish();

    let mut cold_like = c.benchmark_group("parse/rule_file_cold_like");
    for (name, source) in [
        ("simple", SIMPLE_RULES),
        ("lookup", LOOKUP_RULES),
        ("extended", EXTENDED_RULES),
    ] {
        let mut iteration = 0usize;
        cold_like.bench_function(name, |b| {
            b.iter_batched(
                || {
                    iteration += 1;
                    format!("{}\n# bench-{}", source, iteration)
                },
                |source| {
                    let rule = parse_rule_file(black_box(&source)).expect("rule should parse");
                    black_box(rule);
                },
                BatchSize::SmallInput,
            )
        });
    }
    cold_like.finish();
}

criterion_group!(benches, bench_parse_rules);
criterion_main!(benches);
