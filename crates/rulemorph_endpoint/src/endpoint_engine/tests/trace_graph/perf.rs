#[test]
#[ignore]
fn trace_timing_perf_smoke() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
  - mappings:
      - target: upper
        expr: ["@out.name", uppercase]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let iterations = 100u64;
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    }
    let total_us = started.elapsed().as_micros() as u64;
    println!("trace timing avg: {} μs", total_us / iterations);
}
