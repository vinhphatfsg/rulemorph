#[test]
fn rule_nodes_include_step_duration_us() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let duration = trace.nodes[0]
        .get("duration_us")
        .and_then(|value| value.as_u64());
    assert!(duration.is_some());
}
