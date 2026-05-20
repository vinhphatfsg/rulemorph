#[test]
fn mapping_ops_include_duration_us() {
    let mappings = vec![Mapping {
        target: "name".to_string(),
        source: None,
        value: Some(json!("hello")),
        expr: None,
        when: None,
        value_type: None,
        required: false,
        default: None,
    }];
    let record = json!({});
    let mut out = json!({});
    let ops = build_mapping_ops_with_values(&mappings, &record, None, &mut out, 2, 0);
    let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
    assert!(duration.is_some());
}

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

#[test]
fn rule_trace_duration_includes_finalize_duration() {
    let nodes = vec![json!({ "duration_us": 10 }), json!({ "duration_us": 15 })];
    let finalize = json!({ "duration_us": 7 });

    assert_eq!(sum_rule_trace_duration_us(&nodes, Some(&finalize)), 32);
}
