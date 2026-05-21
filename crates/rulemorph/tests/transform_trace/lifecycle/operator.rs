#[test]
fn trace_v1_expression_operator_lifecycle() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "label"
    expr:
      op: "concat"
      args:
        - ref: "input.first"
        - " "
        - ref: "input.last"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"first":"Ada","last":"Lovelace"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_operator_lifecycle(&traced.trace, "concat");

    let text = serde_json::to_string(&traced.trace).unwrap();
    assert!(text.contains("Ada Lovelace"));
}
