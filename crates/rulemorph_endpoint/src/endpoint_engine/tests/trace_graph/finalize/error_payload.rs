#[test]
fn finalize_trace_preserves_error_payload() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  wrap:
    data:
      - "@out"
      - unknown_op
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");
    let error = finalize.get("error").expect("finalize error");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        error.get("code").and_then(|value| value.as_str()),
        Some("ExprError")
    );
    assert_eq!(
        error.get("message").and_then(|value| value.as_str()),
        Some("expr.op is not supported")
    );
    assert_eq!(
        error.get("path").and_then(|value| value.as_str()),
        Some("finalize.wrap.data[1].op")
    );
}
