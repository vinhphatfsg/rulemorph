#[test]
fn trace_step_record_when_false_is_skipped_with_meta() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - record_when:
      eq: ["@input.kind", "allowed"]
  - mappings:
      - target: result
        value: "kept"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "kind": "blocked" });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let node = trace.nodes.first().expect("record_when node");

    assert_eq!(node.get("kind"), Some(&json!("record_when")));
    assert_eq!(node.get("status"), Some(&json!("skipped")));
    assert_eq!(node.get("output"), Some(&json!(null)));
    assert_eq!(
        node.get("meta")
            .and_then(|meta| meta.get("record_when"))
            .and_then(|value| value.as_bool()),
        Some(false)
    );
}

#[test]
fn trace_step_conditions_evaluate_custom_ops_with_rule_context() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  normalize_status:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
  is_active:
    input: string
    returns: bool
    expr: ["$", normalize_status, { eq: ["active"] }]
steps:
  - record_when:
      eq: [["@input.status", is_active], true]
  - asserts:
      - when:
          eq: [["@input.status", is_active], true]
        error:
          code: NOT_ACTIVE
          message: status must be active
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "status": " ACTIVE " });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));

    let record_when = trace.nodes.first().expect("record_when node");
    assert_eq!(record_when.get("status"), Some(&json!("ok")));
    assert_eq!(
        record_when
            .get("meta")
            .and_then(|meta| meta.get("record_when"))
            .and_then(|value| value.as_bool()),
        Some(true)
    );

    let asserts = trace.nodes.get(1).expect("assert node");
    assert_eq!(asserts.get("status"), Some(&json!("ok")));
    assert_eq!(
        asserts
            .get("meta")
            .and_then(|meta| meta.get("asserts_ok"))
            .and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[test]
fn trace_step_assert_failure_records_error_and_asserts_meta() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - asserts:
      - when:
          eq: ["@input.kind", "allowed"]
        error:
          code: NOT_ALLOWED
          message: blocked kind
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "kind": "blocked" });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let node = trace.nodes.first().expect("assert node");

    assert_eq!(node.get("kind"), Some(&json!("asserts")));
    assert_eq!(node.get("status"), Some(&json!("error")));
    assert_eq!(
        node.get("meta")
            .and_then(|meta| meta.get("asserts_ok"))
            .and_then(|value| value.as_bool()),
        Some(false)
    );
    assert_eq!(
        node.get("error").and_then(|error| error.get("code")),
        Some(&json!("AssertionFailed"))
    );
    assert_eq!(
        node.get("error").and_then(|error| error.get("message")),
        Some(&json!("assert failed: NOT_ALLOWED: blocked kind"))
    );
    assert_eq!(
        node.get("error").and_then(|error| error.get("path")),
        Some(&json!("steps[0].asserts[0]"))
    );
}
