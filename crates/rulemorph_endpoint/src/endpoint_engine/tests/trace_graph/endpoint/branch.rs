#[test]
fn endpoint_trace_branch_step_includes_rule_refs_and_child_trace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let base_dir = temp.path();
    std::fs::write(
        base_dir.join("then.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "then"
"#,
    )
    .expect("write then rule");
    std::fs::write(
        base_dir.join("else.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "result"
    value: "else"
"#,
    )
    .expect("write else rule");
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: ["@input.kind", "then"] }
      then: ./then.yaml
      else: ./else.yaml
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "kind": "then" });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, base_dir);
    let node = trace.nodes.first().expect("branch node");
    assert_eq!(node.get("kind"), Some(&json!("branch")));

    let meta = node
        .get("meta")
        .and_then(|value| value.as_object())
        .expect("branch meta");
    assert_eq!(meta.get("branch_taken"), Some(&json!("then")));
    assert_eq!(
        meta.get("rule_refs"),
        Some(&json!(["rules/then.yaml", "rules/else.yaml"]))
    );
    assert_eq!(
        meta.get("rule_ref_labels"),
        Some(&json!(["branch: then", "branch: else"]))
    );
    assert_eq!(meta.get("rule_ref"), Some(&json!("rules/then.yaml")));
    assert_eq!(meta.get("rule_ref_label"), Some(&json!("branch: then")));

    let child_rule_path = node
        .get("child_trace")
        .and_then(|value| value.get("rule"))
        .and_then(|value| value.get("path"));
    assert_eq!(child_rule_path, Some(&json!("rules/then.yaml")));
}
