#[test]
fn endpoint_error_trace_uses_rule_ref_for_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
"#,
    )
    .expect("write endpoint");
    std::fs::create_dir_all(rules_dir.join("rules")).expect("create rules dir");
    std::fs::write(
        rules_dir.join("rules/ok.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.ok"
    value: true
"#,
    )
    .expect("write rule");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data"))
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let resolved = rules_dir.join("rules/ok.yaml");
    let err = EndpointError::invalid("boom").with_path(resolved.clone());
    let trace = engine.endpoint_error_to_trace(&err);
    let path = trace
        .get("path")
        .and_then(|value| value.as_str())
        .expect("path");

    let expected = rule_ref_from_path(&engine.endpoint_rule.base_dir, &resolved);
    assert_eq!(path, expected);
    assert!(!Path::new(path).is_absolute());
}

#[test]
fn build_trace_emits_top_level_status() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
    )
    .expect("write endpoint.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.join(".data"))
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let trace = engine.build_trace(
        &Method::GET,
        "/api/test",
        json!({"input": true}),
        json!({"output": false}),
        "error".to_string(),
        Some(json!({"message": "boom"})),
        Vec::new(),
        12,
    );
    let status = trace.get("status").and_then(|value| value.as_str());
    assert_eq!(status, Some("error"));
}

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
