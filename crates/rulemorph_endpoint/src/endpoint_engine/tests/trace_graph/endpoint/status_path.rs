use crate::endpoint_engine::trace_emit::EndpointTraceInput;

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

    let trace = engine.build_trace(EndpointTraceInput {
        method: &Method::GET,
        path: "/api/test",
        input: json!({"input": true}),
        output: json!({"output": false}),
        status: "error".to_string(),
        error: Some(json!({"message": "boom"})),
        nodes: Vec::new(),
        duration_us: 12,
    });
    let status = trace.get("status").and_then(|value| value.as_str());
    assert_eq!(status, Some("error"));
}
