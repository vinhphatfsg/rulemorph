#[test]
fn build_network_body_body_rule_none_omits_body() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps: []
    reply:
      status: 200
"#,
    )
    .expect("write endpoint.yaml");

    std::fs::write(
        rules_dir.join("body_rule.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: [1, 2]
mappings:
  - target: "name"
    value: "ignored"
"#,
    )
    .expect("write body_rule.yaml");

    let network_path = rules_dir.join("network.yaml");
    std::fs::write(
        &network_path,
        r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 1s
body_rule: body_rule.yaml
"#,
    )
    .expect("write network.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let raw: NetworkRuleFile =
        serde_yaml::from_str(&std::fs::read_to_string(&network_path).expect("read network"))
            .expect("parse network");
    let rule = compile_network_rule(raw, &network_path).expect("compile network");

    let body = engine
        .build_network_body(&rule, &json!({}), None)
        .expect("build body");
    assert!(body.is_none());
}
