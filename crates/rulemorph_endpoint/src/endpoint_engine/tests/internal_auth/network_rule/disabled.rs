#[tokio::test]
async fn internal_auth_rejected_when_disabled() {
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

    let raw = NetworkRuleFile {
        version: 2,
        rule_type: "network".to_string(),
        request: NetworkRequest {
            method: "GET".to_string(),
            url: json!("https://example.com"),
            headers: None,
        },
        timeout: "1s".to_string(),
        internal_auth: true,
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        catch: None,
        retry: None,
    };
    let rule = compile_network_rule(raw, Path::new("network.yaml")).expect("compile rule");

    let err = engine
        .send_network_request(&rule, "https://example.com", &HeaderMap::new(), None, None)
        .await
        .expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("internal_auth"));
}
