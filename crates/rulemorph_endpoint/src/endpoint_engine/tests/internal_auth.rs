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

#[tokio::test]
async fn internal_auth_rejects_disallowed_path() {
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
        EngineConfig::new("http://localhost:1234".to_string(), rules_dir.join(".data"))
            .with_internal_auth_enabled(true)
            .with_internal_auth_path_allowlist(vec![
                "/internal/traces".to_string(),
                "/internal/traces/".to_string(),
            ])
            .with_internal_api_key("secret".to_string()),
    )
    .expect("load engine");
    let raw = NetworkRuleFile {
        version: 2,
        rule_type: "network".to_string(),
        request: NetworkRequest {
            method: "GET".to_string(),
            url: json!("http://localhost:1234/internal/api-keys"),
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
        .send_network_request(
            &rule,
            "http://localhost:1234/internal/api-keys",
            &HeaderMap::new(),
            None,
            None,
        )
        .await
        .expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("internal_auth path"));
}

#[test]
fn context_internal_api_key_is_injected_on_demand() {
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
            .with_internal_api_key("secret".to_string()),
    )
    .expect("load engine");
    let base_context = engine.build_context_json(None);
    assert!(
        base_context
            .get("config")
            .and_then(|value| value.get("internal_api_key"))
            .is_none()
    );
    let injected = engine.context_with_internal_api_key(&base_context, "secret");
    assert_eq!(
        injected
            .get("config")
            .and_then(|value| value.get("internal_api_key"))
            .and_then(|value| value.as_str()),
        Some("secret")
    );
}
