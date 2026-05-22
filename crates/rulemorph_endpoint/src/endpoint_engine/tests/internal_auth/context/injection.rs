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
