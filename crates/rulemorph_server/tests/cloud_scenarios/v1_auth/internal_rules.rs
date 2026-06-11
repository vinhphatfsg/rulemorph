#[tokio::test]
async fn tenant_api_rules_allow_internal_auth() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let rules_dir = data_dir.join("tenants").join("default").join("api_rules");
    fs::create_dir_all(rules_dir.join("rules")).expect("create rules");
    fs::write(
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
      body:
        ok: true
"#,
    )
    .expect("write endpoint.yaml");
    fs::write(
        rules_dir.join("rules/ok.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.value"
    value: 1
finalize:
  wrap:
    key: result
"#,
    )
    .expect("write ok.yaml");

    let registry = TenantRegistry::new(TenantRegistryConfig {
        base_dir: data_dir,
        rules_dir: None,
        api_mode: ApiMode::Rules,
        ui_enabled: true,
        port: 8080,
        ssrf_allowlist: Vec::new(),
        ssrf_allow_private: true,
        internal_api_key: Some("internal-key".to_string()),
    });
    let resources = registry.get_or_init("default").await?;
    let engine = resources.api_engine.as_ref().expect("api engine");

    assert!(engine.allows_internal_auth());
    Ok(())
}
