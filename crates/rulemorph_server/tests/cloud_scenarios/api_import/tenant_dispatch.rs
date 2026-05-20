#[tokio::test]
async fn api_import_dispatch_uses_authenticated_tenant_rules() -> Result<()> {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_rules_dir = data_dir.join("tenants/default/api_rules");
    let tenant_rules_dir = data_dir.join("tenants/tenant-a/api_rules");
    fs::create_dir_all(default_rules_dir.join("rules")).expect("create default rules");
    fs::create_dir_all(tenant_rules_dir.join("rules")).expect("create tenant rules");

    fs::write(
        default_rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/default
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: default
"#,
    )
    .expect("write default endpoint.yaml");
    fs::write(
        default_rules_dir.join("rules/ok.yaml"),
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
    .expect("write default ok.yaml");

    fs::write(
        tenant_rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
    steps:
      - rule: rules/ok.yaml
    reply:
      status: 200
      body:
        kind: tenant-rule
"#,
    )
    .expect("write tenant endpoint.yaml");
    fs::write(
        tenant_rules_dir.join("rules/ok.yaml"),
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
    .expect("write tenant ok.yaml");

    let resolver = Arc::new(StaticTenantResolver {
        api_key: "tenant-key".to_string(),
        tenant_id: "tenant-a".to_string(),
    });
    let registry = Arc::new(TenantRegistry::new(
        data_dir.clone(),
        None,
        ApiMode::Rules,
        true,
        8080,
        Vec::new(),
        true,
        Some("internal-key".to_string()),
    ));
    let default_resources = registry.get_or_init("default").await?;
    let state = AppState {
        default_resources,
        tenant_registry: Some(registry),
        ui_source: None,
        api_mode: ApiMode::Rules,
        tenant_resolver: Some(resolver),
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);

    let (boundary, body) = build_zip_import_payload("zip-tenant-rule-001")?;
    let response = post_api_import(
        &app,
        "tenant import response",
        Some("Bearer tenant-key"),
        Some("zip"),
        &boundary,
        body,
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = read_json::<Value>(response).await?;
    assert_eq!(
        body.get("kind").and_then(|value| value.as_str()),
        Some("tenant-rule")
    );

    Ok(())
}
