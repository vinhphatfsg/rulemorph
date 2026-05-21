#[tokio::test]
async fn multipart_body_is_only_import_special_case() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/traces
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");
    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://127.0.0.1:8080".to_string(), rules_dir.join(".data")),
    )
    .expect("load engine");
    let (boundary, body) = build_multipart_zip_body();
    let request = Request::builder()
        .method("GET")
        .uri("/api/traces")
        .header(
            axum::http::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .expect("request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("multipart should not be parsed on non-import endpoints");
    assert!(!err.to_string().contains("missing bundle file"));
}
