#[tokio::test]
async fn multipart_import_body_is_available_to_network_rule() {
    let app = axum::Router::new().route(
        "/internal/import",
        axum::routing::post(
            |headers: HeaderMap, axum::Json(payload): axum::Json<JsonValue>| async move {
                assert_eq!(
                    headers
                        .get("x-api-key")
                        .and_then(|value| value.to_str().ok()),
                    Some("internal-key")
                );
                assert_eq!(
                    headers
                        .get("x-tenant-id")
                        .and_then(|value| value.to_str().ok()),
                    Some("tenant-a")
                );
                let bundle_path = payload
                    .get("bundle_path")
                    .and_then(|value| value.as_str())
                    .expect("bundle_path");
                assert!(Path::new(bundle_path).join("rules/ok.yaml").exists());
                axum::Json(json!({ "imported": 1, "rules_imported": 1 }))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app.into_make_service()).with_graceful_shutdown(async {
        let _ = shutdown_rx.await;
    });
    let server_handle = tokio::spawn(async move {
        let _ = server.await;
    });

    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let network_dir = rules_dir.join("network");
    std::fs::create_dir_all(&network_dir).expect("create network dir");
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
    steps:
      - rule: ./network/import_bundle.yaml
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");
    std::fs::write(
        network_dir.join("import_bundle.yaml"),
        r#"
version: 2
type: network
request:
  method: POST
  url:
    - "@context.config.internal_base"
    - concat: ["/internal/import"]
  headers:
    x-tenant-id: "@context.tenant_id"
timeout: 1s
internal_auth: true
body_map:
  - target: "bundle_path"
    source: "input.body.bundle_path"
"#,
    )
    .expect("write import_bundle.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new(format!("http://{}", host), rules_dir.join(".data"))
            .with_internal_auth_enabled(true)
            .with_internal_auth_path_allowlist(vec!["/internal/import".to_string()])
            .with_internal_api_key("internal-key".to_string()),
    )
    .expect("load engine");
    let (boundary, body) = build_multipart_zip_body();
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/import")
        .header(
            axum::http::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .expect("request");
    request.extensions_mut().insert(RequestContext {
        tenant_id: Some("tenant-a".to_string()),
        internal_api_key: None,
    });
    let response = engine.handle_request(request).await.expect("response");
    assert_eq!(response.status().as_u16(), 200);
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
    assert_eq!(body, json!({ "imported": 1, "rules_imported": 1 }));

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

#[tokio::test]
async fn multipart_import_requires_bundle_field() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/import
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
    let boundary = "BOUNDARY";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"not_bundle\"\r\n\r\nvalue\r\n--{boundary}--\r\n"
    );
    let request = Request::builder()
        .method("POST")
        .uri("/api/import")
        .header(
            axum::http::header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(axum::body::Body::from(body))
        .expect("request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(err.to_string().contains("missing bundle file"));
}

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

fn build_multipart_zip_body() -> (String, Vec<u8>) {
    let mut zip_writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip_writer
        .start_file("rules/ok.yaml", options)
        .expect("start file");
    zip_writer
        .write_all(
            br#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#,
        )
        .expect("write file");
    let zip_bytes = zip_writer.finish().expect("finish zip").into_inner();
    let boundary = "BOUNDARY".to_string();
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"bundle\"; filename=\"bundle.zip\"\r\n",
    );
    body.extend_from_slice(b"Content-Type: application/zip\r\n\r\n");
    body.extend_from_slice(&zip_bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (boundary, body)
}
