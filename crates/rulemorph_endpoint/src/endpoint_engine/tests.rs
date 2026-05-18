// Tests are kept in a sibling module so endpoint_engine.rs can stay focused on runtime code.
use super::*;
use futures_util::stream;
use rulemorph::parse_rule_file;
use serde_json::json;
use std::fs::File;
use std::io::Write;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod support;
use support::*;

include!("tests/security.rs");
include!("tests/trace_graph.rs");

#[test]
fn endpoint_path_matches_and_captures() {
    let path = EndpointPath::parse("/api/traces/{id}").unwrap();
    assert!(path.matches("/api/traces/abc"));
    let params = path.capture("/api/traces/abc");
    assert_eq!(params.get("id"), Some(&"abc".to_string()));
}

#[test]
fn zip_copy_stops_after_file_limit() {
    let mut input = std::io::Cursor::new(vec![b'x'; 12]);
    let mut output = Vec::new();
    let copied = copy_zip_entry_bounded(&mut input, &mut output, 8).expect("copy");
    assert_eq!(copied, 12);
    assert!(output.len() <= 8);
}

#[test]
fn zip_extract_rejects_too_many_entries() {
    let temp = tempfile::tempdir().expect("tempdir");
    let zip_path = temp.path().join("bundle.zip");
    let file = File::create(&zip_path).expect("create zip");
    let mut zip_writer = zip::ZipWriter::new(file);
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for index in 0..=MULTIPART_IMPORT_MAX_ENTRIES {
        zip_writer
            .start_file(format!("rules/{index}.yaml"), options)
            .expect("start file");
    }
    zip_writer.finish().expect("finish zip");

    let err = extract_zip(&zip_path, temp.path().join("out").as_path())
        .expect_err("zip should be rejected");
    assert!(err.contains("too many entries"));
}

#[test]
fn compile_retry_defaults_to_none() {
    let retry = compile_retry(None).unwrap();
    assert!(retry.is_none());
}

#[test]
fn rule_ref_from_path_avoids_double_rules_prefix() {
    let base_dir = PathBuf::from("/tmp/rules");
    let path = base_dir.join("rules").join("endpoint.yaml");
    let rule_ref = rule_ref_from_path(&base_dir, &path);
    assert_eq!(rule_ref, "rules/endpoint.yaml");
}

#[test]
fn eval_expr_string_rejects_non_string() {
    let expr = parse_v2_expr(&json!(123)).expect("parse expr");
    let input = json!({});
    let err = eval_expr_string(&expr, &input, None).expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("expected string"));
}

#[test]
fn compile_network_rule_rejects_zero_timeout() {
    let raw = NetworkRuleFile {
        version: 2,
        rule_type: "network".to_string(),
        request: NetworkRequest {
            method: "GET".to_string(),
            url: json!("https://example.com"),
            headers: None,
        },
        timeout: "0s".to_string(),
        internal_auth: false,
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        catch: None,
        retry: None,
    };
    let err = compile_network_rule(raw, Path::new("network.yaml")).expect_err("expected error");
    assert!(err.to_string().contains("timeout must be > 0"));
}

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

#[tokio::test]
async fn reply_body_omitted_returns_empty_body() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/empty
    steps: []
    reply:
      status: 204
"#,
    )
    .expect("write endpoint.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/empty")
        .body(axum::body::Body::empty())
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_eq!(response.status().as_u16(), 204);
    assert!(response.headers().get("content-type").is_none());

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn request_body_too_large_writes_trace() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_endpoint_yaml(
        rules_dir,
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
    );

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true)
            .with_max_body_bytes(16),
    )
    .expect("load engine");

    let body = vec![b'a'; 64];
    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .body(axum::body::Body::from(body))
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("payload too large"));

    let items = wait_for_trace_items(rules_dir).await;
    assert!(!items.is_empty());
    assert!(items.iter().any(|item| item.status == "error"));
}

#[tokio::test]
async fn request_body_read_error_returns_network_error() {
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

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let stream = stream::once(async {
        Err::<axum::body::Bytes, std::io::Error>(std::io::Error::new(
            std::io::ErrorKind::Other,
            "boom",
        ))
    });
    let body = axum::body::Body::from_stream(stream);
    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .body(body)
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("request body read error"));
}

include!("tests/catch.rs");

include!("tests/multipart_import.rs");

#[tokio::test]
async fn network_response_too_large_returns_error() {
    let payload = "x".repeat(2048);
    let app = axum::Router::new().route(
        "/data",
        axum::routing::get({
            let payload = payload.clone();
            move || {
                let payload = payload.clone();
                async move { axum::Json(json!({ "data": payload })) }
            }
        }),
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
    let rules_subdir = rules_dir.join("rules");
    std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");

    std::fs::write(
        rules_subdir.join("network.yaml"),
        format!(
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
"#,
            host
        ),
    )
    .expect("write network.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true)
            .with_max_response_bytes(128),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("payload too large"));

    let items = wait_for_trace_items(rules_dir).await;
    assert!(!items.is_empty());
    assert!(items.iter().any(|item| item.status == "error"));

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

#[tokio::test]
async fn network_chunked_response_too_large_returns_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        let headers = concat!(
            "HTTP/1.1 200 OK\r\n",
            "content-type: application/json\r\n",
            "transfer-encoding: chunked\r\n",
            "\r\n"
        );
        let _ = socket.write_all(headers.as_bytes()).await;

        let chunk1 = "{\"data\":\"";
        let chunk2 = format!("{}\"}}", "x".repeat(64));
        let chunk1_line = format!("{:X}\r\n{}\r\n", chunk1.len(), chunk1);
        let chunk2_line = format!("{:X}\r\n{}\r\n", chunk2.len(), chunk2);
        let _ = socket.write_all(chunk1_line.as_bytes()).await;
        let _ = socket.write_all(chunk2_line.as_bytes()).await;
        let _ = socket.write_all(b"0\r\n\r\n").await;
        let _ = socket.flush().await;
        let _ = socket.shutdown().await;
    });

    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = rules_dir.join("rules");
    std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");

    std::fs::write(
        rules_subdir.join("network.yaml"),
        format!(
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
"#,
            host
        ),
    )
    .expect("write network.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true)
            .with_max_response_bytes(32),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("handle request should fail");
    assert!(format!("{err}").contains("payload too large"));

    let _ = server_handle.await;
}

#[tokio::test]
async fn step_rule_record_when_false_returns_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = rules_dir.join("rules");
    std::fs::create_dir_all(&rules_subdir).expect("create rules dir");

    std::fs::write(
        rules_dir.join("endpoint.yaml"),
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/filter
    steps:
      - rule: ./rules/filter.yaml
    reply:
      status: 200
      body: "@input"
"#,
    )
    .expect("write endpoint.yaml");

    std::fs::write(
        rules_subdir.join("filter.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: [1, 2]
mappings:
  - target: "ignored"
    value: "nope"
"#,
    )
    .expect("write filter rule");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/filter")
        .body(axum::body::Body::empty())
        .expect("build request");

    let err = engine
        .handle_request(request)
        .await
        .expect_err("expected error");
    assert!(err.to_string().contains("record"));
}
