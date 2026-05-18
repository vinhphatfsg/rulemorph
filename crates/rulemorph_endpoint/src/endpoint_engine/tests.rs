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

#[test]
fn endpoint_path_matches_and_captures() {
    let path = EndpointPath::parse("/api/traces/{id}").unwrap();
    assert!(path.matches("/api/traces/abc"));
    let params = path.capture("/api/traces/abc");
    assert_eq!(params.get("id"), Some(&"abc".to_string()));
}

#[test]
fn internal_hosts_match_loopback_aliases() {
    assert!(internal_hosts_match(Some("127.0.0.1"), Some("localhost")));
    assert!(internal_hosts_match(Some("localhost"), Some("::1")));
    assert!(!internal_hosts_match(
        Some("127.0.0.1"),
        Some("example.com")
    ));
}

#[test]
fn engine_config_allows_loopback_aliases_for_internal_base() {
    let config = EngineConfig::new(
        "http://localhost:8080".to_string(),
        std::path::PathBuf::from(".data"),
    );

    assert!(
        config
            .ssrf_private_allowlist
            .contains(&"localhost".to_string())
    );
    assert!(
        config
            .ssrf_private_allowlist
            .contains(&"127.0.0.1".to_string())
    );
    assert!(config.ssrf_private_allowlist.contains(&"::1".to_string()));
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
fn build_headers_rejects_host_header() {
    let mut headers = HashMap::new();
    let expr = parse_v2_expr(&json!("example.com")).expect("parse expr");
    headers.insert("Host".to_string(), expr);
    let err = build_headers(&headers, &json!({}), None).expect_err("expected error");
    assert_eq!(err.kind, EndpointErrorKind::Invalid);
    assert!(err.message.contains("disallowed header"));
}

#[test]
fn ssrf_audit_log_populates_fields() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: Some("rules/network.yaml".to_string()),
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let context = RequestContext {
        tenant_id: Some("tenant-1".to_string()),
        internal_api_key: None,
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", Some(&context));
    assert_eq!(log.tenant_id, "tenant-1");
    assert_eq!(log.rule_ref, "rules/network.yaml");
    assert_eq!(log.method, Method::GET);
    assert_eq!(log.url, "https://example.com/");
    assert_eq!(log.reason, "blocked");
}

#[test]
fn ssrf_audit_log_defaults_to_unknown() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::POST,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com", "blocked", None);
    assert_eq!(log.tenant_id, "unknown");
    assert_eq!(log.rule_ref, "unknown");
    assert_eq!(log.method, Method::POST);
}

#[test]
fn ssrf_audit_log_redacts_query() {
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: std::time::Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: None,
        body_rule_ref: None,
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let log = build_ssrf_audit_log(&rule, "https://example.com/path?token=abc", "blocked", None);
    assert_eq!(log.url, "https://example.com/path");
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

    let trace = engine.build_trace(
        &Method::GET,
        "/api/test",
        json!({"input": true}),
        json!({"output": false}),
        "error".to_string(),
        Some(json!({"message": "boom"})),
        Vec::new(),
        12,
    );
    let status = trace.get("status").and_then(|value| value.as_str());
    assert_eq!(status, Some("error"));
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

#[test]
fn mapping_ops_include_duration_us() {
    let mappings = vec![Mapping {
        target: "name".to_string(),
        source: None,
        value: Some(json!("hello")),
        expr: None,
        when: None,
        value_type: None,
        required: false,
        default: None,
    }];
    let record = json!({});
    let mut out = json!({});
    let ops = build_mapping_ops_with_values(&mappings, &record, None, &mut out, 2, 0);
    let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
    assert!(duration.is_some());
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

#[tokio::test]
async fn step_catch_inherits_with_params() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./rules/failing_network.yaml
        with:
          fields: ["name"]
        catch:
          default: ./rules/catch.yaml
    reply:
      status: 200
      body: "@input"
"#,
    );

    std::fs::write(
        rules_subdir.join("failing_network.yaml"),
        r#"
version: 2
type: network
request:
  method: GET
  url: "http://example.com"
timeout: 1s
body: "@input"
"#,
    )
    .expect("write failing network rule");

    std::fs::write(
        rules_subdir.join("catch.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "params"
    expr: "@context.params"
    required: true
"#,
    )
    .expect("write catch rule");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_eq!(response.status().as_u16(), 200);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
    assert_eq!(body, json!({ "params": { "fields": ["name"] } }));
}

#[tokio::test]
async fn endpoint_duplicate_query_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test?dup=1&dup=2");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn endpoint_invalid_json_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = Request::builder()
        .method("POST")
        .uri("/api/test")
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{\"bad\":}"))
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn endpoint_invalid_json_keeps_query_in_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "query"
    expr: "@input.query"
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = Request::builder()
        .method("POST")
        .uri("/api/test?token=abc")
        .header("content-type", "application/json")
        .body(axum::body::Body::from("{\"bad\":}"))
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "query": { "token": "abc" } })).await;
}

#[tokio::test]
async fn endpoint_input_mapping_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    input:
      - target: "user_id"
        source: "input.body.user_id"
        required: true
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("POST", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn reply_eval_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    catch:
      default: ./rules/catch.yaml
    steps: []
    reply:
      status: "@input.status"
      body: "@input.body"
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "status"
    value: 200
  - target: "body"
    value:
      handled: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_url_eval_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
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
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        r#"
version: 2
type: network
request:
  method: GET
  url: "@input.url"
timeout: 1s
catch:
  default: ./catch.yaml
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_body_build_error_runs_catch() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps:
      - rule: ./rules/network.yaml
    reply:
      status: 200
      body: "@input"
"#,
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 1s
body_map:
  - target: "required"
    source: "input.missing"
    required: true
catch:
  default: ./catch.yaml
"#,
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("POST", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;
}

#[tokio::test]
async fn network_select_error_runs_catch() {
    let app = axum::Router::new().route(
        "/data",
        axum::routing::get(|| async { axum::Json(json!({ "data": { "value": 1 } })) }),
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
    let rules_subdir = create_rules_dir(rules_dir);

    write_endpoint_yaml(
        rules_dir,
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
    );

    write_rule_yaml(
        &rules_subdir,
        "network.yaml",
        &format!(
            r#"
version: 2
type: network
request:
  method: GET
  url: "http://{}/data"
timeout: 1s
select: "missing.path"
catch:
  default: ./catch.yaml
"#,
            host
        ),
    );

    write_default_catch_rule(
        &rules_subdir,
        r#"
  - target: "handled"
    value: true
"#,
    );

    let engine = load_test_engine(rules_dir);

    let request = empty_request("GET", "/api/test");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_json_response(response, 200, json!({ "handled": true })).await;

    let _ = shutdown_tx.send(());
    let _ = server_handle.await;
}

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
async fn network_timeout_on_slow_body_runs_catch() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let host = format!("localhost:{}", addr.port());

    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let _ = socket.read(&mut buf).await;
        let body = b"{\"value\":1}";
        let headers = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
            body.len()
        );
        let _ = socket.write_all(headers.as_bytes()).await;
        let _ = socket.flush().await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let _ = socket.write_all(body).await;
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
  url: "http://{}/slow"
timeout: 100ms
catch:
  timeout: ./catch.yaml
"#,
            host
        ),
    )
    .expect("write network.yaml");

    std::fs::write(
        rules_subdir.join("catch.yaml"),
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "handled"
    value: true
"#,
    )
    .expect("write catch.yaml");

    let engine = EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine");

    let request = Request::builder()
        .method("GET")
        .uri("/api/test")
        .body(axum::body::Body::empty())
        .expect("build request");

    let response = engine
        .handle_request(request)
        .await
        .expect("handle request");
    assert_eq!(response.status().as_u16(), 200);

    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body: JsonValue = serde_json::from_slice(&bytes).expect("parse body");
    assert_eq!(body, json!({ "handled": true }));

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

#[test]
fn rule_nodes_include_step_duration_us() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let duration = trace.nodes[0]
        .get("duration_us")
        .and_then(|value| value.as_u64());
    assert!(duration.is_some());
}

#[test]
fn rule_trace_duration_includes_finalize_duration() {
    let nodes = vec![json!({ "duration_us": 10 }), json!({ "duration_us": 15 })];
    let finalize = json!({ "duration_us": 7 });

    assert_eq!(sum_rule_trace_duration_us(&nodes, Some(&finalize)), 32);
}

#[test]
fn finalize_trace_includes_operation_nodes_in_order() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  filter:
    gte: ["@item.score", 10]
  sort:
    by: "score"
    order: "asc"
  limit: 1
  offset: 0
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("ok")
    );
    assert!(
        finalize
            .get("duration_us")
            .and_then(|value| value.as_u64())
            .is_some()
    );
    assert_eq!(
        finalize
            .get("input")
            .and_then(|value| value.as_array())
            .map(|items| items.len()),
        Some(1)
    );
    let labels: Vec<&str> = finalize
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("finalize nodes")
        .iter()
        .map(|node| {
            node.get("label")
                .and_then(|value| value.as_str())
                .expect("node label")
        })
        .collect();
    assert_eq!(labels, vec!["filter", "sort", "limit", "offset", "wrap"]);
    assert_eq!(
        finalize
            .get("nodes")
            .and_then(|value| value.as_array())
            .and_then(|nodes| nodes.first())
            .and_then(|node| node.get("args"))
            .and_then(|args| args.get("expr")),
        Some(&json!({ "gte": ["@item.score", 10] }))
    );
}

#[test]
fn finalize_trace_preserves_error_payload() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  wrap:
    data:
      - "@out"
      - unknown_op
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");
    let error = finalize.get("error").expect("finalize error");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("error")
    );
    assert_eq!(
        error.get("code").and_then(|value| value.as_str()),
        Some("ExprError")
    );
    assert_eq!(
        error.get("message").and_then(|value| value.as_str()),
        Some("expr.op is not supported")
    );
    assert_eq!(
        error.get("path").and_then(|value| value.as_str()),
        Some("finalize.wrap.data[1].op")
    );
}

#[test]
fn network_nodes_include_request_duration_us() {
    let body_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#;
    let body_rule = parse_rule_file(body_yaml).expect("parse body rule");
    let rule = CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method: Method::GET,
            url: parse_v2_expr(&json!("https://example.com")).expect("parse url"),
            headers: HashMap::new(),
        },
        timeout: Duration::from_secs(1),
        select: None,
        body: None,
        body_map: None,
        body_rule: Some(LoadedRule {
            rule: body_rule,
            base_dir: PathBuf::from("."),
        }),
        body_rule_ref: Some("rules/body.yaml".to_string()),
        rule_ref: None,
        catch: None,
        retry: None,
        internal_auth: false,
        base_dir: PathBuf::from("."),
    };
    let timing = NetworkExecution {
        output: json!({}),
        request_us: 12,
        total_us: 34,
        body_rule_trace: Some(json!({
            "rule": { "path": "rules/body.yaml" },
            "records": []
        })),
    };

    let nodes = build_network_nodes_with_timing(&rule, &timing);
    let duration = nodes[0].get("duration_us").and_then(|value| value.as_u64());
    assert_eq!(duration, Some(34));
    let meta = nodes[0]
        .get("meta")
        .and_then(|value| value.as_object())
        .expect("meta");
    assert_eq!(meta.get("rule_ref"), Some(&json!("rules/body.yaml")));
    assert_eq!(meta.get("rule_ref_label"), Some(&json!("body_rule")));
    let child_trace = nodes[0]
        .get("child_trace")
        .and_then(|value| value.get("rule"))
        .and_then(|value| value.get("path"));
    assert_eq!(child_trace, Some(&json!("rules/body.yaml")));

    let children = nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("children");
    assert_eq!(children.len(), 2);
    let request = children[0]
        .get("duration_us")
        .and_then(|value| value.as_u64());
    assert_eq!(request, Some(12));
}

#[test]
#[ignore]
fn trace_timing_perf_smoke() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        value: "hello"
  - mappings:
      - target: upper
        expr: ["@out.name", uppercase]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({});
    let iterations = 100u64;
    let started = Instant::now();
    for _ in 0..iterations {
        let _ = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    }
    let total_us = started.elapsed().as_micros() as u64;
    println!("trace timing avg: {} μs", total_us / iterations);
}
