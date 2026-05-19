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
include!("tests/internal_auth.rs");

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

include!("tests/payload_limits.rs");

include!("tests/catch.rs");

include!("tests/multipart_import.rs");

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
