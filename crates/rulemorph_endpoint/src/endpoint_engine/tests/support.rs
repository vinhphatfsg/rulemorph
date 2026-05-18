use super::*;
use rulemorph_trace::{TraceMeta, TraceStore};

pub(super) fn write_endpoint_yaml(rules_dir: &Path, yaml: &str) {
    std::fs::write(rules_dir.join("endpoint.yaml"), yaml).expect("write endpoint.yaml");
}

pub(super) fn create_rules_dir(rules_dir: &Path) -> PathBuf {
    let rules_subdir = rules_dir.join("rules");
    std::fs::create_dir_all(&rules_subdir).expect("create rules dir");
    rules_subdir
}

pub(super) fn write_rule_yaml(rules_dir: &Path, file_name: &str, yaml: &str) {
    std::fs::write(rules_dir.join(file_name), yaml)
        .unwrap_or_else(|err| panic!("write {file_name}: {err}"));
}

pub(super) fn write_default_catch_rule(rules_subdir: &Path, body_yaml: &str) {
    write_rule_yaml(
        rules_subdir,
        "catch.yaml",
        &format!(
            r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
{body_yaml}
"#
        ),
    );
}

pub(super) fn load_test_engine(rules_dir: &Path) -> EndpointEngine {
    EndpointEngine::load(
        rules_dir.to_path_buf(),
        EngineConfig::new("http://localhost".to_string(), rules_dir.to_path_buf())
            .with_ssrf_allow_private(true),
    )
    .expect("load engine")
}

pub(super) fn empty_request(method: &str, uri: &str) -> Request<axum::body::Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(axum::body::Body::empty())
        .expect("build request")
}

pub(super) async fn response_json(response: axum::response::Response) -> JsonValue {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("parse body")
}

pub(super) async fn assert_json_response(
    response: axum::response::Response,
    status: u16,
    expected: JsonValue,
) {
    assert_eq!(response.status().as_u16(), status);
    let body = response_json(response).await;
    assert_eq!(body, expected);
}

pub(super) async fn wait_for_trace_items(rules_dir: &Path) -> Vec<TraceMeta> {
    let store = TraceStore::new(rules_dir.to_path_buf())
        .await
        .expect("trace store");
    let mut items = Vec::new();
    for _ in 0..20 {
        items = store.list().await.expect("trace list");
        if !items.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    items
}
