use std::fs;
use std::path::{Path, PathBuf};

use rulemorph_ui::validate_rules_dir;

fn write_file(root: &Path, rel: &str, content: &str) -> PathBuf {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent dir");
    }
    fs::write(&path, content).expect("write file");
    path
}

fn basic_rule() -> &'static str {
    r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.ok"
    value: true
"#
}

#[test]
fn validate_rules_dir_ok() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
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
    );
    write_file(rules_dir, "rules/ok.yaml", basic_rule());

    let result = validate_rules_dir(rules_dir);
    assert!(result.is_ok());
}

#[test]
fn validate_rules_dir_missing_reference() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: missing.yaml
    reply:
      status: 200
"#,
    );

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result.errors.iter().any(|err| err.code == "ReadFailed"));
}

#[test]
fn validate_rules_dir_body_rule_parse_error() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
        r#"
version: 2
type: endpoint
endpoints:
  - method: POST
    path: /api/test
    steps:
      - rule: network.yaml
    reply:
      status: 200
"#,
    );
    write_file(
        rules_dir,
        "network.yaml",
        r#"
version: 2
type: network
request:
  method: POST
  url: "https://example.com"
timeout: 5s
body_rule: bad_rule.yaml
"#,
    );
    write_file(rules_dir, "bad_rule.yaml", "version: 2\ninput: [\n");

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result
        .errors
        .iter()
        .any(|err| err.code == "YamlParseFailed"));
}

#[test]
fn validate_rules_dir_catch_rejects_network_rule() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: network/main.yaml
    reply:
      status: 200
"#,
    );
    write_file(
        rules_dir,
        "network/main.yaml",
        r#"
version: 2
type: network
request:
  method: GET
  url: "https://example.com"
timeout: 5s
catch:
  default: other.yaml
"#,
    );
    write_file(
        rules_dir,
        "network/other.yaml",
        r#"
version: 2
type: network
request:
  method: GET
  url: "https://example.com"
timeout: 5s
"#,
    );

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result
        .errors
        .iter()
        .any(|err| err.code == "CatchRuleInvalid"));
}

#[test]
fn validate_rules_dir_branch_reference_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./hello.yaml
    reply:
      status: 200
"#,
    );
    write_file(
        rules_dir,
        "hello.yaml",
        r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ./missing.yaml
      return: true
"#,
    );

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result.errors.iter().any(|err| err.code == "ReadFailed"));
}
