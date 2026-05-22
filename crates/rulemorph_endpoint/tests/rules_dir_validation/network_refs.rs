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
    let error = result
        .errors
        .iter()
        .find(|err| err.code == "RuleParseFailed")
        .expect("expected RuleParseFailed");
    assert!(error.line.is_some(), "expected parse error line");
    assert!(error.column.is_some(), "expected parse error column");
}

#[test]
fn validate_rules_dir_network_header_expr_error() {
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
  method: GET
  url: "https://example.com"
  headers:
    x-test: "@foo-bar"
timeout: 5s
"#,
    );

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result.errors.iter().any(|err| {
        err.code == "InvalidExpr" && err.path.as_deref() == Some("request.headers.x-test")
    }));
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
    assert!(
        result
            .errors
            .iter()
            .any(|err| err.code == "CatchRuleInvalid")
    );
}
