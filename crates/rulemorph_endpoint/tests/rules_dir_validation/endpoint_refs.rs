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
fn validate_rules_dir_accepts_json_rule_reference() {
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
      - rule: rules/ok.json
    reply:
      status: 200
"#,
    );
    write_file(
        rules_dir,
        "rules/ok.json",
        r#"{
  "version": 2,
  "input": { "format": "json", "json": {} },
  "mappings": [{ "target": "output.ok", "value": true }]
}
"#,
    );

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
