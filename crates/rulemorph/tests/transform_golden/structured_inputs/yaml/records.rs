#[test]
fn yaml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("users:\n  - id: 1\n  - id: 2\n"),
        &options,
    )
    .expect_err("record limit should fail before YAML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_input_uses_records_path() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "users:\n  - id: 1\n    name: Alice\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}
