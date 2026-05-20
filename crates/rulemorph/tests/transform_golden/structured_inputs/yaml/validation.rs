#[test]
fn yaml_rejects_non_string_mapping_key() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "value"
    source: "value"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1: value\n", None).expect_err("non-string key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_rejects_trailing_document() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "users:\n  - id: 1\n---\nusers:\n  - id: 2\n", None)
        .expect_err("multi-document YAML should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
