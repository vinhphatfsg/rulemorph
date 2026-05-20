#[test]
fn yaml_rule_rejects_duplicate_key() {
    let source = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: id
    source: id
    source: other_id
"#;
    let err = parse_rule_file_with_format(source, RuleFormat::Yaml)
        .expect_err("duplicate YAML keys must fail");
    assert!(err.message.contains("duplicate key"));
    assert!(err.line_column().is_some());
}

#[test]
fn yaml_rule_parse_error_preserves_location() {
    let source = "version: 2\ninput: [\n";
    let err = parse_rule_file(source).expect_err("malformed YAML must fail");
    assert!(err.location().is_some());

    let err = parse_rule_file_with_format(source, RuleFormat::Yaml)
        .expect_err("malformed YAML must fail");
    assert!(err.line_column().is_some());
}

#[test]
fn yaml_rule_rejects_trailing_document() {
    let source = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
---
version: 2
"#;
    let err = parse_rule_file(source).expect_err("trailing YAML document must fail");
    assert!(err.to_string().contains("exactly one document"));
}
