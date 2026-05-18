#[test]
fn parse_json_rule_file_with_explicit_format() {
    let source = r#"{
      "version": 2,
      "input": { "format": "json", "json": { "records_path": "items" } },
      "mappings": [{ "target": "id", "source": "id" }]
    }"#;
    let rule = parse_rule_file_with_format(source, RuleFormat::Json).expect("parse json rule");
    assert_eq!(rule.version, 2);
}

#[test]
fn json_rule_file_fixture_should_pass_validation() {
    let rules_path = fixtures_dir().join("t30_json_rule_file").join("rules.json");
    let source = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file_with_format(&source, RuleFormat::Json).expect("parse json rule");
    validate_rule_file(&rule).expect("json rule fixture should validate");
}

#[test]
fn json_rule_rejects_duplicate_key() {
    let source = r#"{
      "version": 2,
      "version": 1,
      "input": { "format": "json", "json": {} },
      "mappings": []
    }"#;
    let err = parse_rule_file_with_format(source, RuleFormat::Json)
        .expect_err("duplicate JSON keys must fail");
    assert!(err.message.contains("duplicate key"));
}

#[test]
fn json_rule_rejects_trailing_comma() {
    let source = r#"{ "version": 2, }"#;
    let err =
        parse_rule_file_with_format(source, RuleFormat::Json).expect_err("trailing comma fails");
    assert!(err.message.contains("trailing comma") || err.message.contains("expected"));
}

#[test]
fn json_rule_rejects_trailing_garbage() {
    let source = r#"{
      "version": 2,
      "input": { "format": "json", "json": {} },
      "mappings": []
    } trailing"#;
    let err =
        parse_rule_file_with_format(source, RuleFormat::Json).expect_err("trailing garbage fails");
    assert!(err.message.contains("trailing characters") || err.message.contains("expected"));
}

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

#[test]
fn validation_errors_include_location_with_source() {
    let rules_path = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file(&yaml).unwrap();
    let errors = validate_rule_file_with_source(&rule, &yaml).unwrap_err();
    let error = errors
        .iter()
        .find(|err| err.code == ErrorCode::MissingMappingValue)
        .expect("expected MissingMappingValue");
    let location = error.location.clone().expect("expected location");
    assert_eq!(location.line, 7);
}
