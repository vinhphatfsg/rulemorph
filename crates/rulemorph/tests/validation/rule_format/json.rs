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
