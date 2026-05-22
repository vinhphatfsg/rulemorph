#[test]
fn test_v2_v1_mixed_mappings() {
    // v1 style mapping (source) should still work in version 2
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: name
    source: name
  - target: upper_name
    expr:
      - "@input.name"
      - uppercase
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "alice"}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"name": "alice", "upper_name": "ALICE"}])
    );
}

#[test]
fn test_v1_rules_still_work() {
    // Ensure v1 rules are not affected
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: name
    source: name
  - target: upper
    expr:
      op: uppercase
      args:
        - { ref: input.name }
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "test"}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"name": "test", "upper": "TEST"}])
    );
}
