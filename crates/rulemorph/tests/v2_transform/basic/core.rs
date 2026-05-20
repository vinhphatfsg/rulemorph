#[test]
fn test_v2_simple_ref_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: user_name
    expr:
      - "@input.name"
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "Alice"}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(result, serde_json::json!([{"user_name": "Alice"}]));
}

#[test]
fn test_v2_scalar_ref_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: user_name
    expr: "@input.name"
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "Alice"}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(result, serde_json::json!([{"user_name": "Alice"}]));
}

#[test]
fn test_v2_literal_object_with_lookup_key_is_literal() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: payload
    expr:
      lookup: 1
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"id": 1}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(result, serde_json::json!([{"payload": {"lookup": 1}}]));
}

#[test]
fn test_v2_pipe_with_ops_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: name
    expr:
      - "@input.name"
      - trim
      - uppercase
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "  alice  "}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(result, serde_json::json!([{"name": "ALICE"}]));
}
