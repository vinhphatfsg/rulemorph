#[test]
fn test_v2_context_ref_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: rate
    expr:
      - "@context.rate"
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"id": 1}]"#;
    let context = serde_json::json!({"rate": 1.5});
    let result = transform(&rule, input, Some(&context)).unwrap();
    assert_eq!(result, serde_json::json!([{"rate": 1.5}]));
}

#[test]
fn test_v2_out_ref_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: first_name
    expr:
      - "@input.name"
  - target: greeting
    expr:
      - "Hello, "
      - concat: ["@out.first_name"]
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"name": "Bob"}]"#;
    let result = transform(&rule, input, None).unwrap();
    assert_eq!(
        result,
        serde_json::json!([{"first_name": "Bob", "greeting": "Hello, Bob"}])
    );
}
