use rulemorph::{parse_rule_file, transform};

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

#[test]
fn test_v2_with_let_step_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: total
    expr:
      - "@input.price"
      - let: { base: "$" }
      - multiply: [1.1]
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"price": 100}]"#;
    let result = transform(&rule, input, None).unwrap();
    let total = result[0]["total"].as_f64().unwrap();
    assert!((total - 110.0).abs() < 0.001);
}

#[test]
fn test_v2_with_if_step_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: discount
    expr:
      - "@input.total"
      - if:
          cond:
            gt: ["$", 1000]
          then:
            - "$"
            - multiply: [0.9]
          else:
            - "$"
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"total": 2000}, {"total": 500}]"#;
    let result = transform(&rule, input, None).unwrap();
    let first = result[0]["discount"].as_f64().unwrap();
    let second = result[1]["discount"].as_f64().unwrap();
    assert!((first - 1800.0).abs() < 0.001);
    assert!((second - 500.0).abs() < 0.001);
}

#[test]
fn test_v2_with_map_step_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: items
    expr:
      - "@input.values"
      - map:
        - multiply: [2]
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"values": [1, 2, 3]}]"#;
    let result = transform(&rule, input, None).unwrap();
    // multiply returns f64, so [2.0, 4.0, 6.0]
    assert_eq!(result, serde_json::json!([{"items": [2.0, 4.0, 6.0]}]));
}

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
fn test_v2_lookup_first_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: dept_name
    expr:
      - lookup_first:
        - "@context.departments"
        - id
        - "@input.dept_id"
        - name
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"dept_id": 2}]"#;
    let context = serde_json::json!({
        "departments": [
            {"id": 1, "name": "Engineering"},
            {"id": 2, "name": "Marketing"},
            {"id": 3, "name": "Sales"}
        ]
    });
    let result = transform(&rule, input, Some(&context)).unwrap();
    assert_eq!(result, serde_json::json!([{"dept_name": "Marketing"}]));
}

#[test]
fn test_v2_lookup_first_with_pipe_value_transform() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: dept_name
    expr:
      - "@context.departments"
      - lookup_first:
        - id
        - "@input.dept_id"
        - name
"#;
    let rule = parse_rule_file(yaml).unwrap();
    let input = r#"[{"dept_id": 2}]"#;
    let context = serde_json::json!({
        "departments": [
            {"id": 1, "name": "Engineering"},
            {"id": 2, "name": "Marketing"},
            {"id": 3, "name": "Sales"}
        ]
    });
    let result = transform(&rule, input, Some(&context)).unwrap();
    assert_eq!(result, serde_json::json!([{"dept_name": "Marketing"}]));
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
