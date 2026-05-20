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
