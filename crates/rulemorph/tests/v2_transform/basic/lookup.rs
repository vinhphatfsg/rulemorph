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
