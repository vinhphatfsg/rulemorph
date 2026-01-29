use rulemorph::{parse_rule_file, transform_with_warnings};
use serde_json::json;

#[test]
fn v2_lookup_first_missing_pipe_defaults() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "user_name"
    expr:
      - "@input.users"
      - lookup_first:
        - id
        - "@input.user_id"
        - name
    default: "unknown"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "user_id": 1 }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "user_name": "unknown" }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}

#[test]
fn v2_lookup_missing_pipe_defaults() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "users"
    expr:
      - "@input.users"
      - lookup:
        - id
        - "@input.user_id"
        - name
    default: ["missing"]
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "user_id": 1 }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "users": ["missing"] }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}
