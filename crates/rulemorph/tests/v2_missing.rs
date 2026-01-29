use rulemorph::{parse_rule_file, transform_with_warnings};
use serde_json::json;

#[test]
fn v2_missing_string_op_propagates_to_default() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    expr:
      - "@input.name"
      - trim
    default: "unknown"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "id": 1 }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "name": "unknown" }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}

#[test]
fn v2_missing_number_op_propagates_to_default() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "amount"
    expr:
      - "@input.amount"
      - add: [1]
    default: 0
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "id": 1 }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "amount": 0 }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}
