use rulemorph::{parse_rule_file, transform_with_warnings};
use serde_json::json;

#[test]
fn v2_map_op_missing_input_propagates() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "values"
    expr:
      - "@input.items"
      - { op: "map", args: ["@item.value"] }
    default: ["default"]
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "id": 1 }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "values": ["default"] }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}

#[test]
fn v2_map_op_drops_missing_results() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "values"
    expr:
      - "@input.items"
      - { op: "map", args: [["@item", { op: "get", args: ["value"] }]] }
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "items": [{"value": 1}, {"other": 2}, {"value": 3}] }]"#;
    let (output, warnings) = transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "values": [1, 3] }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}
