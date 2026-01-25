use serde_json::json;
use rulemorph::{parse_rule_file, transform_with_warnings};

#[test]
fn v2_record_when_condition_object_is_evaluated() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.active", true]
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "name": "aaa", "active": true }]"#;
    let (output, warnings) =
        transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "name": "aaa" }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}

#[test]
fn v2_mapping_when_condition_object_is_evaluated() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
    when:
      gt: ["@input.score", 10]
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"[{ "name": "aaa", "score": 20 }]"#;
    let (output, warnings) =
        transform_with_warnings(&rule, input, None).expect("transform failed");

    assert_eq!(output, json!([{ "name": "aaa" }]));
    assert!(warnings.is_empty(), "unexpected warnings: {warnings:?}");
}
