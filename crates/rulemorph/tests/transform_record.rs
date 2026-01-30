use rulemorph::{parse_rule_file, transform_record};
use serde_json::json;

#[test]
fn transform_record_applies_finalize_wrap() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "value"
    source: "input.value"
finalize:
  wrap:
    result: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rule");
    let record = json!({"value": 1});
    let output = transform_record(&rule, &record, None)
        .expect("transform_record failed")
        .expect("expected output");
    assert_eq!(output, json!({"result": [{"value": 1}]}));
}

#[test]
fn transform_record_applies_finalize_filter() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "keep"
    source: "input.keep"
finalize:
  filter:
    eq: ["@item.keep", true]
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rule");
    let record = json!({"keep": false});
    let output = transform_record(&rule, &record, None)
        .expect("transform_record failed")
        .expect("expected output");
    assert_eq!(output, json!([]));
}

#[test]
fn transform_record_finalize_respects_record_when() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq: [1, 2]
mappings:
  - target: "value"
    source: "input.value"
finalize:
  wrap:
    result: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rule");
    let record = json!({"value": 1});
    let output = transform_record(&rule, &record, None).expect("transform_record failed");
    assert!(output.is_none());
}
