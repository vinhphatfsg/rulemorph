use super::*;

#[test]
fn custom_op_call_site_validation_ignores_version_one_literal_arrays() {
    let rule = parse(
        r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - hello
      - foo:
          - with: {}
"#,
    );

    validate_rule_file(&rule).expect("v1 literal arrays are not v2 custom op calls");
}
