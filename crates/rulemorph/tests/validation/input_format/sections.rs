#[test]
fn extended_input_sections_are_validated() {
    let yaml = r#"
version: 2
input:
  format: yaml
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing yaml section should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingYamlSection")
    );
}

#[test]
fn unselected_input_sections_are_ignored_by_normal_validation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
  html:
    records_selector: ""
    fields: {}
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("unselected html section should be ignored");
}
