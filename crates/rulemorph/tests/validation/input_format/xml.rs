#[test]
fn xml_records_path_rejects_non_element_path_syntax() {
    let yaml = r#"
version: 2
input:
  format: xml
  xml:
    records_path: "users/user"
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid XML path should fail");
    assert!(
        errors.iter().any(|err| err.code.as_str() == "InvalidPath"
            && err.path.as_deref() == Some("input.xml.records_path"))
    );
}

#[test]
fn xml_records_path_accepts_non_ascii_element_names() {
    let yaml = r##"
version: 2
input:
  format: xml
  xml:
    records_path: 利用者.名前
mappings:
  - target: "name"
    source: "#text"
"##;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("valid Unicode XML names should pass validation");
}
