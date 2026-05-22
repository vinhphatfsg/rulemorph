#[test]
fn html_attr_value_requires_attr_name() {
    let yaml = r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      url:
        selector: "a"
        value: attr
mappings:
  - target: "url"
    source: "url"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing attr should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.path.as_deref() == Some("input.html.fields.url.attr"))
    );
}

#[test]
fn excel_without_headers_requires_columns() {
    let yaml = r#"
version: 2
input:
  format: excel
  excel:
    has_header: false
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingExcelColumns"
                && err.path.as_deref() == Some("input.excel.columns"))
    );
}
