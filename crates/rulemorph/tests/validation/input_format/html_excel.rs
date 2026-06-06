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

#[test]
fn excel_rejects_invalid_range_syntax_during_validation() {
    let yaml = r#"
version: 2
input:
  format: excel
  excel:
    range: "B3"
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid range should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidInputOption
            && err.path.as_deref() == Some("input.excel.range")
            && err.message.contains("A:D or A1:D100")
    }));
}

#[test]
fn excel_rejects_numeric_column_reference_during_validation() {
    let yaml = r#"
version: 2
input:
  format: excel
  excel:
    has_header: false
    columns:
      - { name: id, column: "1" }
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("numeric column should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidInputOption
            && err.path.as_deref() == Some("input.excel.columns[0].column")
            && err.message.contains("Excel column reference is invalid")
    }));
}

#[test]
fn excel_rejects_header_row_before_selected_range_during_validation() {
    let yaml = r#"
version: 2
input:
  format: excel
  excel:
    range: "B3:F100"
    header_row: 1
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("header before range should fail validation");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidInputOption
            && err.path.as_deref() == Some("input.excel.header_row")
            && err.message.contains("before selected range")
    }));
}
