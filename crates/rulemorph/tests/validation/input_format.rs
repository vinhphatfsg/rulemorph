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
fn csv_columns_must_be_non_empty_and_unique() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: ""
      - name: id
      - name: id
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "InvalidInputOption"
                && err.path.as_deref() == Some("input.csv.columns[0].name"))
    );
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "DuplicateInputField"
                && err.path.as_deref() == Some("input.csv.columns[2].name"))
    );
}

#[test]
fn csv_without_header_rejects_empty_columns() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns: []
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("empty columns should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code.as_str() == "MissingCsvColumns"
                && err.path.as_deref() == Some("input.csv.columns"))
    );
}

#[test]
fn csv_rejects_multibyte_delimiter() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("multibyte delimiter should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidDelimiterLength
                && err.path.as_deref() == Some("input.csv.delimiter"))
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
    assert!(errors.iter().any(|err| err.code.as_str() == "InvalidPath"
        && err.path.as_deref() == Some("input.xml.records_path")));
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
