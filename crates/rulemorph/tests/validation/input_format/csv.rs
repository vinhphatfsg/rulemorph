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
