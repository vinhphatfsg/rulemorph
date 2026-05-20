#[test]
fn csv_trailing_missing_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
    default: "missing-name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,name\n1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id\n1,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("CSV error"), "{}", err.message);
}

#[test]
fn csv_no_header_short_row_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1\n", None).expect_err("short csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(
        err.message.contains("expected 2")
            || err.message.contains("expected 2 fields")
            || err.message.contains("expected 2"),
        "{}",
        err.message
    );
}

#[test]
fn csv_no_header_extra_field_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: false
    columns:
      - name: id
      - name: name
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1,Ada,extra\n", None).expect_err("wide csv row should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("expected 2"), "{}", err.message);
}

#[test]
fn csv_duplicate_header_is_invalid_input() {
    let yaml = r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "id,id\n1,2\n", None).expect_err("duplicate header should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
