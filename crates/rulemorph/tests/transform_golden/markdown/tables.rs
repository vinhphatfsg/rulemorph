#[test]
fn markdown_duplicate_table_headers_fail_in_strict_mode() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
mappings:
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(
        &rule,
        "| Field | Field |\n| --- | --- |\n| id | duplicate |",
        None,
    )
    .expect_err("duplicate strict headers should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("table headers"));
}

#[test]
fn markdown_blank_table_headers_fail_in_strict_mode_even_when_text_is_not_trimmed() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
    trim_text: false
    collapse_whitespace: false
mappings:
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(&rule, "| `   ` | Type |\n| --- | --- |\n| id | string |", None)
        .expect_err("blank strict headers should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("table headers"));
}

#[test]
fn markdown_duplicate_table_headers_can_use_index_policy() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: index
mappings:
  - target: "first"
    source: "input.object.col_0"
  - target: "second"
    source: "input.object.col_1"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| Field | Field |\n| --- | --- |\n| id | duplicate |",
        None,
    )
    .expect("index policy should pass");
    assert_eq!(
        output,
        serde_json::json!([{ "first": "id", "second": "duplicate" }])
    );
}

#[test]
fn markdown_table_rows_projection_ignores_document_table_output_flag() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    include:
      tables: false
mappings:
  - target: "field"
    source: "input.object.Field"
  - target: "type"
    source: "input.object.Type"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| Field | Type |\n| --- | --- |\n| id | string |",
        None,
    )
    .expect("table_rows projection should not depend on include.tables");
    assert_eq!(
        output,
        serde_json::json!([{ "field": "id", "type": "string" }])
    );
}

#[test]
fn markdown_strict_table_keys_match_headers_when_text_is_not_trimmed() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    records: table_rows
    table_header_policy: strict
    trim_text: false
    collapse_whitespace: false
mappings:
  - target: "headers"
    source: "input.headers"
  - target: "object"
    source: "input.object"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(
        &rule,
        "| `  Field  ` | Type |\n| --- | --- |\n| id | string |",
        None,
    )
        .expect("strict header keys should match headers");
    assert_eq!(
        output,
        serde_json::json!([{
            "headers": [" Field ", "Type"],
            "object": {
                " Field ": "id",
                "Type": "string"
            }
        }])
    );
}
