#[test]
fn toml_rejects_depth_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect_err("depth limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_nested_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "a.b.c.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 4,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("[a.b.c]\nvalue = 1\n"), &options)
            .expect("nested TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(
        record,
        serde_json::json!({ "a": { "b": { "c": { "value": 1 } } } })
    );
}

#[test]
fn toml_allows_inline_table_at_equivalent_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let mut records = normalize_records_with_options(
        &rule,
        InputData::Text("record = { value = 1 }\n"),
        &options,
    )
    .expect("inline TOML table should fit within equivalent JSON depth");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "record": { "value": 1 } }));
}

#[test]
fn toml_rejects_nested_inline_table_over_depth_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "value"
    source: "record.inner.value"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_depth: 2,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("record = { inner = { value = 1 } }\n"),
        &options,
    )
    .expect_err("nested inline table should exceed depth limit");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
