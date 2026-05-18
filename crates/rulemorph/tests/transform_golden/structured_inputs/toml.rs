#[test]
fn toml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("[[users]]\nid = 1\n[[users]]\nid = 2\n"),
        &options,
    )
    .expect_err("record limit should fail before TOML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_datetime_is_string() {
    let toml_rule = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "created_at"
    source: "created_at"
"#;
    let rule = parse_rule_file(toml_rule).expect("parse rule");
    let input = "[[users]]\ncreated_at = 2026-05-08T12:00:00Z\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "created_at": "2026-05-08T12:00:00Z" }])
    );
}

#[test]
fn toml_quoted_private_datetime_key_stays_object() {
    let toml_rule = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "metadata"
    source: "metadata"
"#;
    let rule = parse_rule_file(toml_rule).expect("parse rule");
    let input = "[[users]]\n[users.metadata]\n'$__toml_private_datetime' = 2026-05-08T12:00:00Z\n";
    let records = normalize_records_with_options(
        &rule,
        InputData::Text(input),
        &NormalizationOptions::default(),
    )
    .expect("normalize toml")
    .collect::<Result<Vec<_>, _>>()
    .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({
            "metadata": { "$__toml_private_datetime": "2026-05-08T12:00:00Z" }
        })]
    );
}

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

#[test]
fn toml_rejects_array_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("values = [1, 2]\n"), &options)
        .expect_err("array limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: toml
  toml: {}
mappings:
  - target: "values"
    source: "values"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_array_len: 1,
        ..NormalizationOptions::default()
    };
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("values = [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}
