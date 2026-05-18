#[test]
fn csv_normalization_rejects_too_many_records_while_iterating() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
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
    let mut records =
        normalize_records_with_options(&rule, InputData::Text("id\n1\n2\n"), &options)
            .expect("CSV iterator should be created before the second record is read");
    let first = records
        .next()
        .expect("first record should exist")
        .expect("first record should parse");
    assert_eq!(first, serde_json::json!({ "id": "1" }));
    let err = records
        .next()
        .expect("second record should report the record limit")
        .expect_err("record limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
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
        InputData::Text(r#"{ "users": [{ "id": 1 }, { "id": 2 }] }"#),
        &options,
    )
    .expect_err("record limit should fail before records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn json_records_path_rejects_single_object_when_record_limit_is_zero() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json:
    records_path: user
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_records: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(
        &rule,
        InputData::Text(r#"{ "user": { "id": 1 } }"#),
        &options,
    )
    .expect_err("single object should still honor max_records");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_records_path_rejects_too_many_records_before_materializing() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
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
        InputData::Text("users:\n  - id: 1\n  - id: 2\n"),
        &options,
    )
    .expect_err("record limit should fail before YAML records are materialized");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

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
fn csv_rejects_non_byte_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: csv
  csv:
    has_header: true
    delimiter: "，"
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse rule");
    let err = normalize_records_with_options(
        &rule,
        InputData::Text("id，name\n1，Alice\n"),
        &NormalizationOptions::default(),
    )
    .expect_err("non-byte delimiter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_input_uses_records_path() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "users:\n  - id: 1\n    name: Alice\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(output, serde_json::json!([{ "id": 1, "name": "Alice" }]));
}

#[test]
fn yaml_rejects_non_string_mapping_key() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "value"
    source: "value"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "1: value\n", None).expect_err("non-string key should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_rejects_trailing_document() {
    let yaml = r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform(&rule, "users:\n  - id: 1\n---\nusers:\n  - id: 2\n", None)
        .expect_err("multi-document YAML should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_alias_limit_ignores_asterisks_in_scalars() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
  - target: "note"
    source: "note"
"#,
    )
    .expect("parse rule");
    let input = r#"
users:
  - name: Alice
    note: "**********"
    block: |
      **********
"#;
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let records = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect("asterisks in scalar values should not count as aliases")
        .collect::<Result<Vec<_>, _>>()
        .expect("normalized records");
    assert_eq!(
        records,
        vec![serde_json::json!({ "name": "Alice", "note": "**********", "block": "**********\n" })]
    );
}

#[test]
fn yaml_rejects_alias_expansion_limit_exceeded() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let input = "base: &base { name: Alice }\nusers: [*base, *base]\n";
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text(input), &options)
        .expect_err("alias limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn toml_datetime_is_string() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "created_at"
    source: "created_at"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = "[[users]]\ncreated_at = 2026-05-08T12:00:00Z\n";
    let output = transform(&rule, input, None).expect("transform");
    assert_eq!(
        output,
        serde_json::json!([{ "created_at": "2026-05-08T12:00:00Z" }])
    );
}

#[test]
fn toml_quoted_private_datetime_key_stays_object() {
    let yaml = r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "metadata"
    source: "metadata"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
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

#[test]
fn yaml_rejects_text_limit_during_parse() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
mappings:
  - target: "name"
    source: "name"
"#,
    )
    .expect("parse rule");
    let options = NormalizationOptions {
        max_text_bytes: 3,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Text("name: Alice\n"), &options)
        .expect_err("text limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn yaml_allows_array_at_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: yaml
  yaml: {}
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
        normalize_records_with_options(&rule, InputData::Text("values: [1]\n"), &options)
            .expect("array at limit should pass");
    let record = records.next().expect("record").expect("record ok");
    assert_eq!(record, serde_json::json!({ "values": [1] }));
}
