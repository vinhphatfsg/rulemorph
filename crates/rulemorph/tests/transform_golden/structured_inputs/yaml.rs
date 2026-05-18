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
