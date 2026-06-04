#[test]
fn tv28_map_let_binding() {
    let base = fixtures_dir().join("tv28_map_let_binding");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv30_literal_escape() {
    let base = fixtures_dir().join("tv30_literal_escape");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv31_v2_json_ops_pick_omit_reduce_fold() {
    let base = fixtures_dir().join("tv31_v2_json_ops_pick_omit_reduce_fold");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv44_math_ops() {
    let base = fixtures_dir().join("tv44_math_ops");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn tv47_typed_value_dynamodb_item_shorthand() {
    assert_json_fixture("tv47_typed_value_dynamodb_item_shorthand");
}

#[test]
fn tv48_typed_value_dynamodb_item_codec_binding() {
    assert_json_fixture("tv48_typed_value_dynamodb_item_codec_binding");
}

#[test]
fn tv49_typed_value_firestore_document() {
    assert_json_fixture("tv49_typed_value_firestore_document");
}

#[test]
fn tv50_typed_value_mongo_extended_json() {
    assert_json_fixture("tv50_typed_value_mongo_extended_json");
}

#[test]
fn r10_typed_value_dynamodb_attribute_value_requires_single_tag() {
    assert_transform_error_fixture("r10_typed_value_dynamodb_attribute_value_requires_single_tag");
}

#[test]
fn r11_typed_value_mongo_rejects_unhinted_extended_json_wrapper() {
    assert_transform_error_fixture("r11_typed_value_mongo_rejects_unhinted_extended_json_wrapper");
}

#[test]
fn tv44_range_limit_options_apply_to_finalize_wrap() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
finalize:
  wrap:
    count:
      - 10001
      - range: [0, "$"]
      - len
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform_input(&rule, InputData::Text("[{}]"), None).unwrap_err();
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("range length exceeds configured limit"));

    let options = NormalizationOptions {
        max_range_items: None,
        ..NormalizationOptions::default()
    };
    let output = transform_input_with_options(&rule, InputData::Text("[{}]"), None, &options)
        .expect("transform with unlimited range");
    assert_eq!(output["count"].as_u64(), Some(10_001));
}

#[test]
fn tv44_range_unlimited_still_respects_generated_array_limit() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: items
    expr:
      - range: [0, 10001]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let options = NormalizationOptions {
        max_range_items: None,
        max_array_len: 10_000,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(&rule, InputData::Text("[{}]"), None, &options)
        .expect_err("generated array limit should reject oversized unlimited range");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message
            .contains("generated array items exceed configured limit")
    );
}

#[test]
fn tv44_range_nested_generation_respects_array_limit() {
    let map_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: nested
    expr: [3, { range: [0, "$"] }, { op: "map", args: [[3, { range: [0, "$"] }]] }]
"#;
    let flat_map_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: nested
    expr: [3, { range: [0, "$"] }, { flat_map: [[3, { range: [0, "$"] }]] }]
"#;
    let map_step_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: nested
    expr:
      - 4
      - range: [1, "$"]
      - map:
        - range: [0, "$"]
"#;
    let options = NormalizationOptions {
        max_array_len: 8,
        ..NormalizationOptions::default()
    };
    for yaml in [map_yaml, flat_map_yaml, map_step_yaml] {
        let rule = parse_rule_file(yaml).expect("parse rule");
        let err = transform_input_with_options(&rule, InputData::Text("[{}]"), None, &options)
            .expect_err("nested generated arrays should be limited");
        assert_eq!(err.kind, TransformErrorKind::ExprError);
        assert!(
            err.message
                .contains("generated array items exceed configured limit")
        );
    }
}
