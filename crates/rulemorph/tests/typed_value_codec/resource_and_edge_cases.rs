#[test]
fn typed_value_hint_options_and_hint_count_fail_closed() {
    let format_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          field_types:
            price:
              type: number_string
              format: nonsense
"#;
    let message = transform_err(format_yaml, r#"{"price":"1"}"#);
    assert!(message.contains("unsupported field type format"));

    let precision_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          profile: firestore_document
          field_types:
            created_at:
              type: timestamp
              output_precision: milliseconds
"#;
    let message = transform_err(precision_yaml, r#"{"created_at":"2026-06-03T00:00:00Z"}"#);
    assert!(message.contains("unsupported field type output_precision"));

    let mut field_types = String::new();
    for index in 0..1025 {
        field_types.push_str(&format!("            f{}: string_set\n", index));
    }
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          field_types:
{}"#,
        field_types
    );
    let message = transform_err(&yaml, r#"{}"#);
    assert!(message.contains("hint count"));
}

#[test]
fn typed_value_resource_limits_reject_unbounded_codec_and_hint_metadata() {
    let mut codecs = String::new();
    for index in 0..1025 {
        codecs.push_str(&format!("  c{}:\n    profile: dynamodb_item\n", index));
    }
    let too_many_codecs = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
codecs:
{}mappings:
  - target: x
    value: 1
"#,
        codecs
    );
    let rule = parse_rule_file(&too_many_codecs).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("codec count should be bounded");
    assert!(errors.iter().any(|err| err.message.contains("codec count")));

    let long_name = "c".repeat(257);
    let long_name_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
codecs:
  {}:
    profile: dynamodb_item
mappings:
  - target: x
    value: 1
"#,
        long_name
    );
    let rule = parse_rule_file(&long_name_yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("codec name length should be bounded");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("codec name bytes"))
    );

    let long_path = "a".repeat(4097);
    let long_path_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          hints:
            - path: "{}"
              type: string_set
              on_missing: ignore
"#,
        long_path
    );
    let message = transform_err(&long_path_yaml, r#"{}"#);
    assert!(message.contains("hint path bytes"));

    let token_path = (0..257)
        .map(|index| format!("p{}", index))
        .collect::<Vec<_>>()
        .join(".");
    let token_path_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          hints:
            - path: "{}"
              type: string_set
              on_missing: ignore
"#,
        token_path
    );
    let message = transform_err(&token_path_yaml, r#"{}"#);
    assert!(message.contains("hint path token count"));
}

#[test]
fn typed_value_decode_rejects_nested_dynamodb_empty_keys() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: dynamodb_attribute_value
"#;
    let message = transform_err(yaml, r#"{"M":{"":{"S":"x"}}}"#);
    assert!(message.contains("attribute name"));
}

#[test]
fn typed_value_mongodb_canonical_date_millis_are_strict() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: mongo_extended_json
"#;
    let message = transform_err(yaml, r#"{"$date":{"$numberLong":"abc"}}"#);
    assert!(message.contains("$numberLong"));
}

#[test]
fn typed_value_mongodb_canonical_date_roundtrips_through_raw_value() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo_doc:
    profile: mongo_extended_json
    mode: canonical
    field_types:
      created_at: date
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          codec: mongo_doc
  - target: encoded
    expr:
      - "@out.raw"
      - to_typed_value:
          codec: mongo_doc
"#;
    let output = transform_json(
        yaml,
        r#"{"created_at":{"$date":{"$numberLong":"1780444800000"}}}"#,
    );
    assert_eq!(
        output["raw"]["created_at"],
        serde_json::json!("2026-06-03T00:00:00.000Z")
    );
    assert_eq!(
        output["encoded"]["created_at"],
        serde_json::json!({"$date":{"$numberLong":"1780444800000"}})
    );
}

#[test]
fn typed_value_resource_limits_reject_large_generated_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut map = serde_json::Map::new();
    for i in 0..20_001 {
        map.insert(format!("k{}", i), serde_json::json!("v"));
    }
    let input = serde_json::Value::Object(map).to_string();
    let err = transform_input_with_options(
        &rule,
        InputData::Text(&input),
        None,
        &rulemorph::NormalizationOptions::default(),
    )
    .expect_err("field limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("field count"));
}

#[test]
fn typed_value_runtime_rejects_oversized_input_strings_and_keys() {
    let string_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
"#;
    let options = rulemorph::NormalizationOptions {
        max_text_bytes: 16 * 1024 * 1024,
        max_input_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };

    let oversized = "x".repeat(8 * 1024 * 1024 + 1);
    let input = serde_json::json!({"payload": oversized}).to_string();
    let rule = parse_rule_file(string_yaml).expect("parse rule");
    let err = transform_input_with_options(&rule, InputData::Text(&input), None, &options)
        .expect_err("oversized string");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("input string bytes"));

    let key_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          profile: mongo_extended_json
"#;
    let key = "k".repeat(8 * 1024 * 1024 + 1);
    let input = serde_json::json!({key: "v"}).to_string();
    let rule = parse_rule_file(key_yaml).expect("parse rule");
    let err = transform_input_with_options(&rule, InputData::Text(&input), None, &options)
        .expect_err("oversized key");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("input string bytes"));

    let decode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: mongo_extended_json
"#;
    let oversized = "x".repeat(8 * 1024 * 1024 + 1);
    let input = serde_json::json!({"payload": oversized}).to_string();
    let rule = parse_rule_file(decode_yaml).expect("parse rule");
    let err = transform_input_with_options(&rule, InputData::Text(&input), None, &options)
        .expect_err("oversized MongoDB decode string");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("input string bytes"));
}

#[test]
fn typed_value_runtime_rejects_oversized_number_set_and_firestore_double_strings() {
    let options = rulemorph::NormalizationOptions {
        max_text_bytes: 16 * 1024 * 1024,
        max_input_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };

    let number_set_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          field_types:
            scores: number_set
"#;
    let oversized = "0".repeat(8 * 1024 * 1024 + 1);
    let input = serde_json::json!({"scores": [oversized]}).to_string();
    let rule = parse_rule_file(number_set_yaml).expect("parse rule");
    let err = transform_input_with_options(&rule, InputData::Text(&input), None, &options)
        .expect_err("oversized DynamoDB number-set string");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("input string bytes"));

    let firestore_double_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: firestore_value
"#;
    let oversized = "0".repeat(8 * 1024 * 1024 + 1);
    let input = serde_json::json!({"doubleValue": oversized}).to_string();
    let rule = parse_rule_file(firestore_double_yaml).expect("parse rule");
    let err = transform_input_with_options(&rule, InputData::Text(&input), None, &options)
        .expect_err("oversized Firestore double string");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("input string bytes"));
}

#[test]
fn typed_value_dynamodb_sets_count_entries_against_node_limit() {
    let options = rulemorph::NormalizationOptions {
        max_text_bytes: 16 * 1024 * 1024,
        max_input_bytes: 32 * 1024 * 1024,
        ..Default::default()
    };

    let assert_node_count_error = |rule_yaml: &str, input: String| {
        let rule = parse_rule_file(rule_yaml).expect("parse rule");
        match transform_input_with_options(&rule, InputData::Text(&input), None, &options) {
            Ok(_) => panic!("expected DynamoDB set entries to count against node limit"),
            Err(err) => {
                assert_eq!(err.kind, TransformErrorKind::ExprError);
                assert!(err.message.contains("node count"));
            }
        }
    };

    let encode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          field_types:
            tags: string_set
"#;
    let values = (0..100_001)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>();
    assert_node_count_error(encode_yaml, serde_json::json!({"tags": values}).to_string());

    let decode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: dynamodb_item
"#;
    let values = (0..100_001)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>();
    assert_node_count_error(
        decode_yaml,
        serde_json::json!({"tags": {"SS": values}}).to_string(),
    );
}

#[test]
fn typed_value_dynamodb_number_precision_trims_trailing_zeroes_and_rejects_provider_invalid_forms()
{
    let encode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          field_types:
            value: number_string
"#;
    let large_with_trailing_zeroes = r#"{"value":"100000000000000000000000000000000000000"}"#;
    let output = transform_json(encode_yaml, large_with_trailing_zeroes);
    assert_eq!(
        output["item"]["value"],
        serde_json::json!({"N":"100000000000000000000000000000000000000"})
    );

    for invalid in [
        r#"{"value":"+5"}"#,
        r#"{"value":"5."}"#,
        r#"{"value":".5"}"#,
    ] {
        let message = transform_err(encode_yaml, invalid);
        assert!(message.contains("invalid DynamoDB number"));
    }
}

#[test]
fn typed_value_mongodb_decimal_and_binary_subtype_follow_provider_boundaries() {
    let decimal_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          profile: mongo_extended_json
          field_types:
            amount: decimal128
"#;
    let max_decimal = format!("1{}", "0".repeat(33));
    let valid_decimal = format!(r#"{{"amount":"{}"}}"#, max_decimal);
    let output = transform_json(decimal_yaml, &valid_decimal);
    assert_eq!(
        output["doc"]["amount"],
        serde_json::json!({"$numberDecimal": max_decimal})
    );
    let too_precise_decimal = format!(r#"{{"amount":"1.{}"}}"#, "0".repeat(34));
    let message = transform_err(decimal_yaml, &too_precise_decimal);
    assert!(message.contains("$numberDecimal exceeds 34 significant digits"));

    let binary_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          profile: mongo_extended_json
          field_types:
            blob:
              type: binary_base64
              subtype: "4"
"#;
    let output = transform_json(binary_yaml, r#"{"blob":"AQID"}"#);
    assert_eq!(
        output["doc"]["blob"],
        serde_json::json!({"$binary":{"base64":"AQID","subType":"4"}})
    );

    let decode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: mongo_extended_json
"#;
    let output = transform_json(
        decode_yaml,
        r#"{"blob":{"$binary":{"base64":"AQID","subType":"4"}}}"#,
    );
    assert_eq!(output["raw"]["blob"], serde_json::json!("AQID"));

    let non_canonical_base64 = transform_err(
        decode_yaml,
        r#"{"blob":{"$binary":{"base64":"AR==","subType":"4"}}}"#,
    );
    assert!(non_canonical_base64.contains("non-canonical base64"));
}

#[test]
fn typed_value_mongodb_relaxed_date_uses_canonical_wrapper_before_epoch() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          profile: mongo_extended_json
          mode: relaxed
          field_types:
            created_at: date
"#;
    let output = transform_json(yaml, r#"{"created_at":"1500-06-15T08:30:00Z"}"#);
    assert_eq!(
        output["doc"]["created_at"],
        serde_json::json!({"$date":{"$numberLong":"-14817483000000"}})
    );
}

#[test]
fn typed_value_firestore_reference_geo_point_and_depth_are_provider_strict() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: document
    expr:
      - "@input"
      - to_typed_value:
          profile: firestore_document
          field_types:
            ref: reference
            location: geo_point
"#;
    let output = transform_json(
        yaml,
        r#"{"ref":"projects/p/databases/(default)/documents/users/u1","location":{"latitude":35.0,"longitude":139.0}}"#,
    );
    assert_eq!(
        output["document"]["fields"]["ref"],
        serde_json::json!({"referenceValue":"projects/p/databases/(default)/documents/users/u1"})
    );
    assert_eq!(
        output["document"]["fields"]["location"],
        serde_json::json!({"geoPointValue":{"latitude":35.0,"longitude":139.0}})
    );
    let message = transform_err(
        yaml,
        r#"{"ref":"not-a-reference","location":{"latitude":35.0,"longitude":139.0}}"#,
    );
    assert!(message.contains("referenceValue"));

    let depth_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input"
      - to_typed_value:
          profile: firestore_value
"#;
    let mut input = serde_json::json!("leaf");
    for _ in 0..21 {
        input = serde_json::json!({"child": input});
    }
    let message = transform_err(depth_yaml, &input.to_string());
    assert!(message.contains("depth exceeds"));
}

#[test]
fn typed_value_firestore_decode_rejects_oversized_string_value() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: firestore_value
"#;
    let oversized = "x".repeat(1_048_488);
    let input = serde_json::json!({ "stringValue": oversized }).to_string();
    let message = transform_err(yaml, &input);
    assert!(message.contains("Firestore stringValue exceeds size limit"));
}
