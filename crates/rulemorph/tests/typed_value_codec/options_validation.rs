#[test]
fn phase6_rejects_experimental_inline_types_even_when_profile_exists() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: typed
    expr:
      - "@input"
      - to_typed_value:
          profile: dynamodb_item
          types:
            string:
              tag: S
"#;
    let message = transform_err(yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("experimental"));
}

#[test]
fn typed_value_decode_options_are_fail_closed() {
    let non_object_yaml = r#"
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
          decode: safe_json
"#;
    let message = transform_err(non_object_yaml, r#"{"id":{"S":"u1"}}"#);
    assert!(message.contains("decode must be an object"));

    let typo_yaml = r#"
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
          decode:
            number_polciy: parse_json_number_if_safe
"#;
    let message = transform_err(typo_yaml, r#"{"count":{"N":"1"}}"#);
    assert!(message.contains("unknown typed value decode option"));
}

#[test]
fn typed_value_inline_literal_options_are_validated_statically() {
    let cases = [
        (
            r#"
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
          decode: safe_json
"#,
            "decode must be an object",
        ),
        (
            r#"
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
            id: object_id
"#,
            "field type is not supported by dynamodb_item profile",
        ),
        (
            r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input.id"
      - to_typed_value:
          profile: dynamodb_item
          type: object_id
"#,
            "type is not supported by dynamodb_item profile",
        ),
        (
            r#"
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
            created_at: timestamp
"#,
            "field type is not supported by mongo_extended_json profile",
        ),
    ];

    for (yaml, expected_message) in cases {
        let rule = parse_rule_file(yaml).expect("parse rule");
        let errors =
            validate_rule_file(&rule).expect_err("inline typed-value options should validate");
        assert!(
            errors
                .iter()
                .any(|err| err.message.contains(expected_message)),
            "expected message containing {expected_message:?}, got {errors:?}"
        );
    }
}

#[test]
fn typed_value_runtime_rejects_shadowed_decode_policy_values() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input.value"
      - from_typed_value: "@input.options"
"#;
    let message = transform_err(
        yaml,
        r#"{"value":{"count":{"N":"1"}},"options":{"profile":"dynamodb_item","number_policy":"string","decode":{"number_policy":"parse_json_number_if_sfae"}}}"#,
    );
    assert!(message.contains("unsupported typed value number_policy"));
}

#[test]
fn typed_value_runtime_rejects_mongo_only_options_on_non_mongo_profiles() {
    let dynamodb_mode_yaml = r#"
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
          mode: canonical
"#;
    let message = transform_err(dynamodb_mode_yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("MongoDB options require mongo_extended_json profile"));

    let firestore_wrapper_policy_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input.name"
      - to_typed_value:
          profile: firestore_value
          extended_json_wrapper_objects: reject_unhinted
"#;
    let message = transform_err(firestore_wrapper_policy_yaml, r#"{"name":"Ada"}"#);
    assert!(message.contains("MongoDB options require mongo_extended_json profile"));
}

#[test]
fn typed_value_runtime_rejects_profile_incompatible_hint_types() {
    let missing_field_yaml = r#"
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
            missing:
              type: object_id
              on_missing: ignore
"#;
    let message = transform_err(missing_field_yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("field type is not supported by dynamodb_item profile: object_id"));

    let root_type_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input.id"
      - to_typed_value:
          profile: dynamodb_attribute_value
          type: object_id
"#;
    let message = transform_err(root_type_yaml, r#"{"id":"0123456789abcdef01234567"}"#);
    assert!(
        message.contains("type is not supported by dynamodb_attribute_value profile: object_id")
    );
}

#[test]
fn typed_value_mongodb_rejects_timestamp_hint_contracts() {
    let runtime_yaml = r#"
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
            created_at: timestamp
"#;
    let message = transform_err(runtime_yaml, r#"{"created_at":"2026-06-03T00:00:00Z"}"#);
    assert!(message.contains("field type is not supported by mongo_extended_json profile: timestamp"));

    let static_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo:
    profile: mongo_extended_json
    type: timestamp
mappings:
  - target: typed
    expr:
      - "@input.created_at"
      - to_typed_value:
          codec: mongo
"#;
    let rule = parse_rule_file(static_yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("MongoDB timestamp hint should be invalid");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("type is not supported by mongo_extended_json profile: timestamp"))
    );
}

#[test]
fn typed_value_validation_rejects_root_type_on_map_shaped_profiles() {
    let codec_binding_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  doc:
    profile: firestore_document
    type: integer
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          codec: doc
"#;
    let rule = parse_rule_file(codec_binding_yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("map-shaped codec type should be invalid");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("type is not supported by firestore_document profile"))
    );

    let inline_yaml = r#"
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
          type: number_string
"#;
    let message = transform_err(inline_yaml, r#"{"count":"1"}"#);
    assert!(message.contains("type is not supported by dynamodb_item profile"));
}

#[test]
fn typed_value_validation_checks_top_level_codec_profile_constraints() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    profile: dynamodb_item
    field_types:
      id: object_id
mappings:
  - target: unchanged
    expr: "@input"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("top-level codec binding should be invalid");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("field type is not supported by dynamodb_item profile"))
    );
}

#[test]
fn typed_value_validation_rejects_root_hints_on_map_shaped_profiles() {
    let codec_binding_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  doc:
    profile: firestore_document
    field_types:
      ".": integer
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          codec: doc
"#;
    let rule = parse_rule_file(codec_binding_yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("map-shaped root hint should be invalid");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("root field type path is not supported"))
    );

    let inline_yaml = r#"
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
          hints:
            - path: "."
              type: number_string
"#;
    let message = transform_err(inline_yaml, r#"{"count":"1"}"#);
    assert!(message.contains("root field type path is not supported"));
}

#[test]
fn typed_value_validation_rejects_conflicting_root_type_contracts() {
    let static_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo:
    profile: mongo_extended_json
    type: object_id
    field_types:
      ".": date
mappings:
  - target: typed
    expr:
      - "@input.id"
      - to_typed_value:
          codec: mongo
"#;
    let rule = parse_rule_file(static_yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("conflicting root contracts should fail");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("root type and root field type path cannot be used together"))
    );

    let dynamic_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: typed
    expr:
      - "@input.id"
      - to_typed_value: "@input.options"
"#;
    let message = transform_err(
        dynamic_yaml,
        r#"{"id":"0123456789abcdef01234567","options":{"profile":"mongo_extended_json","type":"object_id","field_types":{".":"date"}}}"#,
    );
    assert!(message.contains("root type and root field type path cannot be used together"));
}

#[test]
fn typed_value_requires_profile_argument_at_validation_time() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: typed
    expr:
      - "@input"
      - to_typed_value
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("typed value requires options argument");
    assert!(
        errors
            .iter()
            .any(|err| err.message.contains("requires at least 1 argument"))
    );
}

#[test]
fn typed_value_rejects_malformed_hint_paths() {
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
          field_types:
            events..created_at: number_string
"#;
    let message = transform_err(yaml, r#"{"events":{"created_at":"1"}}"#);
    assert!(message.contains("empty segment"));
}
