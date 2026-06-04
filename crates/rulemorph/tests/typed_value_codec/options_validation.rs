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

