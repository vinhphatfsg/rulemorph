use rulemorph::{
    InputData, TransformErrorKind, parse_rule_file, transform_input, transform_input_with_options,
    validate_rule_file,
};

fn transform_json(rule_yaml: &str, input: &str) -> serde_json::Value {
    let rule = parse_rule_file(rule_yaml).expect("parse rule");
    let output = transform_input(&rule, InputData::Text(input), None).expect("transform");
    output
        .as_array()
        .and_then(|items| items.first())
        .cloned()
        .unwrap_or(output)
}

fn transform_err(rule_yaml: &str, input: &str) -> String {
    let rule = parse_rule_file(rule_yaml).expect("parse rule");
    let err = transform_input(&rule, InputData::Text(input), None).expect_err("transform error");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    err.message
}

#[test]
fn phase0_rejects_inline_codec_surface() {
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
          style: single_key_object
"#;
    let message = transform_err(yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("inline typed value codecs"));
}

#[test]
fn phase0_rejects_disabled_or_unknown_keys_inside_codec_binding() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    profile: dynamodb_item
    types:
      string:
        tag: S
mappings:
  - target: typed
    expr:
      - "@input"
      - to_typed_value:
          codec: bad
"#;
    let message = transform_err(yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("experimental"));

    let typo_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  bad:
    profile: dynamodb_item
    field_type:
      tags: string_set
mappings:
  - target: typed
    expr:
      - "@input"
      - to_typed_value:
          codec: bad
"#;
    let message = transform_err(typo_yaml, r#"{"tags":["a"]}"#);
    assert!(message.contains("unknown typed value option"));
}

#[test]
fn phase1_dynamodb_core_encodes_and_decodes_item() {
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
          on_missing: error
  - target: raw
    expr:
      - "@out.item"
      - from_typed_value:
          profile: dynamodb_item
"#;
    let output = transform_json(
        yaml,
        r#"{"id":"u1","age":31,"active":true,"tags":["a"],"meta":{"tier":"gold"},"none":null}"#,
    );
    assert_eq!(output["item"]["id"], serde_json::json!({"S":"u1"}));
    assert_eq!(output["item"]["age"], serde_json::json!({"N":"31"}));
    assert_eq!(output["item"]["active"], serde_json::json!({"BOOL":true}));
    assert_eq!(output["item"]["none"], serde_json::json!({"NULL":true}));
    assert_eq!(output["raw"]["age"], serde_json::json!("31"));
    assert_eq!(output["raw"]["meta"]["tier"], serde_json::json!("gold"));
}

#[test]
fn phase1_dynamodb_rejects_malformed_attribute_values() {
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
    let message = transform_err(yaml, r#"{"S":"x","N":"1"}"#);
    assert!(message.contains("exactly one tag"));
}

#[test]
fn phase1_dynamodb_decode_restores_collection_and_binary_shapes() {
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
          profile: dynamodb_item
"#;
    let output = transform_json(
        yaml,
        r#"{
          "blob": {"B":"AQID"},
          "strings": {"SS":["a","b"]},
          "numbers": {"NS":["1","2.5"]},
          "binaries": {"BS":["AQID","BAUG"]},
          "list": {"L":[{"S":"x"},{"N":"3"}]},
          "map": {"M":{"nested":{"BOOL":true}}}
        }"#,
    );
    assert_eq!(output["raw"]["blob"], serde_json::json!("AQID"));
    assert_eq!(output["raw"]["strings"], serde_json::json!(["a", "b"]));
    assert_eq!(output["raw"]["numbers"], serde_json::json!(["1", "2.5"]));
    assert_eq!(
        output["raw"]["binaries"],
        serde_json::json!(["AQID", "BAUG"])
    );
    assert_eq!(output["raw"]["list"], serde_json::json!(["x", "3"]));
    assert_eq!(output["raw"]["map"]["nested"], serde_json::json!(true));

    let root_shape_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input.payload"
      - from_typed_value:
          profile: dynamodb_item
"#;
    let root_non_object = transform_err(root_shape_yaml, r#"{"payload":[{"S":"x"}]}"#);
    assert!(root_non_object.contains("dynamodb_item decode requires object"));

    let unknown_tag = transform_err(yaml, r#"{"bad":{"X":"x"}}"#);
    assert!(unknown_tag.contains("unknown DynamoDB AttributeValue tag"));
}

#[test]
fn phase2_field_types_sets_binary_and_shape_roundtrip() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb_user:
    profile: dynamodb_item
    field_types:
      tags: string_set
      scores: number_string_set
      avatar: binary_base64
      age: number_string
    decode:
      mode: json_shape_roundtrip
mappings:
  - target: item
    expr:
      - "@input"
      - to_typed_value:
          codec: ddb_user
  - target: raw
    expr:
      - "@out.item"
      - from_typed_value:
          codec: ddb_user
"#;
    let output = transform_json(
        yaml,
        r#"{"tags":["a","b"],"scores":["1","2"],"avatar":"AQID","age":"31"}"#,
    );
    assert_eq!(output["item"]["tags"], serde_json::json!({"SS":["a","b"]}));
    assert_eq!(
        output["item"]["scores"],
        serde_json::json!({"NS":["1","2"]})
    );
    assert_eq!(output["item"]["avatar"], serde_json::json!({"B":"AQID"}));
    assert_eq!(output["raw"]["age"], serde_json::json!(31));
}

#[test]
fn phase2_decode_root_field_type_hint_is_honored() {
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
          field_types:
            ".": number_string
          decode:
            mode: json_shape_roundtrip
"#;
    let output = transform_json(yaml, r#"{"N":"31"}"#);
    assert_eq!(output["raw"], serde_json::json!(31));
}

#[test]
fn phase2_rejects_unsafe_number_parse_without_string_fallback() {
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
          number_policy: parse_json_number_if_safe
"#;
    let message = transform_err(yaml, r#"{"N":"12345678901234567890123456789012345678"}"#);
    assert!(message.contains("safely") || message.contains("exact"));
}

#[test]
fn phase2_rejects_empty_and_duplicate_sets() {
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
            tags: string_set
"#;
    let empty = transform_err(yaml, r#"{"tags":[]}"#);
    assert!(empty.contains("must not be empty"));
    let duplicate = transform_err(yaml, r#"{"tags":["a","a"]}"#);
    assert!(duplicate.contains("duplicate"));

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
            scores: number_set
"#;
    let numeric_duplicate = transform_err(yaml, r#"{"scores":["1","1.0","10e-1"]}"#);
    assert!(numeric_duplicate.contains("duplicate"));
}

#[test]
fn phase2_field_types_default_to_required_paths() {
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
            tags: string_set
"#;
    let message = transform_err(yaml, r#"{"id":"u1"}"#);
    assert!(message.contains("field type path is missing"));
}

#[test]
fn phase2_decode_field_types_default_to_required_paths() {
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
          profile: dynamodb_item
          field_types:
            tags: string_set
"#;
    let message = transform_err(yaml, r#"{"id":{"S":"u1"}}"#);
    assert!(message.contains("field type path is missing"));

    let optional_yaml = r#"
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
          field_types:
            tags:
              type: string_set
              on_missing: ignore
"#;
    let output = transform_json(optional_yaml, r#"{"id":{"S":"u1"}}"#);
    assert_eq!(output["raw"]["id"], serde_json::json!("u1"));
}

#[test]
fn phase3_wildcard_hints_apply_to_array_items() {
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
            events[*].created_at: number_string
"#;
    let output = transform_json(
        yaml,
        r#"{"events":[{"created_at":"1"},{"created_at":"2"}]}"#,
    );
    assert_eq!(
        output["item"]["events"]["L"][0]["M"]["created_at"],
        serde_json::json!({"N":"1"})
    );

    let message = transform_err(yaml, r#"{"events":[{"created_at":"1"},{}]}"#);
    assert!(message.contains("field type path is missing"));
}

#[test]
fn phase4_firestore_profiles_validate_oneof_and_arrays() {
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
          profile: firestore_document
          field_types:
            age:
              type: integer
              on_missing: ignore
"#;
    let output = transform_json(yaml, r#"{"name":"Ada","age":31,"nested":{"items":[1]}}"#);
    assert_eq!(
        output["doc"]["fields"]["age"],
        serde_json::json!({"integerValue":"31"})
    );

    let nested_array = transform_err(yaml, r#"{"bad":[[1]]}"#);
    assert!(nested_array.contains("direct nested array"));

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
          profile: firestore_value
"#;
    let malformed = transform_err(decode_yaml, r#"{"stringValue":"x","integerValue":"1"}"#);
    assert!(malformed.contains("exactly one"));

    let malformed_array = transform_err(decode_yaml, r#"{"arrayValue":{"values":"oops"}}"#);
    assert!(malformed_array.contains("values must be array"));
}

#[test]
fn phase4_firestore_fields_profile_encodes_and_decodes_fields_map() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: fields
    expr:
      - "@input"
      - to_typed_value:
          profile: firestore_fields
          field_types:
            age: integer
            avatar: bytes_base64
  - target: raw_again
    expr:
      - "@out.fields"
      - from_typed_value:
          profile: firestore_fields
          field_types:
            age: integer
          decode:
            mode: json_shape_roundtrip
"#;
    let output = transform_json(yaml, r#"{"name":"Ada","age":31,"avatar":"AQID"}"#);
    assert_eq!(
        output["fields"]["name"],
        serde_json::json!({"stringValue":"Ada"})
    );
    assert_eq!(
        output["fields"]["age"],
        serde_json::json!({"integerValue":"31"})
    );
    assert_eq!(
        output["fields"]["avatar"],
        serde_json::json!({"bytesValue":"AQID"})
    );
    assert_eq!(
        output["raw_again"],
        serde_json::json!({"name":"Ada","age":31,"avatar":"AQID"})
    );

    let document_decode_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          profile: firestore_document
"#;
    let output = transform_json(
        document_decode_yaml,
        r#"{"fields":{"name":{"stringValue":"Ada"},"tags":{"arrayValue":{"values":[{"stringValue":"admin"}]}}}}"#,
    );
    assert_eq!(output["raw"]["name"], serde_json::json!("Ada"));
    assert_eq!(output["raw"]["tags"], serde_json::json!(["admin"]));
}

#[test]
fn phase4_firestore_root_type_and_bytes_size_use_provider_semantics() {
    let timestamp_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: ts
    expr:
      - "@input.created_at"
      - to_typed_value:
          profile: firestore_value
          type: timestamp
"#;
    let output = transform_json(timestamp_yaml, r#"{"created_at":"2026-06-03T00:00:00Z"}"#);
    assert_eq!(
        output["ts"],
        serde_json::json!({"timestampValue":"2026-06-03T00:00:00.000000Z"})
    );

    let bytes_yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: bytes
    expr:
      - "@input.blob"
      - to_typed_value:
          profile: firestore_value
          type: bytes_base64
"#;
    let long_but_small = "AAAA".repeat(300_000);
    let input = serde_json::json!({ "blob": long_but_small }).to_string();
    let output = transform_json(bytes_yaml, &input);
    assert!(output["bytes"]["bytesValue"].as_str().is_some());
}

#[test]
fn phase5_mongodb_extended_json_policy_is_fail_closed() {
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
            _id:
              type: object_id
              on_missing: ignore
            created_at:
              type: date
              input: rfc3339
              on_missing: ignore
            avatar:
              type: binary_base64
              subtype: "00"
              on_missing: ignore
"#;
    let output = transform_json(
        yaml,
        r#"{"_id":"0123456789abcdef01234567","created_at":"2026-06-03T00:00:00Z","avatar":"AQID"}"#,
    );
    assert_eq!(
        output["doc"]["_id"],
        serde_json::json!({"$oid":"0123456789abcdef01234567"})
    );
    assert_eq!(
        output["doc"]["avatar"],
        serde_json::json!({"$binary":{"base64":"AQID","subType":"00"}})
    );

    let wrapper = transform_err(yaml, r#"{"raw":{"$oid":"0123456789abcdef01234567"}}"#);
    assert!(wrapper.contains("wrapper-shaped"));

    let mixed_wrapper = transform_err(yaml, r#"{"raw":{"$date":"2026-06-03T00:00:00Z","x":1}}"#);
    assert!(mixed_wrapper.contains("wrapper-shaped"));

    let invalid_subtype_encode_yaml = r#"
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
            avatar:
              type: binary_base64
              subtype: "000"
"#;
    let message = transform_err(invalid_subtype_encode_yaml, r#"{"avatar":"AQID"}"#);
    assert!(message.contains("binary subtype must be one or two hex characters"));

    let invalid_subtype_decode_yaml = r#"
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
    let message = transform_err(
        invalid_subtype_decode_yaml,
        r#"{"$binary":{"base64":"AQID","subType":"000"}}"#,
    );
    assert!(message.contains("binary subtype must be one or two hex characters"));
}

#[test]
fn phase5_mongodb_root_type_is_honored() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: oid
    expr:
      - "@input.id"
      - to_typed_value:
          profile: mongo_extended_json
          type: object_id
"#;
    let output = transform_json(yaml, r#"{"id":"0123456789abcdef01234567"}"#);
    assert_eq!(
        output["oid"],
        serde_json::json!({"$oid":"0123456789abcdef01234567"})
    );
}

#[test]
fn phase5_mongodb_decode_dollar_policy_does_not_override_known_wrapper() {
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
          allow_dollar_prefixed_fields: true
"#;
    let malformed = transform_err(yaml, r#"{"$date":"2026-06-03T00:00:00Z","x":1}"#);
    assert!(malformed.contains("malformed"));

    let output = transform_json(yaml, r#"{"$unknown":1}"#);
    assert_eq!(output["raw"]["$unknown"], serde_json::json!(1));
}

#[test]
fn phase5_mongodb_extended_json_passthrough_is_encode_only_escape_hatch() {
    let passthrough_yaml = r#"
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
          allow_extended_json_passthrough: true
"#;
    let output = transform_json(
        passthrough_yaml,
        r#"{"raw":{"$oid":"0123456789abcdef01234567"}}"#,
    );
    assert_eq!(
        output["doc"]["raw"],
        serde_json::json!({"$oid":"0123456789abcdef01234567"})
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
          allow_extended_json_passthrough: true
"#;
    let output = transform_json(decode_yaml, r#"{"$oid":"0123456789abcdef01234567"}"#);
    assert_eq!(output["raw"], serde_json::json!("0123456789abcdef01234567"));
}

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

#[test]
fn typed_value_decode_rejects_malformed_dynamodb_sets() {
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
          profile: dynamodb_item
"#;
    let empty = transform_err(yaml, r#"{"tags":{"SS":[]}}"#);
    assert!(empty.contains("set must not be empty"));

    let duplicate = transform_err(yaml, r#"{"tags":{"SS":["a","a"]}}"#);
    assert!(duplicate.contains("duplicate"));

    let numeric_duplicate = transform_err(yaml, r#"{"scores":{"NS":["1","1.0","10e-1"]}}"#);
    assert!(numeric_duplicate.contains("duplicate"));
}

#[test]
fn typed_value_firestore_decode_rejects_direct_nested_arrays() {
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
    let message = transform_err(
        yaml,
        r#"{"arrayValue":{"values":[{"arrayValue":{"values":[]}}]}}"#,
    );
    assert!(message.contains("direct nested array"));
}

#[test]
fn typed_value_firestore_decode_rejects_map_value_unknown_fields() {
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
    let message = transform_err(yaml, r#"{"mapValue":{"fields":{},"extra":1}}"#);
    assert!(message.contains("mapValue contains unknown field"));

    let message = transform_err(yaml, r#"{"mapValue":{"extra":1}}"#);
    assert!(message.contains("mapValue contains unknown field"));
}

#[test]
fn typed_value_decode_rejects_invalid_provider_number_strings() {
    let dynamodb_yaml = r#"
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
    let message = transform_err(dynamodb_yaml, r#"{"N":"abc"}"#);
    assert!(message.contains("invalid DynamoDB number"));

    let firestore_yaml = r#"
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
    let message = transform_err(firestore_yaml, r#"{"integerValue":"abc"}"#);
    assert!(message.contains("integerValue requires int64 string"));

    let message = transform_err(firestore_yaml, r#"{"doubleValue":"abc"}"#);
    assert!(message.contains("doubleValue requires finite number string"));
}

#[test]
fn typed_value_mongodb_numeric_wrappers_are_strict() {
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
    let message = transform_err(decode_yaml, r#"{"$numberInt":"abc"}"#);
    assert!(message.contains("$numberInt requires int32 string"));

    let message = transform_err(decode_yaml, r#"{"$numberLong":"9223372036854775808"}"#);
    assert!(message.contains("$numberLong requires int64 string"));

    let special_double = transform_json(decode_yaml, r#"{"$numberDouble":"NaN"}"#);
    assert_eq!(special_double["raw"], serde_json::json!("NaN"));

    let special_decimal = transform_json(decode_yaml, r#"{"$numberDecimal":"Infinity"}"#);
    assert_eq!(special_decimal["raw"], serde_json::json!("Infinity"));

    let encode_yaml = r#"
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
            count: int32
"#;
    let message = transform_err(encode_yaml, r#"{"count":"abc"}"#);
    assert!(message.contains("$numberInt requires int32 string"));

    let double_encode_yaml = r#"
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
            score: double
"#;
    let output = transform_json(double_encode_yaml, r#"{"score":"Infinity"}"#);
    assert_eq!(
        output["doc"]["score"],
        serde_json::json!({"$numberDouble":"Infinity"})
    );

    let decimal_decode = transform_err(decode_yaml, r#"{"$numberDecimal":"not-a-decimal"}"#);
    assert!(decimal_decode.contains("$numberDecimal requires decimal string"));

    let decimal_encode_yaml = r#"
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
    let decimal_encode = transform_err(decimal_encode_yaml, r#"{"amount":"not-a-decimal"}"#);
    assert!(decimal_encode.contains("$numberDecimal requires decimal string"));

    let decimal_encode = transform_err(decimal_encode_yaml, r#"{"amount":"0e6145"}"#);
    assert!(decimal_encode.contains("$numberDecimal exponent is out of range"));

    let special_decimal_encode = transform_json(decimal_encode_yaml, r#"{"amount":"-Infinity"}"#);
    assert_eq!(
        special_decimal_encode["doc"]["amount"],
        serde_json::json!({"$numberDecimal":"-Infinity"})
    );
}

#[test]
fn typed_value_provider_constraints_are_strict() {
    let ddb_encode_yaml = r#"
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
            n: number_string
"#;
    let message = transform_err(ddb_encode_yaml, r#"{"n":"1e126"}"#);
    assert!(message.contains("exponent range"));

    let message = transform_err(ddb_encode_yaml, r#"{"n":"0e126"}"#);
    assert!(message.contains("exponent range"));

    let ddb_decode_yaml = r#"
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
    let message = transform_err(ddb_decode_yaml, r#"{"":{"S":"x"}}"#);
    assert!(message.contains("attribute name"));

    let firestore_yaml = r#"
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
    let message = transform_err(firestore_yaml, r#"{"geoPointValue":{"latitude":999}}"#);
    assert!(message.contains("geoPointValue"));

    let message = transform_err(firestore_yaml, r#"{"timestampValue":"not-a-date"}"#);
    assert!(message.contains("timestamp must be RFC3339"));

    let mongo_yaml = r#"
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
          mode: canonical
          field_types:
            created_at: date
"#;
    let output = transform_json(mongo_yaml, r#"{"created_at":"2026-06-03T00:00:00Z"}"#);
    assert_eq!(
        output["doc"]["created_at"],
        serde_json::json!({"$date":{"$numberLong":"1780444800000"}})
    );

    let mongo_decode_yaml = r#"
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
    let message = transform_err(mongo_decode_yaml, r#"{"$date":"not-a-date"}"#);
    assert!(message.contains("date must be RFC3339"));
}

#[test]
fn typed_value_dynamodb_number_set_zero_huge_exponent_is_error_not_panic() {
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
            scores: number_set
"#;
    let message = transform_err(encode_yaml, r#"{"scores":["0e126"]}"#);
    assert!(message.contains("DynamoDB number is outside supported exponent range"));

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
    let message = transform_err(decode_yaml, r#"{"scores":{"NS":["0e126"]}}"#);
    assert!(message.contains("DynamoDB number is outside supported exponent range"));
}

#[test]
fn typed_value_decode_shape_roundtrip_uses_root_and_mongo_field_hints() {
    let firestore_yaml = r#"
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
          type: integer
          decode:
            mode: json_shape_roundtrip
"#;
    let output = transform_json(firestore_yaml, r#"{"integerValue":"31"}"#);
    assert_eq!(output["raw"], serde_json::json!(31));

    let mongo_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo_doc:
    profile: mongo_extended_json
    field_types:
      count: int32
    decode:
      mode: json_shape_roundtrip
mappings:
  - target: doc
    expr:
      - "@input"
      - to_typed_value:
          codec: mongo_doc
  - target: raw_again
    expr:
      - "@out.doc"
      - from_typed_value:
          codec: mongo_doc
"#;
    let output = transform_json(mongo_yaml, r#"{"count":31}"#);
    assert_eq!(
        output["doc"]["count"],
        serde_json::json!({"$numberInt":"31"})
    );
    assert_eq!(output["raw_again"]["count"], serde_json::json!(31));
}

#[test]
fn typed_value_roundtrip_relation_requires_shared_provider_intent() {
    let no_hint_yaml = r#"
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
  - target: encoded_again
    expr:
      - "@out.raw"
      - to_typed_value:
          profile: dynamodb_item
"#;
    let no_hint_output = transform_json(
        no_hint_yaml,
        r#"{"age":{"N":"31"},"tags":{"SS":["admin","paid"]},"avatar":{"B":"AQID"}}"#,
    );
    assert_eq!(no_hint_output["raw"]["age"], serde_json::json!("31"));
    assert_ne!(
        no_hint_output["encoded_again"]["age"],
        serde_json::json!({"N":"31"})
    );
    assert_ne!(
        no_hint_output["encoded_again"]["tags"],
        serde_json::json!({"SS":["admin","paid"]})
    );
    assert_ne!(
        no_hint_output["encoded_again"]["avatar"],
        serde_json::json!({"B":"AQID"})
    );

    let hinted_yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb_user:
    profile: dynamodb_item
    field_types:
      age: number_string
      tags: string_set
      avatar: binary_base64
mappings:
  - target: raw
    expr:
      - "@input"
      - from_typed_value:
          codec: ddb_user
  - target: encoded_again
    expr:
      - "@out.raw"
      - to_typed_value:
          codec: ddb_user
"#;
    let hinted_output = transform_json(
        hinted_yaml,
        r#"{"age":{"N":"31"},"tags":{"SS":["admin","paid"]},"avatar":{"B":"AQID"}}"#,
    );
    assert_eq!(
        hinted_output["encoded_again"],
        serde_json::json!({
            "age": {"N":"31"},
            "tags": {"SS":["admin","paid"]},
            "avatar": {"B":"AQID"}
        })
    );
}

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
    let mut options = rulemorph::NormalizationOptions::default();
    options.max_text_bytes = 16 * 1024 * 1024;
    options.max_input_bytes = 32 * 1024 * 1024;

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
