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

