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

    let empty_output = transform_json(
        document_decode_yaml,
        r#"{"name":"projects/p/databases/d/documents/users/u1","createTime":"2026-06-03T00:00:00Z"}"#,
    );
    assert_eq!(empty_output["raw"], serde_json::json!({}));
}

#[test]
fn phase4_firestore_special_doubles_survive_parse_number_policy() {
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
          number_policy: parse_json_number_if_safe
"#;
    for raw in ["NaN", "Infinity", "-Infinity"] {
        let input = serde_json::json!({"doubleValue": raw}).to_string();
        let output = transform_json(yaml, &input);
        assert_eq!(output["raw"], serde_json::json!(raw));
    }
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
