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

