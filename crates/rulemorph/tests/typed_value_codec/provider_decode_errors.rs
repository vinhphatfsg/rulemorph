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
fn typed_value_dynamodb_decode_rejects_tags_that_contradict_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      tags: string_set
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: ddb
"#;
    let message = transform_err(yaml, r#"{"tags":{"L":[{"S":"a"},{"S":"b"}]}}"#);
    assert!(message.contains("does not match field type"));
}

#[test]
fn typed_value_dynamodb_decode_allows_null_for_nullable_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  ddb:
    profile: dynamodb_item
    field_types:
      tags:
        type: string_set
        nullable: true
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: ddb
"#;
    let output = transform_json(yaml, r#"{"tags":{"NULL":true}}"#);
    assert_eq!(output["decoded"]["tags"], serde_json::Value::Null);
}

#[test]
fn typed_value_firestore_decode_rejects_tags_that_contradict_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  fs:
    profile: firestore_document
    field_types:
      age: integer
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: fs
"#;
    let message = transform_err(yaml, r#"{"fields":{"age":{"stringValue":"31"}}}"#);
    assert!(message.contains("does not match field type"));
}

#[test]
fn typed_value_firestore_decode_rejects_null_value_for_non_nullable_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  fs:
    profile: firestore_document
    field_types:
      age: integer
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: fs
"#;
    let message = transform_err(yaml, r#"{"fields":{"age":{"nullValue":null}}}"#);
    assert!(message.contains("does not match field type"));
}

#[test]
fn typed_value_firestore_decode_allows_null_value_for_nullable_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  fs:
    profile: firestore_document
    field_types:
      age:
        type: integer
        nullable: true
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: fs
"#;
    let output = transform_json(yaml, r#"{"fields":{"age":{"nullValue":null}}}"#);
    assert_eq!(output["decoded"]["age"], serde_json::Value::Null);
}

#[test]
fn typed_value_mongodb_decode_rejects_wrappers_that_contradict_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo:
    profile: mongo_extended_json
    field_types:
      count: int32
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: mongo
"#;
    let message = transform_err(yaml, r#"{"count":{"$numberLong":"31"}}"#);
    assert!(message.contains("does not match field type"));
}

#[test]
fn typed_value_mongodb_decode_rejects_null_for_non_nullable_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo:
    profile: mongo_extended_json
    field_types:
      _id: object_id
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: mongo
"#;
    let message = transform_err(yaml, r#"{"_id":null}"#);
    assert!(message.contains("does not match field type"));
}

#[test]
fn typed_value_mongodb_decode_allows_null_for_nullable_field_types() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
codecs:
  mongo:
    profile: mongo_extended_json
    field_types:
      _id:
        type: object_id
        nullable: true
mappings:
  - target: decoded
    expr:
      - "@input"
      - from_typed_value:
          codec: mongo
"#;
    let output = transform_json(yaml, r#"{"_id":null}"#);
    assert_eq!(output["decoded"]["_id"], serde_json::Value::Null);
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
