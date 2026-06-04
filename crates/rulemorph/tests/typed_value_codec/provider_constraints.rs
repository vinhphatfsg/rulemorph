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
fn typed_value_mongodb_rejects_null_bytes_in_field_names() {
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
          profile: mongo_extended_json
"#;
    let message = transform_err(yaml, r#"{"bad\u0000key":1}"#);
    assert!(message.contains("MongoDB field name"));
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
