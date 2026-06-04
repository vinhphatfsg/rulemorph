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
fn phase5_mongodb_passthrough_validates_wrapper_shape() {
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
          allow_extended_json_passthrough: true
"#;
    let message = transform_err(yaml, r#"{"raw":{"$oid":1}}"#);
    assert!(message.contains("$oid must be string"));

    let message = transform_err(
        yaml,
        r#"{"raw":{"$binary":{"base64":"AQ==","subType":"zz"}}}"#,
    );
    assert!(message.contains("binary subtype"));
}

