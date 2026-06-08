use rulemorph::{
    InputData, NormalizationOptions, TraceAttributeValue, TraceEventKind, TransformErrorKind,
    TransformTraceOptions, parse_rule_file, transform_input, transform_input_with_options,
    transform_input_with_trace, validate_rule_file,
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

fn transform_err(rule_yaml: &str, input: &str) -> rulemorph::TransformError {
    let rule = parse_rule_file(rule_yaml).expect("parse rule");
    transform_input(&rule, InputData::Text(input), None).expect_err("transform error")
}

#[test]
fn object_builder_builds_nested_missing_null_literal_arrays_and_dynamodb_item() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input"
      - object:
          name: ["$.name", uppercase]
          age: ["$.age", int]
          tags:
            value: ["new", "vip"]
          profile:
            - object:
                label: ["$.name", lowercase]
          missing: "$.missing"
          nil: null
      - to_typed_value: dynamodb_item
"#;

    let output = transform_json(yaml, r#"{"name":"Alice","age":"31"}"#);
    assert_eq!(output["value"]["name"], serde_json::json!({"S":"ALICE"}));
    assert_eq!(output["value"]["age"], serde_json::json!({"N":"31"}));
    assert_eq!(
        output["value"]["tags"],
        serde_json::json!({"L":[{"S":"new"},{"S":"vip"}]})
    );
    assert_eq!(
        output["value"]["profile"],
        serde_json::json!({"M":{"label":{"S":"alice"}}})
    );
    assert_eq!(output["value"]["nil"], serde_json::json!({"NULL":true}));
    assert!(output["value"].get("missing").is_none());
}

#[test]
fn object_builder_validates_static_shape_and_field_exprs() {
    let valid = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          id: "@input.id"
"#;
    let rule = parse_rule_file(valid).expect("parse valid rule");
    validate_rule_file(&rule).expect("valid object rule");

    let invalid_args = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - op: object
        args: []
"#;
    let rule = parse_rule_file(invalid_args).expect("parse invalid rule");
    let errors = validate_rule_file(&rule).expect_err("object args should fail");
    assert!(format!("{errors:?}").contains("InvalidExprShape"));

    let invalid_field_expr = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          id: "@bad-ref"
"#;
    let rule = parse_rule_file(invalid_field_expr).expect("parse invalid rule");
    let errors = validate_rule_file(&rule).expect_err("field expr should fail");
    assert!(format!("{errors:?}").contains("InvalidExprShape"));
}

#[test]
fn object_builder_rejects_empty_keys_and_configured_resource_limits() {
    let empty_key = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          "": "@input.id"
"#;
    let err = transform_err(empty_key, r#"{"id":"u1"}"#);
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object field key must not be empty"));

    let field_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          a: 1
          b: 2
"#;
    let rule = parse_rule_file(field_limit).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_object_fields = 1;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("field limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object field count"));

    let key_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          long_key: 1
"#;
    let rule = parse_rule_file(key_limit).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_object_key_bytes = 4;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("key limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object key bytes"));

    let generated_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          a: 1
          b: 2
"#;
    let rule = parse_rule_file(generated_limit).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_generated_json_nodes = 2;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("node limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("generated JSON node count"));

    let depth_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          nested:
            value:
              inner:
                leaf: 1
"#;
    let rule = parse_rule_file(depth_limit).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_object_depth = 1;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("depth limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object depth"));

    let byte_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          payload: "abcdef"
"#;
    let rule = parse_rule_file(byte_limit).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_generated_json_bytes = 8;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("byte limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("generated JSON bytes"));
}

#[test]
fn object_builder_static_validation_does_not_freeze_configurable_key_limit() {
    let long_key = "k".repeat(NormalizationOptions::default().max_object_key_bytes + 1);
    let yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: value
    expr:
      - object:
          ? "{}"
          : 1
"#,
        long_key
    );
    let rule = parse_rule_file(&yaml).expect("parse rule with long static key");

    validate_rule_file(&rule).expect("static validation should not hard-code runtime key limit");

    let mut options = NormalizationOptions::default();
    options.max_object_key_bytes = long_key.len();
    transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect("raised runtime key limit should allow long static key");

    let mut options = NormalizationOptions::default();
    options.max_object_key_bytes = 4;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("runtime key limit should remain fail-closed");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object key bytes"));
}

#[test]
fn object_builder_rejects_nested_generated_object_limits() {
    let nested_key_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          payload:
            value:
              too_long: 1
"#;
    let rule = parse_rule_file(nested_key_limit).expect("parse nested key rule");
    let mut options = NormalizationOptions::default();
    options.max_object_key_bytes = 4;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("nested generated key should respect key limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object key bytes"));

    let nested_field_limit = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          payload:
            value:
              a: 1
              b: 2
"#;
    let rule = parse_rule_file(nested_field_limit).expect("parse nested field rule");
    let mut options = NormalizationOptions::default();
    options.max_object_fields = 1;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("nested generated object should respect field limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("object field count"));
}

#[test]
fn object_builder_rejects_nested_generated_array_items() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          payload:
            value: [1, 2]
"#;
    let rule = parse_rule_file(yaml).expect("parse nested array rule");
    let mut options = NormalizationOptions::default();
    options.max_array_len = 1;
    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("nested generated array should respect array item limit");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("generated array items"));
}

#[test]
fn object_builder_map_accumulates_generated_json_budget() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - "@input.items"
      - map:
        - object:
            payload: "@item"
"#;
    let rule = parse_rule_file(yaml).expect("parse map object rule");
    let mut options = NormalizationOptions::default();
    options.max_generated_json_bytes = 30;
    let err = transform_input_with_options(
        &rule,
        InputData::Text(r#"{"items":["abcdef","abcdef"]}"#),
        None,
        &options,
    )
    .expect_err("map should enforce generated JSON bytes across the full output array");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("generated JSON bytes"));
}

#[test]
fn object_builder_trace_records_fields_and_sanitizes_field_key_metadata() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          "api_token": "@input.api_token"
          "line\nbreak": "@input.name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"{"api_token":"secret-token","name":"Alice"}"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    let field_events = traced
        .trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .filter(|event| {
            event.kind == TraceEventKind::ArgEval && event.operator.as_deref() == Some("object")
        })
        .collect::<Vec<_>>();
    assert_eq!(field_events.len(), 2);

    let secret_event = field_events
        .iter()
        .find(|event| event.attributes.contains_key("field_key_redaction_reason"))
        .expect("secret-like key is redacted");
    assert!(matches!(
        secret_event.attributes.get("field_key_redaction_reason"),
        Some(TraceAttributeValue::String(value)) if value == "secret_like_path"
    ));
    assert!(!secret_event.attributes.contains_key("field_key"));
    assert!(
        !field_events.iter().any(|event| event
            .rule_path
            .as_deref()
            .is_some_and(|path| path.contains("api_token"))),
        "secret-like field key must not appear in redacted trace rule_path"
    );

    let escaped_event = field_events
        .iter()
        .find(|event| match event.attributes.get("field_key") {
            Some(TraceAttributeValue::String(value)) => value.contains("\\n"),
            _ => false,
        })
        .expect("control characters are escaped in field_key attr");
    assert!(matches!(
        escaped_event.attributes.get("field_key"),
        Some(TraceAttributeValue::String(value)) if value == "line\\nbreak"
    ));
}

#[test]
fn object_builder_checks_generated_bytes_before_evaluating_later_fields() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - object:
          payload: "abcdef"
          should_not_eval: ["not-a-number", int]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = NormalizationOptions::default();
    options.max_generated_json_bytes = 8;

    let err = transform_input_with_options(&rule, InputData::Text("{}"), None, &options)
        .expect_err("byte limit should fail before later field evaluation");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message.contains("generated JSON bytes"),
        "unexpected error: {err:?}"
    );
}
