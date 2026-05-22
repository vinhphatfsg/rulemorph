#[test]
fn redacted_mode_hides_secret_like_paths() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "token"
    source: "api_token"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn redacted_mode_hides_secret_source_written_to_public_target() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "public_id"
    source: "api_token"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn redacted_mode_hides_secret_expr_ref_written_to_public_target() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "public_id"
    expr:
      - "@input.api_token"
      - trim
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":" secret-token "}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn metadata_only_mode_never_serializes_raw_values() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "alice");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["alice"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("metadata_only"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn transform_trace_error_debug_display_and_error_are_raw_safe() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "token"
    source: "api_token"
  - target: "bad"
    expr:
      - "@input.age"
      - divide: 0
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token","age":10}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced transform should fail");

    let debug = format!("{err:?}");
    let display = format!("{err}");
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("api_token"));
    assert!(debug.contains("expr_error"));
    assert!(!display.contains("secret-token"));
    assert!(!display.contains("api_token"));
    assert!(display.contains("expr_error"));
    let source = std::error::Error::source(&err);
    assert!(
        source.is_none(),
        "TransformTraceError must not expose raw TransformError as source"
    );
    assert!(err.trace.contains_raw_values);
}
