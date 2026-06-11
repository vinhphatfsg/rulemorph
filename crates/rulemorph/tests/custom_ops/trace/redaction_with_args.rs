use super::*;

#[test]
fn custom_op_trace_redacted_mode_preserves_with_arg_provenance() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { value: "@input.api_token" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_nested_custom_with_arg_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  secretize:
    input: string
    returns: string
    expr: ["secret-token", trim]
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { value: ["@input.username", secretize] }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"username":"alice"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_nested_body_with_arg_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  inner:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
  outer:
    input: { value: string }
    returns: { public_id: string }
    expr:
      - "@input"
      - inner:
          - with: { value: "@input.value" }
mappings:
  - target: result
    expr:
      - "@input"
      - outer:
          - with: { value: "@input.api_token" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_body_collection_item_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { values: [string] }
    returns: [string]
    expr:
      - "@input.values"
      - map: ["@item.value"]
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { values: "@input.api_tokens" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_tokens":["secret-token"]}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!([{ "result": ["secret-token"] }]));
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_multi_path_body_with_arg_out_provenance() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: tmp
    source: api_token
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { value: ["@input.public", "@out.tmp"] }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token","public":"ok"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "tmp": "secret-token", "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_shorthand_literal_with_arg_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { value: "sk_live_abc123" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"value":"ok"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "sk_live_abc123" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("sk_live_abc123"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_direct_body_local_input_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: result
    expr: ["@input.api_token_holder", expose]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token_holder":{"value":"secret-token"}}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_with_pipe_body_local_input_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: public_id
        source: value
mappings:
  - target: result
    expr:
      - "@input.api_token_holder"
      - expose:
          - with: { value: "$.value" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token_holder":{"value":"secret-token"}}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}
