use super::*;

#[test]
fn custom_op_trace_redacted_mode_hides_secret_mappings_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { secret: string }
    mappings:
      - target: public_id
        source: secret
mappings:
  - target: result
    expr: ["@input", expose]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"secret":"secret-token"}]"#),
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
}

#[test]
fn custom_op_trace_redacted_mode_hides_secret_bracket_root_source() {
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
        source: '["value"]'
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
fn custom_op_trace_redacted_mode_hides_secret_array_root_source() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: [string]
    mappings:
      - target: public_id
        source: '[0]'
mappings:
  - target: result
    expr: ["@input.values", expose]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"values":["secret-token"]}]"#),
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
fn custom_op_trace_redacted_mode_hides_secret_body_out_refs() {
    for expr in [r#""@out.tmp""#, r#"{ ref: out.tmp }"#] {
        let yaml = format!(
            r#"
version: 2
input:
  format: json
  json: {{}}
defs:
  expose:
    input: {{ api_token: string }}
    mappings:
      - target: tmp
        source: api_token
      - target: public_id
        expr: {expr}
mappings:
  - target: result
    expr: ["@input", expose]
"#
        );
        let rule = parse(&yaml);
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(r#"[{"api_token":"secret-token"}]"#),
            None,
            &TransformTraceOptions::redacted(),
        )
        .expect("trace succeeds");

        assert_eq!(
            traced.output,
            json!([{ "result": { "tmp": "secret-token", "public_id": "secret-token" } }])
        );
        let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
        assert!(!trace_text.contains("secret-token"));
        assert!(!traced.trace.contains_raw_values);
    }
}

#[test]
fn custom_op_trace_redacted_mode_hides_secret_body_out_source() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { out: string, value: string }
    mappings:
      - target: tmp
        source: value
      - target: public_id
        source: out.tmp
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { out: "@input.public", value: "@input.api_token" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"public":"not-sensitive","api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "tmp": "secret-token", "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_secret_root_expr_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: string
    mappings:
      - target: public_id
        expr: "$"
mappings:
  - target: result
    expr: ["@input.api_token", expose]
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
fn custom_op_trace_redacted_mode_hides_secret_get_expr_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { api_token: string }
    mappings:
      - target: public_id
        expr: ["$", { get: ["api_token"] }]
mappings:
  - target: result
    expr: ["@input", expose]
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
fn custom_op_trace_redacted_mode_hides_secret_value_literal_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: {}
    mappings:
      - target: public_id
        value: "secret-token"
mappings:
  - target: result
    expr: ["@input", expose]
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
        json!([{ "result": { "public_id": "secret-token" } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_secret_default_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { public: json }
    mappings:
      - target: public_id
        source: input.public.missing
        default: "secret-token"
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { public: "@input.public" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"public":{}}]"#),
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
