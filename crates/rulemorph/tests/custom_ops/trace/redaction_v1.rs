use super::*;

#[test]
fn custom_op_trace_redacted_mode_hides_secret_v1_ref_body_output() {
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
        expr: { ref: input.value }
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
fn custom_op_trace_redacted_mode_hides_secret_bracket_root_v1_ref() {
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
        expr: { ref: 'input.["value"]' }
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
fn custom_op_trace_redacted_mode_hides_secret_array_root_v1_ref() {
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
        expr: { ref: 'input.[0]' }
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
fn custom_op_trace_redacted_mode_hides_secret_v1_item_ref_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { values: [string] }
    mappings:
      - target: public_ids
        expr:
          op: map
          args:
            - { ref: input.values }
            - { ref: item.value }
mappings:
  - target: result
    expr: ["@input.api_token_holder", expose]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token_holder":{"values":["secret-token"]}}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "result": { "public_ids": ["secret-token"] } }])
    );
    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("secret-token"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_redacted_mode_hides_secret_v1_acc_ref_body_output() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { values: [string] }
    mappings:
      - target: public_id
        expr:
          op: reduce
          args:
            - { ref: input.values }
            - { ref: acc.value }
mappings:
  - target: result
    expr: ["@input.api_token_holder", expose]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token_holder":{"values":["secret-token","ignored"]}}]"#),
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
