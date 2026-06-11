use super::*;

#[test]
fn custom_op_with_adapter_accepts_json_input_contract() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  echo:
    input: json
    returns: json
    expr: "$"
mappings:
  - target: value
    expr:
      - "@input"
      - echo:
          - with: { a: "@input.a" }
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("json input accepts with adapter object");
    let output =
        transform(&rule, r#"[{"a":1}]"#, None).expect("json input with adapter transforms");

    assert_eq!(output, json!([{ "value": { "a": 1 } }]));
}

#[test]
fn custom_op_mappings_body_synthesizes_contract_for_escaped_targets() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  make_obj:
    input: string
    mappings:
      - target: '["a.b"]'
        expr: "$"
        required: true
mappings:
  - target: result
    expr: ["@input.value", make_obj]
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("escaped target contract validates");
    let output =
        transform(&rule, r#"[{"value":"x"}]"#, None).expect("escaped target contract transforms");

    assert_eq!(output, json!([{ "result": { "a.b": "x" } }]));
}

#[test]
fn custom_op_with_adapter_omits_missing_optional_fields() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  decorate:
    input: { label: string, note?: string }
    returns: { label: string, note?: string }
    mappings:
      - target: label
        expr: "$.label"
        required: true
      - target: note
        expr: "$.note"
mappings:
  - target: decorated
    expr:
      - decorate:
          - with: { label: "@input.label", note: "@input.note" }
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("optional with field validates");

    let output = transform(&rule, r#"[{"label":"ok"}]"#, None)
        .expect("missing optional with field is omitted");
    assert_eq!(output, json!([{ "decorated": { "label": "ok" } }]));

    let err = transform(&rule, r#"[{"note":"present"}]"#, None)
        .expect_err("missing required with field still fails");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message
            .contains("custom op input contract mismatch: with value is missing")
    );
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr[0].with.label"));
}
