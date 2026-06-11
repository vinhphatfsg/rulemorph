use super::*;

#[test]
fn custom_op_contracts_reject_nested_adapter_extra_and_output_extra_without_leaking_values() {
    let nested_extra = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  check_nested:
    input: { nested: { qty: int }, payload: json }
    returns: bool
    expr: [true]
mappings:
  - target: ok
    expr:
      - "@input"
      - check_nested:
          - with:
              nested:
                value: { qty: 1, extra: "secret-nested" }
              payload:
                value: { extra: "allowed-json" }
"#,
    );
    let err =
        transform(&nested_extra, r#"[{}]"#, None).expect_err("nested extra field is rejected");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("custom op input contract mismatch"));
    assert!(!err.message.contains("secret-nested"));
    assert!(!err.message.contains("allowed-json"));

    let output_extra = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  leak:
    input: json
    returns: { ok: bool }
    mappings:
      - target: ok
        value: true
      - target: extra
        value: "secret-output"
mappings:
  - target: value
    expr: ["@input", leak]
"#,
    );
    let err =
        transform(&output_extra, r#"[{}]"#, None).expect_err("output extra field is rejected");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("custom op output contract mismatch"));
    assert!(!err.message.contains("secret-output"));
}

#[test]
fn custom_op_runtime_contract_errors_do_not_leak_raw_values() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr: ["$", { get: ["qty"] }]
mappings:
  - target: total
    expr:
      - "@input.line"
      - line_total:
          - with: { qty: "$.quantity", unit_price: "$.price", extra: "$.secret" }
"#;
    let rule = parse(yaml);
    let err = transform(
        &rule,
        r#"[{"line":{"quantity":"not-an-int","price":10,"secret":"do-not-leak"}}]"#,
        None,
    )
    .expect_err("contract mismatch fails");

    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("custom op input contract mismatch"));
    assert!(!err.message.contains("not-an-int"));
    assert!(!err.message.contains("do-not-leak"));
}
