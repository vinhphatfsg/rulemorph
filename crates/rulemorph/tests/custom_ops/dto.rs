use super::*;

#[test]
fn custom_op_mappings_body_synthesizes_object_return_for_dto_and_runtime() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  enrich_line:
    input: { sku: string, qty: int }
    mappings:
      - target: sku
        source: sku
        required: true
      - target: qty
        source: qty
        required: true
mappings:
  - target: line
    expr:
      - "@input.item"
      - enrich_line
    required: true
"#;
    let rule = parse(yaml);
    let output = transform(
        &rule,
        r#"[{"item":{"sku":"A-1","qty":2,"ignored":"ok"}}]"#,
        None,
    )
    .expect("transform succeeds");
    assert_eq!(output, json!([{ "line": { "sku": "A-1", "qty": 2 } }]));

    let ts = generate_dto(&rule, DtoLanguage::TypeScript, Some("Record")).expect("dto renders");
    assert!(ts.contains("line: RecordLine;"));
    assert!(ts.contains("sku: string;"));
    assert!(ts.contains("qty: number;"));
}

#[test]
fn custom_op_mappings_body_dto_uses_body_local_out_scope() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  make:
    input: json
    mappings:
      - target: a
        value: 1
        required: true
      - target: b
        expr: "@out.a"
        required: true
mappings:
  - target: a
    value: outer
    required: true
  - target: result
    expr: ["@input", make]
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("body-local out ref validates");

    let output = transform(&rule, r#"[{}]"#, None).expect("body-local out ref transforms");
    assert_eq!(
        output,
        json!([{ "a": "outer", "result": { "a": 1, "b": 1 } }])
    );

    let ts = generate_dto(&rule, DtoLanguage::TypeScript, Some("Record")).expect("dto renders");
    assert!(ts.contains("b: number;"));
    assert!(!ts.contains("b: string;"));
}

#[test]
fn custom_op_literal_start_call_feeds_following_steps_for_dto() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  line:
    input: { qty: int, unit_price: number }
    mappings:
      - target: total
        expr: ["$.qty", { "*": ["@input.unit_price"] }]
        required: true
mappings:
  - target: value
    expr:
      - line:
          - with: { qty: "@input.qty", unit_price: "@input.price" }
      - get: ["total"]
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("literal-start custom call with DTO follow-up validates");

    let output = transform(&rule, r#"[{"qty":3,"price":4.5}]"#, None)
        .expect("literal-start custom call DTO case transforms");
    assert_eq!(output, json!([{ "value": 13.5 }]));

    let ts = generate_dto(&rule, DtoLanguage::TypeScript, Some("Record")).expect("dto renders");
    assert!(ts.contains("value: number;"));
}
