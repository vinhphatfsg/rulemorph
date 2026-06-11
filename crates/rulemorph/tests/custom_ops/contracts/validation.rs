use super::*;

#[test]
fn custom_op_validation_rejects_cycles_context_capture_and_direct_adapter() {
    let version_one_defs = parse(
        r#"
version: 1
input:
  format: json
  json: {}
defs:
  bad:
    input: string
    returns: string
    expr: "$"
mappings:
  - target: value
    source: title
"#,
    );
    let errors =
        validate_rule_file(&version_one_defs).expect_err("defs are rejected for version 1");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidStep
            && err.path.as_deref() == Some("defs")
            && err.message.contains("version 2")
    }));

    let cycle = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  a:
    input: string
    returns: string
    expr: ["$", b]
  b:
    input: string
    returns: string
    expr: ["$", a]
mappings:
  - target: value
    expr: ["@input.value", a]
"#,
    );
    let errors = validate_rule_file(&cycle).expect_err("cycle is rejected");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::CyclicDependency)
    );

    let literal_start_cycle = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  a:
    input: {}
    returns: json
    expr:
      - b:
          - with: {}
  b:
    input: {}
    returns: json
    expr:
      - a:
          - with: {}
mappings:
  - target: value
    expr:
      - a:
          - with: {}
"#,
    );
    let errors = validate_rule_file(&literal_start_cycle)
        .expect_err("literal-start custom call dependency cycle is rejected");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::CyclicDependency)
    );

    let when_cycle = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  a:
    input: string
    mappings:
      - target: value
        value: "a"
        when:
          eq: [["$", b], true]
  b:
    input: string
    returns: bool
    expr: ["$", a, { get: ["value"] }, { eq: ["a"] }]
mappings:
  - target: value
    expr: ["@input.value", a]
"#,
    );
    let errors =
        validate_rule_file(&when_cycle).expect_err("condition dependency cycle is rejected");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::CyclicDependency)
    );

    let context_capture = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: string
    returns: string
    expr: ["@context.secret", trim]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    );
    let errors = validate_rule_file(&context_capture).expect_err("context capture is rejected");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidRefNamespace)
    );

    let direct_adapter = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int }
    returns: int
    expr: ["$", { get: ["qty"] }]
mappings:
  - target: total
    expr:
      - "@input.line"
      - line_total:
          qty: "$.quantity"
"#,
    );
    let errors = validate_rule_file(&direct_adapter).expect_err("direct adapter is rejected");
    assert!(errors.iter().any(|err| err.code == ErrorCode::InvalidArgs));

    let literal_start_direct_adapter = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int }
    returns: int
    expr: "$.qty"
mappings:
  - target: total
    expr:
      - line_total:
          qty: "@input.qty"
"#,
    );
    let errors = validate_rule_file(&literal_start_direct_adapter)
        .expect_err("literal-start direct adapter is rejected");
    assert!(errors.iter().any(|err| {
        matches!(
            err.code,
            ErrorCode::InvalidArgs | ErrorCode::InvalidExprShape
        ) && err
            .message
            .contains("custom op call must use with call options")
    }));

    let scalar_with_adapter = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr: ["$", trim]
mappings:
  - target: slug
    expr:
      - "@input.title"
      - slug:
          - with: {}
"#,
    );
    let errors =
        validate_rule_file(&scalar_with_adapter).expect_err("scalar with adapter is rejected");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidArgs
            && err.message.contains("with adapter requires object input")
    }));

    let invalid_builtin_in_body = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: string
    returns: string
    expr: ["$", not_a_builtin]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    );
    let errors = validate_rule_file(&invalid_builtin_in_body)
        .expect_err("invalid builtin op in custom body is rejected");
    assert!(errors.iter().any(|err| err.code == ErrorCode::UnknownOp));

    let reserved_name = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  map:
    input: string
    returns: string
    expr: "$"
mappings:
  - target: value
    expr: ["@input.value", map]
"#,
    );
    let errors =
        validate_rule_file(&reserved_name).expect_err("reserved custom op name is rejected");
    assert!(
        errors
            .iter()
            .any(|err| { err.code == ErrorCode::UnknownOp && err.message.contains("reserved op") })
    );

    let ref_name = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  ref:
    input: {}
    returns: string
    expr: "lit:ok"
mappings:
  - target: value
    expr:
      - ref:
          - with: {}
"#,
    );
    let errors = validate_rule_file(&ref_name).expect_err("ref custom op name is reserved");
    assert!(
        errors
            .iter()
            .any(|err| { err.code == ErrorCode::UnknownOp && err.message.contains("reserved op") })
    );

    let invalid_expr_body = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: string
    returns: string
    expr: "hello"
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    );
    let errors =
        validate_rule_file(&invalid_expr_body).expect_err("non-v2 custom expr is rejected");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidExprShape && err.path.as_deref() == Some("defs.bad.expr")
    }));

    let invalid_mapping_shape = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: json
    mappings:
      - target: ""
        value: 1
      - target: ok
      - target: blocked
        source: context.secret
mappings:
  - target: value
    expr: ["@input", bad]
"#,
    );
    let errors = validate_rule_file(&invalid_mapping_shape)
        .expect_err("custom mapping shape is rejected before runtime");
    assert!(errors.iter().any(|err| err.code == ErrorCode::InvalidPath));
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::MissingMappingValue)
    );
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidRefNamespace
            && err.path.as_deref() == Some("defs.bad.mappings[2].source")
    }));
}
