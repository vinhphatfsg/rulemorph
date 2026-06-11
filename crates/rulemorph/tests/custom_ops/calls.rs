use super::*;

#[test]
fn custom_op_direct_and_with_calls_transform_records() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr:
      - "$"
      - trim
      - lowercase
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$.qty", float]
          price: ["$.unit_price", float]
      - "@qty"
      - "*": ["@price"]
mappings:
  - target: slug
    expr:
      - "@input.title"
      - slug
    required: true
  - target: total
    expr:
      - "@input.line"
      - line_total:
          - with: { qty: "$.quantity", unit_price: "$.price" }
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("custom op rule validates");

    let output = transform(
        &rule,
        r#"[{"title":"  Hello Rulemorph  ","line":{"quantity":3,"price":19.5}}]"#,
        None,
    )
    .expect("transform succeeds");

    assert_eq!(
        output,
        json!([{ "slug": "hello rulemorph", "total": 58.5 }])
    );
}

#[test]
fn custom_op_parser_keeps_builtin_with_literal_args_as_builtin_ops() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: merged
    expr:
      - "@input.obj"
      - merge:
          - with:
              flag: true
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("built-in op with literal object arg validates");
    let output = transform(&rule, r#"[{"obj":{"a":1}}]"#, None)
        .expect("built-in op with literal object arg transforms");

    assert_eq!(
        output,
        json!([{ "merged": { "a": 1, "with": { "flag": true } } }])
    );
}

#[test]
fn custom_op_parser_keeps_unknown_call_shaped_single_object_as_literal() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  other:
    input: json
    returns: json
    expr: "$"
mappings:
  - target: payload
    expr:
      - foo:
          - with:
              a: 1
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("unknown one-key object literal still validates");

    let output = transform(&rule, r#"[{}]"#, None).expect("literal object transforms");
    assert_eq!(
        output,
        json!([{ "payload": { "foo": [{ "with": { "a": 1 } }] } }])
    );
}

#[test]
fn custom_op_mappings_body_synthesized_return_matches_leaf_overwrite() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  make:
    input: json
    mappings:
      - target: a.b
        value: nested
      - target: a
        value: leaf
mappings:
  - target: result
    expr: ["@input", make]
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("overwrite mapping body validates");
    let output =
        transform(&rule, r#"[{}]"#, None).expect("leaf overwrite matches synthesized contract");

    assert_eq!(output, json!([{ "result": { "a": "leaf" } }]));
}

#[test]
fn custom_op_single_step_pipe_call_executes_in_if_branch() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$", { get: ["qty"] }, float]
          price: ["$", { get: ["unit_price"] }, float]
      - "@qty"
      - "*": ["@price"]
mappings:
  - target: total
    expr:
      - "@input.line"
      - if:
          cond: { eq: [true, true] }
          then:
            - line_total:
                - with: { qty: "$.quantity", unit_price: "$.price" }
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("custom op in single-step branch pipe validates");

    let output = transform(&rule, r#"[{"line":{"quantity":3,"price":19.5}}]"#, None)
        .expect("custom op in branch pipe transforms");

    assert_eq!(output, json!([{ "total": 58.5 }]));
}

#[test]
fn custom_op_with_args_accept_pipe_bracket_refs() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  pair:
    input: { first: string, dotted: string }
    mappings:
      - target: first
        expr: "$.first"
        required: true
      - target: dotted
        expr: "$.dotted"
        required: true
mappings:
  - target: value
    expr:
      - "@input"
      - pair:
          - with: { first: ["$.values", "$[0]"], dotted: "$[\"a.b\"]" }
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("pipe bracket refs validate in custom op args");

    let output = transform(&rule, r#"[{"values":["first"],"a.b":"quoted"}]"#, None)
        .expect("pipe bracket refs transform in custom op args");

    assert_eq!(
        output,
        json!([{ "value": { "first": "first", "dotted": "quoted" } }])
    );

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"values":["first"],"a.b":"quoted"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("pipe bracket refs trace in custom op args");
    let ref_paths = traced
        .trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .filter(|event| event.kind == TraceEventKind::RefRead)
        .filter_map(|event| event.input_path.as_deref())
        .collect::<Vec<_>>();
    assert!(ref_paths.contains(&"$[0]"));
    assert!(ref_paths.contains(&"$[\"a.b\"]"));
    assert!(!ref_paths.contains(&"$.[0]"));
    assert!(!ref_paths.contains(&"$.[\"a.b\"]"));
}

#[test]
fn custom_op_literal_start_call_continues_following_steps() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: { title: string }
    returns: string
    expr: ["$.title", trim, lowercase]
mappings:
  - target: value
    expr:
      - slug:
          - with: { title: "@input.title" }
      - uppercase
    required: true
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("literal-start custom call validates");

    let output = transform(&rule, r#"[{"title":"  Hello  "}]"#, None)
        .expect("literal-start custom call transforms through later steps");

    assert_eq!(output, json!([{ "value": "HELLO" }]));
}

#[test]
fn custom_op_literal_start_with_args_use_parent_scope() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  take_title:
    input: { title: string }
    returns: string
    expr: "$.title"
mappings:
  - target: value
    expr:
      - take_title:
          - with: { title: "$.title" }
"#;
    let rule = parse(yaml);
    let errors = validate_rule_file(&rule)
        .expect_err("literal-start with args cannot capture missing pipe scope");

    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidRefNamespace
            && err
                .path
                .as_deref()
                .is_some_and(|path| path.contains("with.title"))
    }));
}

#[test]
fn custom_op_implicit_pipe_direct_call_requires_caller_pipe() {
    let invalid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr: "$"
mappings:
  - target: value
    expr:
      - op: slug
"#,
    );
    let errors = validate_rule_file(&invalid)
        .expect_err("implicit direct custom op without caller pipe is rejected");
    assert!(errors.iter().any(|err| {
        err.code == ErrorCode::InvalidRefNamespace
            && err
                .message
                .contains("custom op direct call requires a pipe value")
    }));

    let valid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
mappings:
  - target: value
    expr:
      - "@input.title"
      - op: slug
"#,
    );
    validate_rule_file(&valid).expect("direct custom op after explicit start validates");
    let output = transform(&valid, r#"[{"title":" Hello "}]"#, None)
        .expect("direct custom op after explicit start transforms");

    assert_eq!(output, json!([{ "value": "hello" }]));
}

#[test]
fn custom_op_conditions_and_finalize_wrap_use_rule_context() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  normalize_status:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
  is_active:
    input: string
    returns: bool
    expr: ["$", normalize_status, { eq: ["active"] }]
steps:
  - record_when:
      eq: [["@input.status", is_active], true]
  - asserts:
      - when:
          eq: [["@input.status", is_active], true]
        error:
          code: NOT_ACTIVE
          message: status must be active
  - mappings:
      - target: status
        expr: ["@input.status", normalize_status]
        when:
          eq: [["@input.status", is_active], true]
        required: true
finalize:
  filter:
    eq: [["@item.status", is_active], true]
  wrap:
    first_status: ["@out", first, { get: ["status"] }, normalize_status]
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("condition custom ops validate");

    let output = transform(
        &rule,
        r#"[{"status":" ACTIVE "},{"status":"inactive"}]"#,
        None,
    )
    .expect("condition and finalize custom ops transform");

    assert_eq!(output, json!({ "first_status": "active" }));
}
