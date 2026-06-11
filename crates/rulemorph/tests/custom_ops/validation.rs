use super::*;

#[test]
fn custom_op_validation_rejects_pipe_value_start_outside_custom_body() {
    let top_level_pipe_ref = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr: "$.foo"
"#,
    );
    let errors = validate_rule_file(&top_level_pipe_ref)
        .expect_err("top-level mapping expr cannot start from pipe value");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidRefNamespace)
    );

    let top_level_bare_pipe = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr: "$"
"#,
    );
    let errors = validate_rule_file(&top_level_bare_pipe)
        .expect_err("top-level mapping expr cannot be a bare pipe value");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidRefNamespace)
    );

    let top_level_pipe_with_step = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr: ["$", uppercase]
"#,
    );
    let errors = validate_rule_file(&top_level_pipe_with_step)
        .expect_err("top-level mapping expr cannot start from pipe value before a step");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::InvalidRefNamespace)
    );

    let custom_body_pipe_ref = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  pick_foo:
    input: { foo: string }
    returns: string
    expr: "$.foo"
mappings:
  - target: value
    expr: ["@input.item", pick_foo]
"#,
    );
    validate_rule_file(&custom_body_pipe_ref).expect("custom op body can start from pipe value");
}

#[test]
fn custom_op_runtime_rejects_builtin_shadow_without_validation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  trim:
    input: string
    returns: string
    expr: ["lit:SHADOW"]
mappings:
  - target: result
    expr: ["@input.name", trim]
"#;
    let rule = parse(yaml);
    let errors = validate_rule_file(&rule).expect_err("shadowing still fails validation");
    assert!(errors.iter().any(|err| err.code == ErrorCode::UnknownOp));

    let err = transform(&rule, r#"[{"name":"  x  "}]"#, None)
        .expect_err("runtime also rejects shadowing when validation is skipped");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(err.message.contains("must not shadow"));
}

#[test]
fn custom_op_validation_rejects_unknown_calls_in_top_level_conditions_and_finalize() {
    let record_when = parse(
        r#"
version: 2
input:
  format: json
  json: {}
record_when:
  eq:
    - - "@input.value"
      - missing_record:
          - with: {}
    - true
mappings:
  - target: value
    expr: "@input.value"
"#,
    );
    let errors =
        validate_rule_file(&record_when).expect_err("top-level record_when custom call rejects");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::UnknownOp && err.message.contains("missing_record")
        })
    );

    let finalize_filter = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr: "@input.value"
finalize:
  filter:
    eq:
      - - "@item.value"
        - missing_filter:
            - with: {}
      - true
"#,
    );
    let errors =
        validate_rule_file(&finalize_filter).expect_err("finalize.filter custom call rejects");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::UnknownOp && err.message.contains("missing_filter")
        })
    );

    let finalize_wrap = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: value
    expr: "@input.value"
finalize:
  wrap:
    value:
      - "@out"
      - missing_wrap:
          - with: {}
"#,
    );
    let errors = validate_rule_file(&finalize_wrap).expect_err("finalize.wrap custom call rejects");
    assert!(
        errors.iter().any(|err| {
            err.code == ErrorCode::UnknownOp && err.message.contains("missing_wrap")
        })
    );
}

#[test]
fn custom_op_resource_limits_fail_closed() {
    let mut too_many_defs = r#"
version: 2
input:
  format: json
  json: {}
defs:
"#
    .to_string();
    for index in 0..129 {
        too_many_defs.push_str(&format!(
            "  f{}:\n    input: string\n    returns: string\n    expr: [\"$\"]\n",
            index
        ));
    }
    too_many_defs.push_str(
        r#"mappings:
  - target: value
    expr: ["@input.value", f0]
"#,
    );
    let rule = parse(&too_many_defs);
    let errors = validate_rule_file(&rule).expect_err("max_defs limit is enforced");
    assert!(errors.iter().any(|err| err.code == ErrorCode::InvalidStep));

    let mut deep_type = "string".to_string();
    for _ in 0..33 {
        deep_type = format!("[{}]", deep_type);
    }
    let deep_type_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
defs:
  deep:
    input: {}
    returns: json
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", deep]
"#,
        deep_type
    );
    let err = parse_rule_file(&deep_type_yaml).expect_err("type depth limit is parse-enforced");
    assert!(
        err.to_string()
            .contains("type exceeds configured depth limit")
    );

    let mut many_fields_type = "{".to_string();
    for index in 0..513 {
        if index > 0 {
            many_fields_type.push_str(", ");
        }
        many_fields_type.push_str(&format!("f{}: string", index));
    }
    many_fields_type.push('}');
    let many_fields_yaml = format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
defs:
  huge_type:
    input: {}
    returns: json
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", huge_type]
"#,
        many_fields_type
    );
    let err = parse_rule_file(&many_fields_yaml).expect_err("type field limit is parse-enforced");
    assert!(
        err.to_string()
            .contains("type exceeds configured field limit")
    );

    let huge_json = serde_json::to_string(&vec![0; 2_100]).expect("huge json serializes");
    let huge_value_rule = parse(&format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
defs:
  huge:
    input: json
    mappings:
      - target: value
        value: {}
mappings:
  - target: value
    expr: ["@input.value", huge]
"#,
        huge_json
    ));
    let errors =
        validate_rule_file(&huge_value_rule).expect_err("literal body value limit is enforced");
    assert!(errors.iter().any(|err| err.code == ErrorCode::InvalidStep));

    let huge_default_rule = parse(&format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
defs:
  huge:
    input: json
    mappings:
      - target: value
        expr: "$.missing"
        default: {}
mappings:
  - target: value
    expr: ["@input.value", huge]
"#,
        huge_json
    ));
    let errors =
        validate_rule_file(&huge_default_rule).expect_err("literal body default limit is enforced");
    assert!(errors.iter().any(|err| err.code == ErrorCode::InvalidStep));

    let mut deep_calls = r#"
version: 2
input:
  format: json
  json: {}
defs:
"#
    .to_string();
    for index in 0..=64 {
        let expr = if index == 64 {
            r#"["$"]"#.to_string()
        } else {
            format!(r#"["$", d{}]"#, index + 1)
        };
        deep_calls.push_str(&format!(
            "  d{}:\n    input: string\n    returns: string\n    expr: {}\n",
            index, expr
        ));
    }
    deep_calls.push_str(
        r#"mappings:
  - target: value
    expr: ["@input.value", d0]
    required: true
"#,
    );
    let rule = parse(&deep_calls);
    validate_rule_file(&rule).expect("acyclic custom op chain validates");
    let err = transform(&rule, r#"[{"value":"x"}]"#, None)
        .expect_err("call depth limit is enforced at runtime");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message
            .contains("custom op call depth exceeds configured limit")
    );
}

#[test]
fn custom_op_with_args_count_toward_call_depth_limit() {
    let mut rule = parse(
        r#"
version: 2
input:
  format: json
  json: {}
mappings: []
"#,
    );
    let json_type = json_rule_type();
    for index in 0..70 {
        rule.defs.insert(
            format!("d{}", index),
            CustomOpDef {
                input: json_type.clone(),
                returns: Some(json_type.clone()),
                expr: Some(Expr::Literal(json!(["$"]))),
                mappings: None,
            },
        );
    }
    rule.mappings.push(Mapping {
        target: "value".to_string(),
        source: None,
        value: None,
        expr: Some(Expr::Literal(JsonValue::Array(vec![
            nested_with_custom_call(0, 70),
        ]))),
        when: None,
        value_type: None,
        required: true,
        default: None,
    });

    validate_rule_file(&rule).expect("acyclic custom op with nesting validates");
    let err = transform(&rule, r#"[{"seed":"x"}]"#, None)
        .expect_err("with arg nesting counts toward call depth");
    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert!(
        err.message
            .contains("custom op call depth exceeds configured limit")
    );
}

#[test]
fn custom_op_body_out_ref_index_requires_produced_array_parent_not_nested_object() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  expose:
    input: { value: string }
    mappings:
      - target: tmp.value
        source: value
      - target: first
        source: out.tmp[0]
mappings:
  - target: result
    expr:
      - "@input"
      - expose:
          - with: { value: "@input.name" }
"#;
    let rule = parse(yaml);
    let errors =
        validate_rule_file(&rule).expect_err("custom op out refs should not ignore index tokens");
    assert!(
        errors
            .iter()
            .any(|err| err.code == ErrorCode::ForwardOutReference),
        "expected ForwardOutReference, got {errors:?}"
    );
}
