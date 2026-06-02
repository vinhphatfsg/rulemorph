use rulemorph::{
    CustomOpDef, DtoLanguage, ErrorCode, Expr, InputData, Mapping, RuleType, RuleTypeKind,
    TraceAttributeValue, TraceEventKind, TransformErrorKind, TransformTraceOptions, generate_dto,
    parse_rule_file, transform, transform_input_with_trace, validate_rule_file,
};
use serde_json::{Value as JsonValue, json};

fn parse(yaml: &str) -> rulemorph::RuleFile {
    parse_rule_file(yaml).expect("rule parses")
}

fn json_rule_type() -> RuleType {
    RuleType {
        kind: RuleTypeKind::Json,
        nullable: false,
    }
}

fn nested_with_custom_call(index: usize, call_count: usize) -> JsonValue {
    if index == call_count {
        return json!("@input.seed");
    }

    let mut with_fields = serde_json::Map::new();
    with_fields.insert(
        "next".to_string(),
        nested_with_custom_call(index + 1, call_count),
    );
    let mut with_option = serde_json::Map::new();
    with_option.insert("with".to_string(), JsonValue::Object(with_fields));
    let mut call = serde_json::Map::new();
    call.insert(
        format!("d{}", index),
        JsonValue::Array(vec![JsonValue::Object(with_option)]),
    );
    JsonValue::Object(call)
}

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
fn custom_op_json_contract_accepts_null() {
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
      - "@input.value"
      - echo
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("json contract accepts any JSON value");

    let output = transform(&rule, r#"[{"value":null}]"#, None)
        .expect("json contract accepts null at runtime");

    assert_eq!(output, json!([{ "value": null }]));
}

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
fn custom_op_type_contracts_cover_canonical_optional_nullable_and_wrappers() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  decorate:
    input:
      label: string
      note: { type: string, nullable: true }
      memo: { type: string, optional: true, nullable: true }
      payload: json
    returns: { label: string, note: string?, payload: json }
    mappings:
      - target: label
        expr: ["$", { get: ["label"] }]
        required: true
      - target: note
        expr: ["$", { get: ["note"] }]
        required: false
      - target: payload
        expr: ["$", { get: ["payload"] }]
        required: true
mappings:
  - target: decorated
    expr:
      - "@input.item"
      - decorate:
          - with:
              label: { value: "$.quantity" }
              note: { expr: "$.note" }
              payload:
                value: { expr: "$.quantity" }
    required: true
"#;
    let rule = parse(yaml);
    let output = transform(&rule, r#"[{"item":{"note":null}}]"#, None)
        .expect("canonical type and wrappers succeed");
    assert_eq!(
        output,
        json!([{ "decorated": { "label": "$.quantity", "note": null, "payload": { "expr": "$.quantity" } } }])
    );

    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input: { type: string, optional: true }
    returns: string
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    )
    .expect_err("top-level object contracts treat `type` as a field name");
    assert!(
        err.to_string()
            .contains("type literal must be a string, array, or object")
    );

    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad:
    input:
      label: { type: string, optional: "true" }
    returns: string
    expr: ["$"]
mappings:
  - target: value
    expr: ["@input.value", bad]
"#,
    )
    .expect_err("canonical type options require booleans");
    assert!(
        err.to_string()
            .contains("type option `optional` must be boolean")
    );
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

#[test]
fn custom_op_object_contract_allows_type_field_name() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  item_type:
    input: { type: string }
    returns: string
    expr: ["$", { get: ["type"] }]
mappings:
  - target: kind
    expr: ["@input.item", item_type]
"#;
    let rule = parse(yaml);
    validate_rule_file(&rule).expect("type field object contract validates");
    let output = transform(&rule, r#"[{"item":{"type":"physical"}}]"#, None)
        .expect("type field object contract transforms");

    assert_eq!(output, json!([{ "kind": "physical" }]));

    let nested_yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  item_type:
    input: { meta: { type: string } }
    returns: string
    expr: ["$", { get: ["meta.type"] }]
mappings:
  - target: kind
    expr: ["@input.item", item_type]
"#;
    let nested_rule = parse(nested_yaml);
    validate_rule_file(&nested_rule).expect("nested type field object contract validates");
    let output = transform(
        &nested_rule,
        r#"[{"item":{"meta":{"type":"physical"}}}]"#,
        None,
    )
    .expect("nested type field object contract transforms");

    assert_eq!(output, json!([{ "kind": "physical" }]));
}

#[test]
fn custom_op_type_contract_rejects_duplicate_fields_after_optional_normalization() {
    let err = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  check:
    input: { id: int, id?: string }
    returns: json
    expr: "$"
mappings:
  - target: result
    expr: ["@input", check]
"#,
    )
    .expect_err("id and id? normalize to the same object field");

    assert!(err.to_string().contains("object field `id` is duplicated"));
}

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
fn custom_op_mappings_body_returns_must_be_object_or_json() {
    let invalid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  scalar:
    input: json
    returns: string
    mappings:
      - target: value
        value: "ok"
  array:
    input: json
    returns: [string]
    mappings:
      - target: value
        value: "ok"
mappings:
  - target: scalar
    expr: ["@input", scalar]
  - target: array
    expr: ["@input", array]
"#,
    );
    let errors = validate_rule_file(&invalid).expect_err("non-object mappings returns fail");
    assert_eq!(
        errors
            .iter()
            .filter(|err| err.code == ErrorCode::InvalidTypeName
                && err
                    .message
                    .contains("custom op mappings body returns must be object or json"))
            .count(),
        2
    );

    let valid = parse(
        r#"
version: 2
input:
  format: json
  json: {}
defs:
  typed:
    input: json
    returns: { value: string }
    mappings:
      - target: value
        value: "ok"
  broad:
    input: json
    returns: json
    mappings:
      - target: value
        value: "ok"
mappings:
  - target: typed
    expr: ["@input", typed]
  - target: broad
    expr: ["@input", broad]
"#,
    );
    validate_rule_file(&valid).expect("object and json mappings returns are valid");
}

#[test]
fn custom_op_call_site_validation_ignores_version_one_literal_arrays() {
    let rule = parse(
        r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: value
    expr:
      - hello
      - foo:
          - with: {}
"#,
    );

    validate_rule_file(&rule).expect("v1 literal arrays are not v2 custom op calls");
}

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

#[test]
fn custom_op_trace_span_uses_metadata_without_raw_with_values_by_default() {
    let yaml = r#"
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
  - target: slug
    expr: ["@input.title", slug]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"title":"  SECRET-TITLE  "}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!([{ "slug": "secret-title" }]));
    let events: Vec<_> = traced.trace.records[0].events.iter().collect();
    let start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("custom op start span");
    assert_eq!(start.operator.as_deref(), Some("slug"));
    assert!(matches!(
        start.attributes.get("with_adapter"),
        Some(TraceAttributeValue::Bool(false))
    ));
    assert!(matches!(
        start.attributes.get("def_path"),
        Some(TraceAttributeValue::String(value)) if value == "defs.slug"
    ));
    assert!(matches!(
        start.attributes.get("input_type"),
        Some(TraceAttributeValue::String(value)) if value == "string"
    ));
    assert!(matches!(
        start.attributes.get("output_type"),
        Some(TraceAttributeValue::String(value)) if value == "string"
    ));

    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("SECRET-TITLE"));
}
