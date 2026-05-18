#[test]
fn trace_v2_missing_short_circuit_does_not_evaluate_later_invalid_arg() {
    let cases = [
        (
            "concat",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.missing"
      - concat: ["@item.invalid"]
"#,
        ),
        (
            "pick",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.obj"
      - pick: ["@input.missing", "@item.invalid"]
"#,
        ),
        (
            "lookup_first",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.missing"
      - lookup_first: ["@input.not_array", "@item.invalid", "@item.invalid"]
"#,
        ),
    ];
    let input = r#"[{"obj":{"a":1},"not_array":"not-array"}]"#;

    for (name, yaml) in cases {
        let rule = parse_rule_file(yaml).unwrap_or_else(|err| panic!("{name} parse: {err:?}"));
        let normal = transform(&rule, input, None)
            .unwrap_or_else(|err| panic!("{name} normal transform: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{name} traced transform: {err:?}"));

        assert_eq!(normal, json!([{}]), "{name} normal output");
        assert_eq!(traced.output, normal, "{name} traced output");
        assert!(
            iter_trace_events(&traced.trace).into_iter().all(|event| {
                event.kind != TraceEventKind::RefRead
                    || event.input_path.as_deref() != Some("@item.invalid")
            }),
            "{name} should not evaluate skipped invalid @item arg"
        );
        assert_trace_shape(&traced.trace);
    }
}

#[test]
fn trace_v1_missing_short_circuit_does_not_evaluate_later_invalid_arg() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "out"
    expr:
      op: concat
      args:
        - { ref: "input.missing" }
        - { ref: "item.value" }
"#;
    let rule = parse_rule(yaml);
    let normal = transform(&rule, r#"[{}]"#, None).expect("normal transform");
    let traced = transform_text_raw_trace(&rule, r#"[{}]"#);

    assert_eq!(normal, json!([{}]));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace).into_iter().all(|event| {
            event.kind != TraceEventKind::RefRead
                || event.input_path.as_deref() != Some("@item.value")
        }),
        "v1 traced missing short-circuit should not evaluate skipped invalid @item arg"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_coalesce_does_not_evaluate_short_circuited_arg() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "name"
    expr:
      op: coalesce
      args:
        - { ref: "input.name" }
        - { ref: "item.value" }
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"name":"alice"}]"#);

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::ArgEval)
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())]
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_and_does_not_evaluate_short_circuited_arg() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "flag"
    expr:
      op: and
      args:
        - false
        - { ref: "item.value" }
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{}]"#);

    assert_eq!(traced.output, json!([{ "flag": false }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::ArgEval)
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())]
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_and_short_circuits_false_pipe_without_evaluating_invalid_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"enabled":false}]"#, json!([{ "flag": false }]));
}

#[test]
fn trace_v2_or_short_circuits_true_pipe_without_evaluating_invalid_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - or: ["@item.enabled"]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"enabled":true}]"#, json!([{ "flag": true }]));
}

#[test]
fn trace_v2_coalesce_short_circuits_after_first_present_arg() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    expr:
      - "@input.primary"
      - coalesce: ["@input.secondary", "@item.name"]
"#;

    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"secondary":"fallback"}]"#);

    assert_eq!(traced.output, json!([{ "name": "fallback" }]));
    let arg_eval_indexes = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| {
            event.kind == TraceEventKind::ArgEval && event.operator.as_deref() == Some("coalesce")
        })
        .filter_map(|event| event.attributes.get("arg_index"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        arg_eval_indexes,
        vec![TraceAttributeValue::Number(0.into())],
        "coalesce should trace evaluated fallback args but not short-circuited args"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_short_circuited_args_are_not_reported_as_arg_eval() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"enabled":false}]"#);

    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .all(|event| event.kind != TraceEventKind::ArgEval),
        "short-circuited v2 args must not be reported as evaluated"
    );
}

#[test]
fn trace_v2_non_short_circuited_invalid_arg_errors_like_normal() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "flag"
    expr:
      - "@input.enabled"
      - and: ["@item.enabled"]
"#;
    let rule = parse_rule(yaml);
    let input = r#"[{"enabled":true}]"#;

    let normal = transform(&rule, input, None).expect_err("normal error");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced error");

    assert_eq!(traced.error, normal);
    assert_trace_shape(&traced.trace);
}
