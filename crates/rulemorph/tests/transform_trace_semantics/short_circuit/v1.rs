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
