#[test]
fn trace_v1_map_ref_read_preserves_item_namespace() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "names"
    expr:
      op: map
      args:
        - { ref: "input.users" }
        - { ref: "item.value.name" }
"#;
    let rule = parse_rule(yaml);
    let traced =
        transform_text_raw_trace(&rule, r#"[{"users":[{"name":"alice"},{"name":"bob"}]}]"#);

    assert_eq!(traced.output, json!([{ "names": ["alice", "bob"] }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@item.value.name")
        }),
        "item-scoped ref reads must not be reported as @input paths"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v1_map_generated_arrays_respect_array_limit() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "nested"
    expr:
      op: map
      args:
        - { op: range, args: [1, 4] }
        - { op: range, args: [0, { ref: "item.value" }] }
"#;
    let rule = parse_rule(yaml);
    let options = NormalizationOptions {
        max_array_len: 8,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text("[{}]"),
        None,
        None,
        &options,
        &TransformTraceOptions::raw(),
    )
    .expect_err("v1 trace should enforce generated array limit");

    assert_eq!(err.error.kind, TransformErrorKind::ExprError);
    assert!(
        err.error
            .message
            .contains("generated array items exceed configured limit")
    );
    assert_trace_shape(&err.trace);
}

#[test]
fn trace_v1_reduce_ref_read_preserves_acc_namespace() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "total"
    expr:
      op: reduce
      args:
        - { ref: "input.values" }
        - { op: "+", args: [ { ref: "acc.value" }, { ref: "item.value" } ] }
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"values":[1,2,3]}]"#);

    assert_eq!(traced.output, json!([{ "total": 6 }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::RefRead && event.input_path.as_deref() == Some("@acc.value")
        }),
        "acc-scoped ref reads must not be reported as @input paths"
    );
    assert_trace_shape(&traced.trace);
}
