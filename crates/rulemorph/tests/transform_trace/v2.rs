#[test]
fn trace_v2_pipe_operator_lifecycle_and_collection_items() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "names"
    expr:
      - "@input.users"
      - map:
          - "@item.name"
          - uppercase
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"users":[{"name":"alice"},{"name":"bob"}]}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "names": ["ALICE", "BOB"] }]));
    assert_operator_lifecycle(&traced.trace, "map");
    assert_operator_lifecycle(&traced.trace, "uppercase");
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);

    let text = serde_json::to_string(&traced.trace).unwrap();
    assert!(text.contains("@item"));
    assert!(text.contains("ALICE"));
    assert!(text.contains("BOB"));
}

#[test]
fn trace_v2_operator_inputs_preserve_missing_values() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    expr:
      - "@input.missing_name"
      - coalesce: ["anonymous"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "anonymous" }]));
    assert_operator_lifecycle(&traced.trace, "coalesce");
    let events = iter_trace_events(&traced.trace);
    let coalesce_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some("coalesce")
        })
        .expect("coalesce op_start");
    assert_eq!(coalesce_start.inputs[0].state, TraceValueState::Missing);
    assert_eq!(coalesce_start.inputs[0].value_type, TraceJsonType::Missing);
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}

#[test]
fn trace_v2_filter_preserves_item_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "active_names"
    expr:
      - "@input.users"
      - filter: ["@item.active"]
      - map:
          - "@item.name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"users":[{"name":"alice","active":true},{"name":"bob","active":false}]}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert_eq!(traced.output, json!([{ "active_names": ["alice"] }]));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}

#[test]
fn trace_v2_sort_by_preserves_item_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "names"
    expr:
      - "@input.users"
      - sort_by: ["@item.rank", "asc"]
      - map:
          - "@item.name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"users":[{"name":"bob","rank":2},{"name":"alice","rank":1}]}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert_eq!(traced.output, json!([{ "names": ["alice", "bob"] }]));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}

#[test]
fn trace_v2_reduce_preserves_acc_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "sum"
    expr:
      - "@input.numbers"
      - reduce:
          - ["@acc", { "+": "@item" }]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"numbers":[1,2,3]}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert_eq!(traced.output, json!([{ "sum": 6.0 }]));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}

#[test]
fn trace_v2_coalesce_preserves_short_circuit() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    expr:
      - "@input.name"
      - coalesce: ["@item.name"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}

#[test]
fn trace_v2_let_binding_is_visible_to_following_steps() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "discounted"
    expr:
      - "@input.price"
      - let: { factor: 0.9 }
      - multiply: ["@factor"]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"price":100}]"#;

    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, normal);
    assert_eq!(traced.output, json!([{ "discounted": 90.0 }]));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);
}
