#[test]
fn trace_v2_collection_operators_emit_item_level_events() {
    let cases = [
        (
            "filter",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - filter: ["@item.active"]
"#,
            json!([{ "out": [
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            ] }]),
        ),
        (
            "flat_map",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - flat_map: ["@item.tags"]
"#,
            json!([{ "out": ["a", "b", "c"] }]),
        ),
        (
            "reduce",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.scores"
      - reduce: [["@acc", { "+": "@item" }]]
"#,
            json!([{ "out": 6.0 }]),
        ),
        (
            "fold",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.scores"
      - fold:
          - 0
          - ["@acc", { "+": "@item" }]
"#,
            json!([{ "out": 6.0 }]),
        ),
        (
            "group_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - group_by: ["@item.role"]
"#,
            json!([{ "out": {
                "admin": [
                    {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                    {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
                ],
                "member": [
                    {"id":"u2","role":"member","active":true,"tags":["c"],"score":1}
                ]
            } }]),
        ),
        (
            "key_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - key_by: ["@item.id"]
"#,
            json!([{ "out": {
                "u1": {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                "u2": {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                "u3": {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            } }]),
        ),
        (
            "partition",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - partition: ["@item.active"]
"#,
            json!([{ "out": [
                [
                    {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                    {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
                ],
                [
                    {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2}
                ]
            ] }]),
        ),
        (
            "distinct_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - distinct_by: ["@item.role"]
"#,
            json!([{ "out": [
                {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1}
            ] }]),
        ),
        (
            "sort_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - sort_by: ["@item.score"]
"#,
            json!([{ "out": [
                {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
                {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
                {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
            ] }]),
        ),
        (
            "find",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - find: ["@item.active"]
"#,
            json!([{ "out": {"id":"u2","role":"member","active":true,"tags":["c"],"score":1} }]),
        ),
        (
            "find_index",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr:
      - "@input.users"
      - find_index: ["@item.active"]
"#,
            json!([{ "out": 1 }]),
        ),
    ];
    let input = r#"[{
        "users":[
            {"id":"u1","role":"admin","active":false,"tags":["a","b"],"score":2},
            {"id":"u2","role":"member","active":true,"tags":["c"],"score":1},
            {"id":"u3","role":"admin","active":true,"tags":[],"score":3}
        ],
        "scores":[1,2,3]
    }]"#;

    for (operator, yaml, expected) in cases {
        let rule = parse_rule_file(yaml).unwrap_or_else(|err| panic!("{operator} parse: {err:?}"));
        let normal = transform(&rule, input, None)
            .unwrap_or_else(|err| panic!("{operator} normal transform: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{operator} traced transform: {err:?}"));

        assert_eq!(normal, expected, "{operator} normal output");
        assert_eq!(traced.output, normal, "{operator} traced output");
        let events = iter_trace_events(&traced.trace);
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::CollectionItemStart
                    && event.operator.as_deref() == Some(operator)
            }),
            "{operator} should emit per-item start events"
        );
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::CollectionItemEnd
                    && event.operator.as_deref() == Some(operator)
                    && event.attributes.contains_key("item_index")
            }),
            "{operator} should emit per-item end/decision events"
        );
        assert_trace_shape(&traced.trace);
    }
}

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
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@acc.value")
        }),
        "acc-scoped ref reads must not be reported as @input paths"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_v2_map_nested_error_closes_collection_and_map_spans() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "items"
    expr:
      - "@input.items"
      - map:
          - divide: [0]
"#;
    let rule = parse_rule(yaml);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace(
            &rule,
            InputData::Text(r#"[{"items":[1]}]"#),
            None,
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(result.is_ok(), "map nested error must not leave open spans");
    let err = result.expect("no panic").expect_err("traced error");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::ExprError);
    assert_trace_shape(&err.trace);
}

#[test]
fn trace_v2_item_scoped_collection_ops_match_normal() {
    let cases = [
        (
            "flat_map",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "tags"
    expr:
      - "@input.users"
      - flat_map: ["@item.tags"]
"#,
            json!([{ "tags": ["a", "b", "c"] }]),
        ),
        (
            "group_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "by_role"
    expr:
      - "@input.users"
      - group_by: ["@item.role"]
"#,
            json!([{ "by_role": {
                "admin": [
                    {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                    {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
                ],
                "member": [
                    {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]}
                ]
            } }]),
        ),
        (
            "key_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "by_id"
    expr:
      - "@input.users"
      - key_by: ["@item.id"]
"#,
            json!([{ "by_id": {
                "u1": {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                "u2": {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
                "u3": {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
            } }]),
        ),
        (
            "partition",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "active_groups"
    expr:
      - "@input.users"
      - partition: ["@item.active"]
"#,
            json!([{ "active_groups": [
                [
                    {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
                    {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
                ],
                [
                    {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]}
                ]
            ] }]),
        ),
        (
            "distinct_by",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_by_role"
    expr:
      - "@input.users"
      - distinct_by: ["@item.role"]
"#,
            json!([{ "first_by_role": [
                {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
                {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]}
            ] }]),
        ),
        (
            "find",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_active"
    expr:
      - "@input.users"
      - find: ["@item.active"]
"#,
            json!([{ "first_active": {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]} }]),
        ),
        (
            "find_index",
            r#"
version: 2
input:
  format: json
mappings:
  - target: "first_active_index"
    expr:
      - "@input.users"
      - find_index: ["@item.active"]
"#,
            json!([{ "first_active_index": 1 }]),
        ),
    ];
    let input = r#"[{"users":[
        {"id":"u1","name":"Alice","role":"admin","active":false,"tags":["a","b"]},
        {"id":"u2","name":"Bob","role":"member","active":true,"tags":["c"]},
        {"id":"u3","name":"Carol","role":"admin","active":true,"tags":[]}
    ]}]"#;

    for (name, yaml, expected) in cases {
        assert_traced_output_matches_normal(yaml, input, expected);
        let rule = parse_rule_file(yaml).unwrap_or_else(|err| panic!("{name} parse: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(input),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{name} traced transform: {err:?}"));
        assert!(
            iter_trace_events(&traced.trace).iter().any(|event| {
                event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(name)
            }),
            "missing op_start for {name}"
        );
    }
}

#[test]
fn trace_v2_fold_preserves_acc_and_item_scope() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "sum"
    expr:
      - "@input.numbers"
      - fold:
          - 0
          - ["@acc", { "+": "@item" }]
"#;

    assert_traced_output_matches_normal(yaml, r#"[{"numbers":[1,2,3]}]"#, json!([{ "sum": 6.0 }]));
}

#[test]
fn trace_v2_let_binding_survives_nested_map_steps() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "totals"
    expr:
      - "@input.prices"
      - map:
          - let: { tax: 1.1 }
          - multiply: ["@tax"]
"#;

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"prices":[100,200]}]"#,
        json!([{ "totals": [110.00000000000001, 220.00000000000003] }]),
    );
}
