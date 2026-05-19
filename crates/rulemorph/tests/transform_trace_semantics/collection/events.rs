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
