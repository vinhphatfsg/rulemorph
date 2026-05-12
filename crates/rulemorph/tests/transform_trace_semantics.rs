use rulemorph::{
    InputData, TraceEvent, TraceEventKind, TransformTrace, TransformTraceOptions, parse_rule_file,
    transform, transform_input_with_trace,
};
use serde_json::{Value as JsonValue, json};

fn iter_trace_events(trace: &TransformTrace) -> Vec<&TraceEvent> {
    trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .chain(trace.finalize.iter().flatten())
        .collect()
}

fn assert_parent_ids_point_to_emitted_events(trace: &TransformTrace) {
    let ids = iter_trace_events(trace)
        .into_iter()
        .map(|event| event.id)
        .collect::<std::collections::BTreeSet<_>>();
    for event in iter_trace_events(trace) {
        if let Some(parent_id) = event.parent_id {
            assert!(
                ids.contains(&parent_id),
                "dangling parent_id {parent_id} on {:?}",
                event.kind
            );
        }
    }
}

fn assert_trace_paths_are_canonical(trace: &TransformTrace) {
    for event in iter_trace_events(trace) {
        if let Some(path) = event.input_path.as_deref() {
            assert!(
                path == "@input"
                    || path.starts_with("@input.")
                    || path.starts_with("@input[")
                    || path == "@item"
                    || path.starts_with("@item.")
                    || path.starts_with("@item[")
                    || path == "@acc"
                    || path.starts_with("@acc.")
                    || path.starts_with("@acc[")
                    || path == "@context"
                    || path.starts_with("@context.")
                    || path.starts_with("@context[")
                    || path == "@out"
                    || path.starts_with("@out.")
                    || path.starts_with("@out["),
                "non-canonical input_path: {path}"
            );
        }
        if let Some(path) = event.output_path.as_deref() {
            assert!(
                path == "$" || path.starts_with("$.") || path.starts_with("$["),
                "non-canonical output_path: {path}"
            );
        }
    }
}

fn assert_trace_shape(trace: &TransformTrace) {
    assert_parent_ids_point_to_emitted_events(trace);
    assert_trace_paths_are_canonical(trace);
}

fn assert_traced_output_matches_normal(yaml: &str, input: &str, expected: JsonValue) {
    let rule = parse_rule_file(yaml).expect("parse rule");
    let normal = transform(&rule, input, None).expect("normal transform");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, expected);
    assert_eq!(traced.output, normal);
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

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"secondary":"fallback"}]"#,
        json!([{ "name": "fallback" }]),
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
    let rule = parse_rule_file(yaml).expect("parse rule");
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

#[test]
fn trace_v2_if_does_not_evaluate_unselected_branch() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.enabled"
      - if:
          cond:
            eq: ["$", true]
          then:
            - "enabled"
          else:
            - "@item.label"
"#;

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"enabled":true}]"#,
        json!([{ "label": "enabled" }]),
    );
}
