use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph::{
    InputData, TraceAttributeValue, TraceEvent, TraceEventKind, TransformTrace,
    TransformTraceOptions, parse_rule_file, transform, transform_input_with_trace,
    transform_input_with_trace_with_base_dir_and_options, transform_record,
    transform_record_with_trace, transform_with_base_dir,
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

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-trace-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[test]
fn trace_output_write_uses_output_snapshot_not_input_slot() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    let output_write = iter_trace_events(&traced.trace)
        .into_iter()
        .find(|event| event.kind == TraceEventKind::OutputWrite)
        .expect("output_write event");
    assert!(
        output_write.inputs.is_empty(),
        "output_write must not put the written value in inputs"
    );
    assert_eq!(
        output_write
            .output
            .as_ref()
            .and_then(|snapshot| snapshot.value.as_ref()),
        Some(&json!("alice"))
    );
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
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

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
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

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
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"users":[{"name":"alice"},{"name":"bob"}]}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

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
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"values":[1,2,3]}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

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
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"enabled":false}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

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
    let rule = parse_rule_file(yaml).expect("parse rule");
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
fn trace_record_api_matches_normal_finalize_behavior() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, Some(json!({"data":[{"name":"alice"}]})));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart),
        "single-record trace API must mirror normal finalize behavior"
    );
}

#[test]
fn trace_record_api_skips_finalize_when_record_is_dropped() {
    let yaml = r#"
version: 2
input:
  format: json
record_when: false
mappings:
  - target: "name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({"name":"alice"});

    let normal = transform_record(&rule, &record, None).expect("normal record transform");
    let traced = transform_record_with_trace(&rule, &record, None, &TransformTraceOptions::raw())
        .expect("traced record transform");

    assert_eq!(normal, None);
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .all(|event| event.kind != TraceEventKind::FinalizeStart),
        "dropped record must not run finalize in trace mode"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_return_preserves_child_finalize_null_output() {
    let dir = unique_temp_dir("branch-null-finalize");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "ignored"
    value: true
finalize:
  wrap: "@context.missing"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - branch:
      when: true
      then: child.yaml
      return: true
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform_with_base_dir(&rule, input, None, &dir).expect("normal transform");
    let traced = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(input),
        None,
        Some(&dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, json!([null]));
    assert_eq!(traced.output, normal);
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_child_rule_applies_finalize_like_normal_execution() {
    let dir = unique_temp_dir("branch-child-finalize");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "final_name"
    source: "name"
finalize:
  wrap:
    data: "@out"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - mappings:
      - target: "name"
        source: "name"
  - branch:
      when: true
      then: child.yaml
      return: true
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"name":"alice"}]"#;

    let normal = transform_with_base_dir(&rule, input, None, &dir).expect("normal transform");
    let traced = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(input),
        None,
        Some(&dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(normal, json!([{ "data": [{ "final_name": "alice" }] }]));
    assert_eq!(traced.output, normal);
    assert!(
        iter_trace_events(&traced.trace)
            .into_iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart),
        "branch child finalize should be visible in the record trace"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_branch_merge_error_closes_branch_and_step_spans() {
    let dir = unique_temp_dir("branch-merge-error");
    fs::write(
        dir.join("child.yaml"),
        r#"
version: 2
input:
  format: json
mappings:
  - target: "ignored"
    value: true
finalize:
  wrap: "@context.missing"
"#,
    )
    .expect("write child rule");
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - branch:
      when: true
      then: child.yaml
      return: false
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace_with_base_dir_and_options(
            &rule,
            InputData::Text(r#"[{}]"#),
            None,
            Some(&dir),
            &Default::default(),
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(
        result.is_ok(),
        "branch merge error must not leave open spans"
    );
    let err = result.expect("no panic").expect_err("traced error");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::InvalidTarget);
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
