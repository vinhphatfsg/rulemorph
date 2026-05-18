use std::fs;

mod common;

use common::trace::{
    assert_trace_shape, assert_traced_output_matches_normal, attr_bool, attr_number,
    iter_trace_events, parse_rule, transform_text_raw_trace, unique_temp_dir,
};
use rulemorph::{
    InputData, TraceAttributeValue, TraceEventKind, TransformTraceOptions, parse_rule_file,
    transform, transform_input_with_trace, transform_input_with_trace_with_base_dir_and_options,
    transform_record, transform_record_with_trace, transform_with_base_dir,
};
use serde_json::json;

#[test]
fn trace_v2_eager_operator_emits_arg_eval_for_actual_args() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.first"
      - concat: ["@input.second"]
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"first":"A","second":"B"}]"#);

    assert_eq!(traced.output, json!([{ "label": "AB" }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("concat")
                && attr_number(event, "arg_index") == Some(0)
                && event
                    .output
                    .as_ref()
                    .and_then(|snapshot| snapshot.value.as_ref())
                    == Some(&json!("B"))
        }),
        "v2 eager operator args should be traced when actually evaluated"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn source_path_helpers_preserve_context_out_and_escaped_input_paths() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: '["@id"]'
  - target: "tenant"
    source: "context.tenant"
  - target: "copied_id"
    source: "out.id"
"#;
    let rule = parse_rule(yaml);
    let context = json!({ "tenant": "acme" });
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"@id":"u1"}]"#),
        Some(&context),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([{ "id": "u1", "tenant": "acme", "copied_id": "u1" }])
    );
    assert_trace_shape(&traced.trace);
    let source_paths = iter_trace_events(&traced.trace)
        .into_iter()
        .filter(|event| event.kind == TraceEventKind::SourceRead)
        .filter_map(|event| event.input_path.as_deref())
        .collect::<Vec<_>>();
    assert!(source_paths.contains(&r#"@input["@id"]"#));
    assert!(source_paths.contains(&"@context.tenant"));
    assert!(source_paths.contains(&"@out.id"));
}

include!("transform_trace_semantics/short_circuit.rs");

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
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"name":"alice"}]"#);

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

include!("transform_trace_semantics/collection.rs");

#[test]
fn trace_record_when_and_mapping_when_include_internal_condition_trace() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.kind", "keep"]
mappings:
  - target: "name"
    when:
      eq: ["@input.enabled", true]
    source: "name"
"#;
    let rule = parse_rule(yaml);
    let traced =
        transform_text_raw_trace(&rule, r#"[{"kind":"keep","enabled":true,"name":"alice"}]"#);

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    let events = iter_trace_events(&traced.trace);
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.kind")
        }),
        "record_when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::RefRead
                && event.input_path.as_deref() == Some("@input.enabled")
        }),
        "mapping.when should expose internal ref evaluation"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("eq")
                && attr_number(event, "arg_index") == Some(1)
        }),
        "condition comparison should expose actual argument evaluation"
    );
    assert_trace_shape(&traced.trace);
}

#[test]
fn trace_finalize_filter_and_sort_emit_item_level_decisions() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "id"
    source: "id"
  - target: "active"
    source: "active"
  - target: "score"
    source: "score"
finalize:
  filter:
    eq: ["@item.active", true]
  sort:
    by: "score"
    order: "desc"
"#;
    let rule = parse_rule(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(
            r#"[
                {"id":"a","active":true,"score":2},
                {"id":"b","active":false,"score":5},
                {"id":"c","active":true,"score":3}
            ]"#,
        ),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([
            {"id":"c","active":true,"score":3},
            {"id":"a","active":true,"score":2}
        ])
    );
    let finalize_events = traced.trace.finalize.as_ref().expect("finalize trace");
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeFilter
                && attr_number(event, "item_index") == Some(1)
                && attr_bool(event, "kept") == Some(false)
        }),
        "finalize.filter should emit per-item kept decisions"
    );
    assert!(
        finalize_events.iter().any(|event| {
            event.kind == TraceEventKind::FinalizeSort
                && attr_number(event, "from_index") == Some(1)
                && attr_number(event, "to_index") == Some(0)
        }),
        "finalize.sort should emit item movement decisions"
    );
    assert_trace_shape(&traced.trace);
}

include!("transform_trace_semantics/operator_inventory.rs");

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
    let rule = parse_rule(yaml);
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
    let rule = parse_rule(yaml);
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
    let rule = parse_rule(yaml);
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
    let rule = parse_rule(yaml);
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
    let rule = parse_rule(yaml);
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
fn trace_branch_base_dir_error_closes_open_spans() {
    let dir = unique_temp_dir("branch-base-dir-error");
    let outside = unique_temp_dir("branch-base-dir-outside");
    let outside_rule = outside.join("outside.yaml");
    fs::write(
        &outside_rule,
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#,
    )
    .expect("write outside rule");
    let yaml = format!(
        r#"version: 2
input:
  format: json
  json: {{}}
steps:
  - branch:
      when: {{ eq: [1, 1] }}
      then: {}
"#,
        outside_rule.display()
    );
    let rule = parse_rule_file(&yaml).expect("parse rule");
    let err = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(r#"[{"id":1}]"#),
        None,
        Some(&dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect_err("outside branch should be rejected");

    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::InvalidInput);
    assert!(err.error.message.contains("base directory"));
    assert_trace_shape(&err.trace);

    let events = iter_trace_events(&err.trace);
    let branch_taken = events
        .iter()
        .find(|event| event.kind == TraceEventKind::BranchTaken)
        .expect("branch taken");
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::Error
                && event.parent_id == Some(branch_taken.id)
                && event
                    .message
                    .as_ref()
                    .is_some_and(|message| message.code == "BRANCH_ERROR")
        }),
        "branch error should close the branch span"
    );
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::Error
                && event
                    .message
                    .as_ref()
                    .is_some_and(|message| message.code == "STEP_ERROR")
        }),
        "step error should close the step span"
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
