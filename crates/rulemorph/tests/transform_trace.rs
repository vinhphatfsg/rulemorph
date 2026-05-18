mod common;

use common::trace::{
    assert_no_raw_leak_in_attributes_or_messages, assert_operator_lifecycle,
    assert_parent_ids_point_to_emitted_events, assert_trace_does_not_contain_string,
    assert_trace_paths_are_canonical, iter_trace_events,
};
use rulemorph::{
    InputData, TraceEvent, TraceEventKind, TraceJsonType, TracePhase, TraceValueSnapshot,
    TraceValueState, TransformTraceOptions, parse_rule_file, transform, transform_input_with_trace,
    transform_input_with_trace_with_base_dir_and_options,
};
use serde_json::json;

include!("transform_trace/schema.rs");
include!("transform_trace/redaction.rs");
include!("transform_trace/v2.rs");

#[test]
fn trace_max_snapshot_bytes_marks_incomplete_without_breaking_parent_ids() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "payload"
    source: "payload"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = TransformTraceOptions::raw();
    options.max_snapshot_bytes = Some(8);

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"payload":"this string is deliberately oversized"}]"#),
        None,
        &options,
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([{ "payload": "this string is deliberately oversized" }])
    );
    assert!(!traced.trace.complete);
    assert_eq!(
        traced
            .trace
            .truncation
            .as_ref()
            .map(|truncation| truncation.reason.as_str()),
        Some("max_snapshot_bytes")
    );
    assert_parent_ids_point_to_emitted_events(&traced.trace);

    let events = iter_trace_events(&traced.trace);
    let snapshots = events
        .iter()
        .flat_map(|event| event.inputs.iter().chain(event.output.iter()));
    assert!(snapshots.into_iter().any(|snapshot| {
        snapshot.visibility.as_deref() == Some("truncated")
            && snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes")
            && !snapshot.contains_raw_value
            && snapshot.value.is_none()
    }));
}

#[test]
fn trace_max_trace_bytes_freezes_with_truncation_reason() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let mut options = TransformTraceOptions::raw();
    options.max_trace_bytes = Some(1);

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        &options,
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!([{ "name": "alice" }]));
    assert!(!traced.trace.complete);
    let truncation = traced.trace.truncation.as_ref().expect("truncation");
    assert_eq!(truncation.reason, "max_trace_bytes");
    assert_eq!(
        truncation.emitted_events,
        iter_trace_events(&traced.trace).len()
    );
    assert_parent_ids_point_to_emitted_events(&traced.trace);
}

#[test]
fn trace_records_source_default_and_output_write_with_raw_values() {
    let yaml = r#"
version: 2
input:
  format: json
record_when:
  eq: ["@input.active", true]
mappings:
  - target: "profile.name"
    source: "name"
  - target: "profile.nickname"
    source: "nickname"
    default: "anonymous"
  - target: "profile.skip"
    source: "skip"
    when:
      eq: ["@input.include_skip", true]
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"active":true,"name":"alice"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    let events = &traced.trace.records[0].events;
    let kinds = events
        .iter()
        .map(|event| serde_json::to_value(&event.kind).unwrap())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&json!("record_when_start")));
    assert!(kinds.contains(&json!("record_when_end")));
    assert!(kinds.contains(&json!("source_read")));
    assert!(kinds.contains(&json!("default_applied")));
    assert!(kinds.contains(&json!("mapping_when_start")));
    assert!(kinds.contains(&json!("mapping_when_end")));
    assert!(kinds.contains(&json!("mapping_decision")));
    assert!(kinds.contains(&json!("output_write")));

    let raw_values = serde_json::to_string(&traced.trace).expect("serialize trace");
    assert!(raw_values.contains("alice"));
    assert!(raw_values.contains("anonymous"));
}

#[test]
fn trace_v1_expression_operator_lifecycle() {
    let yaml = r#"
version: 1
input:
  format: json
mappings:
  - target: "label"
    expr:
      op: "concat"
      args:
        - ref: "input.first"
        - " "
        - ref: "input.last"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"first":"Ada","last":"Lovelace"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_operator_lifecycle(&traced.trace, "concat");

    let text = serde_json::to_string(&traced.trace).unwrap();
    assert!(text.contains("Ada Lovelace"));
}

#[test]
fn trace_step_record_when_error_closes_open_spans() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - record_when: "@input.name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        transform_input_with_trace(
            &rule,
            InputData::Text(r#"[{"name":"alice"}]"#),
            None,
            &TransformTraceOptions::raw(),
        )
    }));

    assert!(
        result.is_ok(),
        "record_when trace error must not leave an open span"
    );
    let err = result
        .expect("no panic")
        .expect_err("record_when should fail");
    assert_eq!(err.error.kind, rulemorph::TransformErrorKind::ExprError);
}

#[test]
fn trace_assert_failure_uses_configured_error_message() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - asserts:
      - when:
          eq: ["@input.ok", true]
        error:
          code: "bad_input"
          message: "expected ok"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"ok":false}]"#;

    let normal = transform(&rule, input, None).expect_err("normal assertion error");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(input),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced assertion error");

    assert_eq!(traced.error, normal);
    assert_eq!(
        traced.error.message,
        "assert failed: bad_input: expected ok"
    );
}

#[test]
fn trace_steps_record_when_asserts_and_finalize_as_spans() {
    let yaml = r#"
version: 2
input:
  format: json
steps:
  - mappings:
      - target: "name"
        source: "name"
  - record_when:
      eq: ["@out.name", "alice"]
  - asserts:
      - when:
          eq: ["@out.name", "alice"]
        error:
          code: "bad_name"
          message: "bad name"
finalize:
  filter:
    eq: ["@item.name", "alice"]
  sort:
    by: "name"
    order: "asc"
  offset: 0
  limit: 1
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"name":"alice"},{"name":"bob"}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(traced.output, json!({"data":[{"name":"alice"}]}));
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);

    let events = iter_trace_events(&traced.trace);
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::StepStart)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::AssertEval)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeStart)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeFilter)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeSort)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeLimit)
    );
    assert!(
        events
            .iter()
            .any(|event| event.kind == TraceEventKind::FinalizeWrap)
    );

    let finalize_start = events
        .iter()
        .find(|event| event.kind == TraceEventKind::FinalizeStart)
        .expect("finalize start");
    for kind in [
        TraceEventKind::FinalizeFilter,
        TraceEventKind::FinalizeSort,
        TraceEventKind::FinalizeLimit,
        TraceEventKind::FinalizeWrap,
    ] {
        assert!(
            events
                .iter()
                .any(|event| event.kind == kind && event.parent_id == Some(finalize_start.id)),
            "missing child finalize event for {kind:?}"
        );
    }
}

#[test]
fn trace_branch_child_rule_stays_in_same_record_trace() {
    let temp_dir =
        std::env::temp_dir().join(format!("rulemorph-trace-branch-{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).expect("create temp dir");
    let child_path = temp_dir.join("child.yaml");
    std::fs::write(
        &child_path,
        r#"
version: 2
input:
  format: json
mappings:
  - target: "branch_value"
    source: "name"
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
      when:
        eq: ["@out.name", "alice"]
      then: "child.yaml"
      return: false
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace_with_base_dir_and_options(
        &rule,
        InputData::Text(r#"[{"name":"alice"}]"#),
        None,
        Some(&temp_dir),
        &Default::default(),
        &TransformTraceOptions::raw(),
    )
    .expect("traced transform");

    assert_eq!(
        traced.output,
        json!([{ "name": "alice", "branch_value": "alice" }])
    );
    assert_eq!(traced.trace.records.len(), 1);
    assert_parent_ids_point_to_emitted_events(&traced.trace);
    assert_trace_paths_are_canonical(&traced.trace);

    let events = iter_trace_events(&traced.trace);
    let branch_taken = events
        .iter()
        .find(|event| event.kind == TraceEventKind::BranchTaken)
        .expect("branch_taken");
    assert!(
        events.iter().any(|event| {
            event.kind == TraceEventKind::MappingStart && event.parent_id == Some(branch_taken.id)
        }),
        "child rule mapping must be nested under branch_taken"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == TraceEventKind::RecordStart)
            .count(),
        1,
        "branch child rule must not call start_record"
    );
}
