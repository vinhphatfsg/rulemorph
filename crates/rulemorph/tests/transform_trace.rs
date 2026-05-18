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
include!("transform_trace/lifecycle.rs");

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
