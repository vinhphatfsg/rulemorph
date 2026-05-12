use rulemorph::{
    InputData, TraceEvent, TraceEventKind, TraceJsonType, TracePhase, TraceValueSnapshot,
    TraceValueState, TransformTraceOptions, parse_rule_file, transform, transform_input_with_trace,
    transform_input_with_trace_with_base_dir_and_options,
};
use serde_json::json;

fn iter_trace_events(trace: &rulemorph::TransformTrace) -> Vec<&rulemorph::TraceEvent> {
    trace
        .records
        .iter()
        .flat_map(|record| record.events.iter())
        .chain(trace.finalize.iter().flatten())
        .collect()
}

fn assert_operator_lifecycle(trace: &rulemorph::TransformTrace, operator: &str) {
    let events = iter_trace_events(trace);
    let op_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(operator)
        })
        .copied()
        .unwrap_or_else(|| panic!("missing op_start for {operator}"));
    let has_end_or_error = events.iter().any(|event| {
        matches!(&event.kind, TraceEventKind::OpEnd | TraceEventKind::OpError)
            && event.operator.as_deref() == Some(operator)
            && event.parent_id == Some(op_start.id)
    });
    assert!(has_end_or_error, "missing op_end/op_error for {operator}");
}

fn assert_parent_ids_point_to_emitted_events(trace: &rulemorph::TransformTrace) {
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

fn assert_trace_paths_are_canonical(trace: &rulemorph::TransformTrace) {
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
                    || path == "@context"
                    || path.starts_with("@context.")
                    || path == "@out"
                    || path.starts_with("@out."),
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

fn assert_json_tree_does_not_contain_string(value: &serde_json::Value, needle: &str) {
    match value {
        serde_json::Value::String(text) => assert!(!text.contains(needle)),
        serde_json::Value::Array(items) => {
            for item in items {
                assert_json_tree_does_not_contain_string(item, needle);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                assert!(!key.contains(needle));
                assert_json_tree_does_not_contain_string(value, needle);
            }
        }
        _ => {}
    }
}

fn assert_trace_does_not_contain_string(trace: &rulemorph::TransformTrace, needle: &str) {
    let value = serde_json::to_value(trace).expect("trace json");
    assert_json_tree_does_not_contain_string(&value, needle);
}

fn assert_no_raw_leak_in_attributes_or_messages(
    trace: &rulemorph::TransformTrace,
    secrets: &[&str],
) {
    for event in iter_trace_events(trace) {
        let metadata = serde_json::json!({
            "attributes": &event.attributes,
            "message": &event.message,
        });
        let text = serde_json::to_string(&metadata).expect("metadata json");
        for secret in secrets {
            assert!(
                !text.contains(secret),
                "secret leaked through attributes/message"
            );
        }
    }
}

#[test]
fn trace_value_raw_mode_preserves_missing_null_and_empty_string() {
    let options = TransformTraceOptions::raw();

    let missing = TraceValueSnapshot::missing(&options, None);
    let null = TraceValueSnapshot::from_json(&json!(null), &options, None);
    let empty = TraceValueSnapshot::from_json(&json!(""), &options, None);

    assert_eq!(missing.state, TraceValueState::Missing);
    assert_eq!(missing.value_type, TraceJsonType::Missing);
    assert_eq!(null.state, TraceValueState::Null);
    assert_eq!(null.value_type, TraceJsonType::Null);
    assert_eq!(null.value, Some(json!(null)));
    assert_eq!(empty.state, TraceValueState::Present);
    assert_eq!(empty.value_type, TraceJsonType::String);
    assert_eq!(empty.value, Some(json!("")));
}

#[test]
fn trace_event_is_json_serializable() {
    let event = TraceEvent {
        id: 1,
        parent_id: None,
        kind: TraceEventKind::SourceRead,
        phase: TracePhase::Instant,
        rule_path: Some("mappings[0].source".to_string()),
        input_path: Some("@input.name".to_string()),
        output_path: None,
        namespace: Some("input".to_string()),
        operator: None,
        message: None,
        inputs: Vec::new(),
        output: Some(TraceValueSnapshot::from_json(
            &json!("alice"),
            &TransformTraceOptions::raw(),
            None,
        )),
        attributes: Default::default(),
    };

    let value = serde_json::to_value(event).expect("serialize trace event");
    assert_eq!(value["kind"], "source_read");
    assert_eq!(value["output"]["value"], "alice");
}

#[test]
fn enabling_trace_does_not_change_transform_output() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
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
    assert!(traced.warnings.is_empty());
    assert_eq!(traced.trace.records.len(), 1);
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
fn redacted_mode_hides_secret_like_paths() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "token"
    source: "api_token"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn redacted_mode_hides_secret_source_written_to_public_target() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "public_id"
    source: "api_token"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token"}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn redacted_mode_hides_secret_expr_ref_written_to_public_target() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "public_id"
    expr:
      - "@input.api_token"
      - trim
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":" secret-token "}]"#),
        None,
        &TransformTraceOptions::redacted(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "secret-token");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["secret-token"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("secret_like_path"));
}

#[test]
fn metadata_only_mode_never_serializes_raw_values() {
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
        &TransformTraceOptions::metadata_only(),
    )
    .expect("traced transform");

    assert_trace_does_not_contain_string(&traced.trace, "alice");
    assert_no_raw_leak_in_attributes_or_messages(&traced.trace, &["alice"]);
    let value = serde_json::to_value(&traced.trace).expect("trace json");
    assert!(value.to_string().contains("metadata_only"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn transform_trace_error_debug_display_and_error_are_raw_safe() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "token"
    source: "api_token"
  - target: "bad"
    expr:
      - "@input.age"
      - divide: 0
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"api_token":"secret-token","age":10}]"#),
        None,
        &TransformTraceOptions::raw(),
    )
    .expect_err("traced transform should fail");

    let debug = format!("{err:?}");
    let display = format!("{err}");
    assert!(!debug.contains("secret-token"));
    assert!(!debug.contains("api_token"));
    assert!(debug.contains("expr_error"));
    assert!(!display.contains("secret-token"));
    assert!(!display.contains("api_token"));
    assert!(display.contains("expr_error"));
    let source = std::error::Error::source(&err);
    assert!(
        source.is_none(),
        "TransformTraceError must not expose raw TransformError as source"
    );
    assert!(err.trace.contains_raw_values);
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
