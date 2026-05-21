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
