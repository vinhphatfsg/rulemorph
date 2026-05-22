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
