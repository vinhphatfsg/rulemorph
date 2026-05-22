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
    assert_eq!(traced.error.message, "assert failed: bad_input: expected ok");
}
