use super::*;

#[test]
fn custom_op_trace_keeps_body_error_inside_custom_op_error_span() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  explode:
    input: number
    returns: number
    expr: ["$", { divide: 0 }]
mappings:
  - target: value
    expr: ["@input.value", explode]
"#;
    let rule = parse(yaml);
    let err = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"value":10}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect_err("custom op body should fail");

    let events = &err.trace.records[0].events;
    let custom_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.operator.as_deref() == Some("explode")
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("custom op start span");
    let body_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::ExprStart
                && event.parent_id == Some(custom_start.id)
                && event.rule_path.as_deref() == Some("defs.explode.expr")
        })
        .expect("body expr start");
    let divide_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.parent_id == Some(body_start.id)
                && event.operator.as_deref() == Some("divide")
                && event.rule_path.as_deref() == Some("defs.explode.expr[1]")
        })
        .expect("inner operator start");
    let inner_error = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpError
                && event.parent_id == Some(divide_start.id)
                && event.operator.as_deref() == Some("divide")
                && event.rule_path.as_deref() == Some("defs.explode.expr[1]")
        })
        .expect("inner operator error");
    assert!(inner_error.id < events.last().expect("trace events").id);
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::OpError
            && event.parent_id == Some(custom_start.id)
            && event.operator.as_deref() == Some("explode")
            && matches!(
                event.attributes.get("kind"),
                Some(TraceAttributeValue::String(value)) if value == "custom_op"
            )
    }));
    assert!(!err.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_preserves_mappings_body_when_error_path() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  check:
    input: { price: json }
    mappings:
      - target: ok
        when: { gt: ["$.price", 0] }
        value: true
mappings:
  - target: result
    expr: ["@input", check]
"#;
    let rule = parse(yaml);
    let normal = transform(&rule, r#"[{"price":{}}]"#, None)
        .expect_err("normal transform should fail in custom body when");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"price":{}}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect_err("trace should preserve normal transform error path");

    assert_eq!(normal.kind, TransformErrorKind::ExprError);
    assert_eq!(traced.error.kind, normal.kind);
    assert_eq!(normal.path.as_deref(), Some("defs.check.mappings[0]"));
    assert_eq!(traced.error.path, normal.path);
    assert!(
        traced.trace.records[0]
            .events
            .iter()
            .any(|event| { event.rule_path.as_deref() == Some("defs.check.mappings[0].when") })
    );
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_preserves_mappings_body_when_nested_error_path() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  check:
    input: { price: number }
    mappings:
      - target: ok
        when: { gt: [["$.price", { divide: 0 }], 0] }
        value: true
mappings:
  - target: result
    expr: ["@input", check]
"#;
    let rule = parse(yaml);
    let normal = transform(&rule, r#"[{"price":5}]"#, None)
        .expect_err("normal transform should fail inside custom body when");
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"price":5}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect_err("trace should preserve nested normal transform error path");

    assert_eq!(normal.kind, TransformErrorKind::ExprError);
    assert_eq!(traced.error.kind, normal.kind);
    assert_eq!(
        normal.path.as_deref(),
        Some("defs.check.mappings[0].args[0][1].args[0]")
    );
    assert_eq!(traced.error.path, normal.path);
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_closes_mappings_body_span_on_target_error() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  bad_target:
    input: string
    mappings:
      - target: items[0]
        expr: "$"
mappings:
  - target: value
    expr: ["@input.value", bad_target]
"#;
    let rule = parse(yaml);
    let err = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"value":"secret-value"}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect_err("custom op mapping target should fail");

    let events = &err.trace.records[0].events;
    let custom_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.operator.as_deref() == Some("bad_target")
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("custom op start span");
    let mapping_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::MappingStart
                && event.parent_id == Some(custom_start.id)
                && event.rule_path.as_deref() == Some("defs.bad_target.mappings[0]")
        })
        .expect("mapping body start");
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::Error
            && event.parent_id == Some(mapping_start.id)
            && event.rule_path.as_deref() == Some("defs.bad_target.mappings[0]")
    }));
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::OpError
            && event.parent_id == Some(custom_start.id)
            && event.operator.as_deref() == Some("bad_target")
            && matches!(
                event.attributes.get("kind"),
                Some(TraceAttributeValue::String(value)) if value == "custom_op"
            )
    }));

    let trace_text = serde_json::to_string(&err.trace).expect("trace json");
    assert!(!trace_text.contains("secret-value"));
    assert!(!err.trace.contains_raw_values);
}
