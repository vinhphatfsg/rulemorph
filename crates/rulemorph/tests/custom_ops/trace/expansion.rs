use super::*;

#[test]
fn custom_op_trace_span_uses_metadata_without_raw_with_values_by_default() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
mappings:
  - target: slug
    expr: ["@input.title", slug]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"title":"  SECRET-TITLE  "}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!([{ "slug": "secret-title" }]));
    let events: Vec<_> = traced.trace.records[0].events.iter().collect();
    let start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("custom op start span");
    assert_eq!(start.operator.as_deref(), Some("slug"));
    assert!(matches!(
        start.attributes.get("with_adapter"),
        Some(TraceAttributeValue::Bool(false))
    ));
    assert!(matches!(
        start.attributes.get("def_path"),
        Some(TraceAttributeValue::String(value)) if value == "defs.slug"
    ));
    assert!(matches!(
        start.attributes.get("input_type"),
        Some(TraceAttributeValue::String(value)) if value == "string"
    ));
    assert!(matches!(
        start.attributes.get("output_type"),
        Some(TraceAttributeValue::String(value)) if value == "string"
    ));

    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("SECRET-TITLE"));
}

#[test]
fn custom_op_trace_expands_finalize_wrap_body_under_custom_op_span() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  normalize_status:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
mappings:
  - target: status
    expr: ["@input.status", normalize_status]
finalize:
  wrap:
    first_status: ["@out", first, { get: ["status"] }, normalize_status]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"status":" ACTIVE "}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!({ "first_status": "active" }));
    let finalize = traced
        .trace
        .finalize
        .as_ref()
        .expect("finalize trace exists");
    let start = finalize
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.rule_path.as_deref() == Some("finalize.wrap.first_status[3]")
                && event.operator.as_deref() == Some("normalize_status")
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("finalize wrap custom op start span");
    assert!(matches!(
        start.attributes.get("def_path"),
        Some(TraceAttributeValue::String(value)) if value == "defs.normalize_status"
    ));
    assert!(finalize.iter().any(|event| {
        event.kind == TraceEventKind::ExprStart
            && event.rule_path.as_deref() == Some("defs.normalize_status.expr")
    }));
}

#[test]
fn custom_op_trace_does_not_execute_ref_shorthand_named_invalid_def() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  "@foo":
    input: string
    returns: string
    expr: ["$", uppercase]
mappings:
  - target: result
    expr:
      - "@input.value"
      - op: "@foo"
"#;
    let rule = parse(yaml);
    let normal = transform(&rule, r#"[{"value":"x"}]"#, None)
        .expect_err("normal transform treats @foo as a ref shorthand");
    assert_eq!(normal.kind, TransformErrorKind::ExprError);
    assert!(normal.message.contains("undefined variable: @foo"));

    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"value":"x"}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect_err("trace should preserve normal transform semantics");
    assert_eq!(traced.error.kind, TransformErrorKind::ExprError);
    assert!(traced.error.message.contains("undefined variable: @foo"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_expands_expr_body_under_custom_op_span() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: string
    returns: string
    expr: ["$", trim, lowercase]
mappings:
  - target: slug
    expr: ["@input.title", slug]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"title":"  SECRET-TITLE  "}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!([{ "slug": "secret-title" }]));
    let events = &traced.trace.records[0].events;
    let custom_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.operator.as_deref() == Some("slug")
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
                && event.rule_path.as_deref() == Some("defs.slug.expr")
        })
        .expect("custom op body expr start");
    let trim_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.parent_id == Some(body_start.id)
                && event.operator.as_deref() == Some("trim")
                && event.rule_path.as_deref() == Some("defs.slug.expr[1]")
        })
        .expect("trim inside custom op body");
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::OpEnd
            && event.parent_id == Some(trim_start.id)
            && event.operator.as_deref() == Some("trim")
    }));

    let trace_text = serde_json::to_string(&traced.trace).expect("trace json");
    assert!(!trace_text.contains("SECRET-TITLE"));
    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_evaluates_with_args_before_body() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  line_total:
    input: { qty: int, unit_price: number }
    returns: number
    expr:
      - "$"
      - let:
          qty: ["$.qty", float]
          price: ["$.unit_price", float]
      - "@qty"
      - "*": ["@price"]
mappings:
  - target: total
    expr:
      - "@input.line"
      - line_total:
          - with: { qty: "$.quantity", unit_price: "$.price" }
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"line":{"quantity":3,"price":19.5}}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(traced.output, json!([{ "total": 58.5 }]));
    let events = &traced.trace.records[0].events;
    let custom_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.operator.as_deref() == Some("line_total")
                && matches!(
                    event.attributes.get("kind"),
                    Some(TraceAttributeValue::String(value)) if value == "custom_op"
                )
        })
        .expect("custom op start span");
    let qty_arg = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::ExprStart
                && event.parent_id == Some(custom_start.id)
                && event.rule_path.as_deref() == Some("mappings[0].expr[1].with.qty")
        })
        .expect("with qty expression");
    let body_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::ExprStart
                && event.parent_id == Some(custom_start.id)
                && event.rule_path.as_deref() == Some("defs.line_total.expr")
        })
        .expect("custom op body expression");
    assert!(qty_arg.id < body_start.id);

    assert!(!traced.trace.contains_raw_values);
}

#[test]
fn custom_op_trace_expands_mappings_body_under_custom_op_span() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  summary:
    input: { name: string, price: number }
    mappings:
      - target: label
        expr: ["$.name", trim, uppercase]
      - target: taxable
        when: { gt: ["$.price", 0] }
        value: true
mappings:
  - target: summary
    expr: ["@input.item", summary]
"#;
    let rule = parse(yaml);
    let traced = transform_input_with_trace(
        &rule,
        InputData::Text(r#"[{"item":{"name":"  pen ","price":120}}]"#),
        None,
        &TransformTraceOptions::metadata_only(),
    )
    .expect("trace succeeds");

    assert_eq!(
        traced.output,
        json!([{ "summary": { "label": "PEN", "taxable": true } }])
    );
    let events = &traced.trace.records[0].events;
    let custom_start = events
        .iter()
        .find(|event| {
            event.kind == TraceEventKind::OpStart
                && event.operator.as_deref() == Some("summary")
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
                && event.rule_path.as_deref() == Some("defs.summary.mappings[0]")
        })
        .expect("mapping body start");
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::ExprStart
            && event.parent_id == Some(mapping_start.id)
            && event.rule_path.as_deref() == Some("defs.summary.mappings[0].expr")
    }));
    assert!(events.iter().any(|event| {
        event.kind == TraceEventKind::OutputWrite
            && event.parent_id == Some(mapping_start.id)
            && event.rule_path.as_deref() == Some("defs.summary.mappings[0].target")
    }));
}
