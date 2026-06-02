#[test]
fn mapping_ops_include_duration_us() {
    let mappings = vec![Mapping {
        target: "name".to_string(),
        source: None,
        value: Some(json!("hello")),
        expr: None,
        when: None,
        value_type: None,
        required: false,
        default: None,
    }];
    let record = json!({});
    let mut out = json!({});
    let ops = build_mapping_ops_with_values(None, &mappings, &record, None, &mut out, 2, 0, None);
    let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
    assert!(duration.is_some());
}

fn chained_custom_op_defs(call_count: usize) -> String {
    let mut defs = "defs:\n".to_string();
    for index in 0..call_count {
        let expr = if index + 1 == call_count {
            r#"["$"]"#.to_string()
        } else {
            format!(r#"["$", d{}]"#, index + 1)
        };
        defs.push_str(&format!(
            "  d{}:\n    input: int\n    returns: int\n    expr: {}\n",
            index, expr
        ));
    }
    defs
}

fn trace_graph_budget_rule() -> rulemorph::RuleFile {
    parse_rule_file(&format!(
        r#"
version: 2
input:
  format: json
  json: {{}}
{}
mappings:
  - target: first
    expr: ["@input.items", {{ map: [d0] }}]
  - target: second
    expr: ["@input.items", {{ map: [d0] }}]
"#,
        chained_custom_op_defs(64)
    ))
    .expect("parse rule")
}

#[test]
fn trace_graph_pipe_steps_evaluate_custom_with_call() {
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
          qty: ["$", { get: ["qty"] }, float]
          price: ["$", { get: ["unit_price"] }, float]
      - "@qty"
      - "*": ["@price"]
mappings:
  - target: total
    expr:
      - "@input.line"
      - line_total:
          - with: { qty: "$.quantity", unit_price: "$.price" }
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "line": { "quantity": 3, "price": 19.5 } });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, std::path::Path::new("."));
    let children = trace.nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("mapping children");
    let op = &children[0];
    assert_eq!(op.get("output"), Some(&json!(58.5)));

    let pipe_steps = op
        .get("pipe_steps")
        .and_then(|value| value.as_array())
        .expect("pipe steps");
    let last = pipe_steps.last().expect("custom call step");
    assert_eq!(last.get("label"), Some(&json!("line_total")));
    assert_eq!(last.get("output"), Some(&json!(58.5)));
}

#[test]
fn trace_graph_pipe_steps_render_literal_start_custom_call() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
defs:
  slug:
    input: { title: string }
    returns: string
    expr: ["$.title", trim, lowercase]
mappings:
  - target: value
    expr:
      - slug:
          - with: { title: "@input.title" }
      - uppercase
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "title": "  Hello  " });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, std::path::Path::new("."));
    let children = trace.nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("mapping children");
    let op = &children[0];
    assert_eq!(op.get("output"), Some(&json!("HELLO")));

    let pipe_steps = op
        .get("pipe_steps")
        .and_then(|value| value.as_array())
        .expect("pipe steps");
    assert_eq!(pipe_steps[0].get("label"), Some(&json!("slug")));
    assert_eq!(pipe_steps[0].get("output"), Some(&json!("hello")));
    let last = pipe_steps.last().expect("uppercase step");
    assert_eq!(last.get("label"), Some(&json!("uppercase")));
    assert_eq!(last.get("output"), Some(&json!("HELLO")));
}

#[test]
fn trace_graph_pipe_steps_share_custom_op_call_budget() {
    let rule = trace_graph_budget_rule();
    let items: Vec<i64> = (0..780).collect();
    let record = json!({ "items": items });

    let trace = build_rule_nodes_from_rule(&rule, &record, None, std::path::Path::new("."));
    let children = trace.nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("mapping children");

    assert_eq!(children.len(), 2);
    assert!(
        children[0]
            .get("output")
            .and_then(|value| value.as_array())
            .is_some()
    );
    let first_pipe_step = children[0]
        .get("pipe_steps")
        .and_then(|value| value.as_array())
        .and_then(|steps| steps.last())
        .expect("first mapping pipe step");
    assert_eq!(first_pipe_step.get("status"), Some(&json!("ok")));
    assert!(
        children[1].get("output").is_some_and(|value| value.is_null()),
        "display-only pipe_steps must share the trace graph custom-op budget"
    );
}

#[test]
fn trace_graph_pipe_steps_report_custom_op_budget_errors() {
    let rule = trace_graph_budget_rule();
    let items: Vec<i64> = (0..782).collect();
    let record = json!({ "items": items });

    let trace = build_rule_nodes_from_rule(&rule, &record, None, std::path::Path::new("."));
    let children = trace.nodes[0]
        .get("children")
        .and_then(|value| value.as_array())
        .expect("mapping children");

    assert_eq!(children.len(), 2);
    assert!(
        children[0]
            .get("output")
            .and_then(|value| value.as_array())
            .is_some()
    );
    let first_pipe_step = children[0]
        .get("pipe_steps")
        .and_then(|value| value.as_array())
        .and_then(|steps| steps.last())
        .expect("first mapping pipe step");
    assert_eq!(first_pipe_step.get("status"), Some(&json!("error")));
    assert!(
        first_pipe_step
            .get("error")
            .and_then(|value| value.get("message"))
            .and_then(|value| value.as_str())
            .is_some_and(|message| message.contains("custom op calls per record")),
        "pipe step should surface the custom-op budget error"
    );
    assert!(
        children[1].get("output").is_some_and(|value| value.is_null()),
        "second mapping output should be null after the shared custom-op call budget is exhausted"
    );
}
