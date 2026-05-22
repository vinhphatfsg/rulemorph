#[test]
fn finalize_trace_includes_operation_nodes_in_order() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "score"
    source: "input.score"
finalize:
  filter:
    gte: ["@item.score", 10]
  sort:
    by: "score"
    order: "asc"
  limit: 1
  offset: 0
  wrap:
    data: "@out"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let record = json!({ "score": 12 });
    let trace = build_rule_nodes_from_rule(&rule, &record, None, Path::new("."));
    let finalize = trace.finalize.expect("finalize trace");

    assert_eq!(
        finalize.get("status").and_then(|value| value.as_str()),
        Some("ok")
    );
    assert!(
        finalize
            .get("duration_us")
            .and_then(|value| value.as_u64())
            .is_some()
    );
    assert_eq!(
        finalize
            .get("input")
            .and_then(|value| value.as_array())
            .map(|items| items.len()),
        Some(1)
    );
    let labels: Vec<&str> = finalize
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("finalize nodes")
        .iter()
        .map(|node| {
            node.get("label")
                .and_then(|value| value.as_str())
                .expect("node label")
        })
        .collect();
    assert_eq!(labels, vec!["filter", "sort", "limit", "offset", "wrap"]);
    assert_eq!(
        finalize
            .get("nodes")
            .and_then(|value| value.as_array())
            .and_then(|nodes| nodes.first())
            .and_then(|node| node.get("args"))
            .and_then(|args| args.get("expr")),
        Some(&json!({ "gte": ["@item.score", 10] }))
    );
}
