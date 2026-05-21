#[test]
fn rule_trace_duration_includes_finalize_duration() {
    let nodes = vec![json!({ "duration_us": 10 }), json!({ "duration_us": 15 })];
    let finalize = json!({ "duration_us": 7 });

    assert_eq!(sum_rule_trace_duration_us(&nodes, Some(&finalize)), 32);
}
