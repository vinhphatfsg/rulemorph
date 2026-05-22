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
    let ops = build_mapping_ops_with_values(&mappings, &record, None, &mut out, 2, 0);
    let duration = ops[0].get("duration_us").and_then(|value| value.as_u64());
    assert!(duration.is_some());
}
