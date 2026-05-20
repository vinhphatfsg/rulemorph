include!("operator_inventory/fixtures.rs");

#[test]
fn trace_v2_operator_fixture_table_covers_valid_inventory_representatives() {
    assert_eq!(
        OPERATOR_CASES
            .iter()
            .map(|(op, _)| *op)
            .collect::<std::collections::BTreeSet<_>>(),
        EXPECTED_INVENTORY
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        "trace fixture table must drift with valid v2 operator inventory"
    );

    for (operator, expr) in OPERATOR_CASES {
        let yaml = format!(
            r#"
version: 2
input:
  format: json
mappings:
  - target: "out"
    expr: {expr}
"#
        );
        let rule = parse_rule_file(&yaml).unwrap_or_else(|err| panic!("{operator} parse: {err:?}"));
        let normal = transform(&rule, OPERATOR_INPUT, None)
            .unwrap_or_else(|err| panic!("{operator} normal transform: {err:?}"));
        let traced = transform_input_with_trace(
            &rule,
            InputData::Text(OPERATOR_INPUT),
            None,
            &TransformTraceOptions::raw(),
        )
        .unwrap_or_else(|err| panic!("{operator} traced transform: {err:?}"));

        assert_eq!(traced.output, normal, "{operator}");
        let events = iter_trace_events(&traced.trace);
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::OpStart && event.operator.as_deref() == Some(operator)
            }),
            "{operator} missing op_start"
        );
        assert!(
            events.iter().any(|event| {
                event.kind == TraceEventKind::OpEnd && event.operator.as_deref() == Some(operator)
            }),
            "{operator} missing op_end"
        );
        assert_trace_shape(&traced.trace);
    }
}
