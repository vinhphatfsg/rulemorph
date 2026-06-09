#[test]
fn v2_valid_rules_should_pass_validation() {
    let cases = [
        "tv22_basic",
        "tv23_steps",
        "tv24_conditions",
        "tv25_lookup",
        "tv27_v1_compat",
        "tv28_map_let_binding",
        "tv29_v2_out_sibling_ok",
        "tv30_literal_escape",
        "tv31_v2_json_ops_pick_omit_reduce_fold",
        "tv32_steps_finalize",
        "tv33_branch_return",
        "tv34_branch_return_true",
        "tv35_finalize_wrap",
        "tv36_branch_uses_out",
        "tv37_root_refs",
        "tv38_finalize_filter_offset",
        "tv39_finalize_filter_index",
        "tv40_branch_return_filter",
        "tv41_branch_return_out_update",
        "tv41_branch_finalize_wrap",
        "tv42_branch_deep_merge",
        "tv44_math_ops",
        "tv45_custom_ops_dot_path_body",
        "tv46_custom_ops_body_input_and_pipe_refs",
        "tv47_typed_value_dynamodb_item_shorthand",
        "tv48_typed_value_dynamodb_item_codec_binding",
        "tv49_typed_value_firestore_document",
        "tv50_typed_value_mongo_extended_json",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}

type OperatorCase = (&'static str, &'static str);

include!("../../transform_trace_semantics/operator_inventory/fixtures/cases.rs");

#[test]
fn v2_operator_inventory_representatives_should_pass_validation() {
    for (operator, expr) in OPERATOR_CASES {
        let yaml = format!(
            r#"
version: 2
input:
  format: json
  json: {{}}
mappings:
  - target: out
    expr: {expr}
"#
        );
        let rule = parse_rule_file(&yaml)
            .unwrap_or_else(|err| panic!("{operator} representative should parse: {err}"));
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|err| err.code.as_str()).collect();
            panic!("{operator} representative should validate, got {codes:?}");
        }
    }
}

#[test]
fn v2_json_pipe_input_ops_should_accept_pipe_value_as_operand() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: flattened
    expr: ["@input.nested", "object_flatten"]
  - target: unflattened
    expr: ["@input.flat", "object_unflatten"]
  - target: from_entries_pairs
    expr: ["@input.entries", "from_entries"]
  - target: from_entries_pair
    expr: ["@input.key", { from_entries: "@input.value" }]
"#,
    )
    .expect("rule parses");

    validate_rule_file(&rule).expect("JSON pipe-input operators validate");
}
