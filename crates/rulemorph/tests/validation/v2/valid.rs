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
        "tv36_branch_uses_out",
        "tv39_finalize_filter_index",
        "tv41_branch_return_out_update",
    ];

    for case in cases {
        let rule = load_rule(case);
        if let Err(errors) = validate_rule_file(&rule) {
            let codes: Vec<&'static str> = errors.iter().map(|e| e.code.as_str()).collect();
            panic!("expected valid rules for {}, got {:?}", case, codes);
        }
    }
}
