#[test]
fn p01_preflight_ok() {
    let base = fixtures_dir().join("p01_preflight_ok");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));

    preflight_validate(&rule, &input, None).expect("preflight failed");
}

#[test]
fn p04_preflight_finalize_should_pass() {
    let base = fixtures_dir().join("tv32_steps_finalize");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read_to_string(base.join("input.json"))
        .unwrap_or_else(|_| panic!("failed to read input.json"));

    preflight_validate(&rule, &input, None).expect("preflight failed");
}
