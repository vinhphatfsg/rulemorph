#[test]
fn validation_errors_include_location_with_source() {
    let rules_path = fixtures_dir()
        .join("v01_missing_mapping_value")
        .join("rules.yaml");
    let yaml = fs::read_to_string(&rules_path)
        .unwrap_or_else(|_| panic!("failed to read {}", rules_path.display()));
    let rule = parse_rule_file(&yaml).unwrap();
    let errors = validate_rule_file_with_source(&rule, &yaml).unwrap_err();
    let error = errors
        .iter()
        .find(|err| err.code == ErrorCode::MissingMappingValue)
        .expect("expected MissingMappingValue");
    let location = error.location.clone().expect("expected location");
    assert_eq!(location.line, 7);
}
