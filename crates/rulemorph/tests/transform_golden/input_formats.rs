#[test]
fn t30_json_rule_file_transform_golden() {
    let base = fixtures_dir().join("t30_json_rule_file");
    let rule = load_rule_with_format(&base.join("rules.json"), RuleFormat::Json);
    let input = fs::read_to_string(base.join("input.json")).expect("read input.json");
    let expected = load_json(&base.join("expected.json"));
    let output = transform(&rule, &input, None).expect("transform failed");
    assert_eq!(output, expected);
}

#[test]
fn t31_yaml_input_transform_golden() {
    assert_text_fixture("t31_yaml_input", "input.yaml");
}

#[test]
fn t32_toml_input_transform_golden() {
    assert_text_fixture("t32_toml_input", "input.toml");
}

#[test]
fn t33_xml_input_transform_golden() {
    assert_text_fixture("t33_xml_input", "input.xml");
}

#[test]
#[cfg(feature = "html")]
fn t35_html_input_transform_golden() {
    assert_text_fixture("t35_html_input", "input.html");
}

#[test]
#[cfg(feature = "excel")]
fn t36_spreadsheets_plugin_products() {
    assert_xlsx_fixture("t36_spreadsheets_plugin_products");
}

#[test]
#[cfg(feature = "excel")]
fn t37_spreadsheets_plugin_orders() {
    assert_xlsx_fixture("t37_spreadsheets_plugin_orders");
}

#[test]
#[cfg(feature = "excel")]
fn t38_spreadsheets_plugin_survey() {
    assert_xlsx_fixture("t38_spreadsheets_plugin_survey");
}

#[test]
fn t39_pyproject_dependency_inventory() {
    assert_text_fixture("t39_pyproject_dependency_inventory", "input.toml");
}

#[test]
fn t40_cargo_dependency_feature_inventory() {
    assert_text_fixture("t40_cargo_dependency_feature_inventory", "input.toml");
}

#[test]
fn t41_github_actions_matrix() {
    assert_text_fixture("t41_github_actions_matrix", "input.yaml");
}

#[test]
fn t42_openapi_endpoint_catalog() {
    assert_text_fixture("t42_openapi_endpoint_catalog", "input.yaml");
}

#[test]
fn t43_mongodb_schema_summary() {
    assert_text_fixture("t43_mongodb_schema_summary", "input.json");
}
