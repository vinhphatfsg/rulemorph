#[test]
fn excel_input_with_header_normalizes_rows() {
    let base = fixtures_dir().join("t34_excel_input");
    let rule = load_rule(&base.join("rules.yaml"));
    let input = fs::read(base.join("input.xlsx")).expect("read xlsx");
    let output =
        transform_input(&rule, InputData::Bytes(&input), None).expect("transform excel input");
    let expected = load_json(&base.join("expected.json"));
    assert_eq!(output, expected);
}
