#[test]
fn transform_stream_input_with_options_uses_normalization_options() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let options = NormalizationOptions {
        max_records: 1,
        ..NormalizationOptions::default()
    };
    let err = match transform_stream_input_with_options(
        &rule,
        InputData::Text(r#"[{"name":"alice"},{"name":"bob"}]"#),
        None,
        &options,
    ) {
        Ok(_) => panic!("record limit should be enforced"),
        Err(err) => err,
    };

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(
        err.message.contains("record"),
        "unexpected error message: {}",
        err.message
    );
}
