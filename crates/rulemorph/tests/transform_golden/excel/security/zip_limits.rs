#[test]
fn excel_rejects_zip_entry_count_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_zip_entries: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry count limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip total uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_zip_entry_uncompressed_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_entry_uncompressed_bytes: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("zip entry uncompressed limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_shared_strings_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_shared_strings: 1,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("shared strings limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}

#[test]
fn excel_rejects_styles_limit_exceeded() {
    let rule = load_rule(&fixtures_dir().join("t34_excel_input").join("rules.yaml"));
    let input =
        fs::read(fixtures_dir().join("t34_excel_input").join("input.xlsx")).expect("read xlsx");
    let options = NormalizationOptions {
        max_excel_styles: 0,
        ..NormalizationOptions::default()
    };
    let err = normalize_records_with_options(&rule, InputData::Bytes(&input), &options)
        .expect_err("styles limit should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
}
