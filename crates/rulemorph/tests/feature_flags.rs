#[cfg(not(feature = "html"))]
#[test]
fn html_input_returns_invalid_input_when_feature_is_disabled() {
    let rule = rulemorph::parse_rule_file(
        r#"
version: 2
input:
  format: html
  html:
    records_selector: ".item"
    fields:
      id:
        selector: ".id"
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse html rule");

    let err = rulemorph::transform(
        &rule,
        r#"<div class="item"><span class="id">1</span></div>"#,
        None,
    )
    .expect_err("html input should fail when html feature is disabled");

    assert_eq!(err.kind, rulemorph::TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "input format html is not enabled in this build"
    );
}

#[cfg(not(feature = "excel"))]
#[test]
fn excel_input_returns_invalid_input_when_feature_is_disabled() {
    let rule = rulemorph::parse_rule_file(
        r#"
version: 2
input:
  format: excel
  excel:
    has_header: true
mappings:
  - target: "id"
    source: "id"
"#,
    )
    .expect("parse excel rule");

    let err = rulemorph::transform_input(&rule, rulemorph::InputData::Bytes(b"not xlsx"), None)
        .expect_err("excel input should fail when excel feature is disabled");

    assert_eq!(err.kind, rulemorph::TransformErrorKind::InvalidInput);
    assert_eq!(
        err.message,
        "input format excel is not enabled in this build"
    );
}
