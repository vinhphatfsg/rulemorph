#[test]
fn markdown_unsupported_body_markdown_is_rejected_during_transform() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_markdown: true
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");

    let err = transform(&rule, "# Guide", None)
        .expect_err("unsupported body_markdown should fail during transform");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("body_markdown"));
}

#[test]
fn markdown_unsupported_sourcepos_is_rejected_during_transform() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      sourcepos: true
mappings:
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");

    let err =
        transform(&rule, "# Guide", None).expect_err("unsupported sourcepos should fail during transform");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("sourcepos"));
}
