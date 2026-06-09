#[test]
fn markdown_section_is_required_when_format_is_markdown() {
    let yaml = r#"
version: 2
input:
  format: markdown
mappings:
  - target: "title"
    source: "title"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("missing markdown section should fail");
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "MissingMarkdownSection"
            && err.path.as_deref() == Some("input.markdown")
    }));
}

#[test]
fn markdown_empty_section_uses_defaults() {
    let yaml = r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "title"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("empty markdown section should be valid");
}

#[test]
fn markdown_section_levels_must_be_unique_heading_levels() {
    let yaml = r#"
version: 2
input:
  format: markdown
  markdown:
    records: sections
    section_levels: [0, 2, 2, 7]
mappings:
  - target: "heading"
    source: "heading"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("invalid section levels should fail");
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "InvalidInputOption"
            && err.path.as_deref() == Some("input.markdown.section_levels[0]")
    }));
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "DuplicateInputField"
            && err.path.as_deref() == Some("input.markdown.section_levels[2]")
    }));
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "InvalidInputOption"
            && err.path.as_deref() == Some("input.markdown.section_levels[3]")
    }));
}

#[test]
fn markdown_body_markdown_and_sourcepos_are_currently_rejected() {
    let yaml = r#"
version: 2
input:
  format: markdown
  markdown:
    include:
      body_markdown: true
      sourcepos: true
mappings:
  - target: "title"
    source: "title"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let errors = validate_rule_file(&rule).expect_err("unsupported source options should fail");
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "InvalidInputOption"
            && err.path.as_deref() == Some("input.markdown.include.body_markdown")
    }));
    assert!(errors.iter().any(|err| {
        err.code.as_str() == "InvalidInputOption"
            && err.path.as_deref() == Some("input.markdown.include.sourcepos")
    }));
}

#[test]
fn unselected_markdown_section_is_ignored_by_normal_validation() {
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
  markdown:
    records: sections
    section_levels: [0]
mappings:
  - target: "id"
    source: "id"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    validate_rule_file(&rule).expect("unselected markdown section should be ignored");
}
