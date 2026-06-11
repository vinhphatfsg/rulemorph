#[test]
fn markdown_frontmatter_must_be_object() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(&rule, "---\n- bad\n---\n# Guide", None)
        .expect_err("array frontmatter should fail");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("frontmatter must be an object"));
}

#[test]
fn markdown_frontmatter_auto_rejects_matched_non_object_separator() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let err = transform(&rule, "---\n# separator-like document\n---\n# Actual title", None)
        .expect_err("matched auto frontmatter delimiter should require object frontmatter");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("frontmatter must be an object"));
}

#[test]
fn markdown_yaml_frontmatter_obeys_alias_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_yaml_aliases: 1,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("---\nbase: &base value\none: *base\ntwo: *base\n---\n# Guide"),
        None,
        &options,
    )
    .expect_err("yaml frontmatter should obey alias limits");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_yaml_aliases"));
}

#[test]
fn markdown_frontmatter_accepts_crlf_delimiters_in_auto_mode() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "owner"
    source: "input.frontmatter.owner"
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "---\r\nowner: docs\r\n---\r\n# Guide", None)
        .expect("crlf frontmatter should parse");
    assert_eq!(
        output,
        serde_json::json!([{ "owner": "docs", "title": "Guide" }])
    );
}

#[test]
fn markdown_frontmatter_accepts_eof_closing_delimiter() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    frontmatter: toml
mappings:
  - target: "owner"
    source: "input.frontmatter.owner"
  - target: "title"
    source: "input.title"
"#,
    )
    .expect("parse markdown rule");
    let output = transform(&rule, "+++\nowner = \"docs\"\ntitle = \"Guide\"\n+++", None)
        .expect("eof frontmatter closing delimiter should parse");
    assert_eq!(
        output,
        serde_json::json!([{ "owner": "docs", "title": "Guide" }])
    );
}

#[test]
fn markdown_frontmatter_auto_treats_unclosed_delimiter_as_body() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown: {}
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
  - target: "title"
    source: "input.title"
  - target: "body_text"
    source: "input.body_text"
"#,
    )
    .expect("parse markdown rule");
    let output =
        transform(&rule, "---\n# Guide", None).expect("unclosed auto delimiter should be body");
    assert_eq!(
        output,
        serde_json::json!([{ "frontmatter": {}, "title": "Guide", "body_text": "Guide" }])
    );
}

#[test]
fn markdown_toml_frontmatter_obeys_text_limit() {
    let rule = parse_rule_file(
        r#"
version: 2
input:
  format: markdown
  markdown:
    frontmatter: toml
mappings:
  - target: "frontmatter"
    source: "input.frontmatter"
"#,
    )
    .expect("parse markdown rule");
    let options = NormalizationOptions {
        max_text_bytes: 4,
        ..NormalizationOptions::default()
    };
    let err = transform_input_with_options(
        &rule,
        InputData::Text("+++\nowner = \"docs-team\"\n+++\n# Guide"),
        None,
        &options,
    )
    .expect_err("toml frontmatter should obey text limits");
    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_text_bytes"));
}
