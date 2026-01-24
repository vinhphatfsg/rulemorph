use rulemorph::{parse_rule_file, transform, TransformErrorKind};

#[test]
fn v1_pipe_ref_outside_pipe_errors() {
    let yaml = r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "value"
    expr:
      ref: "pipe.value"
"#;
    let rule = parse_rule_file(yaml).expect("failed to parse rules");
    let input = r#"{ "id": 1 }"#;
    let err = transform(&rule, input, None).expect_err("expected transform error");

    assert_eq!(err.kind, TransformErrorKind::ExprError);
    assert_eq!(err.path.as_deref(), Some("mappings[0].expr"));
}
