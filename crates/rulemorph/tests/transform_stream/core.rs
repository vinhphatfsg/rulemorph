#[test]
fn transform_stream_yields_outputs_and_warnings() {
    let yaml = r#"
version: 1
input:
  format: json
record_when:
  ref: "input.keep"
mappings:
  - target: "name"
    source: "name"
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let input = r#"[{"keep":true,"name":"alice"},{"keep":"not-bool","name":"bob"}]"#;

    let (normal_output, normal_warnings) =
        transform_with_warnings(&rule, input, None).expect("normal transform");
    let stream = transform_stream(&rule, input, None).expect("stream transform");
    let items = stream.collect::<Result<Vec<_>, _>>().expect("stream items");

    assert_eq!(normal_output, json!([{ "name": "alice" }]));
    assert_eq!(normal_warnings.len(), 2);
    assert_eq!(normal_warnings[0].path.as_deref(), Some("version"));
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].output, Some(json!({ "name": "alice" })));
    assert_eq!(items[0].warnings.len(), 1);
    assert_eq!(items[0].warnings[0], normal_warnings[0]);
    assert_eq!(items[1].output, None);
    assert_eq!(items[1].warnings, normal_warnings[1..]);
}

#[test]
fn transform_stream_rejects_finalize_with_stable_error() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "name"
    source: "name"
finalize:
  limit: 1
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let err = match transform_stream(&rule, r#"[{"name":"alice"}]"#, None) {
        Ok(_) => panic!("expected error"),
        Err(err) => err,
    };

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert_eq!(err.message, "finalize is not supported in stream mode");
    assert_eq!(err.path, None);
}
