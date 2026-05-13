use rulemorph::{
    InputData, NormalizationOptions, TransformErrorKind, parse_rule_file, transform_stream,
    transform_stream_input_with_options, transform_stream_with_base_dir, transform_with_warnings,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-stream-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

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
    assert_eq!(normal_warnings.len(), 1);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].output, Some(json!({ "name": "alice" })));
    assert!(items[0].warnings.is_empty());
    assert_eq!(items[1].output, None);
    assert_eq!(items[1].warnings, normal_warnings);
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

#[test]
fn transform_stream_with_base_dir_resolves_branch_rules() {
    let dir = unique_temp_dir("base-dir-branch");
    fs::write(
        dir.join("child.yaml"),
        r#"version: 2
input:
  format: json
  json: {}
mappings:
  - target: child_name
    source: name
"#,
    )
    .expect("write child");
    let yaml = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - mappings:
      - target: name
        source: name
  - branch:
      when: { eq: ["@out.name", "alice"] }
      then: child.yaml
"#;
    let rule = parse_rule_file(yaml).expect("parse rule");
    let stream = transform_stream_with_base_dir(&rule, r#"[{"name":"alice"}]"#, None, &dir)
        .expect("stream transform");
    let items = stream.collect::<Result<Vec<_>, _>>().expect("stream items");

    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].output,
        Some(json!({ "name": "alice", "child_name": "alice" }))
    );
    assert!(items[0].warnings.is_empty());
}
