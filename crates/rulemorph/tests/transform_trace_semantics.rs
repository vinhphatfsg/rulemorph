use std::fs;

mod common;

use common::trace::{
    assert_trace_shape, assert_traced_output_matches_normal, attr_bool, attr_number,
    iter_trace_events, parse_rule, transform_text_raw_trace, unique_temp_dir,
};
use rulemorph::{
    InputData, TraceAttributeValue, TraceEventKind, TransformTraceOptions, parse_rule_file,
    transform, transform_input_with_trace, transform_input_with_trace_with_base_dir_and_options,
    transform_record, transform_record_with_trace, transform_with_base_dir,
};
use serde_json::json;

#[test]
fn trace_v2_eager_operator_emits_arg_eval_for_actual_args() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.first"
      - concat: ["@input.second"]
"#;
    let rule = parse_rule(yaml);
    let traced = transform_text_raw_trace(&rule, r#"[{"first":"A","second":"B"}]"#);

    assert_eq!(traced.output, json!([{ "label": "AB" }]));
    assert!(
        iter_trace_events(&traced.trace).into_iter().any(|event| {
            event.kind == TraceEventKind::ArgEval
                && event.operator.as_deref() == Some("concat")
                && attr_number(event, "arg_index") == Some(0)
                && event
                    .output
                    .as_ref()
                    .and_then(|snapshot| snapshot.value.as_ref())
                    == Some(&json!("B"))
        }),
        "v2 eager operator args should be traced when actually evaluated"
    );
    assert_trace_shape(&traced.trace);
}

include!("transform_trace_semantics/short_circuit.rs");

include!("transform_trace_semantics/collection.rs");

include!("transform_trace_semantics/operator_inventory.rs");

include!("transform_trace_semantics/branch.rs");
include!("transform_trace_semantics/record_finalize.rs");

#[test]
fn trace_v2_if_does_not_evaluate_unselected_branch() {
    let yaml = r#"
version: 2
input:
  format: json
mappings:
  - target: "label"
    expr:
      - "@input.enabled"
      - if:
          cond:
            eq: ["$", true]
          then:
            - "enabled"
          else:
            - "@item.label"
"#;

    assert_traced_output_matches_normal(
        yaml,
        r#"[{"enabled":true}]"#,
        json!([{ "label": "enabled" }]),
    );
}
