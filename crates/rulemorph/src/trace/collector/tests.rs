use super::*;
use crate::trace::{
    TraceAttributeValue, canonical_acc_path, canonical_context_path, canonical_input_path,
    canonical_item_path, canonical_out_path, canonical_output_path,
};
use serde_json::json;

#[path = "tests/span_stack.rs"]
mod span_stack;

#[test]
fn canonical_path_helpers_preserve_bracket_notation() {
    assert_eq!(canonical_input_path(r#"input["@id"]"#), r#"@input["@id"]"#);
    assert_eq!(canonical_input_path("input[0].name"), "@input[0].name");
    assert_eq!(canonical_item_path("@item[0].name"), "@item[0].name");
    assert_eq!(canonical_output_path("$.items[0].name"), "$.items[0].name");
}

#[test]
fn canonical_path_helpers_cover_all_namespaces_and_output_prefixes() {
    assert_eq!(canonical_acc_path(r#"acc["total"]"#), r#"@acc["total"]"#);
    assert_eq!(
        canonical_context_path(r#"context["tenant"]"#),
        r#"@context["tenant"]"#
    );
    assert_eq!(canonical_out_path(r#"out["name"]"#), r#"@out["name"]"#);
    assert_eq!(canonical_output_path(""), "$");
    assert_eq!(canonical_output_path("output"), "$.output");
    assert_eq!(canonical_output_path("output.name"), "$.name");
    assert_eq!(canonical_output_path("out.items[0]"), "$.items[0]");
    assert_eq!(canonical_output_path("$[0].name"), "$[0].name");
}

#[test]
fn trace_builder_attributes_are_scalar_metadata() {
    let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());
    collector.start_record(0, &json!({"name":"alice"}));
    collector
        .emit(TraceEventKind::MappingDecision, TracePhase::Instant)
        .attr_bool("applied", true)
        .attr_index("mapping_index", 1)
        .attr_count("output_count", 2)
        .attr_enum("skip_reason", "none")
        .attr_path("target_path", "$.name")
        .finish(&mut collector);

    let trace = collector.finish();
    let event = trace.records[0]
        .events
        .iter()
        .find(|event| event.kind == TraceEventKind::MappingDecision)
        .expect("mapping decision");
    assert_eq!(
        event.attributes.get("applied"),
        Some(&TraceAttributeValue::Bool(true))
    );
    assert_eq!(
        event
            .attributes
            .get("mapping_index")
            .and_then(|value| match value {
                TraceAttributeValue::Number(number) => number.as_u64(),
                _ => None,
            }),
        Some(1)
    );
    assert_eq!(
        event
            .attributes
            .get("output_count")
            .and_then(|value| match value {
                TraceAttributeValue::Number(number) => number.as_u64(),
                _ => None,
            }),
        Some(2)
    );
    assert_eq!(
        event.attributes.get("skip_reason"),
        Some(&TraceAttributeValue::String("none".to_string()))
    );
    assert_eq!(
        event.attributes.get("target_path"),
        Some(&TraceAttributeValue::String("$.name".to_string()))
    );
    assert!(event.inputs.is_empty());
    assert!(event.output.is_none());
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "semantic path namespace mismatch")]
fn canonical_path_helpers_detect_namespace_mismatch() {
    let _ = canonical_input_path("@item.name");
}
