use super::*;
use serde_json::json;

#[test]
fn collector_assigns_parent_ids_from_span_stack() {
    let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());
    collector.start_record(0, &json!({"name":"alice"}));

    collector
        .start_span(TraceEventKind::MappingStart, TracePhase::Start)
        .rule_path("mappings[0]")
        .finish(&mut collector);
    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .operator("uppercase")
        .finish(&mut collector);
    collector
        .end_span(TraceEventKind::OpEnd, TracePhase::End)
        .operator("uppercase")
        .finish(&mut collector);
    collector
        .end_span(TraceEventKind::MappingEnd, TracePhase::End)
        .rule_path("mappings[0]")
        .finish(&mut collector);

    let trace = collector.finish();
    let events = &trace.records[0].events;
    let mapping_start = events
        .iter()
        .find(|event| event.kind == TraceEventKind::MappingStart)
        .unwrap();
    let op_start = events
        .iter()
        .find(|event| event.kind == TraceEventKind::OpStart)
        .unwrap();
    let op_end = events
        .iter()
        .find(|event| event.kind == TraceEventKind::OpEnd)
        .unwrap();
    let mapping_end = events
        .iter()
        .find(|event| event.kind == TraceEventKind::MappingEnd)
        .unwrap();
    assert_eq!(op_start.parent_id, Some(mapping_start.id));
    assert_eq!(op_end.parent_id, Some(op_start.id));
    assert_eq!(mapping_end.parent_id, Some(mapping_start.id));
}

#[test]
fn collector_freezes_before_unemitted_span_enters_stack() {
    let mut options = TransformTraceOptions::metadata_only();
    options.max_events = Some(2);
    let mut collector = TraceCollector::new(options);
    collector.start_record(0, &json!({"name":"alice"}));

    assert!(
        collector
            .start_span(TraceEventKind::MappingStart, TracePhase::Start)
            .rule_path("mappings[0]")
            .finish(&mut collector)
    );
    assert!(
        !collector
            .start_span(TraceEventKind::OpStart, TracePhase::Start)
            .operator("uppercase")
            .finish(&mut collector)
    );
    collector
        .emit(TraceEventKind::SourceRead, TracePhase::Instant)
        .input_path("@input.name")
        .finish(&mut collector);

    let trace = collector.finish();
    assert!(!trace.complete);
    let ids = trace.records[0]
        .events
        .iter()
        .map(|event| event.id)
        .collect::<std::collections::BTreeSet<_>>();
    for event in &trace.records[0].events {
        if let Some(parent_id) = event.parent_id {
            assert!(
                ids.contains(&parent_id),
                "parent_id points to a non-emitted event"
            );
        }
    }
}

#[test]
fn collector_error_span_records_error_and_closes_span() {
    let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());
    collector.start_record(0, &json!({"name":"alice"}));

    collector
        .start_span(TraceEventKind::OpStart, TracePhase::Start)
        .operator("uppercase")
        .finish(&mut collector);
    collector
        .error_span(TraceEventKind::OpError, "OP_ERROR", "operator failed")
        .operator("uppercase")
        .finish(&mut collector);
    collector
        .emit(TraceEventKind::SourceRead, TracePhase::Instant)
        .input_path("@input.name")
        .finish(&mut collector);

    let trace = collector.finish();
    let events = &trace.records[0].events;
    let op_start = events
        .iter()
        .find(|event| event.kind == TraceEventKind::OpStart)
        .unwrap();
    let op_error = events
        .iter()
        .find(|event| event.kind == TraceEventKind::OpError)
        .unwrap();
    let source_read = events
        .iter()
        .find(|event| event.kind == TraceEventKind::SourceRead)
        .unwrap();
    assert_eq!(op_error.parent_id, Some(op_start.id));
    assert_eq!(source_read.parent_id, None);
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "complete trace finished with open spans")]
fn collector_finish_rejects_open_span_after_snapshot_only_truncation() {
    let mut options = TransformTraceOptions::raw();
    options.max_snapshot_bytes = Some(1);
    let mut collector = TraceCollector::new(options);
    collector.start_record(0, &json!({"name":"alice"}));
    collector
        .start_span(TraceEventKind::MappingStart, TracePhase::Start)
        .finish_with_output(&mut collector, &json!("oversized"), None);
    let _ = collector.finish();
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "start_record called while a span is open")]
fn collector_start_record_requires_empty_span_stack() {
    let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());
    collector.start_record(0, &json!({"name":"alice"}));
    collector
        .start_span(TraceEventKind::MappingStart, TracePhase::Start)
        .finish(&mut collector);
    collector.start_record(1, &json!({"name":"bob"}));
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "start_finalize called while a span is open")]
fn collector_start_finalize_requires_empty_span_stack() {
    let mut collector = TraceCollector::new(TransformTraceOptions::metadata_only());
    collector.start_record(0, &json!({"name":"alice"}));
    collector
        .start_span(TraceEventKind::MappingStart, TracePhase::Start)
        .finish(&mut collector);
    collector.start_finalize(&json!([]));
}
