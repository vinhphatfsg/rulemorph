use serde_json::Value as JsonValue;

use super::builder::TraceEventBuilder;
use super::schema::{
    RecordTrace, TraceEvent, TraceEventKind, TracePhase, TraceTruncation, TransformTrace,
    TransformTraceOptions,
};
use super::snapshot::value_size_bytes;

pub(crate) struct TraceCollector {
    options: TransformTraceOptions,
    next_id: u64,
    complete: bool,
    truncation: Option<TraceTruncation>,
    contains_raw_values: bool,
    emitted_bytes: usize,
    frozen: bool,
    structural_truncated: bool,
    span_stack: Vec<u64>,
    records: Vec<RecordTrace>,
    current_record: Option<RecordTrace>,
    finalize_events: Vec<TraceEvent>,
    scope: TraceScope,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpanAction {
    Push(u64),
    Pop(Option<u64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TraceScope {
    Record,
    Finalize,
}

impl TraceCollector {
    pub(crate) fn new(options: TransformTraceOptions) -> Self {
        Self {
            options,
            next_id: 1,
            complete: true,
            truncation: None,
            contains_raw_values: false,
            emitted_bytes: 0,
            frozen: false,
            structural_truncated: false,
            span_stack: Vec::new(),
            records: Vec::new(),
            current_record: None,
            finalize_events: Vec::new(),
            scope: TraceScope::Record,
        }
    }

    pub(crate) fn start_record(&mut self, record_index: usize, record: &JsonValue) {
        if self.frozen {
            return;
        }
        debug_assert!(
            self.span_stack.is_empty(),
            "start_record called while a span is open: {:?}",
            self.span_stack
        );
        if let Some(record) = self.current_record.take() {
            self.records.push(record);
        }
        self.current_record = Some(RecordTrace {
            record_index,
            events: Vec::new(),
        });
        self.emit(TraceEventKind::RecordStart, TracePhase::Instant)
            .finish_with_output(self, record, None);
    }

    pub(crate) fn start_finalize(&mut self, output_before_finalize: &JsonValue) {
        if self.frozen {
            return;
        }
        debug_assert!(
            self.span_stack.is_empty(),
            "start_finalize called while a span is open: {:?}",
            self.span_stack
        );
        if let Some(record) = self.current_record.take() {
            self.records.push(record);
        }
        self.scope = TraceScope::Finalize;
        self.start_span(TraceEventKind::FinalizeStart, TracePhase::Start)
            .finish_with_output(self, output_before_finalize, None);
    }

    pub(crate) fn emit(&mut self, kind: TraceEventKind, phase: TracePhase) -> TraceEventBuilder {
        let id = self.next_id;
        self.next_id += 1;
        TraceEventBuilder::new(id, self.span_stack.last().copied(), kind, phase)
    }

    pub(crate) fn start_span(
        &mut self,
        kind: TraceEventKind,
        phase: TracePhase,
    ) -> TraceEventBuilder {
        let id = self.next_id;
        self.next_id += 1;
        let parent_id = self.span_stack.last().copied();
        TraceEventBuilder::new(id, parent_id, kind, phase).span_action(SpanAction::Push(id))
    }

    pub(crate) fn end_span(
        &mut self,
        kind: TraceEventKind,
        phase: TracePhase,
    ) -> TraceEventBuilder {
        let span_id = self.span_stack.last().copied();
        let id = self.next_id;
        self.next_id += 1;
        TraceEventBuilder::new(id, span_id, kind, phase).span_action(SpanAction::Pop(span_id))
    }

    pub(crate) fn error_span(
        &mut self,
        kind: TraceEventKind,
        code: &'static str,
        message: &'static str,
    ) -> TraceEventBuilder {
        let span_id = self.span_stack.last().copied();
        let id = self.next_id;
        self.next_id += 1;
        TraceEventBuilder::new(id, span_id, kind, TracePhase::Error)
            .diagnostic(code, message)
            .span_action(SpanAction::Pop(span_id))
    }

    pub(crate) fn push(&mut self, event: TraceEvent) -> bool {
        if self.frozen {
            return false;
        }
        let snapshot_truncated = event.output.as_ref().is_some_and(|snapshot| {
            snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes")
        }) || event
            .inputs
            .iter()
            .any(|snapshot| snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes"));
        if let Some(max_events) = self.options.max_events {
            let emitted = self.emitted_events();
            if emitted >= max_events {
                self.complete = false;
                self.truncation = Some(TraceTruncation {
                    reason: "max_events".to_string(),
                    emitted_events: emitted,
                    emitted_bytes: self.emitted_bytes,
                });
                self.frozen = true;
                self.structural_truncated = true;
                return false;
            }
        }
        let event_bytes =
            value_size_bytes(&serde_json::to_value(&event).unwrap_or(JsonValue::Null));
        if let Some(max_trace_bytes) = self.options.max_trace_bytes
            && self.emitted_bytes.saturating_add(event_bytes) > max_trace_bytes
        {
            self.complete = false;
            self.truncation = Some(TraceTruncation {
                reason: "max_trace_bytes".to_string(),
                emitted_events: self.emitted_events(),
                emitted_bytes: self.emitted_bytes,
            });
            self.frozen = true;
            self.structural_truncated = true;
            return false;
        }
        self.emitted_bytes = self.emitted_bytes.saturating_add(event_bytes);
        if snapshot_truncated {
            self.complete = false;
            let emitted_events = self.emitted_events();
            self.truncation.get_or_insert_with(|| TraceTruncation {
                reason: "max_snapshot_bytes".to_string(),
                emitted_events,
                emitted_bytes: self.emitted_bytes,
            });
        }
        self.contains_raw_values |= event
            .output
            .as_ref()
            .is_some_and(|snapshot| snapshot.contains_raw_value)
            || event
                .inputs
                .iter()
                .any(|snapshot| snapshot.contains_raw_value);
        match self.scope {
            TraceScope::Record => {
                if let Some(record) = &mut self.current_record {
                    record.events.push(event);
                }
            }
            TraceScope::Finalize => {
                self.finalize_events.push(event);
            }
        }
        true
    }

    pub(super) fn apply_span_action(&mut self, action: Option<SpanAction>) {
        match action {
            Some(SpanAction::Push(id)) => self.span_stack.push(id),
            Some(SpanAction::Pop(Some(expected))) => {
                if self.span_stack.last().copied() == Some(expected) {
                    self.span_stack.pop();
                }
            }
            Some(SpanAction::Pop(None)) | None => {}
        }
    }

    fn emitted_events(&self) -> usize {
        self.records
            .iter()
            .map(|record| record.events.len())
            .sum::<usize>()
            + self
                .current_record
                .as_ref()
                .map(|record| record.events.len())
                .unwrap_or(0)
            + self.finalize_events.len()
    }

    pub(crate) fn finish(mut self) -> TransformTrace {
        debug_assert!(
            self.structural_truncated || self.span_stack.is_empty(),
            "complete trace finished with open spans: {:?}",
            self.span_stack
        );
        if let Some(record) = self.current_record.take() {
            self.records.push(record);
        }
        TransformTrace {
            schema_version: 1,
            value_mode: self.options.value_mode.name(),
            contains_raw_values: self.contains_raw_values,
            complete: self.complete,
            truncation: self.truncation,
            records: self.records,
            finalize: (!self.finalize_events.is_empty()).then_some(self.finalize_events),
        }
    }

    pub(crate) fn options(&self) -> &TransformTraceOptions {
        &self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::{
        TraceAttributeValue, canonical_acc_path, canonical_context_path, canonical_input_path,
        canonical_item_path, canonical_out_path, canonical_output_path,
    };
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
}
