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
mod tests;
