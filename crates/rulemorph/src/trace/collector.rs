use serde_json::Value as JsonValue;

use super::builder::TraceEventBuilder;
use super::schema::{
    RecordTrace, TraceEvent, TraceEventKind, TracePhase, TraceTruncation, TransformTrace,
    TransformTraceOptions,
};

mod budget;
mod span;

pub(super) use span::SpanAction;
use span::SpanStack;

pub(crate) struct TraceCollector {
    options: TransformTraceOptions,
    next_id: u64,
    complete: bool,
    truncation: Option<TraceTruncation>,
    contains_raw_values: bool,
    emitted_bytes: usize,
    frozen: bool,
    structural_truncated: bool,
    span_stack: SpanStack,
    records: Vec<RecordTrace>,
    current_record: Option<RecordTrace>,
    finalize_events: Vec<TraceEvent>,
    scope: TraceScope,
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
            span_stack: SpanStack::new(),
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
        TraceEventBuilder::new(id, self.span_stack.current_parent(), kind, phase)
    }

    pub(crate) fn start_span(
        &mut self,
        kind: TraceEventKind,
        phase: TracePhase,
    ) -> TraceEventBuilder {
        let id = self.next_id;
        self.next_id += 1;
        let parent_id = self.span_stack.current_parent();
        TraceEventBuilder::new(id, parent_id, kind, phase).span_action(SpanAction::Push(id))
    }

    pub(crate) fn end_span(
        &mut self,
        kind: TraceEventKind,
        phase: TracePhase,
    ) -> TraceEventBuilder {
        let span_id = self.span_stack.current_parent();
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
        let span_id = self.span_stack.current_parent();
        let id = self.next_id;
        self.next_id += 1;
        TraceEventBuilder::new(id, span_id, kind, TracePhase::Error)
            .diagnostic(code, message)
            .span_action(SpanAction::Pop(span_id))
    }

    pub(super) fn apply_span_action(&mut self, action: Option<SpanAction>) {
        self.span_stack.apply(action);
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
