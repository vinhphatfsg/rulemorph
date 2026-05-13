use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use super::collector::{SpanAction, TraceCollector};
use super::schema::{
    TraceAttributeValue, TraceDiagnostic, TraceEvent, TraceEventKind, TracePhase,
    TransformTraceOptions,
};
use super::snapshot::{TraceSnapshotValue, TraceValueSnapshot};

pub(crate) struct TraceEventBuilder {
    event: TraceEvent,
    span_action: Option<SpanAction>,
}

impl TraceEventBuilder {
    pub(super) fn new(
        id: u64,
        parent_id: Option<u64>,
        kind: TraceEventKind,
        phase: TracePhase,
    ) -> Self {
        Self {
            event: TraceEvent {
                id,
                parent_id,
                kind,
                phase,
                rule_path: None,
                input_path: None,
                output_path: None,
                namespace: None,
                operator: None,
                message: None,
                inputs: Vec::new(),
                output: None,
                attributes: BTreeMap::new(),
            },
            span_action: None,
        }
    }

    pub(super) fn span_action(mut self, action: SpanAction) -> Self {
        self.span_action = Some(action);
        self
    }

    pub(crate) fn rule_path(mut self, path: impl Into<String>) -> Self {
        self.event.rule_path = Some(path.into());
        self
    }

    pub(crate) fn input_path(mut self, path: impl Into<String>) -> Self {
        self.event.input_path = Some(path.into());
        self
    }

    pub(crate) fn output_path(mut self, path: impl Into<String>) -> Self {
        self.event.output_path = Some(path.into());
        self
    }

    pub(crate) fn operator(mut self, operator: impl Into<String>) -> Self {
        self.event.operator = Some(operator.into());
        self
    }

    fn attr(mut self, key: impl Into<String>, value: TraceAttributeValue) -> Self {
        self.event.attributes.insert(key.into(), value);
        self
    }

    pub(crate) fn attr_bool(self, key: impl Into<String>, value: bool) -> Self {
        self.attr(key, TraceAttributeValue::Bool(value))
    }

    pub(crate) fn attr_index(self, key: impl Into<String>, value: usize) -> Self {
        self.attr(
            key,
            TraceAttributeValue::Number(serde_json::Number::from(value as u64)),
        )
    }

    pub(crate) fn attr_count(self, key: impl Into<String>, value: usize) -> Self {
        self.attr_index(key, value)
    }

    pub(crate) fn attr_enum(self, key: impl Into<String>, value: &'static str) -> Self {
        self.attr(key, TraceAttributeValue::String(value.to_string()))
    }

    pub(crate) fn attr_path(self, key: impl Into<String>, path: impl Into<String>) -> Self {
        self.attr(key, TraceAttributeValue::String(path.into()))
    }

    pub(crate) fn diagnostic(mut self, code: &'static str, message: &'static str) -> Self {
        self.event.message = Some(TraceDiagnostic { code, message });
        self
    }

    pub(crate) fn input_value(
        mut self,
        value: &JsonValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        self.event
            .inputs
            .push(TraceValueSnapshot::from_json(value, options, path_hint));
        self
    }

    pub(crate) fn input_eval_value<T: TraceSnapshotValue>(
        mut self,
        value: &T,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        self.event
            .inputs
            .push(value.to_trace_snapshot(options, path_hint));
        self
    }

    pub(crate) fn input_v2_eval_value<T: TraceSnapshotValue>(
        mut self,
        value: &T,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        self.event
            .inputs
            .push(value.to_trace_snapshot(options, path_hint));
        self
    }

    pub(crate) fn finish_with_eval_output<T: TraceSnapshotValue>(
        mut self,
        collector: &mut TraceCollector,
        output: &T,
        path_hint: Option<&str>,
    ) -> bool {
        self.event.output = Some(output.to_trace_snapshot(collector.options(), path_hint));
        self.finish(collector)
    }

    pub(crate) fn finish_with_v2_eval_output<T: TraceSnapshotValue>(
        mut self,
        collector: &mut TraceCollector,
        output: &T,
        path_hint: Option<&str>,
    ) -> bool {
        self.event.output = Some(output.to_trace_snapshot(collector.options(), path_hint));
        self.finish(collector)
    }

    pub(crate) fn finish(self, collector: &mut TraceCollector) -> bool {
        let TraceEventBuilder { event, span_action } = self;
        let emitted = collector.push(event);
        if emitted {
            collector.apply_span_action(span_action);
        }
        emitted
    }

    pub(crate) fn finish_with_output(
        mut self,
        collector: &mut TraceCollector,
        output: &JsonValue,
        path_hint: Option<&str>,
    ) -> bool {
        self.event.output = Some(TraceValueSnapshot::from_json(
            output,
            collector.options(),
            path_hint,
        ));
        self.finish(collector)
    }
}
