use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformWarning};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformTraceOptions {
    pub value_mode: TraceValueMode,
    pub max_events: Option<usize>,
    pub max_trace_bytes: Option<usize>,
    pub max_snapshot_bytes: Option<usize>,
}

impl TransformTraceOptions {
    pub fn raw() -> Self {
        Self {
            value_mode: TraceValueMode::Raw,
            max_events: None,
            max_trace_bytes: None,
            max_snapshot_bytes: None,
        }
    }

    pub fn redacted() -> Self {
        Self {
            value_mode: TraceValueMode::Redacted(TraceRedactionOptions::default()),
            max_events: None,
            max_trace_bytes: None,
            max_snapshot_bytes: None,
        }
    }

    pub fn metadata_only() -> Self {
        Self {
            value_mode: TraceValueMode::MetadataOnly,
            max_events: None,
            max_trace_bytes: None,
            max_snapshot_bytes: None,
        }
    }
}

impl Default for TransformTraceOptions {
    fn default() -> Self {
        Self::raw()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceValueMode {
    Raw,
    Redacted(TraceRedactionOptions),
    MetadataOnly,
}

impl TraceValueMode {
    pub fn name(&self) -> TraceValueModeName {
        match self {
            TraceValueMode::Raw => TraceValueModeName::Raw,
            TraceValueMode::Redacted(_) => TraceValueModeName::Redacted,
            TraceValueMode::MetadataOnly => TraceValueModeName::MetadataOnly,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceValueModeName {
    Raw,
    Redacted,
    MetadataOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceRedactionOptions {
    pub secret_key_fragments: Vec<String>,
    pub oversized_value_bytes: Option<usize>,
}

impl Default for TraceRedactionOptions {
    fn default() -> Self {
        Self {
            secret_key_fragments: vec![
                "password".to_string(),
                "token".to_string(),
                "secret".to_string(),
                "authorization".to_string(),
                "api_key".to_string(),
                "api-key".to_string(),
                "apikey".to_string(),
                "bearer".to_string(),
            ],
            oversized_value_bytes: Some(64 * 1024),
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TransformTrace {
    pub schema_version: u8,
    pub value_mode: TraceValueModeName,
    pub contains_raw_values: bool,
    pub complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<TraceTruncation>,
    pub records: Vec<RecordTrace>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalize: Option<Vec<TraceEvent>>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct RecordTrace {
    pub record_index: usize,
    pub events: Vec<TraceEvent>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TraceTruncation {
    pub reason: String,
    pub emitted_events: usize,
    pub emitted_bytes: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TraceEvent {
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<u64>,
    pub kind: TraceEventKind,
    pub phase: TracePhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<TraceDiagnostic>,
    pub inputs: Vec<TraceValueSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<TraceValueSnapshot>,
    pub attributes: BTreeMap<String, TraceAttributeValue>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct TraceDiagnostic {
    pub code: &'static str,
    pub message: &'static str,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(untagged)]
pub enum TraceAttributeValue {
    Bool(bool),
    Number(serde_json::Number),
    String(String),
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceEventKind {
    RecordStart,
    RecordWhenStart,
    RecordWhenEnd,
    RecordDecision,
    MappingStart,
    MappingWhenStart,
    MappingWhenEnd,
    MappingDecision,
    MappingEnd,
    SourceRead,
    LiteralEval,
    DefaultApplied,
    TypeCast,
    ExprStart,
    ExprEnd,
    ChainStart,
    ChainStep,
    RefRead,
    OpStart,
    ArgEval,
    OpEnd,
    OpError,
    CollectionItemStart,
    CollectionItemEnd,
    OutputWrite,
    StepStart,
    AssertEval,
    BranchEval,
    BranchTaken,
    BranchMerge,
    FinalizeStart,
    FinalizeFilter,
    FinalizeSort,
    FinalizeOffset,
    FinalizeLimit,
    FinalizeWrap,
    FinalizeEnd,
    Error,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TracePhase {
    Start,
    End,
    Instant,
    Error,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct TraceValueSnapshot {
    pub state: TraceValueState,
    #[serde(rename = "type")]
    pub value_type: TraceJsonType,
    pub contains_raw_value: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redaction_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceValueState {
    Missing,
    Null,
    Present,
    Error,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TraceJsonType {
    Missing,
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl TraceValueSnapshot {
    pub fn missing(_options: &TransformTraceOptions, _path_hint: Option<&str>) -> Self {
        Self {
            state: TraceValueState::Missing,
            value_type: TraceJsonType::Missing,
            contains_raw_value: false,
            value: None,
            visibility: None,
            redaction_reason: None,
            bytes: None,
        }
    }

    pub(crate) fn from_eval_value(
        value: &crate::transform::EvalValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        match value {
            crate::transform::EvalValue::Missing => Self::missing(options, path_hint),
            crate::transform::EvalValue::Value(value) => Self::from_json(value, options, path_hint),
        }
    }

    pub(crate) fn from_v2_eval_value(
        value: &crate::v2_eval::EvalValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        match value {
            crate::v2_eval::EvalValue::Missing => Self::missing(options, path_hint),
            crate::v2_eval::EvalValue::Value(value) => Self::from_json(value, options, path_hint),
        }
    }

    pub fn from_json(
        value: &JsonValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        let value_type = json_value_type(value);
        let state = if value.is_null() {
            TraceValueState::Null
        } else {
            TraceValueState::Present
        };
        let bytes = value_size_bytes(value);
        if options
            .max_snapshot_bytes
            .is_some_and(|limit| bytes > limit)
        {
            return Self {
                state,
                value_type,
                contains_raw_value: false,
                value: None,
                visibility: Some("truncated".to_string()),
                redaction_reason: Some("max_snapshot_bytes".to_string()),
                bytes: Some(bytes),
            };
        }

        match &options.value_mode {
            TraceValueMode::Raw => Self {
                state,
                value_type,
                contains_raw_value: true,
                value: Some(value.clone()),
                visibility: Some("raw".to_string()),
                redaction_reason: None,
                bytes: Some(bytes),
            },
            TraceValueMode::MetadataOnly => Self {
                state,
                value_type,
                contains_raw_value: false,
                value: None,
                visibility: Some("metadata_only".to_string()),
                redaction_reason: None,
                bytes: Some(bytes),
            },
            TraceValueMode::Redacted(redaction) => {
                if value.is_object() || value.is_array() {
                    return Self {
                        state,
                        value_type,
                        contains_raw_value: false,
                        value: None,
                        visibility: Some("metadata_only".to_string()),
                        redaction_reason: Some("composite_snapshot".to_string()),
                        bytes: Some(bytes),
                    };
                }
                let reason = redaction_reason(value, path_hint, redaction);
                Self {
                    state,
                    value_type,
                    contains_raw_value: reason.is_none(),
                    value: if reason.is_none() {
                        Some(value.clone())
                    } else {
                        None
                    },
                    visibility: Some(if reason.is_some() { "redacted" } else { "raw" }.to_string()),
                    redaction_reason: reason,
                    bytes: Some(bytes),
                }
            }
        }
    }
}

fn json_value_type(value: &JsonValue) -> TraceJsonType {
    match value {
        JsonValue::Null => TraceJsonType::Null,
        JsonValue::Bool(_) => TraceJsonType::Boolean,
        JsonValue::Number(_) => TraceJsonType::Number,
        JsonValue::String(_) => TraceJsonType::String,
        JsonValue::Array(_) => TraceJsonType::Array,
        JsonValue::Object(_) => TraceJsonType::Object,
    }
}

fn value_size_bytes(value: &JsonValue) -> usize {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .unwrap_or(0)
}

fn redaction_reason(
    value: &JsonValue,
    path_hint: Option<&str>,
    redaction: &TraceRedactionOptions,
) -> Option<String> {
    if let Some(path) = path_hint {
        let path = path.to_ascii_lowercase();
        if redaction
            .secret_key_fragments
            .iter()
            .any(|fragment| path.contains(fragment))
        {
            return Some("secret_like_path".to_string());
        }
    }
    if let Some(limit) = redaction.oversized_value_bytes
        && value_size_bytes(value) > limit
    {
        return Some("oversized_value".to_string());
    }
    if let Some(text) = value.as_str()
        && text.to_ascii_lowercase().starts_with("bearer ")
    {
        return Some("bearer_credential".to_string());
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransformTraceResult {
    pub output: JsonValue,
    pub warnings: Vec<TransformWarning>,
    pub trace: TransformTrace,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransformRecordTraceResult {
    pub output: Option<JsonValue>,
    pub warnings: Vec<TransformWarning>,
    pub trace: TransformTrace,
}

#[derive(Clone, PartialEq)]
pub struct TransformTraceError {
    pub error: TransformError,
    pub warnings: Vec<TransformWarning>,
    pub trace: TransformTrace,
}

impl fmt::Debug for TransformTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransformTraceError")
            .field("error_kind", &self.error.kind_name())
            .field("warnings_len", &self.warnings.len())
            .field("trace_schema_version", &self.trace.schema_version)
            .field("trace_complete", &self.trace.complete)
            .field("trace_contains_raw_values", &self.trace.contains_raw_values)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for TransformTraceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "transform trace failed: error_kind={}, warnings_len={}, trace_complete={}, trace_contains_raw_values={}",
            self.error.kind_name(),
            self.warnings.len(),
            self.trace.complete,
            self.trace.contains_raw_values
        )
    }
}

impl std::error::Error for TransformTraceError {}

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
enum SpanAction {
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

    fn apply_span_action(&mut self, action: Option<SpanAction>) {
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

pub(crate) fn canonical_input_path(path: &str) -> String {
    canonical_at_path("@input", &["@input", "input"], path)
}

pub(crate) fn canonical_item_path(path: &str) -> String {
    canonical_at_path("@item", &["@item", "item"], path)
}

pub(crate) fn canonical_acc_path(path: &str) -> String {
    canonical_at_path("@acc", &["@acc", "acc"], path)
}

pub(crate) fn canonical_context_path(path: &str) -> String {
    canonical_at_path("@context", &["@context", "context"], path)
}

pub(crate) fn canonical_out_path(path: &str) -> String {
    canonical_at_path("@out", &["@out", "out"], path)
}

pub(crate) fn canonical_output_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed == "$" || trimmed.starts_with("$.") || trimmed.starts_with("$[") {
        return trimmed.to_string();
    }
    let stripped = trimmed
        .strip_prefix("output.")
        .or_else(|| trimmed.strip_prefix("out."))
        .unwrap_or(trimmed)
        .trim_start_matches('.');
    if stripped.is_empty() {
        "$".to_string()
    } else if stripped.starts_with('[') {
        format!("${stripped}")
    } else {
        format!("$.{stripped}")
    }
}

fn canonical_at_path(prefix: &'static str, strip_prefixes: &[&str], path: &str) -> String {
    let trimmed = path.trim();
    if trimmed == prefix
        || trimmed.starts_with(&format!("{prefix}."))
        || trimmed.starts_with(&format!("{prefix}["))
    {
        return trimmed.to_string();
    }
    for other_prefix in ["@input", "@item", "@acc", "@context", "@out"] {
        debug_assert!(
            other_prefix == prefix
                || !(trimmed == other_prefix
                    || trimmed.starts_with(&format!("{other_prefix}."))
                    || trimmed.starts_with(&format!("{other_prefix}["))),
            "semantic path namespace mismatch: expected {prefix}, got {trimmed}"
        );
    }
    let suffix = strip_prefixes
        .iter()
        .find_map(|candidate| {
            if trimmed == *candidate {
                Some("")
            } else if let Some(rest) = trimmed.strip_prefix(candidate) {
                (rest.starts_with('.') || rest.starts_with('[')).then_some(rest)
            } else {
                None
            }
        })
        .unwrap_or(trimmed);
    if suffix.is_empty() {
        prefix.to_string()
    } else if suffix.starts_with('.') || suffix.starts_with('[') {
        format!("{prefix}{suffix}")
    } else {
        format!("{prefix}.{suffix}")
    }
}

pub(crate) struct TraceEventBuilder {
    event: TraceEvent,
    span_action: Option<SpanAction>,
}

impl TraceEventBuilder {
    fn new(id: u64, parent_id: Option<u64>, kind: TraceEventKind, phase: TracePhase) -> Self {
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

    fn span_action(mut self, action: SpanAction) -> Self {
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

    pub(crate) fn input_eval_value(
        mut self,
        value: &crate::transform::EvalValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        self.event.inputs.push(TraceValueSnapshot::from_eval_value(
            value, options, path_hint,
        ));
        self
    }

    pub(crate) fn input_v2_eval_value(
        mut self,
        value: &crate::v2_eval::EvalValue,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> Self {
        self.event
            .inputs
            .push(TraceValueSnapshot::from_v2_eval_value(
                value, options, path_hint,
            ));
        self
    }

    pub(crate) fn finish_with_eval_output(
        mut self,
        collector: &mut TraceCollector,
        output: &crate::transform::EvalValue,
        path_hint: Option<&str>,
    ) -> bool {
        self.event.output = Some(TraceValueSnapshot::from_eval_value(
            output,
            collector.options(),
            path_hint,
        ));
        self.finish(collector)
    }

    pub(crate) fn finish_with_v2_eval_output(
        mut self,
        collector: &mut TraceCollector,
        output: &crate::v2_eval::EvalValue,
        path_hint: Option<&str>,
    ) -> bool {
        self.event.output = Some(TraceValueSnapshot::from_v2_eval_value(
            output,
            collector.options(),
            path_hint,
        ));
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

#[cfg(test)]
mod tests {
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

    #[test]
    fn canonical_path_helpers_preserve_bracket_notation() {
        assert_eq!(canonical_input_path(r#"input["@id"]"#), r#"@input["@id"]"#);
        assert_eq!(canonical_input_path("input[0].name"), "@input[0].name");
        assert_eq!(canonical_item_path("@item[0].name"), "@item[0].name");
        assert_eq!(canonical_output_path("$.items[0].name"), "$.items[0].name");
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
