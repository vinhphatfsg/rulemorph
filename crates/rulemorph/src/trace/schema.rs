use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformWarning};

use super::snapshot::TraceValueSnapshot;

mod options;

pub use self::options::{
    TraceRedactionOptions, TraceValueMode, TraceValueModeName, TransformTraceOptions,
};

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
