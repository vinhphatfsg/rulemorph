use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::trace::{TraceSnapshotValue, TraceValueSnapshot, TransformTraceOptions};
use crate::v2_eval::EvalValue as V2EvalValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Namespace {
    Input,
    Context,
    Out,
    Item,
    Acc,
    Pipe,
    Local,
}

#[derive(Clone, Copy)]
pub(crate) struct EvalItem<'a> {
    pub(crate) value: &'a JsonValue,
    pub(crate) index: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct EvalLocals<'a> {
    pub(crate) item: Option<EvalItem<'a>>,
    pub(crate) acc: Option<&'a JsonValue>,
    pub(crate) pipe: Option<&'a EvalValue>,
    pub(crate) locals: Option<&'a HashMap<String, EvalValue>>,
    pub(crate) precomputed_op_args: Option<(&'a str, &'a [EvalValue])>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EvalValue {
    Missing,
    Value(JsonValue),
}

impl TraceSnapshotValue for EvalValue {
    fn to_trace_snapshot(
        &self,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> TraceValueSnapshot {
        match self {
            EvalValue::Missing => TraceValueSnapshot::missing(options, path_hint),
            EvalValue::Value(value) => TraceValueSnapshot::from_json(value, options, path_hint),
        }
    }
}

impl TraceSnapshotValue for V2EvalValue {
    fn to_trace_snapshot(
        &self,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> TraceValueSnapshot {
        match self {
            V2EvalValue::Missing => TraceValueSnapshot::missing(options, path_hint),
            V2EvalValue::Value(value) => TraceValueSnapshot::from_json(value, options, path_hint),
        }
    }
}
