use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::trace::{TraceSnapshotValue, TraceValueSnapshot, TransformTraceOptions};
use crate::v2_eval::EvalValue as V2EvalValue;

#[derive(Clone, Copy)]
pub(crate) struct EvalLimits {
    pub(crate) max_range_items: Option<usize>,
    pub(crate) max_generated_array_items: usize,
}

impl Default for EvalLimits {
    fn default() -> Self {
        Self {
            max_range_items: Some(10_000),
            max_generated_array_items: 1_000_000,
        }
    }
}

impl From<&crate::normalization::NormalizationOptions> for EvalLimits {
    fn from(options: &crate::normalization::NormalizationOptions) -> Self {
        Self {
            max_range_items: options.max_range_items,
            max_generated_array_items: options.max_array_len,
        }
    }
}

impl EvalLimits {
    pub(crate) fn check_generated_array_items(
        self,
        count: usize,
        path: &str,
    ) -> Result<(), TransformError> {
        if count > self.max_generated_array_items {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "generated array items exceed configured limit",
            )
            .with_path(path));
        }
        Ok(())
    }
}

pub(crate) fn generated_array_item_count(
    value: &JsonValue,
    path: &str,
) -> Result<usize, TransformError> {
    match value {
        JsonValue::Array(items) => {
            let mut count = items.len();
            for item in items {
                count = count
                    .checked_add(generated_array_item_count(item, path)?)
                    .ok_or_else(|| generated_array_items_overflow(path))?;
            }
            Ok(count)
        }
        JsonValue::Object(map) => {
            let mut count = 0usize;
            for value in map.values() {
                count = count
                    .checked_add(generated_array_item_count(value, path)?)
                    .ok_or_else(|| generated_array_items_overflow(path))?;
            }
            Ok(count)
        }
        _ => Ok(0),
    }
}

pub(crate) fn push_generated_array_item(
    output: &mut Vec<JsonValue>,
    value: JsonValue,
    limits: EvalLimits,
    path: &str,
    generated_items: &mut usize,
) -> Result<(), TransformError> {
    let item_cost = 1usize
        .checked_add(generated_array_item_count(&value, path)?)
        .ok_or_else(|| generated_array_items_overflow(path))?;
    *generated_items = generated_items
        .checked_add(item_cost)
        .ok_or_else(|| generated_array_items_overflow(path))?;
    limits.check_generated_array_items(*generated_items, path)?;
    output.push(value);
    Ok(())
}

pub(crate) fn extend_generated_array_items(
    output: &mut Vec<JsonValue>,
    values: Vec<JsonValue>,
    limits: EvalLimits,
    path: &str,
    generated_items: &mut usize,
) -> Result<(), TransformError> {
    let mut item_cost = values.len();
    for value in &values {
        item_cost = item_cost
            .checked_add(generated_array_item_count(value, path)?)
            .ok_or_else(|| generated_array_items_overflow(path))?;
    }
    *generated_items = generated_items
        .checked_add(item_cost)
        .ok_or_else(|| generated_array_items_overflow(path))?;
    limits.check_generated_array_items(*generated_items, path)?;
    output.extend(values);
    Ok(())
}

fn generated_array_items_overflow(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "generated array item count is out of bounds",
    )
    .with_path(path)
}

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
    pub(crate) limits: EvalLimits,
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
