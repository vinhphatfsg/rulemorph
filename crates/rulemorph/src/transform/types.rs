use std::collections::HashMap;
use std::io::{self, Write};

use serde_json::Value as JsonValue;

use crate::error::{TransformError, TransformErrorKind};
use crate::trace::{TraceSnapshotValue, TraceValueSnapshot, TransformTraceOptions};
use crate::v2_eval::EvalValue as V2EvalValue;

#[derive(Clone, Copy)]
pub(crate) struct EvalLimits {
    pub(crate) max_range_items: Option<usize>,
    pub(crate) max_generated_array_items: usize,
    pub(crate) max_object_fields: usize,
    pub(crate) max_object_key_bytes: usize,
    pub(crate) max_object_depth: usize,
    pub(crate) max_generated_json_nodes: usize,
    pub(crate) max_generated_json_bytes: usize,
    pub(crate) max_custom_op_call_depth: usize,
    pub(crate) max_custom_op_calls_per_record: usize,
}

impl Default for EvalLimits {
    fn default() -> Self {
        Self {
            max_range_items: Some(10_000),
            max_generated_array_items: 1_000_000,
            max_object_fields: 10_000,
            max_object_key_bytes: 4 * 1024,
            max_object_depth: 64,
            max_generated_json_nodes: 100_000,
            max_generated_json_bytes: 10 * 1024 * 1024,
            max_custom_op_call_depth: crate::custom_ops::MAX_CUSTOM_OP_CALL_DEPTH,
            max_custom_op_calls_per_record: crate::custom_ops::MAX_CUSTOM_OP_CALLS_PER_RECORD,
        }
    }
}

impl From<&crate::normalization::NormalizationOptions> for EvalLimits {
    fn from(options: &crate::normalization::NormalizationOptions) -> Self {
        Self {
            max_range_items: options.max_range_items,
            max_generated_array_items: options.max_array_len,
            max_object_fields: options.max_object_fields,
            max_object_key_bytes: options.max_object_key_bytes,
            max_object_depth: options.max_object_depth,
            max_generated_json_nodes: options.max_generated_json_nodes,
            max_generated_json_bytes: options.max_generated_json_bytes,
            max_custom_op_call_depth: crate::custom_ops::MAX_CUSTOM_OP_CALL_DEPTH,
            max_custom_op_calls_per_record: crate::custom_ops::MAX_CUSTOM_OP_CALLS_PER_RECORD,
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

    pub(crate) fn check_object_field_count(
        self,
        count: usize,
        path: &str,
    ) -> Result<(), TransformError> {
        if count > self.max_object_fields {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object field count exceeds configured limit",
            )
            .with_path(path));
        }
        Ok(())
    }

    pub(crate) fn check_object_key(self, key: &str, path: &str) -> Result<(), TransformError> {
        if key.is_empty() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object field key must not be empty",
            )
            .with_path(path));
        }
        if key.len() > self.max_object_key_bytes {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object key bytes exceed configured limit",
            )
            .with_path(path));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratedObjectBudget {
    fields: usize,
    nodes: usize,
    array_items: usize,
    object_depth: usize,
    bytes: usize,
}

impl GeneratedObjectBudget {
    pub(crate) fn new(limits: EvalLimits, path: &str) -> Result<Self, TransformError> {
        let budget = Self {
            fields: 0,
            nodes: 1,
            array_items: 0,
            object_depth: 1,
            bytes: 2,
        };
        budget.check(limits, path)?;
        Ok(budget)
    }

    pub(crate) fn try_push_field(
        &mut self,
        key: &str,
        value: &JsonValue,
        limits: EvalLimits,
        path: &str,
    ) -> Result<(), TransformError> {
        let stats = generated_json_stats(value, limits, path)?;
        let key_bytes = serialized_json_str_bytes(key, path)?;
        let value_bytes = serialized_json_bytes(value, path)?;
        let separator_bytes = usize::from(self.fields > 0);
        let entry_bytes = key_bytes
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .and_then(|bytes| bytes.checked_add(separator_bytes))
            .ok_or_else(|| generated_json_overflow(path))?;
        let next_nodes = self
            .nodes
            .checked_add(stats.nodes)
            .ok_or_else(|| generated_json_overflow(path))?;
        let next_array_items = self
            .array_items
            .checked_add(stats.array_items)
            .ok_or_else(|| generated_array_items_overflow(path))?;
        let nested_object_depth = if stats.object_depth == 0 {
            1
        } else {
            stats
                .object_depth
                .checked_add(1)
                .ok_or_else(|| generated_json_overflow(path))?
        };
        let next_bytes = self
            .bytes
            .checked_add(entry_bytes)
            .ok_or_else(|| generated_json_overflow(path))?;
        let next = Self {
            fields: self
                .fields
                .checked_add(1)
                .ok_or_else(|| generated_json_overflow(path))?,
            nodes: next_nodes,
            array_items: next_array_items,
            object_depth: self.object_depth.max(nested_object_depth),
            bytes: next_bytes,
        };
        next.check(limits, path)?;
        *self = next;
        Ok(())
    }

    fn check(self, limits: EvalLimits, path: &str) -> Result<(), TransformError> {
        if self.nodes > limits.max_generated_json_nodes {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "generated JSON node count exceeds configured limit",
            )
            .with_path(path));
        }
        limits.check_generated_array_items(self.array_items, path)?;
        if self.object_depth > limits.max_object_depth {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object depth exceeds configured limit",
            )
            .with_path(path));
        }
        if self.bytes > limits.max_generated_json_bytes {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "generated JSON bytes exceed configured limit",
            )
            .with_path(path));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratedArrayBudget {
    values: usize,
    nodes: usize,
    array_items: usize,
    object_depth: usize,
    bytes: usize,
}

impl GeneratedArrayBudget {
    pub(crate) fn new(limits: EvalLimits, path: &str) -> Result<Self, TransformError> {
        let budget = Self {
            values: 0,
            nodes: 1,
            array_items: 0,
            object_depth: 0,
            bytes: 2,
        };
        budget.check(limits, path)?;
        Ok(budget)
    }

    pub(crate) fn try_push_value(
        &mut self,
        value: &JsonValue,
        limits: EvalLimits,
        path: &str,
    ) -> Result<(), TransformError> {
        let stats = generated_json_stats(value, limits, path)?;
        let value_bytes = serialized_json_bytes(value, path)?;
        let separator_bytes = usize::from(self.values > 0);
        let next = Self {
            values: self
                .values
                .checked_add(1)
                .ok_or_else(|| generated_json_overflow(path))?,
            nodes: self
                .nodes
                .checked_add(stats.nodes)
                .ok_or_else(|| generated_json_overflow(path))?,
            array_items: self
                .array_items
                .checked_add(1)
                .and_then(|items| items.checked_add(stats.array_items))
                .ok_or_else(|| generated_array_items_overflow(path))?,
            object_depth: self.object_depth.max(stats.object_depth),
            bytes: self
                .bytes
                .checked_add(separator_bytes)
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .ok_or_else(|| generated_json_overflow(path))?,
        };
        next.check(limits, path)?;
        *self = next;
        Ok(())
    }

    fn check(self, limits: EvalLimits, path: &str) -> Result<(), TransformError> {
        if self.nodes > limits.max_generated_json_nodes {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "generated JSON node count exceeds configured limit",
            )
            .with_path(path));
        }
        limits.check_generated_array_items(self.array_items, path)?;
        if self.object_depth > limits.max_object_depth {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "object depth exceeds configured limit",
            )
            .with_path(path));
        }
        if self.bytes > limits.max_generated_json_bytes {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "generated JSON bytes exceed configured limit",
            )
            .with_path(path));
        }
        Ok(())
    }
}

#[derive(Default)]
struct GeneratedJsonStats {
    nodes: usize,
    array_items: usize,
    object_depth: usize,
}

fn generated_json_stats(
    value: &JsonValue,
    limits: EvalLimits,
    path: &str,
) -> Result<GeneratedJsonStats, TransformError> {
    let mut stats = GeneratedJsonStats::default();
    collect_generated_json_stats(value, 0, &mut stats, limits, path)?;
    Ok(stats)
}

fn collect_generated_json_stats(
    value: &JsonValue,
    object_depth: usize,
    stats: &mut GeneratedJsonStats,
    limits: EvalLimits,
    path: &str,
) -> Result<(), TransformError> {
    stats.nodes = stats
        .nodes
        .checked_add(1)
        .ok_or_else(|| generated_json_overflow(path))?;
    match value {
        JsonValue::Object(map) => {
            limits.check_object_field_count(map.len(), path)?;
            for key in map.keys() {
                limits.check_object_key(key, path)?;
            }
            let next_depth = object_depth
                .checked_add(1)
                .ok_or_else(|| generated_json_overflow(path))?;
            stats.object_depth = stats.object_depth.max(next_depth);
            for value in map.values() {
                collect_generated_json_stats(value, next_depth, stats, limits, path)?;
            }
        }
        JsonValue::Array(items) => {
            stats.array_items = stats
                .array_items
                .checked_add(items.len())
                .ok_or_else(|| generated_array_items_overflow(path))?;
            for value in items {
                collect_generated_json_stats(value, object_depth, stats, limits, path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn generated_json_overflow(path: &str) -> TransformError {
    TransformError::new(
        TransformErrorKind::ExprError,
        "generated JSON size is out of bounds",
    )
    .with_path(path)
}

struct JsonByteCounter {
    bytes: usize,
}

impl Write for JsonByteCounter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::other("generated JSON size is out of bounds"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_bytes(value: &JsonValue, path: &str) -> Result<usize, TransformError> {
    let mut counter = JsonByteCounter { bytes: 0 };
    serde_json::to_writer(&mut counter, value).map_err(|_| generated_json_overflow(path))?;
    Ok(counter.bytes)
}

fn serialized_json_str_bytes(value: &str, path: &str) -> Result<usize, TransformError> {
    let mut counter = JsonByteCounter { bytes: 0 };
    serde_json::to_writer(&mut counter, value).map_err(|_| generated_json_overflow(path))?;
    Ok(counter.bytes)
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
