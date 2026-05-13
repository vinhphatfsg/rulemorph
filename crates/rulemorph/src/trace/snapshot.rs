use serde::Serialize;
use serde_json::Value as JsonValue;

use super::schema::{TraceRedactionOptions, TraceValueMode, TransformTraceOptions};

pub(crate) trait TraceSnapshotValue {
    fn to_trace_snapshot(
        &self,
        options: &TransformTraceOptions,
        path_hint: Option<&str>,
    ) -> TraceValueSnapshot;
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
                if path_hint.is_none() {
                    return Self {
                        state,
                        value_type,
                        contains_raw_value: false,
                        value: None,
                        visibility: Some("metadata_only".to_string()),
                        redaction_reason: Some("unknown_provenance".to_string()),
                        bytes: Some(bytes),
                    };
                }
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

pub(crate) fn value_size_bytes(value: &JsonValue) -> usize {
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
