use serde::Serialize;

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
