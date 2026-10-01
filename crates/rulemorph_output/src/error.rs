use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputErrorKind {
    InputSyntax,
    IncompleteInput,
    AmbiguousRepair,
    DuplicateKey,
    LimitExceeded,
    UnsupportedSchema,
    InvalidSchema,
    UnsupportedRules,
    InvalidRules,
    Transform,
    OutputValidation,
}

/// Byte positions refer to the original UTF-8 input, not the repaired candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepairEdit {
    pub byte_offset: usize,
    pub inserted: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SchemaDiagnostic {
    pub instance_path: String,
    pub schema_path: String,
}

/// Errors carry no raw input/output values. Repair edits survive later failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutputError {
    pub kind: OutputErrorKind,
    pub message: String,
    pub byte_offset: Option<usize>,
    pub repairs: Vec<RepairEdit>,
    pub diagnostics: Vec<SchemaDiagnostic>,
}

impl OutputError {
    pub(crate) fn new(kind: OutputErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            byte_offset: None,
            repairs: vec![],
            diagnostics: vec![],
        }
    }
}

impl fmt::Display for OutputError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for OutputError {}
