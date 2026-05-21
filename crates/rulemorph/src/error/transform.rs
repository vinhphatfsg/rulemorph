#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformErrorKind {
    InvalidInput,
    InvalidRecordsPath,
    InvalidRef,
    InvalidTarget,
    MissingRequired,
    TypeCastFailed,
    ExprError,
    AssertionFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformWarning {
    pub kind: TransformErrorKind,
    pub message: String,
    pub path: Option<String>,
}

impl TransformWarning {
    pub fn new(kind: TransformErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            path: None,
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformError {
    pub kind: TransformErrorKind,
    pub message: String,
    pub path: Option<String>,
}

impl TransformError {
    pub fn new(kind: TransformErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            path: None,
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn kind_name(&self) -> &'static str {
        match &self.kind {
            TransformErrorKind::InvalidInput => "invalid_input",
            TransformErrorKind::InvalidRecordsPath => "invalid_records_path",
            TransformErrorKind::InvalidRef => "invalid_ref",
            TransformErrorKind::InvalidTarget => "invalid_target",
            TransformErrorKind::MissingRequired => "missing_required",
            TransformErrorKind::TypeCastFailed => "type_cast_failed",
            TransformErrorKind::ExprError => "expr_error",
            TransformErrorKind::AssertionFailed => "assertion_failed",
        }
    }
}

impl std::fmt::Display for TransformError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(path) = &self.path {
            write!(f, "{} (path: {})", self.message, path)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for TransformError {}

impl From<TransformError> for TransformWarning {
    fn from(err: TransformError) -> Self {
        let mut warning = TransformWarning::new(err.kind, err.message);
        if let Some(path) = err.path {
            warning = warning.with_path(path);
        }
        warning
    }
}

impl From<csv::Error> for TransformError {
    fn from(err: csv::Error) -> Self {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("csv error: {}", err),
        )
    }
}

impl From<serde_json::Error> for TransformError {
    fn from(err: serde_json::Error) -> Self {
        TransformError::new(
            TransformErrorKind::InvalidInput,
            format!("json error: {}", err),
        )
    }
}
