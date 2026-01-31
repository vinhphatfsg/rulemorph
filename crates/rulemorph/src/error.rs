#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidVersion,
    MissingInputFormat,
    InvalidInputFormat,
    MissingCsvSection,
    MissingJsonSection,
    InvalidDelimiterLength,
    MissingCsvColumns,

    MissingTarget,
    DuplicateTarget,
    SourceValueExprExclusive,
    MissingMappingValue,
    InvalidWhenType,

    InvalidRefNamespace,
    ForwardOutReference,
    UnknownOp,
    InvalidArgs,
    InvalidExprShape,
    InvalidPath,

    InvalidTypeName,

    // v2 validation errors
    UndefinedVariable,
    InvalidItemRef,
    InvalidAccRef,
    CyclicDependency,
    EmptyPipe,
    InvalidPipeStep,

    // v2 rule structure errors
    MissingMappings,
    StepsMappingExclusive,
    InvalidStep,
    InvalidFinalize,
}

impl ErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::InvalidVersion => "InvalidVersion",
            ErrorCode::MissingInputFormat => "MissingInputFormat",
            ErrorCode::InvalidInputFormat => "InvalidInputFormat",
            ErrorCode::MissingCsvSection => "MissingCsvSection",
            ErrorCode::MissingJsonSection => "MissingJsonSection",
            ErrorCode::InvalidDelimiterLength => "InvalidDelimiterLength",
            ErrorCode::MissingCsvColumns => "MissingCsvColumns",
            ErrorCode::MissingTarget => "MissingTarget",
            ErrorCode::DuplicateTarget => "DuplicateTarget",
            ErrorCode::SourceValueExprExclusive => "SourceValueExprExclusive",
            ErrorCode::MissingMappingValue => "MissingMappingValue",
            ErrorCode::InvalidWhenType => "InvalidWhenType",
            ErrorCode::InvalidRefNamespace => "InvalidRefNamespace",
            ErrorCode::ForwardOutReference => "ForwardOutReference",
            ErrorCode::UnknownOp => "UnknownOp",
            ErrorCode::InvalidArgs => "InvalidArgs",
            ErrorCode::InvalidExprShape => "InvalidExprShape",
            ErrorCode::InvalidPath => "InvalidPath",
            ErrorCode::InvalidTypeName => "InvalidTypeName",
            ErrorCode::UndefinedVariable => "UndefinedVariable",
            ErrorCode::InvalidItemRef => "InvalidItemRef",
            ErrorCode::InvalidAccRef => "InvalidAccRef",
            ErrorCode::CyclicDependency => "CyclicDependency",
            ErrorCode::EmptyPipe => "EmptyPipe",
            ErrorCode::InvalidPipeStep => "InvalidPipeStep",
            ErrorCode::MissingMappings => "MissingMappings",
            ErrorCode::StepsMappingExclusive => "StepsMappingExclusive",
            ErrorCode::InvalidStep => "InvalidStep",
            ErrorCode::InvalidFinalize => "InvalidFinalize",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YamlLocation {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleError {
    pub code: ErrorCode,
    pub message: String,
    pub location: Option<YamlLocation>,
    pub path: Option<String>,
}

impl RuleError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            location: None,
            path: None,
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn with_location(mut self, line: usize, column: usize) -> Self {
        self.location = Some(YamlLocation { line, column });
        self
    }
}

pub type ValidationResult = Result<(), Vec<RuleError>>;

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
