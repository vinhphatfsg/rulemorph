use super::ErrorCode;

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
