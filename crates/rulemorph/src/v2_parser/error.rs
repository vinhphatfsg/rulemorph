#[derive(Debug, Clone, PartialEq)]
pub enum V2ParseError {
    EmptyPipe,
    InvalidStart(String),
    InvalidStep(String),
    InvalidArgs(String),
    InvalidCondition(String),
}

impl std::fmt::Display for V2ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            V2ParseError::EmptyPipe => write!(f, "pipe array cannot be empty"),
            V2ParseError::InvalidStart(msg) => write!(f, "invalid start value: {}", msg),
            V2ParseError::InvalidStep(msg) => write!(f, "invalid step: {}", msg),
            V2ParseError::InvalidArgs(msg) => write!(f, "invalid args: {}", msg),
            V2ParseError::InvalidCondition(msg) => write!(f, "invalid condition: {}", msg),
        }
    }
}

impl std::error::Error for V2ParseError {}
