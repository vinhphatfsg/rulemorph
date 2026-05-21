mod access;
mod parser;

pub use self::access::get_path;
pub use self::parser::parse_path;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PathToken {
    Key(String),
    Index(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Empty,
    InvalidSyntax,
    InvalidEscape,
    EmptyKey,
}

impl PathError {
    pub fn message(&self) -> &'static str {
        match self {
            PathError::Empty => "path is empty",
            PathError::InvalidSyntax => "path syntax is invalid",
            PathError::InvalidEscape => "path escape is invalid",
            PathError::EmptyKey => "path segment is empty",
        }
    }
}

#[cfg(test)]
mod tests;
