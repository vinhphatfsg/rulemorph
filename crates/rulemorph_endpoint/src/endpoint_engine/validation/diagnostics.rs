use std::fmt;
use std::path::{Path, PathBuf};

use rulemorph::RuleError;

#[derive(Debug, Clone)]
pub struct RulesDirError {
    pub code: String,
    pub file: PathBuf,
    pub path: Option<String>,
    pub line: Option<usize>,
    pub column: Option<usize>,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct RulesDirErrors {
    pub errors: Vec<RulesDirError>,
}

impl fmt::Display for RulesDirErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, err) in self.errors.iter().enumerate() {
            if index > 0 {
                writeln!(f)?;
            }
            let mut parts = Vec::new();
            parts.push(format!("E {}", err.code));
            parts.push(format!("file={}", err.file.display()));
            if let Some(path) = &err.path {
                parts.push(format!("path={}", path));
            }
            if let Some(line) = err.line {
                parts.push(format!("line={}", line));
            }
            if let Some(column) = err.column {
                parts.push(format!("col={}", column));
            }
            parts.push(format!("msg=\"{}\"", err.message));
            write!(f, "{}", parts.join(" "))?;
        }
        Ok(())
    }
}

impl std::error::Error for RulesDirErrors {}

pub(super) fn push_parse_error(
    errors: &mut Vec<RulesDirError>,
    path: &Path,
    message: &str,
    location: Option<(usize, usize)>,
) {
    push_error(
        errors,
        "RuleParseFailed",
        path,
        message.to_string(),
        None,
        location,
    );
}

pub(super) fn push_error(
    errors: &mut Vec<RulesDirError>,
    code: impl Into<String>,
    file: &Path,
    message: impl Into<String>,
    path: Option<String>,
    location: Option<(usize, usize)>,
) {
    let (line, column) = location
        .map(|(line, column)| (Some(line), Some(column)))
        .unwrap_or((None, None));
    errors.push(RulesDirError {
        code: code.into(),
        file: file.to_path_buf(),
        path,
        line,
        column,
        message: message.into(),
    });
}

pub(super) fn push_rule_error(errors: &mut Vec<RulesDirError>, path: &Path, err: &RuleError) {
    let location = err.location.as_ref().map(|loc| (loc.line, loc.column));
    push_error(
        errors,
        err.code.as_str(),
        path,
        err.message.clone(),
        err.path.clone(),
        location,
    );
}
