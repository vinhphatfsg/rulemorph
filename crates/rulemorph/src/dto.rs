mod render;
mod schema;
mod support;

use self::render::{
    render_go, render_java, render_kotlin, render_python, render_rust, render_swift,
    render_typescript,
};
use self::schema::build_schema;
use self::support::safe_type_name;
use crate::model::RuleFile;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtoLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

#[derive(Debug, Clone)]
pub struct DtoError {
    message: String,
}

impl DtoError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DtoError {}

pub fn generate_dto(
    rule: &RuleFile,
    language: DtoLanguage,
    name: Option<&str>,
) -> Result<String, DtoError> {
    let name = safe_type_name(language, name.unwrap_or("Record"));
    let schema = build_schema(rule)?;

    match language {
        DtoLanguage::Rust => render_rust(&schema, &name),
        DtoLanguage::TypeScript => render_typescript(&schema, &name),
        DtoLanguage::Python => render_python(&schema, &name),
        DtoLanguage::Go => render_go(&schema, &name),
        DtoLanguage::Java => render_java(&schema, &name),
        DtoLanguage::Kotlin => render_kotlin(&schema, &name),
        DtoLanguage::Swift => render_swift(&schema, &name),
    }
}
