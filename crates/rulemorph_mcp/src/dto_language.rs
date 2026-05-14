use rulemorph::DtoLanguage;
use serde_json::{Value, json};

pub(crate) fn parse_dto_language(value: &str) -> Result<DtoLanguage, String> {
    match value.to_lowercase().as_str() {
        "rust" => Ok(DtoLanguage::Rust),
        "typescript" => Ok(DtoLanguage::TypeScript),
        "python" => Ok(DtoLanguage::Python),
        "go" => Ok(DtoLanguage::Go),
        "java" => Ok(DtoLanguage::Java),
        "kotlin" => Ok(DtoLanguage::Kotlin),
        "swift" => Ok(DtoLanguage::Swift),
        _ => Err(
            "language must be one of rust, typescript, python, go, java, kotlin, swift".to_string(),
        ),
    }
}

pub(crate) fn dto_language_to_str(language: DtoLanguage) -> &'static str {
    match language {
        DtoLanguage::Rust => "rust",
        DtoLanguage::TypeScript => "typescript",
        DtoLanguage::Python => "python",
        DtoLanguage::Go => "go",
        DtoLanguage::Java => "java",
        DtoLanguage::Kotlin => "kotlin",
        DtoLanguage::Swift => "swift",
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DtoSourceLanguage {
    Rust,
    TypeScript,
    Python,
    Go,
    Java,
    Kotlin,
    Swift,
}

pub(crate) fn parse_dto_source_language(value: &str) -> Result<DtoSourceLanguage, String> {
    match value.to_lowercase().as_str() {
        "rust" => Ok(DtoSourceLanguage::Rust),
        "typescript" => Ok(DtoSourceLanguage::TypeScript),
        "python" => Ok(DtoSourceLanguage::Python),
        "go" => Ok(DtoSourceLanguage::Go),
        "java" => Ok(DtoSourceLanguage::Java),
        "kotlin" => Ok(DtoSourceLanguage::Kotlin),
        "swift" => Ok(DtoSourceLanguage::Swift),
        _ => Err(
            "dto_language must be rust, typescript, python, go, java, kotlin, or swift".to_string(),
        ),
    }
}

pub(crate) fn dto_error_json(message: &str) -> Value {
    json!({
        "type": "dto",
        "message": message,
    })
}
