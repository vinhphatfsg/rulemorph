use crate::dto_language::DtoSourceLanguage;
use crate::dto_schema::DtoSchema;

mod go;
mod jvm;
mod python;
mod rust_dto;
mod swift;
mod typescript;

use self::go::parse_go_types;
use self::jvm::{parse_java_types, parse_kotlin_types};
use self::python::parse_python_types;
use self::rust_dto::parse_rust_types;
use self::swift::parse_swift_types;
use self::typescript::parse_typescript_types;

pub(crate) fn parse_dto_schema(
    text: &str,
    language: DtoSourceLanguage,
) -> Result<DtoSchema, String> {
    let (types, order) = match language {
        DtoSourceLanguage::TypeScript => parse_typescript_types(text)?,
        DtoSourceLanguage::Rust => parse_rust_types(text)?,
        DtoSourceLanguage::Python => parse_python_types(text)?,
        DtoSourceLanguage::Go => parse_go_types(text)?,
        DtoSourceLanguage::Java => parse_java_types(text)?,
        DtoSourceLanguage::Kotlin => parse_kotlin_types(text)?,
        DtoSourceLanguage::Swift => parse_swift_types(text)?,
    };

    let root = if types.contains_key("Record") {
        "Record".to_string()
    } else {
        order
            .first()
            .cloned()
            .ok_or_else(|| "no dto types found".to_string())?
    };

    Ok(DtoSchema { root, types })
}

fn parse_first_quoted_value(text: &str) -> Option<String> {
    let mut best: Option<(usize, char)> = None;
    for quote in ['"', '\''] {
        if let Some(pos) = text.find(quote) {
            if best.map_or(true, |(best_pos, _)| pos < best_pos) {
                best = Some((pos, quote));
            }
        }
    }

    let (pos, quote) = best?;
    let after = &text[pos + 1..];
    let end = after.find(quote)?;
    Some(after[..end].to_string())
}

fn parse_named_argument(line: &str, key: &str) -> Option<String> {
    let start = line.find(key)?;
    let after = &line[start + key.len()..];
    let eq_pos = after.find('=')?;
    let after_eq = after[eq_pos + 1..].trim_start();
    parse_first_quoted_value(after_eq)
}
