use std::collections::HashMap;

use crate::dto::DtoLanguage;

use super::{capitalize, lower_camel, pascal_case, snake_case, words_from_key};

mod reserved;

use self::reserved::is_reserved;

pub(in crate::dto) fn field_identifier(
    lang: DtoLanguage,
    key: &str,
    used: &mut HashMap<String, usize>,
) -> String {
    let base = match lang {
        DtoLanguage::Rust | DtoLanguage::Python => snake_case(&words_from_key(key)),
        DtoLanguage::TypeScript | DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => {
            lower_camel(&words_from_key(key))
        }
        DtoLanguage::Go => pascal_case(&words_from_key(key)),
    };

    let mut ident = if base.is_empty() {
        match lang {
            DtoLanguage::Go => "Field".to_string(),
            DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => "field".to_string(),
            _ => "field".to_string(),
        }
    } else {
        base
    };

    if ident
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(true)
    {
        ident = match lang {
            DtoLanguage::Go => format!("Field{}", ident),
            DtoLanguage::Java | DtoLanguage::Kotlin | DtoLanguage::Swift => {
                format!("field{}", capitalize(&ident))
            }
            _ => format!("_{}", ident),
        };
    }

    if is_reserved(lang, &ident) {
        ident = match lang {
            DtoLanguage::Go => format!("{}Field", ident),
            _ => format!("{}_", ident),
        };
    }

    let entry = used.entry(ident.clone()).or_insert(0);
    if *entry > 0 {
        *entry += 1;
        format!("{}_{}", ident, *entry)
    } else {
        *entry = 1;
        ident
    }
}

pub(in crate::dto) fn safe_type_name(lang: DtoLanguage, name: &str) -> String {
    let words = words_from_key(name);
    let mut ident = pascal_case(&words);
    if ident.is_empty() {
        ident = "Record".to_string();
    }
    if ident
        .chars()
        .next()
        .map(|c| c.is_ascii_digit())
        .unwrap_or(true)
    {
        ident = format!("Record{}", ident);
    }
    if is_reserved(lang, &ident) {
        ident.push_str("Record");
    }
    ident
}
