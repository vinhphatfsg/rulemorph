use std::collections::HashMap;

mod go;
mod jvm;
mod python;
mod rust;
mod swift;
mod typescript;

use super::DtoLanguage;
use super::schema::{FieldType, SchemaNode, field_is_optional};
use super::support::field_identifier;

pub(super) use self::go::render_go;
pub(super) use self::jvm::{render_java, render_kotlin};
pub(super) use self::python::render_python;
pub(super) use self::rust::render_rust;
pub(super) use self::swift::render_swift;
pub(super) use self::typescript::render_typescript;

fn schema_has_optional(node: &SchemaNode) -> bool {
    for field in &node.fields {
        if field_is_optional(field) || field_type_has_optional(&field.field_type) {
            return true;
        }
    }
    false
}

fn field_type_has_optional(field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Nullable(_) => true,
        FieldType::Object(child) => schema_has_optional(child),
        FieldType::Array(inner) | FieldType::Map(inner) => field_type_has_optional(inner),
        FieldType::Primitive(_) | FieldType::JsonValue => false,
    }
}

fn schema_has_rename(node: &SchemaNode, lang: DtoLanguage) -> bool {
    let mut used = HashMap::new();
    for field in &node.fields {
        let ident = field_identifier(lang, &field.key, &mut used);
        if ident != field.key {
            return true;
        }
        if field_type_has_rename(&field.field_type, lang) {
            return true;
        }
    }
    false
}

fn field_type_has_rename(field_type: &FieldType, lang: DtoLanguage) -> bool {
    match field_type {
        FieldType::Object(child) => schema_has_rename(child, lang),
        FieldType::Array(inner) | FieldType::Map(inner) | FieldType::Nullable(inner) => {
            field_type_has_rename(inner, lang)
        }
        FieldType::Primitive(_) | FieldType::JsonValue => false,
    }
}
