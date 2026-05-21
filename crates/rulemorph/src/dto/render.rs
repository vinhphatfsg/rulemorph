use std::collections::HashMap;

mod go;
mod jvm;
mod python;
mod rust;
mod swift;
mod typescript;

use super::DtoLanguage;
use super::schema::{FieldType, SchemaNode, node_has_required};
use super::support::field_identifier;

pub(super) use self::go::render_go;
pub(super) use self::jvm::{render_java, render_kotlin};
pub(super) use self::python::render_python;
pub(super) use self::rust::render_rust;
pub(super) use self::swift::render_swift;
pub(super) use self::typescript::render_typescript;

fn schema_has_optional(node: &SchemaNode) -> bool {
    for field in &node.fields {
        match &field.field_type {
            FieldType::Object(child) => {
                if !node_has_required(child) || schema_has_optional(child) {
                    return true;
                }
            }
            _ => {
                if field.optional {
                    return true;
                }
            }
        }
    }
    false
}

fn schema_has_rename(node: &SchemaNode, lang: DtoLanguage) -> bool {
    let mut used = HashMap::new();
    for field in &node.fields {
        let ident = field_identifier(lang, &field.key, &mut used);
        if ident != field.key {
            return true;
        }
        if let FieldType::Object(child) = &field.field_type {
            if schema_has_rename(child, lang) {
                return true;
            }
        }
    }
    false
}
