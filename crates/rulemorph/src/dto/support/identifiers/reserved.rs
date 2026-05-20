use crate::dto::DtoLanguage;

mod go;
mod java;
mod kotlin;
mod python;
mod rust;
mod swift;
mod typescript;

use self::go::is_reserved_go;
use self::java::is_reserved_java;
use self::kotlin::is_reserved_kotlin;
use self::python::is_reserved_python;
use self::rust::is_reserved_rust;
use self::swift::is_reserved_swift;
use self::typescript::is_reserved_typescript;

pub(super) fn is_reserved(lang: DtoLanguage, ident: &str) -> bool {
    match lang {
        DtoLanguage::Rust => is_reserved_rust(ident),
        DtoLanguage::TypeScript => is_reserved_typescript(ident),
        DtoLanguage::Python => is_reserved_python(ident),
        DtoLanguage::Go => is_reserved_go(ident),
        DtoLanguage::Java => is_reserved_java(ident),
        DtoLanguage::Kotlin => is_reserved_kotlin(ident),
        DtoLanguage::Swift => is_reserved_swift(ident),
    }
}
