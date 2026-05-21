mod braced;
mod python;
mod rust;
mod typescript;

pub(crate) use braced::{normalize_java_text, normalize_kotlin_text, normalize_swift_text};
pub(crate) use python::normalize_python_text;
pub(crate) use rust::normalize_rust_text;
pub(crate) use typescript::normalize_typescript_text;
