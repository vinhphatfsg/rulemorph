use std::collections::{HashMap, HashSet};

mod render;
mod schema;

use self::render::{
    render_go, render_java, render_kotlin, render_python, render_rust, render_swift,
    render_typescript,
};
use self::schema::{FieldType, SchemaNode, build_schema};
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

struct TypeDef<'a> {
    name: String,
    node: &'a SchemaNode,
    path: Vec<String>,
}

struct NameRegistry {
    base: String,
    used: HashSet<String>,
    names: HashMap<Vec<String>, String>,
}

impl NameRegistry {
    fn new(base: &str) -> Self {
        Self {
            base: base.to_string(),
            used: HashSet::new(),
            names: HashMap::new(),
        }
    }

    fn type_name_for_path(&mut self, path: &[String]) -> String {
        if let Some(name) = self.names.get(path) {
            return name.clone();
        }

        let mut name = self.base.clone();
        for segment in path {
            name.push_str(&pascal_case(&words_from_key(segment)));
        }

        if name.is_empty() {
            name = "Record".to_string();
        }

        let mut unique = name.clone();
        let mut suffix = 2;
        while self.used.contains(&unique) {
            unique = format!("{}_{}", name, suffix);
            suffix += 1;
        }
        self.used.insert(unique.clone());
        self.names.insert(path.to_vec(), unique.clone());
        unique
    }

    fn get(&self, path: &[String]) -> Option<&String> {
        self.names.get(path)
    }
}

fn collect_types<'a>(
    node: &'a SchemaNode,
    path: Vec<String>,
    registry: &mut NameRegistry,
    out: &mut Vec<TypeDef<'a>>,
) {
    for field in &node.fields {
        if let FieldType::Object(child) = &field.field_type {
            let mut child_path = path.clone();
            child_path.push(field.key.clone());
            registry.type_name_for_path(&child_path);
            collect_types(child, child_path, registry, out);
        }
    }

    let name = registry.type_name_for_path(&path);
    out.push(TypeDef { name, node, path });
}

fn field_identifier(lang: DtoLanguage, key: &str, used: &mut HashMap<String, usize>) -> String {
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

fn safe_type_name(lang: DtoLanguage, name: &str) -> String {
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

fn rust_string_literal(value: &str) -> String {
    format!("{:?}", value)
}

fn json_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization should not fail")
}

fn swift_string_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{{{:x}}}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn safe_comment_text(value: &str) -> String {
    value.replace("*/", "* /").replace(['\r', '\n'], "\\n")
}

fn go_json_tag_literal(key: &str, optional: bool) -> String {
    if !key.contains('`')
        && !key.contains('"')
        && !key.contains('\\')
        && !key.contains('\r')
        && !key.contains('\n')
    {
        if optional {
            return format!("`json:\"{},omitempty\"`", key);
        }
        return format!("`json:\"{}\"`", key);
    }
    let mut tag_key = String::with_capacity(key.len());
    for ch in key.chars() {
        match ch {
            '\\' => tag_key.push_str("\\\\"),
            '"' => tag_key.push_str("\\\""),
            '\n' => tag_key.push_str("\\n"),
            '\r' => tag_key.push_str("\\r"),
            '\t' => tag_key.push_str("\\t"),
            ch => tag_key.push(ch),
        }
    }
    let tag = if optional {
        format!("json:\"{},omitempty\"", tag_key)
    } else {
        format!("json:\"{}\"", tag_key)
    };
    json_string_literal(&tag)
}

fn words_from_key(key: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            words.push(current);
            current = String::new();
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    if words.is_empty() {
        words.push("field".to_string());
    }
    words
}

fn snake_case(words: &[String]) -> String {
    words
        .iter()
        .map(|word| word.to_lowercase())
        .collect::<Vec<String>>()
        .join("_")
}

fn lower_camel(words: &[String]) -> String {
    if words.is_empty() {
        return String::new();
    }

    let mut iter = words.iter();
    let first = iter
        .next()
        .map(|word| word.to_lowercase())
        .unwrap_or_default();
    let mut result = first;
    for word in iter {
        result.push_str(&capitalize(word));
    }
    result
}

fn pascal_case(words: &[String]) -> String {
    let mut result = String::new();
    for word in words {
        result.push_str(&capitalize(word));
    }
    result
}

fn capitalize(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + &chars.as_str().to_lowercase(),
        None => String::new(),
    }
}

fn is_reserved(lang: DtoLanguage, ident: &str) -> bool {
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

fn is_reserved_rust(value: &str) -> bool {
    matches!(
        value,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
    )
}

fn is_reserved_typescript(value: &str) -> bool {
    matches!(
        value,
        "break"
            | "case"
            | "catch"
            | "class"
            | "const"
            | "continue"
            | "debugger"
            | "default"
            | "delete"
            | "do"
            | "else"
            | "enum"
            | "export"
            | "extends"
            | "false"
            | "finally"
            | "for"
            | "function"
            | "if"
            | "import"
            | "in"
            | "instanceof"
            | "new"
            | "null"
            | "return"
            | "super"
            | "switch"
            | "this"
            | "throw"
            | "true"
            | "try"
            | "typeof"
            | "var"
            | "void"
            | "while"
            | "with"
            | "as"
            | "implements"
            | "interface"
            | "let"
            | "package"
            | "private"
            | "protected"
            | "public"
            | "static"
            | "yield"
            | "any"
            | "boolean"
            | "number"
            | "string"
            | "symbol"
            | "type"
            | "from"
            | "of"
    )
}

fn is_reserved_python(value: &str) -> bool {
    matches!(
        value,
        "False"
            | "None"
            | "True"
            | "and"
            | "as"
            | "assert"
            | "async"
            | "await"
            | "break"
            | "class"
            | "continue"
            | "def"
            | "del"
            | "elif"
            | "else"
            | "except"
            | "finally"
            | "for"
            | "from"
            | "global"
            | "if"
            | "import"
            | "in"
            | "is"
            | "lambda"
            | "nonlocal"
            | "not"
            | "or"
            | "pass"
            | "raise"
            | "return"
            | "try"
            | "while"
            | "with"
            | "yield"
    )
}

fn is_reserved_go(value: &str) -> bool {
    matches!(
        value,
        "break"
            | "default"
            | "func"
            | "interface"
            | "select"
            | "case"
            | "defer"
            | "go"
            | "map"
            | "struct"
            | "chan"
            | "else"
            | "goto"
            | "package"
            | "switch"
            | "const"
            | "fallthrough"
            | "if"
            | "range"
            | "type"
            | "continue"
            | "for"
            | "import"
            | "return"
            | "var"
    )
}

fn is_reserved_java(value: &str) -> bool {
    matches!(
        value,
        "abstract"
            | "assert"
            | "boolean"
            | "break"
            | "byte"
            | "case"
            | "catch"
            | "char"
            | "class"
            | "const"
            | "continue"
            | "default"
            | "do"
            | "double"
            | "else"
            | "enum"
            | "extends"
            | "final"
            | "finally"
            | "float"
            | "for"
            | "goto"
            | "if"
            | "implements"
            | "import"
            | "instanceof"
            | "int"
            | "interface"
            | "long"
            | "native"
            | "new"
            | "package"
            | "private"
            | "protected"
            | "public"
            | "return"
            | "short"
            | "static"
            | "strictfp"
            | "super"
            | "switch"
            | "synchronized"
            | "this"
            | "throw"
            | "throws"
            | "transient"
            | "try"
            | "void"
            | "volatile"
            | "while"
    )
}

fn is_reserved_kotlin(value: &str) -> bool {
    matches!(
        value,
        "as" | "break"
            | "class"
            | "continue"
            | "do"
            | "else"
            | "false"
            | "for"
            | "fun"
            | "if"
            | "in"
            | "interface"
            | "is"
            | "null"
            | "object"
            | "package"
            | "return"
            | "super"
            | "this"
            | "throw"
            | "true"
            | "try"
            | "typealias"
            | "val"
            | "var"
            | "when"
            | "while"
    )
}

fn is_reserved_swift(value: &str) -> bool {
    matches!(
        value,
        "class"
            | "deinit"
            | "enum"
            | "extension"
            | "func"
            | "import"
            | "init"
            | "let"
            | "protocol"
            | "static"
            | "struct"
            | "subscript"
            | "typealias"
            | "var"
            | "break"
            | "case"
            | "continue"
            | "default"
            | "defer"
            | "do"
            | "else"
            | "fallthrough"
            | "for"
            | "guard"
            | "if"
            | "in"
            | "repeat"
            | "return"
            | "switch"
            | "where"
            | "while"
            | "as"
            | "Any"
            | "catch"
            | "false"
            | "is"
            | "nil"
            | "rethrows"
            | "super"
            | "self"
            | "Self"
            | "throw"
            | "throws"
            | "true"
            | "try"
    )
}
