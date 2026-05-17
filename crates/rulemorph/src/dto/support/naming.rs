use std::collections::HashMap;

use crate::dto::DtoLanguage;

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

pub(super) fn words_from_key(key: &str) -> Vec<String> {
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

pub(super) fn pascal_case(words: &[String]) -> String {
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
