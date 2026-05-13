use crate::v2_model::V2Ref;

/// Parse a v2 reference string into V2Ref
///
/// Supported formats:
/// - `@input.path.to.field` -> V2Ref::Input("path.to.field")
/// - `@context.data[0].id` -> V2Ref::Context("data[0].id")
/// - `@out.previous_field` -> V2Ref::Out("previous_field")
/// - `@item.value` -> V2Ref::Item("value")
/// - `@acc.total` -> V2Ref::Acc("total")
/// - `@myVar` -> V2Ref::Local("myVar")
pub fn parse_v2_ref(s: &str) -> Option<V2Ref> {
    if !s.starts_with('@') {
        return None;
    }

    let rest = &s[1..]; // Remove '@' prefix

    // Check for namespace prefixes
    if let Some(path) = rest.strip_prefix("input.") {
        if path.is_empty() {
            return None;
        }
        return Some(V2Ref::Input(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("context.") {
        if path.is_empty() {
            return None;
        }
        return Some(V2Ref::Context(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("out.") {
        if path.is_empty() {
            return None;
        }
        return Some(V2Ref::Out(path.to_string()));
    }
    if rest == "input" {
        return Some(V2Ref::Input(String::new()));
    }
    if rest == "context" {
        return Some(V2Ref::Context(String::new()));
    }
    if rest == "out" {
        return Some(V2Ref::Out(String::new()));
    }
    if let Some(path) = rest.strip_prefix("item.") {
        if path.is_empty() {
            return None;
        }
        return Some(V2Ref::Item(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("item") {
        if path.is_empty() {
            return Some(V2Ref::Item(String::new()));
        }
    }
    if let Some(path) = rest.strip_prefix("acc.") {
        if path.is_empty() {
            return None;
        }
        return Some(V2Ref::Acc(path.to_string()));
    }
    if let Some(path) = rest.strip_prefix("acc") {
        if path.is_empty() {
            return Some(V2Ref::Acc(String::new()));
        }
    }

    // Check for reserved namespaces that should not be local variables
    if rest == "input" || rest == "context" || rest == "out" {
        return None; // These require a path after the dot
    }

    // Otherwise, it's a local variable reference
    if is_valid_identifier(rest) {
        return Some(V2Ref::Local(rest.to_string()));
    }

    None
}

/// Check if a string is a valid identifier (for local variable names)
fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let mut chars = s.chars();
    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    // Rest can be alphanumeric or underscore
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Check if a string represents a pipe value ($)
pub fn is_pipe_value(s: &str) -> bool {
    s == "$"
}

/// Check if a string is a literal escape (lit:...)
pub fn is_literal_escape(s: &str) -> bool {
    s.starts_with("lit:")
}

/// Extract the literal value from a lit: escaped string
pub fn extract_literal(s: &str) -> Option<&str> {
    s.strip_prefix("lit:")
}

/// Check if a string looks like a v2 reference (starts with @)
pub fn is_v2_ref(s: &str) -> bool {
    s.starts_with('@')
}
