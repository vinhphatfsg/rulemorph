use crate::error::ErrorCode;
use crate::path::{PathToken, parse_path};
use crate::v2_model::V2Ref;

use super::{V2Scope, V2ValidationCtx};

/// Validate a v2 reference
pub fn validate_v2_ref(
    v2_ref: &V2Ref,
    base_path: &str,
    scope: &V2Scope,
    ctx: &mut V2ValidationCtx<'_>,
) {
    match v2_ref {
        V2Ref::Input(path) => {
            validate_path_syntax(path, base_path, ctx);
        }
        V2Ref::Context(path) => {
            validate_path_syntax(path, base_path, ctx);
            ctx.context_referenced = true;
        }
        V2Ref::Out(path) => {
            validate_path_syntax(path, base_path, ctx);
            validate_out_not_forward(path, base_path, ctx);
        }
        V2Ref::Item(path) => {
            if !scope.allows_item() {
                ctx.push_error(
                    ErrorCode::InvalidItemRef,
                    "@item is only valid inside map/filter operations",
                    base_path,
                );
            } else {
                validate_item_path(path, base_path, ctx);
            }
        }
        V2Ref::Acc(path) => {
            if !scope.allows_acc() {
                ctx.push_error(
                    ErrorCode::InvalidAccRef,
                    "@acc is only valid inside reduce/fold operations",
                    base_path,
                );
            } else if !path.is_empty() {
                validate_path_syntax(path, base_path, ctx);
            }
        }
        V2Ref::Local(name) => {
            if !scope.has_binding(name) {
                ctx.push_error(
                    ErrorCode::UndefinedVariable,
                    format!("undefined variable: @{}", name),
                    base_path,
                );
            }
        }
    }
}

/// Validate path syntax
fn validate_path_syntax(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if path.is_empty() {
        return; // Empty path is valid (returns entire namespace)
    }
    if parse_path(path).is_err() {
        ctx.push_error(ErrorCode::InvalidPath, "invalid path syntax", base_path);
    }
}

/// Validate @item path (supports @item, @item.path, @item.index)
fn validate_item_path(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if path.is_empty() {
        return; // @item with no path is valid
    }
    if path == "index" || path == "value" {
        return; // @item.index / @item.value are valid
    }
    // Direct field access on item value is also valid
    validate_path_syntax(path, base_path, ctx);
}

/// Validate @out reference is not a forward reference
fn validate_out_not_forward(path: &str, base_path: &str, ctx: &mut V2ValidationCtx<'_>) {
    if ctx.allow_any_out_ref {
        return;
    }
    if path.is_empty() {
        return;
    }

    let tokens = match parse_path(path) {
        Ok(t) => t,
        Err(_) => return, // Path syntax error handled elsewhere
    };

    // Extract key tokens (ignore array indices for forward reference check)
    let key_tokens: Vec<PathToken> = tokens
        .iter()
        .filter_map(|t| match t {
            PathToken::Key(k) => Some(PathToken::Key(k.clone())),
            PathToken::Index(_) => None,
        })
        .collect();

    if key_tokens.is_empty() {
        ctx.push_error(
            ErrorCode::ForwardOutReference,
            "out reference must have at least one key",
            base_path,
        );
        return;
    }

    // Check if any prefix of the path has been produced
    for end in (1..=key_tokens.len()).rev() {
        if ctx.produced_targets.contains(&key_tokens[..end].to_vec()) {
            return; // Found a matching prefix
        }
    }

    ctx.push_error(
        ErrorCode::ForwardOutReference,
        "out reference must point to previous mappings",
        base_path,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_item_ref_outside_map() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new(); // No @item scope
        let v2_ref = V2Ref::Item("value".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::InvalidItemRef);
    }

    #[test]
    fn test_validate_item_ref_inside_map() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new().with_item();
        let v2_ref = V2Ref::Item("value".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_validate_item_index() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new().with_item();
        let v2_ref = V2Ref::Item("index".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }

    #[test]
    fn test_validate_item_ref_invalid_subpath() {
        let scope = V2Scope::new().with_item();

        let mut ctx = V2ValidationCtx::new(None);
        let v2_ref = V2Ref::Item("value..foo".to_string());
        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
        assert!(
            ctx.errors()
                .iter()
                .any(|err| err.code == ErrorCode::InvalidPath)
        );

        let mut ctx = V2ValidationCtx::new(None);
        let v2_ref = V2Ref::Item("index..foo".to_string());
        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);
        assert!(
            ctx.errors()
                .iter()
                .any(|err| err.code == ErrorCode::InvalidPath)
        );
    }

    #[test]
    fn test_validate_undefined_local() {
        let mut ctx = V2ValidationCtx::new(None);
        let scope = V2Scope::new();
        let v2_ref = V2Ref::Local("undefined_var".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(ctx.has_errors());
        assert_eq!(ctx.errors()[0].code, ErrorCode::UndefinedVariable);
    }

    #[test]
    fn test_validate_defined_local() {
        let mut ctx = V2ValidationCtx::new(None);
        let mut scope = V2Scope::new();
        scope.add_binding("x".to_string());
        let v2_ref = V2Ref::Local("x".to_string());

        validate_v2_ref(&v2_ref, "test", &scope, &mut ctx);

        assert!(!ctx.has_errors());
    }
}
