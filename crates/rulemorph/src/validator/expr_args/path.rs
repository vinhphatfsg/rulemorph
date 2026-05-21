use crate::error::ErrorCode;
use crate::model::Expr;
use crate::path::{PathToken, parse_path};

use super::super::ValidationCtx;

pub(in crate::validator) fn validate_path_array_arg(
    expr: &Expr,
    base_path: &str,
    allow_terminal_index: bool,
    ctx: &mut ValidationCtx<'_>,
) {
    let value = match expr {
        Expr::Literal(value) => value,
        _ => return,
    };

    let mut items: Vec<(String, String)> = Vec::new();
    if let Some(path) = value.as_str() {
        items.push((base_path.to_string(), path.to_string()));
    } else if let Some(array) = value.as_array() {
        for (index, item) in array.iter().enumerate() {
            let item_path = format!("{}[{}]", base_path, index);
            let path = match item.as_str() {
                Some(path) => path,
                None => {
                    ctx.push(
                        ErrorCode::InvalidArgs,
                        "paths must be a string or array of strings",
                        item_path,
                    );
                    continue;
                }
            };
            items.push((item_path, path.to_string()));
        }
    } else {
        ctx.push(
            ErrorCode::InvalidArgs,
            "paths must be a string or array of strings",
            base_path,
        );
        return;
    }

    let mut paths: Vec<Vec<PathToken>> = Vec::new();
    for (item_path, path) in items {
        let tokens = match parse_path(&path) {
            Ok(tokens) => tokens,
            Err(_) => {
                ctx.push(
                    ErrorCode::InvalidArgs,
                    "paths must be valid path strings",
                    item_path,
                );
                continue;
            }
        };

        if !allow_terminal_index && matches!(tokens.last(), Some(PathToken::Index(_))) {
            ctx.push(
                ErrorCode::InvalidArgs,
                "path must not end with array index",
                item_path,
            );
            continue;
        }

        if paths.iter().any(|existing| existing == &tokens) {
            continue;
        }
        if has_path_conflict(&paths, &tokens) {
            ctx.push(
                ErrorCode::InvalidArgs,
                "path conflicts with another path",
                item_path,
            );
            continue;
        }
        paths.push(tokens);
    }
}

pub(in crate::validator) fn validate_path_arg(
    expr: &Expr,
    base_path: &str,
    ctx: &mut ValidationCtx<'_>,
) {
    let value = match expr {
        Expr::Literal(value) => value,
        _ => return,
    };

    let path = match value.as_str() {
        Some(path) => path,
        None => {
            ctx.push(ErrorCode::InvalidArgs, "path must be a string", base_path);
            return;
        }
    };

    if path.is_empty() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "path must be a non-empty string",
            base_path,
        );
        return;
    }

    if parse_path(path).is_err() {
        ctx.push(
            ErrorCode::InvalidArgs,
            "path must be a valid path string",
            base_path,
        );
    }
}

fn has_path_conflict(paths: &[Vec<PathToken>], tokens: &[PathToken]) -> bool {
    paths
        .iter()
        .any(|existing| is_path_prefix(existing, tokens) || is_path_prefix(tokens, existing))
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    if prefix.len() > tokens.len() {
        return false;
    }
    prefix.iter().zip(tokens).all(|(left, right)| left == right)
}
