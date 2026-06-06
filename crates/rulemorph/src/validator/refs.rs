use std::collections::HashSet;

use crate::error::ErrorCode;
use crate::model::ExprRef;
use crate::path::{PathToken, parse_path};

use super::ValidationCtx;
use super::scope::LocalScope;

pub(super) fn validate_source(
    source: &str,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
) {
    let full_path = format!("{}.source", base_path);
    let (namespace, path) = match parse_source(source) {
        Some(parsed) => parsed,
        None => {
            ctx.push(
                ErrorCode::InvalidRefNamespace,
                "ref namespace must be input|context|out",
                full_path,
            );
            return;
        }
    };

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            ctx.push(ErrorCode::InvalidPath, "path is invalid", full_path);
            return;
        }
    };

    if namespace == Namespace::Out
        && !ctx.allow_any_out_ref
        && !out_ref_resolves(&tokens, produced_targets, ctx)
    {
        ctx.push(
            ErrorCode::ForwardOutReference,
            "out reference must point to previous mappings",
            full_path,
        );
    }
}

pub(super) fn validate_ref(
    expr_ref: &ExprRef,
    base_path: &str,
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &mut ValidationCtx<'_>,
    scope: LocalScope,
) {
    let (namespace, path) = match parse_ref(&expr_ref.ref_path) {
        Some(parsed) => parsed,
        None => {
            ctx.push(
                ErrorCode::InvalidRefNamespace,
                "ref namespace must be input|context|out|item|acc",
                base_path,
            );
            return;
        }
    };

    match namespace {
        Namespace::Item => {
            if !scope.allows_item() {
                ctx.push(
                    ErrorCode::InvalidRefNamespace,
                    "item refs are only allowed inside array ops",
                    base_path,
                );
                return;
            }
        }
        Namespace::Acc => {
            if !scope.allows_acc() {
                ctx.push(
                    ErrorCode::InvalidRefNamespace,
                    "acc refs are only allowed inside reduce/fold ops",
                    base_path,
                );
                return;
            }
        }
        _ => {}
    }

    let tokens = match parse_path(path) {
        Ok(tokens) => tokens,
        Err(_) => {
            ctx.push(ErrorCode::InvalidPath, "path is invalid", base_path);
            return;
        }
    };

    match namespace {
        Namespace::Out => {
            if !ctx.allow_any_out_ref && !out_ref_resolves(&tokens, produced_targets, ctx) {
                ctx.push(
                    ErrorCode::ForwardOutReference,
                    "out reference must point to previous mappings",
                    base_path,
                );
            }
        }
        Namespace::Item => {
            let ok = matches!(tokens.first(), Some(PathToken::Key(key)) if key == "value" || key == "index");
            if !ok {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "item ref must start with value or index",
                    base_path,
                );
            }
        }
        Namespace::Acc => {
            let ok = matches!(tokens.first(), Some(PathToken::Key(key)) if key == "value");
            if !ok {
                ctx.push(
                    ErrorCode::InvalidPath,
                    "acc ref must start with value",
                    base_path,
                );
            }
        }
        _ => {}
    }
}

fn out_ref_resolves(
    tokens: &[PathToken],
    produced_targets: &HashSet<Vec<PathToken>>,
    ctx: &ValidationCtx<'_>,
) -> bool {
    out_ref_resolves_in_targets(tokens, produced_targets)
        || out_ref_resolves_in_targets(tokens, &ctx.branch_out_ref_targets)
}

pub(super) fn out_ref_resolves_in_targets(
    tokens: &[PathToken],
    produced_targets: &HashSet<Vec<PathToken>>,
) -> bool {
    if !tokens
        .iter()
        .any(|token| matches!(token, PathToken::Key(_)))
    {
        return false;
    }

    for produced in produced_targets {
        if is_path_prefix(produced, tokens) || is_path_prefix(tokens, produced) {
            return true;
        }
    }
    false
}

fn is_path_prefix(prefix: &[PathToken], tokens: &[PathToken]) -> bool {
    prefix.len() <= tokens.len() && prefix.iter().zip(tokens).all(|(left, right)| left == right)
}

fn parse_ref(value: &str) -> Option<(Namespace, &str)> {
    let mut parts = value.splitn(2, '.');
    let namespace = parts.next()?;
    let path = parts.next()?;
    if path.is_empty() {
        return None;
    }

    let namespace = match namespace {
        "input" => Namespace::Input,
        "context" => Namespace::Context,
        "out" => Namespace::Out,
        "item" => Namespace::Item,
        "acc" => Namespace::Acc,
        _ => return None,
    };

    Some((namespace, path))
}

fn parse_source(value: &str) -> Option<(Namespace, &str)> {
    if let Some((prefix, path)) = value.split_once('.') {
        if path.is_empty() {
            return None;
        }
        let namespace = match prefix {
            "input" => Namespace::Input,
            "context" => Namespace::Context,
            "out" => Namespace::Out,
            _ => return None,
        };
        Some((namespace, path))
    } else {
        if value.is_empty() {
            return None;
        }
        Some((Namespace::Input, value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Namespace {
    Input,
    Context,
    Out,
    Item,
    Acc,
}
