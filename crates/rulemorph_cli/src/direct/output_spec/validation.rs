use std::cmp::Ordering;
use std::collections::HashSet;

use rulemorph::{NormalizationOptions, PathToken, parse_path};

use super::{DirectCliError, DirectOutputMode, DirectOutputSpec, FieldSpec};

const MAX_DIRECT_OUTPUT_FIELDS: usize = 10_000;
const MAX_DIRECT_OUTPUT_SPEC_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_BYTES: usize = 256 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_TARGET_DEPTH: usize = 256;
const MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL: usize = 1_000_000;
const MAX_DIRECT_OUTPUT_EXPR_DEPTH: usize = 256;
const MAX_DIRECT_OUTPUT_EXPR_NODES: usize = 1_000_000;
const MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES: usize = 8 * 1024 * 1024;
const MAX_DIRECT_OUTPUT_CELLS: usize = 10_000_000;

pub(super) fn validate_output_spec_size(bytes: usize) -> Result<(), String> {
    if bytes > MAX_DIRECT_OUTPUT_SPEC_BYTES {
        return Err(format!(
            "direct output spec must be at most {} bytes",
            MAX_DIRECT_OUTPUT_SPEC_BYTES
        ));
    }
    Ok(())
}

pub(super) fn validate_target(target: &str) -> Result<Vec<PathToken>, String> {
    if target.len() > MAX_DIRECT_OUTPUT_TARGET_BYTES {
        return Err(format!(
            "direct output target names must be at most {} bytes each",
            MAX_DIRECT_OUTPUT_TARGET_BYTES
        ));
    }
    let tokens = parse_path(target)
        .map_err(|err| format!("direct output target path is invalid: {}", err.message()))?;
    if tokens
        .iter()
        .any(|token| matches!(token, PathToken::Index(_)))
    {
        return Err("direct output target path must not include indexes".to_string());
    }
    if tokens.len() > MAX_DIRECT_OUTPUT_TARGET_DEPTH {
        return Err(format!(
            "direct output target path exceeds configured depth limit ({})",
            MAX_DIRECT_OUTPUT_TARGET_DEPTH
        ));
    }
    Ok(tokens)
}

pub(super) fn validate_field_specs(fields: &[FieldSpec]) -> Result<(), String> {
    if fields.len() > MAX_DIRECT_OUTPUT_FIELDS {
        return Err(format!(
            "direct output has too many fields; maximum is {}",
            MAX_DIRECT_OUTPUT_FIELDS
        ));
    }
    let mut total_target_bytes = 0usize;
    let mut total_tokens = 0usize;
    let mut seen = HashSet::new();
    for field in fields {
        total_target_bytes = checked_add_limit(total_target_bytes, field.target.len())?;
        if total_target_bytes > MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL {
            return Err(format!(
                "direct output target names total size must be at most {} bytes",
                MAX_DIRECT_OUTPUT_TARGET_BYTES_TOTAL
            ));
        }
        total_tokens = checked_add_limit(total_tokens, field.tokens.len())?;
        if total_tokens > MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL {
            return Err(format!(
                "direct output target paths have too many tokens; maximum is {}",
                MAX_DIRECT_OUTPUT_TARGET_TOKENS_TOTAL
            ));
        }
        if !seen.insert(field.tokens.clone()) {
            return Err("direct output target is duplicated".to_string());
        }
    }
    if has_parent_child_target_conflict(fields) {
        return Err("direct output target conflicts with another target".to_string());
    }
    Ok(())
}

fn has_parent_child_target_conflict(fields: &[FieldSpec]) -> bool {
    let mut paths = fields
        .iter()
        .map(|field| field.tokens.as_slice())
        .collect::<Vec<_>>();
    paths.sort_unstable_by(|left, right| compare_path_tokens(left, right));
    paths
        .windows(2)
        .any(|pair| pair[0].len() < pair[1].len() && pair[1].starts_with(pair[0]))
}

fn compare_path_tokens(left: &[PathToken], right: &[PathToken]) -> Ordering {
    for (left_token, right_token) in left.iter().zip(right.iter()) {
        let ordering = compare_path_token(left_token, right_token);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

fn compare_path_token(left: &PathToken, right: &PathToken) -> Ordering {
    match (left, right) {
        (PathToken::Key(left), PathToken::Key(right)) => left.cmp(right),
        (PathToken::Index(left), PathToken::Index(right)) => left.cmp(right),
        (PathToken::Key(_), PathToken::Index(_)) => Ordering::Less,
        (PathToken::Index(_), PathToken::Key(_)) => Ordering::Greater,
    }
}

pub(super) fn validate_direct_output_cells(
    spec: &DirectOutputSpec,
    options: &NormalizationOptions,
    output_cell_record_budget: usize,
) -> Result<(), DirectCliError> {
    if spec.output_mode() != DirectOutputMode::RecordObject {
        return Ok(());
    }
    let records = output_cell_record_budget.min(options.max_records);
    let cells = records
        .checked_mul(spec.output_field_count())
        .ok_or_else(|| DirectCliError::validation("direct output cells are too large"))?;
    if cells > MAX_DIRECT_OUTPUT_CELLS {
        return Err(DirectCliError::validation(format!(
            "direct output cells must be at most {}",
            MAX_DIRECT_OUTPUT_CELLS
        )));
    }
    Ok(())
}

pub(super) fn validate_expr_limits(expr: &serde_json::Value) -> Result<(), String> {
    let mut stats = ExprStats::default();
    collect_expr_stats(expr, 0, &mut stats)?;
    if stats.max_depth > MAX_DIRECT_OUTPUT_EXPR_DEPTH {
        return Err(format!(
            "direct output expr exceeds configured depth limit ({})",
            MAX_DIRECT_OUTPUT_EXPR_DEPTH
        ));
    }
    if stats.nodes > MAX_DIRECT_OUTPUT_EXPR_NODES {
        return Err(format!(
            "direct output expr has too many nodes; maximum is {}",
            MAX_DIRECT_OUTPUT_EXPR_NODES
        ));
    }
    Ok(())
}

#[derive(Default)]
struct ExprStats {
    max_depth: usize,
    nodes: usize,
}

fn collect_expr_stats(
    value: &serde_json::Value,
    depth: usize,
    stats: &mut ExprStats,
) -> Result<(), String> {
    stats.nodes = checked_add_limit(stats.nodes, 1)?;
    stats.max_depth = stats.max_depth.max(depth);
    match value {
        serde_json::Value::String(text) if text.len() > MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES => {
            Err(format!(
                "direct output expr string values must be at most {} bytes each",
                MAX_DIRECT_OUTPUT_EXPR_STRING_BYTES
            ))
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_expr_stats(item, depth + 1, stats)?;
            }
            Ok(())
        }
        serde_json::Value::Object(object) => {
            for value in object.values() {
                collect_expr_stats(value, depth + 1, stats)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

pub(super) fn checked_add_limit(left: usize, right: usize) -> Result<usize, String> {
    left.checked_add(right)
        .ok_or_else(|| "direct output spec is too large".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub(super) fn validate_field_specs_rejects_large_parent_child_conflict() {
        let shared_prefix = (0..255)
            .map(|index| PathToken::Key(format!("p{}", index)))
            .collect::<Vec<_>>();
        let mut fields = (0..1000)
            .map(|index| {
                let mut tokens = shared_prefix.clone();
                tokens.push(PathToken::Key(format!("k{}", index)));
                FieldSpec {
                    target: format!("t{}", index),
                    expr: serde_json::Value::Null,
                    tokens,
                }
            })
            .collect::<Vec<_>>();
        fields.push(FieldSpec {
            target: "parent".to_string(),
            expr: serde_json::Value::Null,
            tokens: shared_prefix,
        });

        let error = validate_field_specs(&fields).unwrap_err();

        assert_eq!(error, "direct output target conflicts with another target");
    }
}
