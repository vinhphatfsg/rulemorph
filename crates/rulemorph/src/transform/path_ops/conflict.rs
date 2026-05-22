use super::*;

pub(in crate::transform) fn has_duplicate_path(
    paths: &[Vec<PathToken>],
    tokens: &[PathToken],
) -> bool {
    paths.iter().any(|existing| existing == tokens)
}

pub(in crate::transform) fn has_path_conflict(
    paths: &[Vec<PathToken>],
    tokens: &[PathToken],
) -> bool {
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
