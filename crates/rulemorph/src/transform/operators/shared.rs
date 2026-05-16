use super::*;

pub(in crate::transform) fn locals_with_item<'a>(
    locals: Option<&EvalLocals<'a>>,
    item: EvalItem<'a>,
) -> EvalLocals<'a> {
    EvalLocals {
        item: Some(item),
        acc: locals.and_then(|locals| locals.acc),
        pipe: locals.and_then(|locals| locals.pipe),
        locals: locals.and_then(|locals| locals.locals),
        precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
    }
}

pub(in crate::transform) fn locals_with_precomputed_args<'a>(
    locals: Option<&EvalLocals<'a>>,
    base_path: &'a str,
    arg_values: &'a [EvalValue],
) -> EvalLocals<'a> {
    EvalLocals {
        item: locals.and_then(|locals| locals.item),
        acc: locals.and_then(|locals| locals.acc),
        pipe: locals.and_then(|locals| locals.pipe),
        locals: locals.and_then(|locals| locals.locals),
        precomputed_op_args: Some((base_path, arg_values)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::transform) enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
pub(in crate::transform) enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    pub(in crate::transform) fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

pub(in crate::transform) fn compare_sort_keys(left: &SortKey, right: &SortKey) -> Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}
