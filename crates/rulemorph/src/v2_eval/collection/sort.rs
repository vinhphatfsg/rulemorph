use serde_json::Value as JsonValue;

use super::eval_v2_array_from_eval_value;
use crate::error::{TransformError, TransformErrorKind};
use crate::v2_eval::{EvalItem, EvalValue, V2EvalContext, eval_v2_expr, value_to_string};
use crate::v2_model::{V2Expr, V2OpStep};

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

pub(super) fn eval_sort_by<'a>(
    op_step: &V2OpStep,
    pipe_value: EvalValue,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<EvalValue, TransformError> {
    if !(1..=2).contains(&op_step.args.len()) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort_by requires one or two arguments",
        )
        .with_path(path));
    }
    let array = eval_v2_array_from_eval_value(pipe_value.clone(), path)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }
    let expr_path = format!("{}.args[0]", path);
    let order = if op_step.args.len() == 2 {
        let order_path = format!("{}.args[1]", path);
        let order_value = eval_v2_expr(&op_step.args[1], record, context, out, &order_path, ctx)?;
        let order = match order_value {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value_to_string(&value, &order_path)?,
        };
        if order != "asc" && order != "desc" {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "order must be asc or desc",
            )
            .with_path(order_path));
        }
        order
    } else {
        "asc".to_string()
    };

    struct SortItem {
        key: SortKey,
        index: usize,
        value: JsonValue,
    }

    let mut items = Vec::with_capacity(array.len());
    let mut key_kind: Option<SortKeyKind> = None;
    for (index, item) in array.iter().enumerate() {
        let item_ctx = ctx
            .clone()
            .with_pipe_value(EvalValue::Value(item.clone()))
            .with_item(EvalItem { value: item, index });
        let key = eval_v2_sort_key(
            &op_step.args[0],
            record,
            context,
            out,
            &expr_path,
            &item_ctx,
        )?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "sort_by keys must be all the same type",
                )
                .with_path(expr_path));
            }
        } else {
            key_kind = Some(kind);
        }
        items.push(SortItem {
            key,
            index,
            value: item.clone(),
        });
    }

    items.sort_by(|left, right| {
        let mut ordering = compare_sort_keys(&left.key, &right.key);
        if order == "desc" {
            ordering = ordering.reverse();
        }
        if ordering == std::cmp::Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ordering
        }
    });

    let results = items.into_iter().map(|item| item.value).collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn compare_sort_keys(left: &SortKey, right: &SortKey) -> std::cmp::Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => {
            l.partial_cmp(r).unwrap_or(std::cmp::Ordering::Equal)
        }
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => std::cmp::Ordering::Equal,
    }
}

fn eval_v2_sort_key<'a>(
    expr: &V2Expr,
    record: &'a JsonValue,
    context: Option<&'a JsonValue>,
    out: &'a JsonValue,
    path: &str,
    ctx: &V2EvalContext<'a>,
) -> Result<SortKey, TransformError> {
    let value = match eval_v2_expr(expr, record, context, out, path, ctx)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| {
                    TransformError::new(
                        TransformErrorKind::ExprError,
                        "sort_by key must be a finite number",
                    )
                    .with_path(path)
                })?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort_by key must be string/number/bool",
        )
        .with_path(path)),
    }
}
