use super::*;

mod args;
mod boolean;
mod date;
mod json;
mod lookup;
mod number;
mod string;
mod value;

pub(super) use self::args::{arg_expr_at, args_len};
use self::args::{
    eval_arg_string_at, eval_arg_value_at, eval_expr_at_index, eval_expr_value_or_null_at,
};
use self::boolean::{compare_eq, eval_bool_and_or, eval_bool_not, eval_compare};
use self::date::{eval_date_format, eval_to_unixtime};
use self::json::{
    eval_json_entries, eval_json_from_entries, eval_json_get, eval_json_keys, eval_json_merge,
    eval_json_object_flatten, eval_json_object_unflatten, eval_json_omit, eval_json_pick,
    eval_json_values, eval_len,
};
use self::lookup::eval_lookup;
use self::number::{eval_numeric_op, eval_round, eval_to_base};
use self::string::{eval_pad, eval_replace, eval_split, eval_unary_string_op};
pub(super) use self::value::{cast_value, value_as_bool, value_to_string};
use self::value::{
    expr_type_error, json_number_from_f64, to_radix_string, value_as_string, value_to_i64,
    value_to_number, value_to_string_optional,
};

pub(crate) fn eval_op(
    expr_op: &ExprOp,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    injected: Option<&EvalValue>,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(&expr_op.args, injected);
    if total_len == 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must be a non-empty array",
        )
        .with_path(format!("{}.args", base_path)));
    }

    match expr_op.op.as_str() {
        "concat" => {
            let mut parts = Vec::new();
            for index in 0..total_len {
                let arg_path = format!("{}.args[{}]", base_path, index);
                let value = eval_expr_at_index(
                    index,
                    &expr_op.args,
                    injected,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                )?;
                match value {
                    EvalValue::Missing => return Ok(EvalValue::Missing),
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            return Err(TransformError::new(
                                TransformErrorKind::ExprError,
                                "concat does not accept null",
                            )
                            .with_path(arg_path));
                        }
                        let part = value_to_string(&value, &arg_path)?;
                        parts.push(part);
                    }
                }
            }
            Ok(EvalValue::Value(JsonValue::String(parts.join(""))))
        }
        "coalesce" => {
            for index in 0..total_len {
                let value = eval_expr_at_index(
                    index,
                    &expr_op.args,
                    injected,
                    record,
                    context,
                    out,
                    base_path,
                    locals,
                )?;
                match value {
                    EvalValue::Missing => continue,
                    EvalValue::Value(value) => {
                        if value.is_null() {
                            continue;
                        }
                        return Ok(EvalValue::Value(value));
                    }
                }
            }
            Ok(EvalValue::Missing)
        }
        "to_string" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| value_to_string(value, path).map(JsonValue::String),
        ),
        "trim" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.trim().to_string()))
            },
        ),
        "lowercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_lowercase()))
            },
        ),
        "uppercase" => eval_unary_string_op(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
            |value, path| {
                let s = value_as_string(value, path)?;
                Ok(JsonValue::String(s.to_uppercase()))
            },
        ),
        "replace" => eval_replace(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "split" => eval_split(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pad_start" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "pad_end" => eval_pad(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "lookup" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "lookup_first" => eval_lookup(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "deep_merge" => eval_json_merge(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "get" => eval_json_get(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "pick" => eval_json_pick(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "omit" => eval_json_omit(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "keys" => eval_json_keys(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "values" => eval_json_values(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "entries" => eval_json_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "len" => eval_len(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "from_entries" => eval_json_from_entries(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_flatten" => eval_json_object_flatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "object_unflatten" => eval_json_object_unflatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "map" => eval_array_map(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "filter" => eval_array_filter(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "flat_map" => eval_array_flat_map(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "flatten" => eval_array_flatten(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "take" => eval_array_take(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "drop" => eval_array_drop(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "slice" => eval_array_slice(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "chunk" => eval_array_chunk(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "zip" => eval_array_zip(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "zip_with" => eval_array_zip_with(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "unzip" => eval_array_unzip(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "group_by" => eval_array_group_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "key_by" => eval_array_key_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "partition" => eval_array_partition(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "unique" => eval_array_unique(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "distinct_by" => eval_array_distinct_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "sort_by" => eval_array_sort_by(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "find" => eval_array_find(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "find_index" => eval_array_find_index(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "index_of" => eval_array_index_of(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "contains" => eval_array_contains(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "sum" => eval_array_sum(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "avg" => eval_array_avg(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "min" => eval_array_min(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "max" => eval_array_max(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "reduce" => eval_array_reduce(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "fold" => eval_array_fold(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "+" | "-" | "*" | "/" => {
            eval_numeric_op(expr_op, injected, record, context, out, base_path, locals)
        }
        "round" => eval_round(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_base" => eval_to_base(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "date_format" => eval_date_format(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "to_unixtime" => eval_to_unixtime(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "and" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            true,
            locals,
        ),
        "or" => eval_bool_and_or(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            false,
            locals,
        ),
        "not" => eval_bool_not(
            &expr_op.args,
            injected,
            record,
            context,
            out,
            base_path,
            locals,
        ),
        "==" | "!=" | "<" | "<=" | ">" | ">=" | "~=" => {
            eval_compare(expr_op, injected, record, context, out, base_path, locals)
        }
        _ => Err(
            TransformError::new(TransformErrorKind::ExprError, "expr.op is not supported")
                .with_path(format!("{}.op", base_path)),
        ),
    }
}

pub(super) fn locals_with_item<'a>(
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

pub(super) fn locals_with_precomputed_args<'a>(
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

fn eval_array_arg(
    index: usize,
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<Vec<JsonValue>, TransformError> {
    let arg_path = format!("{}.args[{}]", base_path, index);
    match eval_expr_at_index(
        index, args, injected, record, context, out, base_path, locals,
    )? {
        EvalValue::Missing => Ok(Vec::new()),
        EvalValue::Value(value) => {
            if value.is_null() {
                Ok(Vec::new())
            } else if let JsonValue::Array(items) = value {
                Ok(items)
            } else {
                Err(
                    TransformError::new(TransformErrorKind::ExprError, "expr arg must be an array")
                        .with_path(arg_path),
                )
            }
        }
    }
}

fn eval_expr_or_null(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<JsonValue, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(JsonValue::Null),
        EvalValue::Value(value) => Ok(value),
    }
}

fn eval_predicate_expr(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<bool, TransformError> {
    match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => Ok(false),
        EvalValue::Value(value) => {
            if value.is_null() {
                return Ok(false);
            }
            let flag = value_as_bool(&value, base_path)?;
            Ok(flag)
        }
    }
}

fn eval_key_expr_string(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<String, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }
    value_to_string(&value, base_path)
}

fn ensure_eq_compatible(value: &JsonValue, path: &str) -> Result<(), TransformError> {
    if value.is_null() {
        return Ok(());
    }
    if value_to_string_optional(value).is_some() {
        return Ok(());
    }
    Err(expr_type_error(
        "value must be string/number/bool or null",
        path,
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SortKeyKind {
    Number,
    String,
    Bool,
}

#[derive(Clone)]
pub(super) enum SortKey {
    Number(f64),
    String(String),
    Bool(bool),
}

impl SortKey {
    pub(super) fn kind(&self) -> SortKeyKind {
        match self {
            SortKey::Number(_) => SortKeyKind::Number,
            SortKey::String(_) => SortKeyKind::String,
            SortKey::Bool(_) => SortKeyKind::Bool,
        }
    }
}

pub(super) fn compare_sort_keys(left: &SortKey, right: &SortKey) -> Ordering {
    match (left, right) {
        (SortKey::Number(l), SortKey::Number(r)) => l.partial_cmp(r).unwrap_or(Ordering::Equal),
        (SortKey::String(l), SortKey::String(r)) => l.cmp(r),
        (SortKey::Bool(l), SortKey::Bool(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

fn eval_sort_key(
    expr: &Expr,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<SortKey, TransformError> {
    let value = match eval_expr(expr, record, context, out, base_path, locals)? {
        EvalValue::Missing => {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be missing",
            )
            .with_path(base_path));
        }
        EvalValue::Value(value) => value,
    };
    if value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(base_path));
    }

    match value {
        JsonValue::Number(number) => {
            let value = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| expr_type_error("sort_by key must be a finite number", base_path))?;
            Ok(SortKey::Number(value))
        }
        JsonValue::String(value) => Ok(SortKey::String(value)),
        JsonValue::Bool(value) => Ok(SortKey::Bool(value)),
        _ => Err(expr_type_error(
            "sort_by key must be string/number/bool",
            base_path,
        )),
    }
}

fn eval_array_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::with_capacity(array.len());
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_filter(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_flat_map(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        match value {
            JsonValue::Array(items) => results.extend(items),
            value => results.push(value),
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn flatten_value(value: &JsonValue, depth: usize, out: &mut Vec<JsonValue>) {
    if depth == 0 {
        out.push(value.clone());
        return;
    }

    if let JsonValue::Array(items) = value {
        for item in items {
            flatten_value(item, depth - 1, out);
        }
    } else {
        out.push(value.clone());
    }
}

fn eval_array_flatten(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(1..=2).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain one or two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let depth = if total_len == 2 {
        let depth_path = format!("{}.args[1]", base_path);
        let depth_value =
            match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if depth_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(depth_path));
        }
        let depth = value_to_i64(
            &depth_value,
            &depth_path,
            "depth must be a non-negative integer",
        )?;
        if depth < 0 {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "depth must be a non-negative integer",
            )
            .with_path(depth_path));
        }
        usize::try_from(depth).map_err(|_| {
            TransformError::new(TransformErrorKind::ExprError, "depth is too large")
                .with_path(depth_path)
        })?
    } else {
        1
    };

    let mut results = Vec::new();
    for item in &array {
        flatten_value(item, depth, &mut results);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_take(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let take_count = count.min(len).max(0) as usize;
        array[..take_count].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let take_count = abs_count.min(array.len() as u64) as usize;
        let start = array.len().saturating_sub(take_count);
        array[start..].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_drop(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let count_path = format!("{}.args[1]", base_path);
    let count_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if count_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(count_path));
    }
    let count = value_to_i64(&count_value, &count_path, "count must be an integer")?;

    let len = array.len() as i64;
    let results = if count >= 0 {
        let drop_count = count.min(len).max(0) as usize;
        array[drop_count..].to_vec()
    } else {
        let abs_count = if count == i64::MIN {
            (i64::MAX as u64) + 1
        } else {
            (-count) as u64
        };
        let drop_count = abs_count.min(array.len() as u64) as usize;
        let end = array.len().saturating_sub(drop_count);
        array[..end].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_slice(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let len = array.len() as i64;

    let start_path = format!("{}.args[1]", base_path);
    let start_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if start_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(start_path));
    }
    let start = value_to_i64(&start_value, &start_path, "start must be an integer")?;

    let end = if total_len == 3 {
        let end_path = format!("{}.args[2]", base_path);
        let end_value =
            match eval_arg_value_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if end_value.is_null() {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "expr arg must not be null",
            )
            .with_path(end_path));
        }
        value_to_i64(&end_value, &end_path, "end must be an integer")?
    } else {
        len
    };

    let mut start_index = if start < 0 { len + start } else { start };
    let mut end_index = if end < 0 { len + end } else { end };
    start_index = start_index.clamp(0, len);
    end_index = end_index.clamp(0, len);

    let results = if end_index <= start_index {
        Vec::new()
    } else {
        array[start_index as usize..end_index as usize].to_vec()
    };

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_chunk(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let size_path = format!("{}.args[1]", base_path);
    let size_value =
        match eval_arg_value_at(1, args, injected, record, context, out, base_path, locals)? {
            None => return Ok(EvalValue::Missing),
            Some(value) => value,
        };
    if size_value.is_null() {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr arg must not be null",
        )
        .with_path(size_path));
    }
    let size = value_to_i64(&size_value, &size_path, "size must be a positive integer")?;
    if size <= 0 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "size must be a positive integer",
        )
        .with_path(size_path));
    }
    let size = usize::try_from(size).map_err(|_| {
        TransformError::new(TransformErrorKind::ExprError, "size is too large").with_path(size_path)
    })?;

    let mut chunks = Vec::new();
    let mut index = 0;
    while index < array.len() {
        let end = (index + size).min(array.len());
        chunks.push(JsonValue::Array(array[index..end].to_vec()));
        index = end;
    }

    Ok(EvalValue::Value(JsonValue::Array(chunks)))
}

fn eval_array_zip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let mut arrays = Vec::new();
    for index in 0..total_len {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        results.push(JsonValue::Array(row));
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_zip_with(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len < 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain at least three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let expr_index = total_len - 1;
    let expr = arg_expr_at(expr_index, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[{}]", base_path, expr_index))
    })?;
    let expr_arg_index = if injected.is_some() {
        expr_index - 1
    } else {
        expr_index
    };
    let expr_path = format!("{}.args[{}]", base_path, expr_arg_index);

    let mut arrays = Vec::new();
    for index in 0..expr_index {
        arrays.push(eval_array_arg(
            index, args, injected, record, context, out, base_path, locals,
        )?);
    }

    let min_len = arrays.iter().map(|items| items.len()).min().unwrap_or(0);
    let mut results = Vec::with_capacity(min_len);
    for idx in 0..min_len {
        let mut row = Vec::with_capacity(arrays.len());
        for array in &arrays {
            row.push(array[idx].clone());
        }
        let row_value = JsonValue::Array(row);
        let item_locals = locals_with_item(
            locals,
            EvalItem {
                value: &row_value,
                index: idx,
            },
        );
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.push(value);
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_unzip(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let mut columns: Vec<Vec<JsonValue>> = Vec::new();
    let mut expected_len: Option<usize> = None;
    for item in &array {
        let items = match item {
            JsonValue::Array(items) => items,
            _ => {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must be arrays",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        };
        if let Some(expected) = expected_len {
            if items.len() != expected {
                return Err(TransformError::new(
                    TransformErrorKind::ExprError,
                    "unzip items must have the same length",
                )
                .with_path(format!("{}.args[0]", base_path)));
            }
        } else {
            expected_len = Some(items.len());
            columns = vec![Vec::with_capacity(array.len()); items.len()];
        }
        for (index, value) in items.iter().enumerate() {
            if let Some(column) = columns.get_mut(index) {
                column.push(value.clone());
            }
        }
    }

    let output = columns
        .into_iter()
        .map(JsonValue::Array)
        .collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(output)))
}

fn eval_array_group_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let entry = results
            .entry(key)
            .or_insert_with(|| JsonValue::Array(Vec::new()));
        if let JsonValue::Array(items) = entry {
            items.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_key_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Map::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        results.insert(key, item.clone());
    }

    Ok(EvalValue::Value(JsonValue::Object(results)))
}

fn eval_array_partition(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut matched = Vec::new();
    let mut unmatched = Vec::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            matched.push(item.clone());
        } else {
            unmatched.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(vec![
        JsonValue::Array(matched),
        JsonValue::Array(unmatched),
    ])))
}

fn eval_array_unique(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let item_path = format!("{}.args[0]", base_path);

    let mut results: Vec<JsonValue> = Vec::new();
    for item in array {
        ensure_eq_compatible(&item, &item_path)?;
        let mut exists = false;
        for existing in &results {
            if compare_eq(&item, existing, &item_path, &item_path)? {
                exists = true;
                break;
            }
        }
        if !exists {
            results.push(item);
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_distinct_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut results = Vec::new();
    let mut seen = HashSet::new();
    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_key_expr_string(expr, record, context, out, &expr_path, Some(&item_locals))?;
        if seen.insert(key) {
            results.push(item.clone());
        }
    }

    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_sort_by(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if !(2..=3).contains(&total_len) {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain two or three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Array(Vec::new())));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let order = if total_len == 3 {
        let order_path = format!("{}.args[2]", base_path);
        let value =
            match eval_arg_string_at(2, args, injected, record, context, out, base_path, locals)? {
                None => return Ok(EvalValue::Missing),
                Some(value) => value,
            };
        if value != "asc" && value != "desc" {
            return Err(TransformError::new(
                TransformErrorKind::ExprError,
                "order must be asc or desc",
            )
            .with_path(order_path));
        }
        value
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
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        let key = eval_sort_key(expr, record, context, out, &expr_path, Some(&item_locals))?;
        let kind = key.kind();
        if let Some(existing) = key_kind {
            if existing != kind {
                return Err(expr_type_error(
                    "sort_by keys must be all the same type",
                    &expr_path,
                ));
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
        if ordering == Ordering::Equal {
            left.index.cmp(&right.index)
        } else {
            ordering
        }
    });

    let results = items.into_iter().map(|item| item.value).collect::<Vec<_>>();
    Ok(EvalValue::Value(JsonValue::Array(results)))
}

fn eval_array_find(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(item.clone()));
        }
    }

    Ok(EvalValue::Value(JsonValue::Null))
}

fn eval_array_find_index(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    for (index, item) in array.iter().enumerate() {
        let item_locals = locals_with_item(locals, EvalItem { value: item, index });
        if eval_predicate_expr(expr, record, context, out, &expr_path, Some(&item_locals))? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_index_of(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for (index, item) in array.iter().enumerate() {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Number((index as i64).into())));
        }
    }

    Ok(EvalValue::Value(JsonValue::Number((-1).into())))
}

fn eval_array_contains(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let value_path = format!("{}.args[1]", base_path);
    let value =
        eval_expr_value_or_null_at(1, args, injected, record, context, out, base_path, locals)?;

    ensure_eq_compatible(&value, &value_path)?;
    let item_path = format!("{}.args[0]", base_path);
    for item in &array {
        ensure_eq_compatible(item, &item_path)?;
        if compare_eq(item, &value, &item_path, &value_path)? {
            return Ok(EvalValue::Value(JsonValue::Bool(true)));
        }
    }

    Ok(EvalValue::Value(JsonValue::Bool(false)))
}

fn eval_array_sum(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }

    Ok(EvalValue::Value(json_number_from_f64(sum, base_path)?))
}

fn eval_array_avg(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut sum = 0.0;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        sum += value;
    }
    let avg = sum / array.len() as f64;

    Ok(EvalValue::Value(json_number_from_f64(avg, base_path)?))
}

fn eval_array_min(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut min_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        min_value = Some(match min_value {
            Some(current) => current.min(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        min_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_max(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 1 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly one item",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let item_path = format!("{}.args[0]", base_path);
    let mut max_value: Option<f64> = None;
    for item in &array {
        let value = value_to_number(item, &item_path, "array item must be a number")?;
        max_value = Some(match max_value {
            Some(current) => current.max(value),
            None => value,
        });
    }

    Ok(EvalValue::Value(json_number_from_f64(
        max_value.unwrap_or(0.0),
        base_path,
    )?))
}

fn eval_array_reduce(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 2 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly two items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    if array.is_empty() {
        return Ok(EvalValue::Value(JsonValue::Null));
    }

    let expr = arg_expr_at(1, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[1]", base_path))
    })?;
    let expr_index = if injected.is_some() { 0 } else { 1 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = array[0].clone();
    for (index, item) in array.iter().enumerate().skip(1) {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}

fn eval_array_fold(
    args: &[Expr],
    injected: Option<&EvalValue>,
    record: &JsonValue,
    context: Option<&JsonValue>,
    out: &JsonValue,
    base_path: &str,
    locals: Option<&EvalLocals<'_>>,
) -> Result<EvalValue, TransformError> {
    let total_len = args_len(args, injected);
    if total_len != 3 {
        return Err(TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args must contain exactly three items",
        )
        .with_path(format!("{}.args", base_path)));
    }

    let array = eval_array_arg(0, args, injected, record, context, out, base_path, locals)?;
    let initial =
        match eval_expr_at_index(1, args, injected, record, context, out, base_path, locals)? {
            EvalValue::Missing => return Ok(EvalValue::Missing),
            EvalValue::Value(value) => value,
        };

    let expr = arg_expr_at(2, args, injected).ok_or_else(|| {
        TransformError::new(
            TransformErrorKind::ExprError,
            "expr.args index is out of bounds",
        )
        .with_path(format!("{}.args[2]", base_path))
    })?;
    let expr_index = if injected.is_some() { 1 } else { 2 };
    let expr_path = format!("{}.args[{}]", base_path, expr_index);

    let mut acc = initial;
    for (index, item) in array.iter().enumerate() {
        let item_locals = EvalLocals {
            item: Some(EvalItem { value: item, index }),
            acc: Some(&acc),
            pipe: locals.and_then(|locals| locals.pipe),
            locals: locals.and_then(|locals| locals.locals),
            precomputed_op_args: locals.and_then(|locals| locals.precomputed_op_args),
        };
        let value = eval_expr_or_null(expr, record, context, out, &expr_path, Some(&item_locals))?;
        acc = value;
    }

    Ok(EvalValue::Value(acc))
}
