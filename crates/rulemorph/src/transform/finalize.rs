use super::*;

mod pagination;
mod sort;
mod wrap;

use pagination::{apply_limit, apply_limit_traced, apply_offset, apply_offset_traced};
pub(super) use sort::sort_key_from_value;
use wrap::eval_wrap_value;

pub(super) fn apply_finalize(
    finalize: &FinalizeSpec,
    output: JsonValue,
    context: Option<&JsonValue>,
) -> Result<JsonValue, TransformError> {
    let mut records = match output {
        JsonValue::Array(records) => records,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "finalize expects array output",
            )
            .with_path("finalize"));
        }
    };

    if let Some(filter) = &finalize.filter {
        let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "finalize.filter must be a v2 condition",
            )
            .with_path("finalize.filter")
        })?;
        let cond = parse_v2_condition(&raw).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path("finalize.filter")
        })?;
        let base_out = JsonValue::Array(records.clone());
        let mut filtered = Vec::new();
        for (index, item) in records.iter().enumerate() {
            let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
            let keep = eval_v2_condition(&cond, item, context, &base_out, "finalize.filter", &ctx)?;
            if keep {
                filtered.push(item.clone());
            }
        }
        records = filtered;
    }

    if let Some(sort) = &finalize.sort {
        let tokens = parse_path(&sort.by).map_err(|_| {
            TransformError::new(
                TransformErrorKind::InvalidRecordsPath,
                "finalize.sort.by is invalid",
            )
            .with_path("finalize.sort.by")
        })?;

        struct SortItem {
            key: SortKey,
            index: usize,
            value: JsonValue,
        }

        let mut items = Vec::with_capacity(records.len());
        for (index, item) in records.iter().enumerate() {
            let key_value = get_path(item, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "finalize.sort.by path not found",
                )
                .with_path("finalize.sort.by")
            })?;
            let key = sort_key_from_value(key_value, "finalize.sort.by")?;
            items.push(SortItem {
                key,
                index,
                value: item.clone(),
            });
        }

        items.sort_by(|left, right| {
            let mut ordering = compare_sort_keys(&left.key, &right.key);
            if sort.order == "desc" {
                ordering = ordering.reverse();
            }
            if ordering == Ordering::Equal {
                left.index.cmp(&right.index)
            } else {
                ordering
            }
        });

        records = items.into_iter().map(|item| item.value).collect();
    }

    if let Some(offset) = finalize.offset {
        apply_offset(&mut records, offset);
    }

    if let Some(limit) = finalize.limit {
        apply_limit(&mut records, limit);
    }

    let output = JsonValue::Array(records);
    if let Some(wrap) = &finalize.wrap {
        let wrapped = eval_wrap_value(wrap, &output, context, "finalize.wrap")?;
        return Ok(wrapped);
    }

    Ok(output)
}

pub(super) fn apply_finalize_traced(
    finalize: &FinalizeSpec,
    output: JsonValue,
    context: Option<&JsonValue>,
    collector: &mut TraceCollector,
) -> Result<JsonValue, TransformError> {
    let mut records = match output {
        JsonValue::Array(records) => records,
        _ => {
            return Err(TransformError::new(
                TransformErrorKind::InvalidInput,
                "finalize expects array output",
            )
            .with_path("finalize"));
        }
    };

    if let Some(filter) = &finalize.filter {
        let raw = expr_to_json_for_v2_condition(filter).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "finalize.filter must be a v2 condition",
            )
            .with_path("finalize.filter")
        })?;
        let cond = parse_v2_condition(&raw).map_err(|err| {
            TransformError::new(
                TransformErrorKind::ExprError,
                format!("invalid v2 condition: {}", err),
            )
            .with_path("finalize.filter")
        })?;
        let base_out = JsonValue::Array(records.clone());
        let before_count = records.len();
        let mut filtered = Vec::new();
        for (index, item) in records.iter().enumerate() {
            let ctx = V2EvalContext::new().with_item(V2EvalItem { value: item, index });
            let item_path = format!("finalize.filter[{}]", index);
            let keep = eval_v2_condition_traced(
                &cond, item, context, &base_out, &item_path, &ctx, collector,
            )?;
            collector
                .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
                .rule_path(&item_path)
                .input_path(canonical_item_path(""))
                .attr_index("item_index", index)
                .attr_bool("kept", keep)
                .input_value(item, collector.options(), Some("@item"))
                .finish_with_output(collector, &JsonValue::Bool(keep), None);
            if keep {
                filtered.push(item.clone());
            }
        }
        records = filtered;
        collector
            .emit(TraceEventKind::FinalizeFilter, TracePhase::Instant)
            .rule_path("finalize.filter")
            .attr_count("input_count", before_count)
            .attr_count("output_count", records.len())
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    if let Some(sort) = &finalize.sort {
        let tokens = parse_path(&sort.by).map_err(|_| {
            TransformError::new(
                TransformErrorKind::InvalidRecordsPath,
                "finalize.sort.by is invalid",
            )
            .with_path("finalize.sort.by")
        })?;

        struct SortItem {
            key: SortKey,
            index: usize,
            value: JsonValue,
        }

        let mut items = Vec::with_capacity(records.len());
        for (index, item) in records.iter().enumerate() {
            let key_value = get_path(item, &tokens).ok_or_else(|| {
                TransformError::new(
                    TransformErrorKind::InvalidRef,
                    "finalize.sort.by path not found",
                )
                .with_path("finalize.sort.by")
            })?;
            let key = sort_key_from_value(key_value, "finalize.sort.by")?;
            items.push(SortItem {
                key,
                index,
                value: item.clone(),
            });
        }

        items.sort_by(|left, right| {
            let mut ordering = compare_sort_keys(&left.key, &right.key);
            if sort.order == "desc" {
                ordering = ordering.reverse();
            }
            if ordering == Ordering::Equal {
                left.index.cmp(&right.index)
            } else {
                ordering
            }
        });

        for (to_index, item) in items.iter().enumerate() {
            collector
                .emit(TraceEventKind::FinalizeSort, TracePhase::Instant)
                .rule_path(format!("finalize.sort[{}]", item.index))
                .attr_index("from_index", item.index)
                .attr_index("to_index", to_index)
                .attr_enum("order", if sort.order == "desc" { "desc" } else { "asc" })
                .input_value(&item.value, collector.options(), Some("@item"))
                .finish_with_output(collector, &sort_key_to_json(&item.key), None);
        }

        records = items.into_iter().map(|item| item.value).collect();
        collector
            .emit(TraceEventKind::FinalizeSort, TracePhase::Instant)
            .rule_path("finalize.sort")
            .attr_enum("order", if sort.order == "desc" { "desc" } else { "asc" })
            .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    }

    if let Some(offset) = finalize.offset {
        apply_offset_traced(&mut records, offset, collector);
    }

    if let Some(limit) = finalize.limit {
        apply_limit_traced(&mut records, limit, collector);
    }

    let output = JsonValue::Array(records);
    if let Some(wrap) = &finalize.wrap {
        let wrapped = eval_wrap_value(wrap, &output, context, "finalize.wrap")?;
        collector
            .emit(TraceEventKind::FinalizeWrap, TracePhase::Instant)
            .rule_path("finalize.wrap")
            .finish_with_output(collector, &wrapped, None);
        return Ok(wrapped);
    }

    Ok(output)
}
