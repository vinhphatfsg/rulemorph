use super::*;
use crate::model::FinalizeSort;

struct SortItem {
    key: SortKey,
    index: usize,
    value: JsonValue,
}

pub(super) fn apply_sort(
    records: &mut Vec<JsonValue>,
    sort: &FinalizeSort,
) -> Result<(), TransformError> {
    let items = sorted_items(records, sort)?;
    *records = items.into_iter().map(|item| item.value).collect();
    Ok(())
}

pub(super) fn apply_sort_traced(
    records: &mut Vec<JsonValue>,
    sort: &FinalizeSort,
    collector: &mut TraceCollector,
) -> Result<(), TransformError> {
    let items = sorted_items(records, sort)?;

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

    *records = items.into_iter().map(|item| item.value).collect();
    collector
        .emit(TraceEventKind::FinalizeSort, TracePhase::Instant)
        .rule_path("finalize.sort")
        .attr_enum("order", if sort.order == "desc" { "desc" } else { "asc" })
        .finish_with_output(collector, &JsonValue::Array(records.clone()), None);
    Ok(())
}

fn sorted_items(
    records: &[JsonValue],
    sort: &FinalizeSort,
) -> Result<Vec<SortItem>, TransformError> {
    let tokens = parse_path(&sort.by).map_err(|_| {
        TransformError::new(
            TransformErrorKind::InvalidRecordsPath,
            "finalize.sort.by is invalid",
        )
        .with_path("finalize.sort.by")
    })?;

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

    Ok(items)
}

pub(in crate::transform) fn sort_key_from_value(
    value: &JsonValue,
    path: &str,
) -> Result<SortKey, TransformError> {
    match value {
        JsonValue::Number(number) => number.as_f64().map(SortKey::Number).ok_or_else(|| {
            TransformError::new(
                TransformErrorKind::ExprError,
                "sort key must be a finite number",
            )
            .with_path(path)
        }),
        JsonValue::String(value) => Ok(SortKey::String(value.clone())),
        JsonValue::Bool(value) => Ok(SortKey::Bool(*value)),
        _ => Err(TransformError::new(
            TransformErrorKind::ExprError,
            "sort key must be string/number/bool",
        )
        .with_path(path)),
    }
}
