use super::*;

mod filter;
mod pagination;
mod sort;
mod wrap;

use filter::{apply_filter, apply_filter_traced};
use pagination::{apply_limit, apply_limit_traced, apply_offset, apply_offset_traced};
pub(super) use sort::sort_key_from_value;
use sort::{apply_sort, apply_sort_traced};
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
        apply_filter(&mut records, filter, context)?;
    }

    if let Some(sort) = &finalize.sort {
        apply_sort(&mut records, sort)?;
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
        apply_filter_traced(&mut records, filter, context, collector)?;
    }

    if let Some(sort) = &finalize.sort {
        apply_sort_traced(&mut records, sort, collector)?;
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
