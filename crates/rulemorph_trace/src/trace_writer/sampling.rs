use serde_json::Value as JsonValue;

use super::{DEFAULT_SAMPLING_RATE, TraceWriteOptions};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(super) enum TracePriority {
    High,
    Normal,
}

pub(super) fn should_keep_full_detail(
    trace: &JsonValue,
    records: &[JsonValue],
    options: &TraceWriteOptions,
) -> bool {
    let rate = normalize_sampling_rate(options.sampling_rate);
    if rate >= 1.0 {
        return true;
    }
    if trace_is_error(trace, records) || trace_is_slow(trace, records, options) {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    let key = trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .or_else(|| trace.get("timestamp").and_then(|value| value.as_str()))
        .unwrap_or("trace");
    let bucket = sampling_bucket(key);
    bucket < rate
}

pub(super) fn trace_priority(trace: &JsonValue, options: &TraceWriteOptions) -> TracePriority {
    let records = trace
        .get("records")
        .and_then(|value| value.as_array())
        .map(|value| value.as_slice())
        .unwrap_or(&[]);
    if trace_is_error(trace, records) || trace_is_slow(trace, records, options) {
        TracePriority::High
    } else {
        TracePriority::Normal
    }
}

fn normalize_sampling_rate(rate: f64) -> f64 {
    if rate.is_nan() {
        return DEFAULT_SAMPLING_RATE;
    }
    rate.clamp(0.0, 1.0)
}

fn sampling_bucket(value: &str) -> f64 {
    let hash = fnv1a64(value.as_bytes());
    (hash as f64) / (u64::MAX as f64)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn trace_is_error(trace: &JsonValue, records: &[JsonValue]) -> bool {
    if let Some(status) = trace.get("status").and_then(|value| value.as_str()) {
        let status = status.to_ascii_lowercase();
        if status != "ok" && status != "success" {
            return true;
        }
    }
    if let Some(failed) = trace
        .get("summary")
        .and_then(|summary| summary.get("record_failed"))
        .and_then(|value| value.as_u64())
    {
        if failed > 0 {
            return true;
        }
    }
    records.iter().any(|record| {
        record
            .get("status")
            .and_then(|value| value.as_str())
            .map(|value| value.eq_ignore_ascii_case("error"))
            .unwrap_or(false)
    })
}

fn trace_is_slow(trace: &JsonValue, records: &[JsonValue], options: &TraceWriteOptions) -> bool {
    let Some(threshold) = options.sampling_slow_threshold_us else {
        return false;
    };
    trace_duration_us(trace, records)
        .map(|duration| duration >= threshold)
        .unwrap_or(false)
}

fn trace_duration_us(trace: &JsonValue, records: &[JsonValue]) -> Option<u64> {
    if let Some(duration) = trace
        .get("summary")
        .and_then(|summary| summary.get("duration_us"))
        .and_then(|value| value.as_u64())
    {
        return Some(duration);
    }
    if let Some(duration) = trace
        .get("summary")
        .and_then(|summary| summary.get("duration_ms"))
        .and_then(|value| value.as_u64())
    {
        return Some(duration.saturating_mul(1000));
    }
    if let Some(duration) = trace.get("duration_us").and_then(|value| value.as_u64()) {
        return Some(duration);
    }
    if let Some(duration) = trace.get("duration_ms").and_then(|value| value.as_u64()) {
        return Some(duration.saturating_mul(1000));
    }

    let mut total = 0u64;
    let mut found = false;
    for record in records {
        if let Some(duration) = record.get("duration_us").and_then(|value| value.as_u64()) {
            total = total.saturating_add(duration);
            found = true;
        } else if let Some(duration) = record.get("duration_ms").and_then(|value| value.as_u64()) {
            total = total.saturating_add(duration.saturating_mul(1000));
            found = true;
        }
    }

    if found { Some(total) } else { None }
}
