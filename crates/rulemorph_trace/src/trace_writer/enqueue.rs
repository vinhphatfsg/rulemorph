use serde_json::Value as JsonValue;
use tracing::warn;

use super::detail::{append_detail_reason, strip_trace_detail};
use super::masking::{apply_masking, normalize_masking_rules};
use super::options::{TraceDetailLevel, TraceWriteOptions};
use super::queue::{
    TraceQueue, TraceWriteRequest, estimate_trace_bytes, evict_normal, push_request, queue_bytes,
    trace_id_for_log,
};
use super::sampling::{TracePriority, trace_priority};

pub(super) fn enqueue_trace(
    queue: &TraceQueue,
    trace: JsonValue,
    options: TraceWriteOptions,
) -> bool {
    let priority = trace_priority(&trace, &options);
    let mut request = TraceWriteRequest {
        trace,
        options,
        priority,
        downgraded: false,
        approx_bytes: 0,
    };
    if request.options.detail_level != TraceDetailLevel::Full {
        strip_trace_detail(&mut request.trace);
    }
    if request.options.masking_enabled {
        let masking_rules = normalize_masking_rules(&request.options.masking_rules);
        apply_masking(&mut request.trace, &masking_rules);
    }
    request.approx_bytes = estimate_trace_bytes(&request.trace);
    let mut guard = queue.items.lock().expect("trace queue lock");
    let mut current_bytes = queue_bytes(&guard);
    let mut queue_full = guard.len() >= queue.capacity
        || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes;

    if queue_full && priority == TracePriority::High {
        while guard.len() >= queue.capacity
            || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes
        {
            if !evict_normal(&mut guard) {
                break;
            }
            current_bytes = queue_bytes(&guard);
        }
        queue_full = guard.len() >= queue.capacity
            || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes;
    }

    if queue_full && request.options.detail_level == TraceDetailLevel::Full {
        request.options.detail_level = TraceDetailLevel::Basic;
        request.options.detail_reason =
            append_detail_reason(request.options.detail_reason.take(), "queue_full");
        request.downgraded = true;
        strip_trace_detail(&mut request.trace);
        request.approx_bytes = estimate_trace_bytes(&request.trace);
        queue_full = guard.len() >= queue.capacity
            || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes;

        if queue_full && priority == TracePriority::High {
            while guard.len() >= queue.capacity
                || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes
            {
                if !evict_normal(&mut guard) {
                    break;
                }
                current_bytes = queue_bytes(&guard);
            }
            queue_full = guard.len() >= queue.capacity
                || current_bytes.saturating_add(request.approx_bytes) > queue.max_bytes;
        }
    }

    if queue_full {
        let can_enqueue = match priority {
            TracePriority::High => {
                guard.len() < queue.capacity
                    && current_bytes.saturating_add(request.approx_bytes) <= queue.max_bytes
            }
            TracePriority::Normal => false,
        };
        if !can_enqueue {
            warn!(
                "trace queue full; dropping trace {}",
                trace_id_for_log(&request.trace)
            );
            return false;
        }
    }
    push_request(&mut guard, request);
    queue.cvar.notify_one();
    true
}
