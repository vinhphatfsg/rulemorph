use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Condvar, Mutex};

use serde_json::Value as JsonValue;
use tracing::warn;

use crate::trace_backend::TraceWriteBackend;

use super::TraceWriteOptions;
use super::sampling::TracePriority;

pub(super) struct TraceWriteRequest {
    pub(super) trace: JsonValue,
    pub(super) options: TraceWriteOptions,
    pub(super) priority: TracePriority,
    pub(super) downgraded: bool,
    pub(super) approx_bytes: usize,
}

pub(super) struct TraceQueue {
    pub(super) backend: Arc<dyn TraceWriteBackend>,
    pub(super) capacity: usize,
    pub(super) max_bytes: usize,
    pub(super) items: Mutex<VecDeque<TraceWriteRequest>>,
    pub(super) cvar: Condvar,
}

impl TraceQueue {
    pub(super) fn new(
        backend: Arc<dyn TraceWriteBackend>,
        capacity: usize,
        max_bytes: usize,
    ) -> Self {
        Self {
            backend,
            capacity: capacity.max(1),
            max_bytes: max_bytes.max(1),
            items: Mutex::new(VecDeque::new()),
            cvar: Condvar::new(),
        }
    }
}

pub(super) fn trace_writer_loop(queue: Arc<TraceQueue>) {
    loop {
        let request = {
            let mut guard = queue.items.lock().expect("trace queue lock");
            while guard.is_empty() {
                guard = queue.cvar.wait(guard).expect("trace queue wait");
            }
            guard.pop_front()
        };
        let Some(request) = request else {
            continue;
        };
        if request.downgraded {
            warn!(
                "trace queue full; downgraded trace {} to basic",
                trace_id_for_log(&request.trace)
            );
        }
        if let Err(err) = queue
            .backend
            .write_trace_bundle(&request.trace, &request.options)
        {
            warn!("failed to write trace bundle: {}", err);
        }
    }
}

pub(super) fn push_request(queue: &mut VecDeque<TraceWriteRequest>, request: TraceWriteRequest) {
    match request.priority {
        TracePriority::High => queue.push_front(request),
        TracePriority::Normal => queue.push_back(request),
    }
}

pub(super) fn evict_normal(queue: &mut VecDeque<TraceWriteRequest>) -> bool {
    let position = queue
        .iter()
        .position(|item| item.priority == TracePriority::Normal);
    if let Some(index) = position {
        queue.remove(index);
        return true;
    }
    false
}

pub(super) fn trace_id_for_log(trace: &JsonValue) -> String {
    trace
        .get("trace_id")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string()
}

pub(super) fn estimate_trace_bytes(trace: &JsonValue) -> usize {
    struct CountingWriter {
        size: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.size = self.size.saturating_add(buf.len());
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut writer = CountingWriter { size: 0 };
    if serde_json::to_writer(&mut writer, trace).is_ok() {
        writer.size
    } else {
        0
    }
}

pub(super) fn queue_bytes(queue: &VecDeque<TraceWriteRequest>) -> usize {
    queue
        .iter()
        .map(|request| request.approx_bytes)
        .sum::<usize>()
}
