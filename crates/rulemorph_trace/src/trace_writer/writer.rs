use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value as JsonValue;

use super::enqueue::enqueue_trace;
use super::options::{TraceWriteOptions, TraceWriterConfig};
use super::queue::{TraceQueue, trace_writer_loop};
use super::write_trace_bundle_sync;
use crate::trace_backend::TraceWriteBackend;

#[derive(Clone)]
struct FileTraceWriteBackend {
    data_dir: PathBuf,
}

impl FileTraceWriteBackend {
    fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }
}

impl TraceWriteBackend for FileTraceWriteBackend {
    fn write_trace_bundle(&self, trace: &JsonValue, options: &TraceWriteOptions) -> Result<()> {
        write_trace_bundle_sync(&self.data_dir, trace, options).map(|_| ())
    }
}

#[derive(Clone)]
pub struct TraceWriter {
    pub(super) queue: Arc<TraceQueue>,
    default_options: TraceWriteOptions,
}

impl TraceWriter {
    pub fn new(data_dir: PathBuf) -> Self {
        Self::with_config(data_dir, TraceWriterConfig::default())
    }

    pub fn with_config(data_dir: PathBuf, config: TraceWriterConfig) -> Self {
        let backend = config
            .write_backend
            .unwrap_or_else(|| Arc::new(FileTraceWriteBackend::new(data_dir)));
        let queue = Arc::new(TraceQueue::new(
            backend,
            config.queue_capacity,
            config.queue_max_bytes,
        ));
        if config.spawn_worker {
            let worker_queue = queue.clone();
            std::thread::spawn(move || trace_writer_loop(worker_queue));
        }
        Self {
            queue,
            default_options: config.write_options,
        }
    }

    pub fn enqueue(&self, trace: JsonValue) -> bool {
        self.enqueue_with_options(trace, None)
    }

    pub fn enqueue_with_options(
        &self,
        trace: JsonValue,
        options: Option<TraceWriteOptions>,
    ) -> bool {
        let options = options.unwrap_or_else(|| self.default_options.clone());
        enqueue_trace(&self.queue, trace, options)
    }
}
