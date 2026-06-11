mod common;

use anyhow::Result;
use common::trace_store::{
    assert_detail_array_empty, assert_detail_reason, assert_detail_status, assert_finalize_absent,
    assert_top_level_array_empty, create_trace_dir, write_records_inline_trace_json,
    write_trace_json,
};
use rulemorph_trace::{
    TRACE_CHUNK_BYTES_COMPRESSED_OVERHEAD_MAX, TRACE_JSON_MAX_BYTES, TRACE_NODE_COUNT_HARD_MAX,
    TRACE_RECORD_COUNT_HARD_MAX, TraceStore,
};
use serde_json::json;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use tempfile::tempdir;

include!("trace_store/import_bundle.rs");

include!("trace_store/index.rs");

include!("trace_store/count_limits.rs");

include!("trace_store/chunk_downgrade.rs");
