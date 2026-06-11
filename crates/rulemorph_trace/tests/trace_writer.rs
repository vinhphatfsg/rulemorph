mod common;

use std::collections::HashMap;
use std::fs;

use common::io::{read_ndjson_lines, read_ndjson_values, write_ndjson_lines};
use common::sampling::sampling_bucket;
use common::trace_writer::{
    array_field, array_member, assert_no_detail_artifacts, create_temp_dir, first_record_object,
    load_trace, object_field, object_member, read_manifest, read_manifest_payload,
    read_manifest_value, trace_dir, unique_temp_dir,
};
use rulemorph_trace::{
    TRACE_CHUNK_COUNT_HARD_MAX, TRACE_JSON_MAX_BYTES, TraceCompression, TraceDetailLevel,
    TraceManifest, TraceStore, TraceWriteOptions, write_trace_bundle,
};
use serde_json::json;

include!("trace_writer/budget_sampling.rs");

include!("trace_writer/chunk_errors.rs");

include!("trace_writer/finalize.rs");

include!("trace_writer/nodes.rs");

include!("trace_writer/node_record_indexes.rs");

#[tokio::test]
async fn write_trace_bundle_default_compression_loads() -> anyhow::Result<()> {
    let temp_dir = create_temp_dir()?;

    let trace = json!({
        "trace_id": "trace-default-compression",
        "records": [
            {
                "status": "ok",
                "nodes": [{ "id": "n1", "kind": "mappings", "status": "ok" }]
            }
        ]
    });

    let _manifest_path = write_trace_bundle(&temp_dir, &trace, None).await?;
    let store = TraceStore::new(temp_dir.clone()).await?;
    let loaded = store
        .get("trace-default-compression")
        .await?
        .expect("trace should load");
    let records = loaded
        .get("records")
        .and_then(|value| value.as_array())
        .expect("records should exist");
    let nodes = records[0]
        .get("nodes")
        .and_then(|value| value.as_array())
        .expect("nodes should be array");
    assert_eq!(
        nodes[0].get("id").and_then(|value| value.as_str()),
        Some("n1")
    );

    Ok(())
}

include!("trace_writer/trace_ids.rs");

include!("trace_writer/masking_payloads.rs");
