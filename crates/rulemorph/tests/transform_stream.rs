use rulemorph::{
    InputData, NormalizationOptions, TransformErrorKind, parse_rule_file, transform_stream,
    transform_stream_input_with_options, transform_stream_with_base_dir, transform_with_warnings,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-stream-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

include!("transform_stream/core.rs");
include!("transform_stream/options.rs");
include!("transform_stream/branch.rs");
