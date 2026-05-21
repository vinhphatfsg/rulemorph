use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use rulemorph::{TransformErrorKind, parse_rule_file, transform_with_base_dir};

fn unique_temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rulemorph-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

include!("branch_security/rejection.rs");
include!("branch_security/path_safety.rs");
include!("branch_security/rule_formats.rs");
