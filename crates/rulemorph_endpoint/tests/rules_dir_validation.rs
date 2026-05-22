use std::fs;
use std::path::{Path, PathBuf};

use rulemorph_endpoint::validate_rules_dir;

fn write_file(root: &Path, rel: &str, content: &str) -> PathBuf {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent dir");
    }
    fs::write(&path, content).expect("write file");
    path
}

fn basic_rule() -> &'static str {
    r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: "output.ok"
    value: true
"#
}

include!("rules_dir_validation/endpoint_refs.rs");
include!("rules_dir_validation/network_refs.rs");
include!("rules_dir_validation/branch_refs.rs");
