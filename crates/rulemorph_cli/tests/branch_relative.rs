use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::cargo::cargo_bin_cmd;

fn write_file(root: &Path, rel: &str, content: &str) -> PathBuf {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent dir");
    }
    fs::write(&path, content).expect("write file");
    path
}

#[test]
fn transform_resolves_branch_paths_relative_to_rule() {
    let temp = tempfile::tempdir().expect("tempdir");
    let dir = temp.path();

    let main_rule = r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ./child.yaml
      return: true
"#;
    let child_rule = r#"
version: 2
input:
  format: json
  json: {}
mappings:
  - target: ok
    value: true
"#;

    let rule_path = write_file(dir, "main.yaml", main_rule);
    write_file(dir, "child.yaml", child_rule);
    let input_path = write_file(dir, "input.json", r#"{"foo": "bar"}"#);

    let mut cmd = cargo_bin_cmd!("rulemorph");
    cmd.arg("transform")
        .arg("-r")
        .arg(rule_path)
        .arg("-i")
        .arg(input_path);

    cmd.assert().success();
}
