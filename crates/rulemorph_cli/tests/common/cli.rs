use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;

pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("rulemorph")
        .join("tests")
        .join("fixtures")
}

pub fn read_json(path: &Path) -> serde_json::Value {
    let data =
        fs::read_to_string(path).unwrap_or_else(|_| panic!("failed to read {}", path.display()));
    serde_json::from_str(&data).unwrap_or_else(|_| panic!("invalid json: {}", path.display()))
}

pub fn stdout_string(output: Output) -> String {
    String::from_utf8(output.stdout).expect("stdout should be UTF-8")
}

pub fn stderr_string(output: Output) -> String {
    String::from_utf8(output.stderr).expect("stderr should be UTF-8")
}

pub fn stdout_json(output: Output) -> serde_json::Value {
    let stdout = stdout_string(output);
    serde_json::from_str(&stdout).unwrap_or_else(|_| panic!("invalid json stdout: {}", stdout))
}

pub fn stderr_json(output: Output) -> serde_json::Value {
    let stderr = stderr_string(output);
    serde_json::from_str(&stderr).unwrap_or_else(|_| panic!("invalid json stderr: {}", stderr))
}

pub fn assert_json_stdout_eq(output: Output, expected: &serde_json::Value) {
    let actual = stdout_json(output);
    assert_eq!(&actual, expected);
}
