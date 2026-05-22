include!("input_formats/structured.rs");

include!("input_formats/markup.rs");

fn assert_simple_transform(rules: &std::path::Path, input: &std::path::Path) {
    let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("rulemorph");
    let output = cmd
        .arg("transform")
        .arg("-r")
        .arg(rules)
        .arg("-i")
        .arg(input)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    cli_common::assert_json_stdout_eq(
        output,
        &serde_json::json!([{ "id": "1", "name": "Alice" }]),
    );
}
