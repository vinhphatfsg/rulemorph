#[test]
fn validate_rules_dir_branch_reference_missing() {
    let temp = tempfile::tempdir().expect("tempdir");
    let rules_dir = temp.path();
    write_file(
        rules_dir,
        "endpoint.yaml",
        r#"
version: 2
type: endpoint
endpoints:
  - method: GET
    path: /api/test
    steps:
      - rule: ./hello.yaml
    reply:
      status: 200
"#,
    );
    write_file(
        rules_dir,
        "hello.yaml",
        r#"
version: 2
input:
  format: json
  json: {}
steps:
  - branch:
      when: { eq: [1, 1] }
      then: ./missing.yaml
      return: true
"#,
    );

    let result = validate_rules_dir(rules_dir).unwrap_err();
    assert!(result.errors.iter().any(|err| err.code == "ReadFailed"));
}
