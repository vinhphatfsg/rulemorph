#[cfg(feature = "server")]
#[test]
fn api_keys_issue_list_and_revoke_json() {
    let temp_dir = tempfile::tempdir().unwrap();

    let mut issue_cmd = cargo_bin_cmd!("rulemorph");
    let issue = issue_cmd
        .arg("api-keys")
        .arg("issue")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--label")
        .arg("alpha")
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(issue.status.code(), Some(0));

    let issued = stdout_json(issue);
    let id = issued["id"].as_str().expect("issued id").to_string();
    assert_eq!(issued["label"], "alpha");
    assert!(issued["key"].as_str().is_some_and(|key| !key.is_empty()));

    let mut list_cmd = cargo_bin_cmd!("rulemorph");
    let list = list_cmd
        .arg("api-keys")
        .arg("list")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(list.status.code(), Some(0));

    let keys = stdout_json(list);
    assert_eq!(keys.as_array().expect("api key array").len(), 1);
    assert_eq!(keys[0]["id"], id);
    assert_eq!(keys[0]["label"], "alpha");
    assert!(keys[0]["revoked_at"].is_null());

    let mut revoke_cmd = cargo_bin_cmd!("rulemorph");
    let revoke = revoke_cmd
        .arg("api-keys")
        .arg("revoke")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--id")
        .arg(id)
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(revoke.status.code(), Some(0));

    let revoked = stdout_json(revoke);
    assert_eq!(revoked["revoked"], true);
}
