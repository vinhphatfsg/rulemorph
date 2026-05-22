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

#[cfg(feature = "server")]
#[test]
fn api_keys_rotate_json_revokes_and_issues_replacement() {
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
    let old_id = issued["id"].as_str().expect("issued id").to_string();

    let mut rotate_cmd = cargo_bin_cmd!("rulemorph");
    let rotate = rotate_cmd
        .arg("api-keys")
        .arg("rotate")
        .arg("--tenant-id")
        .arg("tenant-a")
        .arg("--data-dir")
        .arg(temp_dir.path())
        .arg("--id")
        .arg(&old_id)
        .arg("--label")
        .arg("beta")
        .arg("--json")
        .output()
        .unwrap();
    assert_eq!(rotate.status.code(), Some(0));

    let rotated = stdout_json(rotate);
    let new_id = rotated["id"].as_str().expect("rotated id").to_string();
    assert_ne!(new_id, old_id);
    assert_eq!(rotated["label"], "beta");
    assert!(rotated["key"].as_str().is_some_and(|key| !key.is_empty()));

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
    let keys = keys.as_array().expect("api key array");
    assert_eq!(keys.len(), 2);
    let old_key = keys
        .iter()
        .find(|key| key["id"] == old_id)
        .expect("old key should remain listed");
    assert!(old_key["revoked_at"].as_str().is_some());
    let new_key = keys
        .iter()
        .find(|key| key["id"] == new_id)
        .expect("new key should be listed");
    assert!(new_key["revoked_at"].is_null());
    assert_eq!(new_key["label"], "beta");
}
