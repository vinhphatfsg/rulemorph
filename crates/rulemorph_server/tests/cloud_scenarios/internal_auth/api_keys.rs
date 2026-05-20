#[tokio::test]
async fn internal_api_key_issue_is_serialized_per_store() {
    let temp = tempdir().expect("tempdir");
    let data_dir = temp.path().join("data");
    let default_resources = default_test_resources(data_dir.clone(), data_dir.join("api_rules"))
        .await
        .expect("default resources");
    let state = AppState {
        default_resources,
        tenant_registry: None,
        ui_source: None,
        api_mode: ApiMode::UiOnly,
        tenant_resolver: None,
        internal_api_key: Some("internal-key".to_string()),
        allow_unauth_internal: false,
        rate_limiter: None,
    };
    let app = build_router(state, true);
    let issue_count = 32usize;

    let mut handles = Vec::with_capacity(issue_count);
    for i in 0..issue_count {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let (status, payload) = request_json_post_with_headers(
                &app,
                "/internal/api-keys".to_string(),
                &[("authorization", "Bearer internal-key")],
                json!({ "label": format!("parallel-{i}") }),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            payload
                .get("id")
                .and_then(|value| value.as_str())
                .expect("issued id")
                .to_string()
        }));
    }

    let mut ids = Vec::with_capacity(issue_count);
    for handle in handles {
        ids.push(handle.await.expect("join"));
    }
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), issue_count);

    let (status, list) = request_json_with_headers(
        &app,
        "/internal/api-keys".to_string(),
        &[("authorization", "Bearer internal-key")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let keys = list
        .get("keys")
        .and_then(|value| value.as_array())
        .expect("keys");
    assert_eq!(keys.len(), issue_count);
}
