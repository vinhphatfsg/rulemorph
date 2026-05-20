async fn request_json(app: &Router, path: String) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn request_json_with_headers(
    app: &Router,
    path: String,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(&path);
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    let response = app
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn request_json_post_with_headers(
    app: &Router,
    path: String,
    headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(&path)
        .header("content-type", "application/json");
    for (key, value) in headers {
        builder = builder.header(*key, *value);
    }
    let payload = serde_json::to_vec(&body).expect("serialize request body");
    let response = app
        .clone()
        .oneshot(builder.body(Body::from(payload)).unwrap())
        .await
        .expect("request");
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("collect")
        .to_bytes();
    let value = if body.is_empty() {
        json!(null)
    } else {
        serde_json::from_slice(&body).expect("json")
    };
    (status, value)
}

async fn post_api_import(
    app: &Router,
    label: &str,
    authorization: Option<&str>,
    import_kind: Option<&str>,
    boundary: &str,
    body: Vec<u8>,
) -> Response {
    let mut builder = Request::builder().method("POST").uri("/api/import").header(
        "content-type",
        format!("multipart/form-data; boundary={boundary}"),
    );
    if let Some(authorization) = authorization {
        builder = builder.header("authorization", authorization);
    }
    if let Some(import_kind) = import_kind {
        builder = builder.header("x-rulemorph-import", import_kind);
    }

    app.clone()
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap_or_else(|_| panic!("{label}"))
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T> {
    let payload = response.into_body().collect().await?.to_bytes();
    Ok(serde_json::from_slice(&payload)?)
}

async fn wait_for_trace_id(app: &Router) -> String {
    for _ in 0..40 {
        let (status, list) = request_json(app, "/internal/traces".to_string()).await;
        if status == StatusCode::OK {
            if let Some(trace_id) = list
                .get("traces")
                .and_then(|value| value.as_array())
                .and_then(|values| values.first())
                .and_then(|value| value.get("trace_id"))
                .and_then(|value| value.as_str())
            {
                return trace_id.to_string();
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("trace not found after waiting");
}
