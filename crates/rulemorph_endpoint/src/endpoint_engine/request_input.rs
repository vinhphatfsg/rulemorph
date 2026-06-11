use std::collections::HashMap;

use anyhow::Result;
use axum::http::{HeaderMap, Method};
use serde_json::{Value as JsonValue, json};

use super::error::EndpointError;

pub(super) fn build_input(
    parts: &axum::http::request::Parts,
    path_params: &HashMap<String, String>,
    body: Option<JsonValue>,
) -> Result<JsonValue, EndpointError> {
    let query = parse_query(parts.uri.query())?;
    Ok(build_input_from_parts(parts, path_params, body, query))
}

pub(super) fn build_input_from_parts(
    parts: &axum::http::request::Parts,
    path_params: &HashMap<String, String>,
    body: Option<JsonValue>,
    query: JsonValue,
) -> JsonValue {
    let mut headers: HashMap<String, String> = HashMap::new();
    for (name, value) in parts.headers.iter() {
        let key = name.as_str().to_lowercase();
        let value = value.to_str().unwrap_or_default();
        if let Some(existing) = headers.get_mut(&key) {
            existing.push(',');
            existing.push_str(value);
        } else {
            headers.insert(key, value.to_string());
        }
    }

    let mut input = json!({
        "method": parts.method.as_str(),
        "path": path_params,
        "query": query,
        "headers": headers,
    });

    if let Some(body) = body
        && let JsonValue::Object(ref mut map) = input
    {
        map.insert("body".to_string(), body);
    }

    input
}

pub(super) fn is_multipart_form_data(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| multer::parse_boundary(value).is_ok())
}

pub(super) fn is_multipart_import_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
) -> bool {
    method == Method::POST && path == "/api/import" && is_multipart_form_data(headers)
}

pub(super) fn parse_query(query: Option<&str>) -> Result<JsonValue, EndpointError> {
    let mut map: HashMap<String, String> = HashMap::new();
    if let Some(q) = query {
        for (key, value) in url::form_urlencoded::parse(q.as_bytes()) {
            let key = key.into_owned();
            let value = value.into_owned();
            if map.contains_key(&key) {
                return Err(EndpointError::invalid(format!(
                    "duplicate query param: {}",
                    key
                )));
            }
            map.insert(key, value);
        }
    }
    serde_json::to_value(map).map_err(|err| EndpointError::invalid(err.to_string()))
}
