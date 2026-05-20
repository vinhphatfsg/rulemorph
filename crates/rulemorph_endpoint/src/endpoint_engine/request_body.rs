use axum::body::Body;
use axum::http::{HeaderMap, Method};
use http_body_util::LengthLimitError;
use serde_json::Value as JsonValue;

use super::error::EndpointError;
use super::multipart_import::build_multipart_import_body;
use super::request_input::is_multipart_import_request;

pub(super) struct RequestBody {
    pub(super) value: Option<JsonValue>,
    pub(super) multipart_temp_dir: Option<tempfile::TempDir>,
}

pub(super) async fn read_request_body(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: Body,
    max_body_bytes: usize,
) -> Result<RequestBody, EndpointError> {
    if is_multipart_import_request(method, path, headers) {
        let (body, temp_dir) = build_multipart_import_body(headers, body).await?;
        return Ok(RequestBody {
            value: Some(body),
            multipart_temp_dir: Some(temp_dir),
        });
    }

    match axum::body::to_bytes(body, max_body_bytes).await {
        Ok(body_bytes) if body_bytes.is_empty() => Ok(RequestBody {
            value: None,
            multipart_temp_dir: None,
        }),
        Ok(body_bytes) => serde_json::from_slice::<JsonValue>(&body_bytes)
            .map(|value| RequestBody {
                value: Some(value),
                multipart_temp_dir: None,
            })
            .map_err(|err| EndpointError::invalid(err.to_string())),
        Err(err) if is_length_limit_error(&err) => {
            Err(EndpointError::payload_too_large(max_body_bytes))
        }
        Err(err) => Err(EndpointError::network(format!(
            "request body read error: {}",
            err
        ))),
    }
}

fn is_length_limit_error(err: &axum::Error) -> bool {
    let mut current: &(dyn std::error::Error + 'static) = err;
    if current.is::<LengthLimitError>() {
        return true;
    }
    while let Some(source) = current.source() {
        if source.is::<LengthLimitError>() {
            return true;
        }
        current = source;
    }
    false
}
