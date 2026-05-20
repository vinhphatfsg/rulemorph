use reqwest::Response;
use serde_json::Value as JsonValue;

use crate::endpoint_engine::error::EndpointError;

pub(super) async fn read_network_response(
    mut response: Response,
    max_response_bytes: usize,
) -> Result<JsonValue, EndpointError> {
    let status = response.status();
    let status_u16 = status.as_u16();
    if status.is_client_error() || status.is_server_error() {
        return Err(EndpointError::http_status(status_u16));
    }

    if let Some(length) = response.content_length() {
        if length > max_response_bytes as u64 {
            return Err(EndpointError::payload_too_large(max_response_bytes));
        }
    }
    let mut bytes: Vec<u8> = Vec::new();
    let mut total = 0usize;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| EndpointError::network(err.to_string()))?
    {
        total = total.saturating_add(chunk.len());
        if total > max_response_bytes {
            return Err(EndpointError::payload_too_large(max_response_bytes));
        }
        bytes.extend_from_slice(&chunk);
    }
    let value = if bytes.is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_slice::<JsonValue>(&bytes)
            .map_err(|err| EndpointError::network(err.to_string()))?
    };
    Ok(value)
}
