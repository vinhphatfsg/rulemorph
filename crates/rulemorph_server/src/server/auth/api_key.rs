use axum::http::HeaderMap;

pub(super) fn extract_api_key(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(axum::http::header::AUTHORIZATION) {
        let value = value.to_str().ok()?;
        let mut parts = value.split_whitespace();
        let scheme = parts.next()?;
        if scheme.eq_ignore_ascii_case("bearer") {
            return parts.next().map(|part| part.to_string());
        }
    }
    headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
}
