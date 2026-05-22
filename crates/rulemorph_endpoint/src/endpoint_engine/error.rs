use std::path::PathBuf;

use axum::http::StatusCode;
use rulemorph::TransformError;
use serde_json::{Value as JsonValue, json};

#[derive(Debug, Clone)]
pub(super) struct EndpointError {
    pub(super) kind: EndpointErrorKind,
    pub(super) status: Option<u16>,
    pub(super) message: String,
    pub(super) path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum EndpointErrorKind {
    Timeout,
    HttpStatus,
    Network,
    Transform,
    Invalid,
}

impl EndpointError {
    pub(super) fn timeout() -> Self {
        Self {
            kind: EndpointErrorKind::Timeout,
            status: None,
            message: "timeout".to_string(),
            path: None,
        }
    }

    pub(super) fn http_status(status: u16) -> Self {
        Self {
            kind: EndpointErrorKind::HttpStatus,
            status: Some(status),
            message: format!("http status {}", status),
            path: None,
        }
    }

    pub(super) fn network(message: String) -> Self {
        Self {
            kind: EndpointErrorKind::Network,
            status: None,
            message,
            path: None,
        }
    }

    pub(super) fn invalid(message: impl Into<String>) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: None,
            message: message.into(),
            path: None,
        }
    }

    pub(super) fn bad_request(message: impl Into<String>) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: Some(StatusCode::BAD_REQUEST.as_u16()),
            message: message.into(),
            path: None,
        }
    }

    pub(super) fn payload_too_large(limit: usize) -> Self {
        Self {
            kind: EndpointErrorKind::Invalid,
            status: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            message: format!("payload too large (limit {} bytes)", limit),
            path: None,
        }
    }

    pub(super) fn from_transform(err: TransformError) -> Self {
        Self {
            kind: EndpointErrorKind::Transform,
            status: None,
            message: err.to_string(),
            path: None,
        }
    }

    pub(super) fn with_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub(super) fn to_json(&self) -> JsonValue {
        json!({
            "kind": format!("{:?}", self.kind),
            "status": self.status,
            "message": self.message,
            "path": self.path.as_ref().map(|p| p.display().to_string()),
        })
    }
}

impl std::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
