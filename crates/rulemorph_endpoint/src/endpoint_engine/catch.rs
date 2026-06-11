use std::collections::HashMap;
use std::path::PathBuf;

use super::error::{EndpointError, EndpointErrorKind};

#[derive(Debug)]
pub(super) struct CatchSpec(HashMap<String, String>);

impl From<HashMap<String, String>> for CatchSpec {
    fn from(value: HashMap<String, String>) -> Self {
        CatchSpec(value)
    }
}

impl CatchSpec {
    pub(super) fn match_target(&self, error: &EndpointError) -> Option<PathBuf> {
        let map = &self.0;
        if let Some(status) = error.status {
            let key = status.to_string();
            if let Some(value) = map.get(&key) {
                return Some(PathBuf::from(value));
            }
            let pattern = if (400..500).contains(&status) {
                "4xx"
            } else if (500..600).contains(&status) {
                "5xx"
            } else {
                ""
            };
            if !pattern.is_empty()
                && let Some(value) = map.get(pattern)
            {
                return Some(PathBuf::from(value));
            }
        }
        if error.kind == EndpointErrorKind::Timeout
            && let Some(value) = map.get("timeout")
        {
            return Some(PathBuf::from(value));
        }
        map.get("default").map(PathBuf::from)
    }
}
