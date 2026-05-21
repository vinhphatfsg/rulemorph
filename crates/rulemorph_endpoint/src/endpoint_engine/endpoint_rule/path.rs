use std::collections::HashMap;

use anyhow::{Result, anyhow};

#[derive(Debug)]
pub(in crate::endpoint_engine) struct EndpointPath {
    segments: Vec<PathSegment>,
}

#[derive(Debug)]
pub(super) enum PathSegment {
    Literal(String),
    Param(String),
}

impl EndpointPath {
    pub(in crate::endpoint_engine) fn parse(path: &str) -> Result<Self> {
        if !path.starts_with('/') {
            return Err(anyhow!("endpoint path must start with /"));
        }
        let segments = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .map(|seg| {
                if let Some(param) = seg.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                    if param.is_empty() {
                        return Err(anyhow!("empty path param"));
                    }
                    Ok(PathSegment::Param(param.to_string()))
                } else {
                    Ok(PathSegment::Literal(seg.to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { segments })
    }

    pub(in crate::endpoint_engine) fn matches(&self, path: &str) -> bool {
        let parts: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .collect();
        if parts.len() != self.segments.len() {
            return false;
        }
        for (seg, part) in self.segments.iter().zip(parts.iter()) {
            match seg {
                PathSegment::Literal(lit) if lit != part => return false,
                _ => {}
            }
        }
        true
    }

    pub(in crate::endpoint_engine) fn capture(&self, path: &str) -> HashMap<String, String> {
        let parts: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|seg| !seg.is_empty())
            .collect();
        let mut params = HashMap::new();
        for (seg, part) in self.segments.iter().zip(parts.iter()) {
            if let PathSegment::Param(name) = seg {
                params.insert(name.clone(), (*part).to_string());
            }
        }
        params
    }
}
