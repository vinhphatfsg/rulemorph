use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use axum::http::Method;
use rulemorph::Mapping;
use rulemorph::v2_parser::{parse_v2_condition, parse_v2_expr};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::CatchSpec;

#[derive(Debug)]
pub(super) struct CompiledEndpointRule {
    pub(super) base_dir: PathBuf,
    pub(super) source_path: PathBuf,
    endpoints: Vec<CompiledEndpoint>,
}

impl CompiledEndpointRule {
    pub(super) fn compile(raw: EndpointRuleFile, source_path: &Path) -> Result<Self> {
        let base_dir = source_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let endpoints = raw
            .endpoints
            .into_iter()
            .map(|endpoint| CompiledEndpoint::compile(endpoint, &base_dir))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            base_dir,
            source_path: source_path.to_path_buf(),
            endpoints,
        })
    }

    pub(super) fn match_endpoint(&self, method: &Method, path: &str) -> Option<EndpointMatch<'_>> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.matches(method, path))
            .map(|endpoint| EndpointMatch {
                params: endpoint.matcher.capture(path),
                endpoint,
            })
    }
}

pub(super) struct EndpointMatch<'a> {
    pub(super) endpoint: &'a CompiledEndpoint,
    pub(super) params: HashMap<String, String>,
}

#[derive(Debug)]
pub(super) struct CompiledEndpoint {
    method: Method,
    matcher: EndpointPath,
    pub(super) input: Option<Vec<Mapping>>,
    pub(super) steps: Vec<CompiledStep>,
    pub(super) reply: CompiledReply,
    pub(super) catch: Option<CatchSpec>,
}

impl CompiledEndpoint {
    pub(super) fn compile(raw: EndpointDef, _base_dir: &Path) -> Result<Self> {
        let method =
            Method::from_bytes(raw.method.as_bytes()).map_err(|_| anyhow!("invalid method"))?;
        let matcher = EndpointPath::parse(&raw.path)?;
        let steps = raw
            .steps
            .into_iter()
            .map(CompiledStep::compile)
            .collect::<Result<Vec<_>>>()?;
        let reply = CompiledReply::compile(raw.reply)?;
        Ok(Self {
            method,
            matcher,
            input: raw.input,
            steps,
            reply,
            catch: raw.catch.map(CatchSpec::from),
        })
    }

    pub(super) fn matches(&self, method: &Method, path: &str) -> bool {
        if &self.method != method {
            return false;
        }
        self.matcher.matches(path)
    }
}

#[derive(Debug)]
pub(super) struct CompiledStep {
    pub(super) rule: String,
    pub(super) with: Option<JsonValue>,
    pub(super) when: Option<rulemorph::v2_model::V2Condition>,
    pub(super) catch: Option<CatchSpec>,
}

impl CompiledStep {
    pub(super) fn compile(raw: EndpointStep) -> Result<Self> {
        let when = match raw.when {
            Some(value) => Some(parse_v2_condition(&value).map_err(|err| anyhow!(err))?),
            None => None,
        };
        Ok(Self {
            rule: raw.rule,
            with: raw.with,
            when,
            catch: raw.catch.map(CatchSpec::from),
        })
    }
}

#[derive(Debug)]
pub(super) struct CompiledReply {
    pub(super) status: rulemorph::v2_model::V2Expr,
    pub(super) headers: HashMap<String, String>,
    pub(super) body: Option<rulemorph::v2_model::V2Expr>,
}

impl CompiledReply {
    pub(super) fn compile(raw: EndpointReply) -> Result<Self> {
        let status = parse_v2_expr(&raw.status).map_err(|err| anyhow!(err))?;
        let body = match raw.body {
            Some(value) => Some(parse_v2_expr(&value).map_err(|err| anyhow!(err))?),
            None => None,
        };
        let headers = raw
            .headers
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k.to_lowercase(), v))
            .collect();
        Ok(Self {
            status,
            headers,
            body,
        })
    }
}

#[derive(Debug)]
pub(super) struct EndpointPath {
    pub(super) segments: Vec<PathSegment>,
}

#[derive(Debug)]
pub(super) enum PathSegment {
    Literal(String),
    Param(String),
}

impl EndpointPath {
    pub(super) fn parse(path: &str) -> Result<Self> {
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

    pub(super) fn matches(&self, path: &str) -> bool {
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

    pub(super) fn capture(&self, path: &str) -> HashMap<String, String> {
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

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EndpointRuleFile {
    pub(super) version: u8,
    #[serde(rename = "type")]
    pub(super) rule_type: String,
    pub(super) endpoints: Vec<EndpointDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EndpointDef {
    pub(super) method: String,
    pub(super) path: String,
    #[serde(default)]
    pub(super) input: Option<Vec<Mapping>>,
    pub(super) steps: Vec<EndpointStep>,
    pub(super) reply: EndpointReply,
    #[serde(default)]
    pub(super) catch: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EndpointStep {
    pub(super) rule: String,
    #[serde(default)]
    pub(super) with: Option<JsonValue>,
    #[serde(default)]
    pub(super) when: Option<JsonValue>,
    #[serde(default)]
    pub(super) catch: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct EndpointReply {
    pub(super) status: JsonValue,
    #[serde(default)]
    pub(super) headers: Option<HashMap<String, String>>,
    #[serde(default)]
    pub(super) body: Option<JsonValue>,
}
