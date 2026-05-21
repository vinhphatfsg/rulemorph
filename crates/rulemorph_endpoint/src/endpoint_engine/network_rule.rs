use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use axum::http::Method;
use rulemorph::v2_parser::parse_v2_expr;
use rulemorph::{Mapping, RuleFormat, parse_rule_file_with_format, validate_rule_file_with_source};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use super::{catch::CatchSpec, resolve_rule_path, rule_loader::LoadedRule, rule_ref_from_path};

mod retry;

use retry::NetworkRetry;
pub(super) use retry::{RetryConfig, compile_retry, parse_duration};

#[derive(Debug)]
pub(super) struct CompiledNetworkRule {
    pub(super) request: CompiledNetworkRequest,
    pub(super) timeout: Duration,
    pub(super) select: Option<String>,
    pub(super) body: Option<rulemorph::v2_model::V2Expr>,
    pub(super) body_map: Option<Vec<Mapping>>,
    pub(super) body_rule: Option<LoadedRule>,
    pub(super) body_rule_ref: Option<String>,
    pub(super) rule_ref: Option<String>,
    pub(super) catch: Option<CatchSpec>,
    pub(super) retry: Option<RetryConfig>,
    pub(super) internal_auth: bool,
    pub(super) base_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
pub(super) struct NetworkRuleFile {
    pub(super) version: u8,
    #[serde(rename = "type")]
    pub(super) rule_type: String,
    pub(super) request: NetworkRequest,
    pub(super) timeout: String,
    #[serde(default)]
    pub(super) internal_auth: bool,
    #[serde(default)]
    pub(super) select: Option<String>,
    #[serde(default)]
    pub(super) body: Option<JsonValue>,
    #[serde(default)]
    pub(super) body_map: Option<Vec<Mapping>>,
    #[serde(default)]
    pub(super) body_rule: Option<String>,
    #[serde(default)]
    pub(super) catch: Option<HashMap<String, String>>,
    #[serde(default)]
    pub(super) retry: Option<NetworkRetry>,
}

#[derive(Debug, Deserialize)]
pub(super) struct NetworkRequest {
    pub(super) method: String,
    pub(super) url: JsonValue,
    #[serde(default)]
    pub(super) headers: Option<HashMap<String, JsonValue>>,
}

#[derive(Debug)]
pub(super) struct CompiledNetworkRequest {
    pub(super) method: Method,
    pub(super) url: rulemorph::v2_model::V2Expr,
    pub(super) headers: HashMap<String, rulemorph::v2_model::V2Expr>,
}

pub(super) fn compile_network_rule(
    raw: NetworkRuleFile,
    path: &Path,
) -> Result<CompiledNetworkRule> {
    if raw.version != 2 {
        return Err(anyhow!("network rule version must be 2"));
    }
    if raw.rule_type != "network" {
        return Err(anyhow!("network rule type must be network"));
    }
    if raw.body.is_some() && raw.body_map.is_some() {
        return Err(anyhow!("body and body_map are mutually exclusive"));
    }
    if raw.body.is_some() && raw.body_rule.is_some() {
        return Err(anyhow!("body and body_rule are mutually exclusive"));
    }
    if raw.body_map.is_some() && raw.body_rule.is_some() {
        return Err(anyhow!("body_map and body_rule are mutually exclusive"));
    }

    let method =
        Method::from_bytes(raw.request.method.as_bytes()).map_err(|_| anyhow!("invalid method"))?;
    if method == Method::GET
        && (raw.body.is_some() || raw.body_map.is_some() || raw.body_rule.is_some())
    {
        return Err(anyhow!("GET with body is not allowed"));
    }
    let url_expr = parse_v2_expr(&raw.request.url).map_err(|err| anyhow!(err))?;
    let mut headers: HashMap<String, rulemorph::v2_model::V2Expr> = HashMap::new();
    for (key, value) in raw.request.headers.unwrap_or_default() {
        let expr = parse_v2_expr(&value).map_err(|err| anyhow!(err))?;
        headers.insert(key.to_lowercase(), expr);
    }
    let timeout = parse_duration(&raw.timeout)?;
    if timeout.is_zero() {
        return Err(anyhow!("timeout must be > 0"));
    }
    let body = match raw.body {
        Some(value) => Some(parse_v2_expr(&value).map_err(|err| anyhow!(err))?),
        None => None,
    };
    let body_rule = match raw.body_rule.as_ref() {
        Some(path_str) => {
            let resolved =
                resolve_rule_path(path.parent().unwrap_or_else(|| Path::new(".")), path_str);
            let source = std::fs::read_to_string(&resolved)
                .with_context(|| format!("failed to read {}", resolved.display()))?;
            let rule = parse_rule_file_with_format(&source, RuleFormat::from_path(&resolved))
                .with_context(|| format!("failed to parse {}", resolved.display()))?;
            validate_rule_file_with_source(&rule, &source)
                .map_err(|err| anyhow!("failed to validate {}: {:?}", resolved.display(), err))?;
            let base_dir = resolved
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            Some(LoadedRule { rule, base_dir })
        }
        None => None,
    };
    let body_rule_ref = raw.body_rule.as_ref().map(|path_str| {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let resolved = resolve_rule_path(base_dir, path_str);
        rule_ref_from_path(base_dir, &resolved)
    });

    let retry = compile_retry(raw.retry.as_ref())?;
    let rule_ref = {
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        Some(rule_ref_from_path(base_dir, path))
    };
    Ok(CompiledNetworkRule {
        request: CompiledNetworkRequest {
            method,
            url: url_expr,
            headers,
        },
        timeout,
        select: raw.select,
        body,
        body_map: raw.body_map,
        body_rule,
        body_rule_ref,
        rule_ref,
        catch: raw.catch.map(CatchSpec::from),
        retry,
        internal_auth: raw.internal_auth,
        base_dir: path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf(),
    })
}
