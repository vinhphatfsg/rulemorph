#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
#[cfg(test)]
use std::time::Instant;

use anyhow::{Context, Result, anyhow};
#[cfg(test)]
use axum::http::HeaderMap;
use axum::http::Method;
#[cfg(test)]
use axum::http::Request;
#[cfg(test)]
use rulemorph::Mapping;
use rulemorph::serde_guard::parse_yaml_value_strict;
#[cfg(test)]
use rulemorph::v2_parser::parse_v2_expr;
use rulemorph_trace::{TraceWriter, TraceWriterConfig};
use serde_json::{Value as JsonValue, json};

const MULTIPART_IMPORT_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MULTIPART_IMPORT_MAX_ENTRIES: usize = 4096;

mod catch;
mod catch_runtime;
mod config;
mod endpoint_rule;
mod error;
mod expr;
mod host;
mod multipart_import;
mod network_exec;
mod network_rule;
mod reply_context;
mod request_input;
mod request_runtime;
mod rule_exec;
mod rule_loader;
mod rule_ref;
mod ssrf_audit;
mod trace_emit;
mod trace_graph;
mod validation;

pub use self::config::{ApiMode, EngineConfig, RequestContext};
#[cfg(test)]
use self::endpoint_rule::EndpointPath;
use self::endpoint_rule::{CompiledEndpointRule, EndpointRuleFile};
use self::error::EndpointError;
#[cfg(test)]
use self::error::EndpointErrorKind;
#[cfg(test)]
use self::expr::{build_headers, eval_expr_string};
#[cfg(test)]
use self::host::internal_hosts_match;
#[cfg(test)]
use self::multipart_import::{copy_zip_entry_bounded, extract_zip};
#[cfg(test)]
use self::network_rule::compile_network_rule;
#[cfg(test)]
use self::network_rule::{CompiledNetworkRequest, NetworkRequest};
use self::network_rule::{CompiledNetworkRule, NetworkRuleFile, compile_retry, parse_duration};
#[cfg(test)]
use self::rule_loader::LoadedRule;
use self::rule_ref::{
    resolve_rule_path, rule_display_name, rule_ref_from_path, rule_ref_from_rule,
};
#[cfg(test)]
use self::ssrf_audit::build_ssrf_audit_log;
#[cfg(test)]
use self::trace_graph::{build_mapping_ops_with_values, sum_rule_trace_duration_us};
#[cfg(test)]
use self::trace_graph::{build_network_nodes_with_timing, build_rule_nodes_from_rule};
pub use self::validation::{RulesDirError, RulesDirErrors, validate_rules_dir};
#[cfg(test)]
use std::time::Duration;

pub struct EndpointEngine {
    endpoint_rule: CompiledEndpointRule,
    raw_rule_source: JsonValue,
    config: EngineConfig,
    trace_writer: TraceWriter,
}

struct NetworkExecution {
    output: JsonValue,
    request_us: u64,
    total_us: u64,
    body_rule_trace: Option<JsonValue>,
}

impl EndpointEngine {
    pub fn load(rules_dir: PathBuf, config: EngineConfig) -> Result<Self> {
        let endpoint_path = rules_dir.join("endpoint.yaml");
        let source = std::fs::read_to_string(&endpoint_path)
            .with_context(|| format!("failed to read {}", endpoint_path.display()))?;
        let raw_source = parse_yaml_value_strict(&source)
            .map_err(|err| anyhow!(err))
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        let raw_rule_source =
            serde_json::to_value(raw_source.clone()).unwrap_or_else(|_| json!({}));
        let raw: EndpointRuleFile = serde_yaml::from_value(raw_source)
            .with_context(|| format!("failed to parse {}", endpoint_path.display()))?;
        if raw.version != 2 {
            return Err(anyhow!("endpoint rule version must be 2"));
        }
        if raw.rule_type != "endpoint" {
            return Err(anyhow!("endpoint rule type must be endpoint"));
        }
        let compiled = CompiledEndpointRule::compile(raw.clone(), &endpoint_path)?;
        let trace_writer = TraceWriter::with_config(
            config.data_dir.clone(),
            TraceWriterConfig {
                write_options: config.trace_write_options.clone(),
                ..TraceWriterConfig::default()
            },
        );
        Ok(Self {
            endpoint_rule: compiled,
            raw_rule_source,
            config,
            trace_writer,
        })
    }

    pub fn allows_internal_auth(&self) -> bool {
        self.config.allow_internal_auth
    }

    pub fn has_endpoint(&self, method: &Method, path: &str) -> bool {
        self.endpoint_rule.match_endpoint(method, path).is_some()
    }
}

fn empty_object() -> JsonValue {
    JsonValue::Object(serde_json::Map::new())
}

#[cfg(test)]
mod tests;
