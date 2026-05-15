use std::path::PathBuf;

use rulemorph_trace::TraceWriteOptions;

use super::host::{is_loopback_host, normalize_internal_host};

const DEFAULT_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 10 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiMode {
    UiOnly,
    Rules,
}

#[derive(Clone, Debug, Default)]
pub struct RequestContext {
    pub tenant_id: Option<String>,
    pub internal_api_key: Option<String>,
}

impl Default for ApiMode {
    fn default() -> Self {
        ApiMode::Rules
    }
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub internal_base: String,
    pub data_dir: PathBuf,
    pub trace_write_options: TraceWriteOptions,
    pub max_body_bytes: usize,
    pub max_response_bytes: usize,
    pub ssrf_allowlist: Vec<String>,
    pub ssrf_allow_private: bool,
    pub ssrf_private_allowlist: Vec<String>,
    pub allow_internal_auth: bool,
    pub internal_auth_path_allowlist: Vec<String>,
    pub internal_api_key: Option<String>,
}

impl EngineConfig {
    pub fn new(internal_base: String, data_dir: PathBuf) -> Self {
        let mut config = Self {
            internal_base,
            data_dir,
            trace_write_options: TraceWriteOptions::default(),
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            ssrf_allowlist: Vec::new(),
            ssrf_allow_private: false,
            ssrf_private_allowlist: Vec::new(),
            allow_internal_auth: false,
            internal_auth_path_allowlist: Vec::new(),
            internal_api_key: None,
        };
        if let Ok(parsed) = url::Url::parse(&config.internal_base) {
            if let Some(host) = parsed.host_str() {
                config.ssrf_private_allowlist.push(host.to_string());
                if is_loopback_host(&normalize_internal_host(host)) {
                    config
                        .ssrf_private_allowlist
                        .extend(["localhost", "127.0.0.1", "::1"].map(str::to_string));
                    config.ssrf_private_allowlist.sort();
                    config.ssrf_private_allowlist.dedup();
                }
            }
        }
        config
    }

    pub fn with_trace_write_options(mut self, trace_write_options: TraceWriteOptions) -> Self {
        self.trace_write_options = trace_write_options;
        self
    }

    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes;
        self
    }

    pub fn with_ssrf_allowlist(mut self, ssrf_allowlist: Vec<String>) -> Self {
        self.ssrf_allowlist = ssrf_allowlist;
        self
    }

    pub fn with_ssrf_allow_private(mut self, ssrf_allow_private: bool) -> Self {
        self.ssrf_allow_private = ssrf_allow_private;
        self
    }

    pub fn with_ssrf_private_allowlist(mut self, ssrf_private_allowlist: Vec<String>) -> Self {
        self.ssrf_private_allowlist = ssrf_private_allowlist;
        self
    }

    pub fn with_internal_auth_enabled(mut self, enabled: bool) -> Self {
        self.allow_internal_auth = enabled;
        self
    }

    pub fn with_internal_auth_path_allowlist(mut self, allowlist: Vec<String>) -> Self {
        self.internal_auth_path_allowlist = allowlist;
        self
    }

    pub fn with_internal_api_key(mut self, internal_api_key: String) -> Self {
        self.internal_api_key = Some(internal_api_key);
        self.allow_internal_auth = true;
        self
    }
}
