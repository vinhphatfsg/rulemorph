use std::collections::HashMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use http_body_util::BodyExt;
use rulemorph_endpoint::{EndpointEngine, EngineConfig};
use rulemorph_server::{
    ApiMode, AppState, RateLimiter, TenantContext, TenantRegistry, TenantRegistryConfig,
    TenantResolver, TenantResources, build_router,
};
use rulemorph_trace::{ImportResult, TraceStore};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::broadcast;
use tower::Service;
use tower::ServiceExt;
use zip::{CompressionMethod, write::FileOptions};

include!("cloud_scenarios/trace_detail.rs");
include!("cloud_scenarios/support/http.rs");
include!("cloud_scenarios/support/resources.rs");
include!("cloud_scenarios/support/tenant_resolvers.rs");
include!("cloud_scenarios/support/app_builder.rs");
include!("cloud_scenarios/support/import_bundle.rs");

include!("cloud_scenarios/v1_auth.rs");
include!("cloud_scenarios/internal_auth.rs");
include!("cloud_scenarios/tenant_traces.rs");

include!("cloud_scenarios/api_import.rs");
