use std::sync::Arc;

use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::Request,
    middleware::{Next, from_fn_with_state},
    routing::{any, get, post},
};

use crate::{TenantContext, TenantResolver};
use rulemorph_endpoint::{ApiMode, EndpointEngine, RequestContext};
#[cfg(test)]
use rulemorph_trace::TraceStore;

mod api_import;
mod api_key_routes;
mod auth;
mod error;
mod import_zip;
mod rate_limit;
mod tenant_registry;
mod trace_routes;
mod ui;

use self::api_import::{
    handle_api_import_only, handle_api_import_or_rules, handle_api_import_or_rules_strict,
    import_bundle_path,
};
use self::api_key_routes::{issue_api_key, list_api_keys, revoke_api_key, rotate_api_key};
#[cfg(test)]
use self::auth::pre_auth_rate_limit_key;
use self::auth::{
    DispatchInvalidApiKeyForRules, api_rate_limit, apply_v1_auth_context, enforce_api_rate_limit,
    ensure_pre_auth_rate_limit_for_request, internal_auth, internal_auth_required, internal_tenant,
    pre_auth_rate_limit, rate_limit_key, v1_auth,
};
use self::error::ApiError;
#[cfg(test)]
use self::import_zip::copy_zip_entry_bounded;
#[cfg(test)]
use self::import_zip::extract_zip;
pub use self::rate_limit::RateLimiter;
pub(crate) use self::tenant_registry::internal_auth_path_allowlist;
pub use self::tenant_registry::{TenantRegistry, TenantResources};
use self::trace_routes::{
    get_api_graph, get_trace, get_trace_finalize, get_trace_manifest, get_trace_nodes_chunk,
    get_trace_records_chunk, list_traces, stream_traces,
};
pub use self::ui::UiSource;
use self::ui::apply_ui_source_fallback;

#[derive(Clone)]
pub struct AppState {
    pub default_resources: Arc<TenantResources>,
    pub tenant_registry: Option<Arc<TenantRegistry>>,
    pub ui_source: Option<UiSource>,
    pub api_mode: ApiMode,
    pub tenant_resolver: Option<Arc<dyn TenantResolver>>,
    pub internal_api_key: Option<String>,
    pub allow_unauth_internal: bool,
    pub rate_limiter: Option<Arc<RateLimiter>>,
}

const IMPORT_ZIP_MAX_FILE_BYTES: u64 = 20 * 1024 * 1024;
const IMPORT_ZIP_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const IMPORT_ZIP_MAX_ENTRIES: usize = 4096;

pub fn build_router(state: AppState, ui_enabled: bool) -> Router {
    let api = match state.api_mode {
        ApiMode::UiOnly => {
            if ui_enabled {
                Router::new()
                    .route("/api/import", any(handle_api_import_only))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            } else {
                Router::new()
            }
        }
        ApiMode::Rules => {
            let mut api_router = Router::new().route("/api/*path", any(handle_rules_api));
            let api_import = if ui_enabled {
                Router::new()
                    .route("/api/import", any(handle_api_import_or_rules))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            } else {
                Router::new()
                    .route("/api/import", any(handle_api_import_or_rules_strict))
                    .route_layer(DefaultBodyLimit::max(IMPORT_ZIP_MAX_TOTAL_BYTES as usize))
            };
            if state.rate_limiter.is_some() {
                api_router = api_router.layer(from_fn_with_state(state.clone(), api_rate_limit));
            }
            if state.tenant_resolver.is_some() {
                api_router = api_router.layer(from_fn_with_state(state.clone(), v1_auth));
                if state.rate_limiter.is_some() {
                    api_router =
                        api_router.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
                }
            }
            let api = Router::new().merge(api_import).merge(api_router);
            if state.tenant_resolver.is_some() {
                let mut v1 = Router::new().route("/v1/*path", any(handle_rules_api));
                if state.rate_limiter.is_some() {
                    v1 = v1.layer(from_fn_with_state(state.clone(), api_rate_limit));
                }
                v1 = v1.layer(from_fn_with_state(state.clone(), v1_auth));
                if state.rate_limiter.is_some() {
                    v1 = v1.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
                }
                api.merge(v1)
            } else {
                api
            }
        }
    };

    let mut app = Router::new().merge(api);

    if ui_enabled {
        let mut internal = Router::new()
            .route("/internal/traces", get(list_traces))
            .route("/internal/traces/:id", get(get_trace))
            .route("/internal/traces/:id/manifest", get(get_trace_manifest))
            .route(
                "/internal/traces/:id/records/:chunk",
                get(get_trace_records_chunk),
            )
            .route(
                "/internal/traces/:id/nodes/:chunk",
                get(get_trace_nodes_chunk),
            )
            .route("/internal/traces/:id/finalize", get(get_trace_finalize))
            .route("/internal/stream", get(stream_traces))
            .route("/internal/api-graph", get(get_api_graph))
            .route("/internal/import", post(import_bundle_path));

        let mut internal_admin = Router::new()
            .route("/internal/api-keys", get(list_api_keys).post(issue_api_key))
            .route("/internal/api-keys/:id/revoke", post(revoke_api_key))
            .route("/internal/api-keys/:id/rotate", post(rotate_api_key));

        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), api_rate_limit));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), api_rate_limit));
        }
        if state.tenant_resolver.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), internal_tenant));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), internal_tenant));
        }
        if !state.allow_unauth_internal
            || state.internal_api_key.is_some()
            || state.tenant_resolver.is_some()
        {
            internal = internal.layer(from_fn_with_state(state.clone(), internal_auth));
        }
        internal_admin =
            internal_admin.layer(from_fn_with_state(state.clone(), internal_auth_required));
        if state.rate_limiter.is_some() {
            internal = internal.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
            internal_admin =
                internal_admin.layer(from_fn_with_state(state.clone(), pre_auth_rate_limit));
        }

        app = app.merge(internal);
        app = app.merge(internal_admin);
        if let Some(ui_source) = state.ui_source.clone() {
            app = apply_ui_source_fallback(app, ui_source);
        }
    }

    app.layer(from_fn_with_state(state.clone(), inject_default_resources))
        .with_state(state)
}

fn request_resources(
    state: &AppState,
    request: &Request<axum::body::Body>,
) -> Arc<TenantResources> {
    request
        .extensions()
        .get::<Arc<TenantResources>>()
        .cloned()
        .unwrap_or_else(|| state.default_resources.clone())
}

fn request_engine<'a>(
    state: &'a AppState,
    resources: &'a Arc<TenantResources>,
) -> std::result::Result<&'a Arc<EndpointEngine>, ApiError> {
    resources
        .api_engine
        .as_ref()
        .or(state.default_resources.api_engine.as_ref())
        .ok_or_else(|| ApiError::internal("api engine not configured"))
}

async fn run_rules_api_request(
    state: &AppState,
    mut request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    if state.tenant_resolver.is_some() && state.rate_limiter.is_some() {
        ensure_pre_auth_rate_limit_for_request(state, &mut request).await?;
    }
    if state.tenant_resolver.is_some() && request.extensions().get::<TenantContext>().is_none() {
        if request
            .extensions()
            .get::<DispatchInvalidApiKeyForRules>()
            .is_some()
        {
            return Err(ApiError::unauthorized("invalid api key"));
        }
        apply_v1_auth_context(state, &mut request).await?;
    }
    if state.rate_limiter.is_some() {
        let key = rate_limit_key(state, &request);
        enforce_api_rate_limit(state, key).await?;
    }
    handle_rules_api_core(state, request).await
}

async fn handle_rules_api(
    state: State<AppState>,
    request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    handle_rules_api_core(&state.0, request).await
}

async fn handle_rules_api_core(
    state: &AppState,
    mut request: Request<axum::body::Body>,
) -> std::result::Result<axum::response::Response, ApiError> {
    let resources = request_resources(state, &request);
    let engine = request_engine(state, &resources)?;
    let mut request_context = RequestContext::default();
    if let Some(context) = request.extensions().get::<TenantContext>() {
        request_context.tenant_id = Some(context.tenant_id.clone());
    }
    if engine.allows_internal_auth() {
        if let Some(internal_api_key) = state.internal_api_key.clone() {
            request_context.internal_api_key = Some(internal_api_key);
        }
    }
    request.extensions_mut().insert(request_context);
    match engine.handle_request(request).await {
        Ok(response) => Ok(response),
        Err(err) => {
            let message = err.to_string();
            if message.contains("no endpoint matched") {
                Err(ApiError::not_found(message))
            } else {
                Err(ApiError::internal(message))
            }
        }
    }
}

async fn inject_default_resources(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> std::result::Result<axum::response::Response, ApiError> {
    if request.extensions().get::<Arc<TenantResources>>().is_none() {
        request
            .extensions_mut()
            .insert(state.default_resources.clone());
    }
    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use async_trait::async_trait;
    use axum::extract::ConnectInfo;
    use axum::http::{HeaderValue, Request, header::AUTHORIZATION};

    use crate::{TenantContext, TenantResolver};

    use super::{
        ApiMode, AppState, IMPORT_ZIP_MAX_ENTRIES, TenantResources, TraceStore, build_router,
        copy_zip_entry_bounded, extract_zip, pre_auth_rate_limit_key,
    };

    struct StaticTenantResolver;

    #[async_trait]
    impl TenantResolver for StaticTenantResolver {
        async fn resolve(&self, _api_key: &str) -> anyhow::Result<Option<TenantContext>> {
            Ok(Some(TenantContext::new("tenant-a")))
        }
    }

    #[test]
    fn pre_auth_rate_limit_uses_api_key_when_present() {
        let mut request = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer rmk_tenant-a.secret"),
        );
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        let key = pre_auth_rate_limit_key(&request);
        assert!(key.starts_with("preauth:key:"));
        assert!(!key.contains("rmk_tenant-a.secret"));
    }

    #[test]
    fn pre_auth_rate_limit_separates_distinct_api_keys() {
        let mut first = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        first
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer key-a"));
        first
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));
        let mut second = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        second
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer key-b"));
        second
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        assert_ne!(
            pre_auth_rate_limit_key(&first),
            pre_auth_rate_limit_key(&second)
        );
    }

    #[test]
    fn pre_auth_rate_limit_falls_back_to_ip_without_api_key() {
        let mut request = Request::builder()
            .uri("/v1/traces")
            .body(axum::body::Body::empty())
            .expect("request");
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from((Ipv4Addr::LOCALHOST, 3000))));

        let key = pre_auth_rate_limit_key(&request);
        assert_eq!(key, "preauth:ip:127.0.0.1");
    }

    #[test]
    fn zip_copy_stops_after_file_limit() {
        let mut input = std::io::Cursor::new(vec![b'x'; 12]);
        let mut output = Vec::new();
        let copied = copy_zip_entry_bounded(&mut input, &mut output, 8).expect("copy");
        assert_eq!(copied, 12);
        assert!(output.len() <= 8);
    }

    #[test]
    fn zip_extract_rejects_too_many_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let zip_path = temp.path().join("bundle.zip");
        let file = std::fs::File::create(&zip_path).expect("create zip");
        let mut zip_writer = zip::ZipWriter::new(file);
        let options =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        for index in 0..=IMPORT_ZIP_MAX_ENTRIES {
            zip_writer
                .start_file(format!("traces/{index}/trace.json"), options)
                .expect("start file");
        }
        zip_writer.finish().expect("finish zip");

        let err = extract_zip(&zip_path, temp.path().join("out").as_path())
            .expect_err("zip should be rejected");
        assert!(err.contains("too many entries"));
    }

    #[tokio::test]
    async fn unauth_internal_is_not_allowed_with_tenant_resolver() {
        let temp = tempfile::tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let store = TraceStore::new(data_dir.clone())
            .await
            .expect("trace store");
        let (trace_events, _) = tokio::sync::broadcast::channel(16);
        let auth_dir = data_dir.join("auth");
        tokio::fs::create_dir_all(&auth_dir)
            .await
            .expect("create auth dir");
        let resources = TenantResources {
            tenant_id: "default".to_string(),
            data_dir: data_dir.clone(),
            rules_dir: data_dir.join("api_rules"),
            auth_dir,
            store: Arc::new(store),
            api_engine: None,
            trace_events,
        };
        let state = AppState {
            default_resources: Arc::new(resources),
            tenant_registry: None,
            ui_source: None,
            api_mode: ApiMode::UiOnly,
            tenant_resolver: Some(Arc::new(StaticTenantResolver)),
            internal_api_key: None,
            allow_unauth_internal: true,
            rate_limiter: None,
        };
        let app = build_router(state, true);
        let response = tower::ServiceExt::oneshot(
            app,
            Request::builder()
                .uri("/internal/traces")
                .header("x-tenant-id", "tenant-a")
                .body(axum::body::Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
