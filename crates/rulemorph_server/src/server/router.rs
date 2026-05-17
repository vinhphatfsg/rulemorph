use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware::from_fn_with_state,
    routing::{any, get, post},
};
use rulemorph_endpoint::ApiMode;

use super::api_import::{
    handle_api_import_only, handle_api_import_or_rules, handle_api_import_or_rules_strict,
    import_bundle_path,
};
use super::api_key_routes::{issue_api_key, list_api_keys, revoke_api_key, rotate_api_key};
use super::auth::{
    api_rate_limit, internal_auth, internal_auth_required, internal_tenant, pre_auth_rate_limit,
    v1_auth,
};
use super::rules_api::{handle_rules_api, inject_default_resources};
use super::trace_routes::{
    get_api_graph, get_trace, get_trace_finalize, get_trace_manifest, get_trace_nodes_chunk,
    get_trace_records_chunk, list_traces, stream_traces,
};
use super::ui::apply_ui_source_fallback;
use super::{AppState, IMPORT_ZIP_MAX_TOTAL_BYTES};

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
