use std::sync::Arc;

use crate::TenantResolver;
use rulemorph_endpoint::ApiMode;
#[cfg(test)]
use rulemorph_trace::TraceStore;

mod api_import;
mod api_key_routes;
mod auth;
mod error;
mod import_zip;
mod rate_limit;
mod router;
mod rules_api;
mod tenant_registry;
mod trace_routes;
mod ui;

#[cfg(test)]
use self::auth::pre_auth_rate_limit_key;
use self::error::ApiError;
#[cfg(test)]
use self::import_zip::copy_zip_entry_bounded;
#[cfg(test)]
use self::import_zip::extract_zip;
pub use self::rate_limit::RateLimiter;
pub use self::router::build_router;
pub(crate) use self::tenant_registry::internal_auth_path_allowlist;
pub use self::tenant_registry::{TenantRegistry, TenantResources};
pub use self::ui::UiSource;

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
