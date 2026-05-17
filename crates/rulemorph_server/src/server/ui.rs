use std::path::PathBuf;

use axum::Router;
use tower_http::services::{ServeDir, ServeFile};

#[cfg(feature = "embedded-ui")]
use axum::{
    extract::OriginalUri,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
#[cfg(feature = "embedded-ui")]
use include_dir::{Dir, include_dir};

#[cfg(feature = "embedded-ui")]
static UI_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/../rulemorph_ui/ui/dist");

#[derive(Clone)]
pub enum UiSource {
    Filesystem(PathBuf),
    #[cfg(feature = "embedded-ui")]
    Embedded,
}

pub(super) fn apply_ui_source_fallback<S>(app: Router<S>, ui_source: UiSource) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    match ui_source {
        UiSource::Filesystem(dir) => {
            let static_service =
                ServeDir::new(dir.clone()).fallback(ServeFile::new(dir.join("index.html")));
            app.fallback_service(static_service)
        }
        #[cfg(feature = "embedded-ui")]
        UiSource::Embedded => app.fallback(serve_embedded_ui),
    }
}

#[cfg(feature = "embedded-ui")]
async fn serve_embedded_ui(OriginalUri(uri): OriginalUri) -> impl IntoResponse {
    let mut path = uri.path().trim_start_matches('/').to_string();
    if path.is_empty() {
        path = "index.html".to_string();
    }

    if let Some(file) = UI_DIR.get_file(&path) {
        return embedded_response(file.path().to_str(), file.contents());
    }

    if let Some(index) = UI_DIR.get_file("index.html") {
        return embedded_response(Some("index.html"), index.contents());
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "embedded ui missing index.html",
    )
        .into_response()
}

#[cfg(feature = "embedded-ui")]
fn embedded_response(path: Option<&str>, contents: &'static [u8]) -> axum::response::Response {
    let mut headers = HeaderMap::new();
    let mime = match path {
        Some(path) => mime_guess::from_path(path).first_or_octet_stream(),
        None => mime_guess::mime::APPLICATION_OCTET_STREAM,
    };
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        mime.as_ref()
            .parse()
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("application/octet-stream")),
    );
    (headers, contents).into_response()
}
