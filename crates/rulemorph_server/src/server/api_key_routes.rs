use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use axum::{
    Json,
    extract::{Extension, Path as AxumPath},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use crate::{ApiKeyInfo, ApiKeyIssueResult, ApiKeyStore};

use super::{ApiError, TenantResources};

static API_KEY_FILE_LOCKS: OnceLock<StdMutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();

fn api_key_file_lock(path: &Path) -> Arc<Mutex<()>> {
    let lock_map = API_KEY_FILE_LOCKS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = lock_map
        .lock()
        .expect("api key lock map should not be poisoned");
    guard
        .entry(path.to_path_buf())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[derive(Deserialize)]
pub(super) struct ApiKeyIssueRequest {
    label: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ApiKeyRotateRequest {
    label: Option<String>,
}

#[derive(Serialize)]
pub(super) struct ApiKeyListResponse {
    keys: Vec<ApiKeyInfo>,
}

pub(super) async fn list_api_keys(
    Extension(resources): Extension<Arc<TenantResources>>,
) -> std::result::Result<Json<ApiKeyListResponse>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let store = ApiKeyStore::load(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let keys = store.map(|store| store.list()).unwrap_or_default();
    Ok(Json(ApiKeyListResponse { keys }))
}

pub(super) async fn issue_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    Json(payload): Json<ApiKeyIssueRequest>,
) -> std::result::Result<Json<ApiKeyIssueResult>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store =
        ApiKeyStore::load_or_init(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let issued = store.issue(payload.label).map_err(ApiError::internal)?;
    Ok(Json(issued))
}

pub(super) async fn revoke_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
) -> std::result::Result<Json<serde_json::Value>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store = ApiKeyStore::load(path, &resources.tenant_id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("api key store not found"))?;
    let revoked = store.revoke(&id).map_err(ApiError::internal)?;
    Ok(Json(json!({ "revoked": revoked })))
}

pub(super) async fn rotate_api_key(
    Extension(resources): Extension<Arc<TenantResources>>,
    AxumPath(id): AxumPath<String>,
    Json(payload): Json<ApiKeyRotateRequest>,
) -> std::result::Result<Json<ApiKeyIssueResult>, ApiError> {
    let path = resources.auth_dir.join("api_keys.json");
    let lock = api_key_file_lock(&path);
    let _guard = lock.lock().await;
    let mut store =
        ApiKeyStore::load_or_init(path, &resources.tenant_id).map_err(ApiError::internal)?;
    let issued = store
        .rotate(&id, payload.label)
        .map_err(ApiError::internal)?;
    let Some(issued) = issued else {
        return Err(ApiError::not_found("api key not found"));
    };
    Ok(Json(issued))
}
