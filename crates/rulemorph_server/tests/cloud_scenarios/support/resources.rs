async fn default_test_resources(
    data_dir: PathBuf,
    rules_dir: PathBuf,
) -> Result<Arc<TenantResources>> {
    let store = TraceStore::new(data_dir.clone()).await?;
    let (trace_events, _) = broadcast::channel(16);
    let auth_dir = data_dir.join("auth");
    fs::create_dir_all(&auth_dir)?;
    Ok(Arc::new(TenantResources {
        tenant_id: "default".to_string(),
        data_dir,
        rules_dir,
        auth_dir,
        store: Arc::new(store),
        api_engine: None,
        trace_events,
    }))
}
