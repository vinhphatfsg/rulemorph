struct StaticTenantResolver {
    api_key: String,
    tenant_id: String,
}

#[async_trait]
impl TenantResolver for StaticTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        if api_key == self.api_key {
            Ok(Some(TenantContext::new(self.tenant_id.clone())))
        } else {
            Ok(None)
        }
    }
}

struct CountingTenantResolver {
    api_key: String,
    tenant_id: String,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TenantResolver for CountingTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if api_key == self.api_key {
            Ok(Some(TenantContext::new(self.tenant_id.clone())))
        } else {
            Ok(None)
        }
    }
}

struct RejectTenantResolver;

#[async_trait]
impl TenantResolver for RejectTenantResolver {
    async fn resolve(&self, _api_key: &str) -> Result<Option<TenantContext>> {
        Ok(None)
    }
}

struct MapTenantResolver {
    map: HashMap<String, String>,
}

#[async_trait]
impl TenantResolver for MapTenantResolver {
    async fn resolve(&self, api_key: &str) -> Result<Option<TenantContext>> {
        Ok(self
            .map
            .get(api_key)
            .map(|tenant_id| TenantContext::new(tenant_id.clone())))
    }
}
