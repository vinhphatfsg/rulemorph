import { getApiKey } from "./auth";

const TENANT_ID_STORAGE = "rulemorph_tenant_id";

let cachedTenantId: string | null | undefined;

function normalizeTenantId(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (!/^[A-Za-z0-9_-]+$/.test(trimmed)) return null;
  return trimmed;
}

function getTenantIdFromApiKey(apiKey: string | null): string | null {
  if (!apiKey) return null;
  const trimmed = apiKey.trim();
  if (!trimmed.startsWith("rmk_")) {
    return null;
  }
  const rest = trimmed.slice("rmk_".length);
  const [tenantId] = rest.split(".", 2);
  if (!tenantId || !tenantId.trim()) {
    return null;
  }
  return normalizeTenantId(tenantId);
}

function getTenantIdFromQueryOrStorage(): string | null {
  if (typeof window === "undefined") {
    cachedTenantId = null;
    return cachedTenantId;
  }

  const params = new URLSearchParams(window.location.search);
  const tenantParam = normalizeTenantId(params.get("tenant_id"));
  if (tenantParam) {
    try {
      window.localStorage.setItem(TENANT_ID_STORAGE, tenantParam);
    } catch {
      // ignore storage failures
    }
    try {
      const url = new URL(window.location.href);
      url.searchParams.delete("tenant_id");
      if (url.toString() !== window.location.href) {
        window.history.replaceState(null, "", url.toString());
      }
    } catch {
      // ignore history failures
    }
    cachedTenantId = tenantParam;
    return tenantParam;
  }

  if (cachedTenantId !== undefined) {
    return cachedTenantId;
  }

  try {
    const stored = normalizeTenantId(window.localStorage.getItem(TENANT_ID_STORAGE));
    if (stored) {
      cachedTenantId = stored;
      return stored;
    }
  } catch {
    // ignore storage failures
  }

  cachedTenantId = null;
  return cachedTenantId;
}

export function resolveTenantId(
  apiKey: string | null,
  tenantFromQueryOrStorage: string | null
): string | null {
  const tenantFromApiKey = getTenantIdFromApiKey(apiKey);
  if (tenantFromApiKey) {
    return tenantFromApiKey;
  }
  if (tenantFromQueryOrStorage) {
    return tenantFromQueryOrStorage;
  }
  return null;
}

export function __getTenantIdFromQueryOrStorageForTest(): string | null {
  return getTenantIdFromQueryOrStorage();
}

export function __resetTenantCachesForTest(): void {
  cachedTenantId = undefined;
}

export function getTenantId(): string | null {
  const apiKey = getApiKey();
  const tenantFromQuery = getTenantIdFromQueryOrStorage();
  return resolveTenantId(apiKey, tenantFromQuery);
}
