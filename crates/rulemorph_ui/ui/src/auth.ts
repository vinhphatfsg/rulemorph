const API_KEY_STORAGE = "rulemorph_api_key";
const INTERNAL_KEY_STORAGE = "rulemorph_internal_key";

let cachedApiKey: string | null | undefined;
let cachedInternalKey: string | null | undefined;
let pendingApiKeyFromUrl: string | null | undefined;
let pendingInternalKeyFromUrl: string | null | undefined;
let capturedAuthLocation: string | undefined;

export function captureAuthSecretsFromLocation(): void {
  if (typeof window === "undefined") return;
  const currentLocation = window.location.href;
  if (capturedAuthLocation === currentLocation) {
    return;
  }
  capturedAuthLocation = currentLocation;

  const apiKey = getSecretParam(["api_key"]);
  const internalKey = getSecretParam(["internal_key", "internal_api_key"]);
  if (apiKey) {
    pendingApiKeyFromUrl = apiKey;
    cachedApiKey = apiKey;
  } else if (pendingApiKeyFromUrl === undefined) {
    pendingApiKeyFromUrl = null;
  }
  if (internalKey) {
    pendingInternalKeyFromUrl = internalKey;
    cachedInternalKey = internalKey;
  } else if (pendingInternalKeyFromUrl === undefined) {
    pendingInternalKeyFromUrl = null;
  }

  if (apiKey || internalKey) {
    removeSecretParams(["api_key", "internal_key", "internal_api_key"]);
  }
}

function getSecretParam(names: string[]): string | null {
  if (typeof window === "undefined") return null;
  const sources = [
    ...secretParamSourcesFromHash(window.location.hash),
    window.location.search
  ];
  for (const source of sources) {
    const params = new URLSearchParams(source.startsWith("?") ? source : `?${source}`);
    for (const name of names) {
      const trimmed = params.get(name)?.trim();
      if (trimmed) return trimmed;
    }
  }
  return null;
}

function secretParamSourcesFromHash(hash: string): string[] {
  if (!hash) return [];
  const raw = hash.replace(/^#/, "");
  if (!raw) return [];
  if (raw.startsWith("/")) {
    const queryIndex = raw.indexOf("?");
    return queryIndex === -1 ? [] : [raw.slice(queryIndex + 1)];
  }
  if (raw.startsWith("?")) {
    return [raw.slice(1)];
  }
  return raw.includes("=") ? [raw] : [];
}

function removeSecretParams(names: string[]): void {
  if (typeof window === "undefined") return;
  try {
    const url = new URL(window.location.href);
    let changed = false;
    for (const name of names) {
      if (url.searchParams.has(name)) {
        url.searchParams.delete(name);
        changed = true;
      }
    }
    const hashResult = removeSecretParamsFromHash(url.hash, names);
    if (hashResult.changed) {
      url.hash = hashResult.hash;
      changed = true;
    }
    if (changed) {
      window.history.replaceState(null, "", url.toString());
    }
  } catch {
    // ignore history failures
  }
}

function removeSecretParamsFromHash(hash: string, names: string[]): { hash: string; changed: boolean } {
  if (!hash) return { hash, changed: false };
  const raw = hash.replace(/^#/, "");
  if (!raw) return { hash, changed: false };

  if (raw.startsWith("/")) {
    const queryIndex = raw.indexOf("?");
    if (queryIndex === -1) return { hash, changed: false };
    const route = raw.slice(0, queryIndex);
    const query = raw.slice(queryIndex + 1);
    const params = new URLSearchParams(query);
    if (!deleteNamedParams(params, names)) return { hash, changed: false };
    const nextQuery = params.toString();
    return { hash: nextQuery ? `#${route}?${nextQuery}` : `#${route}`, changed: true };
  }

  const source = raw.startsWith("?") ? raw.slice(1) : raw;
  if (!source.includes("=")) return { hash, changed: false };
  const params = new URLSearchParams(source);
  if (!deleteNamedParams(params, names)) return { hash, changed: false };
  const next = params.toString();
  if (raw.startsWith("?")) {
    return { hash: next ? `#?${next}` : "", changed: true };
  }
  return { hash: next ? `#${next}` : "", changed: true };
}

function deleteNamedParams(params: URLSearchParams, names: string[]): boolean {
  let changed = false;
  for (const name of names) {
    if (params.has(name)) {
      params.delete(name);
      changed = true;
    }
  }
  return changed;
}

export function getApiKey(): string | null {
  if (typeof window !== "undefined") {
    captureAuthSecretsFromLocation();
  }
  if (cachedApiKey !== undefined) {
    return cachedApiKey;
  }
  if (typeof window === "undefined") {
    cachedApiKey = null;
    return cachedApiKey;
  }
  const trimmed = pendingApiKeyFromUrl?.trim();
  if (trimmed) {
    cachedApiKey = trimmed;
    return trimmed;
  }
  try {
    const stored = window.localStorage.getItem(API_KEY_STORAGE);
    if (stored && stored.trim()) {
      cachedApiKey = stored.trim();
      return cachedApiKey;
    }
  } catch {
    // ignore storage failures
  }
  cachedApiKey = null;
  return cachedApiKey;
}

export function getInternalKey(): string | null {
  if (typeof window !== "undefined") {
    captureAuthSecretsFromLocation();
  }
  if (cachedInternalKey !== undefined) {
    return cachedInternalKey;
  }
  if (typeof window === "undefined") {
    cachedInternalKey = null;
    return cachedInternalKey;
  }
  const trimmed = pendingInternalKeyFromUrl?.trim();
  if (trimmed) {
    cachedInternalKey = trimmed;
    return trimmed;
  }
  try {
    const stored = window.localStorage.getItem(INTERNAL_KEY_STORAGE);
    if (stored && stored.trim()) {
      cachedInternalKey = stored.trim();
      return cachedInternalKey;
    }
  } catch {
    // ignore storage failures
  }
  cachedInternalKey = null;
  return cachedInternalKey;
}

export function __resetAuthCachesForTest(): void {
  cachedApiKey = undefined;
  cachedInternalKey = undefined;
  pendingApiKeyFromUrl = undefined;
  pendingInternalKeyFromUrl = undefined;
  capturedAuthLocation = undefined;
}
