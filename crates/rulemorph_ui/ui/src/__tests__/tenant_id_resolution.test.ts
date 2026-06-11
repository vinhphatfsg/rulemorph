import { afterEach, describe, expect, it, vi } from "vitest";
import {
  __getTenantIdFromQueryOrStorageForTest,
  __resetAuthCachesForTest,
  resolveTenantId,
} from "../app/App";

function installLocalStorageMock(initial: Record<string, string> = {}) {
  const store = new Map<string, string>(Object.entries(initial));
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: {
      getItem: vi.fn((key: string) => store.get(key) ?? null),
      setItem: vi.fn((key: string, value: string) => {
        store.set(key, value);
      }),
      removeItem: vi.fn((key: string) => {
        store.delete(key);
      }),
    },
  });
}

afterEach(() => {
  __resetAuthCachesForTest();
  vi.restoreAllMocks();
});

describe("resolveTenantId", () => {
  it("prefers tenant from API key over stored tenant_id", () => {
    expect(resolveTenantId("rmk_tenant-b.secret", "tenant-a")).toBe("tenant-b");
  });

  it("falls back to stored tenant_id when API key does not encode tenant", () => {
    expect(resolveTenantId("plain-key", "tenant-a")).toBe("tenant-a");
  });

  it("does not guess a tenant when api key exists without tenant and no saved tenant", () => {
    expect(resolveTenantId("plain-key", null)).toBeNull();
  });

  it("returns null when neither api key nor tenant_id is available", () => {
    expect(resolveTenantId(null, null)).toBeNull();
  });
});

describe("tenant id query/storage resolution", () => {
  it("captures a new tenant_id after the URL changes in the same tab", () => {
    installLocalStorageMock({ rulemorph_tenant_id: "tenant-a" });
    window.history.replaceState(null, "", "/");

    expect(__getTenantIdFromQueryOrStorageForTest()).toBe("tenant-a");

    window.history.replaceState(null, "", "/?tenant_id=tenant-b");

    expect(__getTenantIdFromQueryOrStorageForTest()).toBe("tenant-b");
    expect(window.location.search).toBe("");
    expect(window.localStorage.setItem).toHaveBeenCalledWith(
      "rulemorph_tenant_id",
      "tenant-b",
    );
  });
});
