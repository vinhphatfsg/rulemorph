import { afterEach, describe, expect, it, vi } from "vitest";
import {
  __resetAuthCachesForTest,
  getApiKey,
  getInternalKey,
} from "../App";

function setUrl(url: string) {
  installLocalStorageMock();
  window.history.replaceState(null, "", url);
  __resetAuthCachesForTest();
}

afterEach(() => {
  __resetAuthCachesForTest();
  vi.restoreAllMocks();
});

function installLocalStorageMock() {
  const store = new Map<string, string>();
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

describe("auth secret URL params", () => {
  it("uses query api_key for the current page without persisting it", () => {
    setUrl("/?api_key=query-secret");

    expect(getApiKey()).toBe("query-secret");
    expect(window.location.search).toBe("");
    expect(window.localStorage.setItem).not.toHaveBeenCalledWith(
      "rulemorph_api_key",
      "query-secret",
    );
  });

  it("uses fragment internal_key without persisting it", () => {
    setUrl("/#internal_key=fragment-secret");

    expect(getInternalKey()).toBe("fragment-secret");
    expect(window.location.hash).toBe("");
    expect(window.localStorage.setItem).not.toHaveBeenCalledWith(
      "rulemorph_internal_key",
      "fragment-secret",
    );
  });

  it("preserves regular hash fragments when removing query secrets", () => {
    setUrl("/?api_key=query-secret#section");

    expect(getApiKey()).toBe("query-secret");
    expect(window.location.search).toBe("");
    expect(window.location.hash).toBe("#section");
  });

  it("preserves hash routes when removing query secrets", () => {
    setUrl("/?api_key=query-secret#/route?x=1");

    expect(getApiKey()).toBe("query-secret");
    expect(window.location.search).toBe("");
    expect(window.location.hash).toBe("#/route?x=1");
  });

  it("uses and removes auth params from hash route queries", () => {
    setUrl("/#/route?internal_key=fragment-secret&x=1");

    expect(getInternalKey()).toBe("fragment-secret");
    expect(window.location.search).toBe("");
    expect(window.location.hash).toBe("#/route?x=1");
  });

  it("captures new auth params after the URL changes in the same tab", () => {
    setUrl("/?api_key=query-secret");
    expect(getApiKey()).toBe("query-secret");

    window.history.replaceState(null, "", "/#/route?internal_key=fragment-secret&x=1");

    expect(getInternalKey()).toBe("fragment-secret");
    expect(window.location.hash).toBe("#/route?x=1");
  });

  it("captures new hash auth params after a cached miss", () => {
    setUrl("/#plain-section");
    expect(getInternalKey()).toBeNull();

    window.history.replaceState(null, "", "/#/route?internal_key=fragment-secret&x=1");

    expect(getInternalKey()).toBe("fragment-secret");
    expect(window.location.hash).toBe("#/route?x=1");
  });
});
