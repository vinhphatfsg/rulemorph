import { describe, expect, it } from "vitest";
import { resolveTenantId } from "../App";

describe("resolveTenantId", () => {
  it("prefers tenant from API key over stored tenant_id", () => {
    expect(resolveTenantId("rmk_tenant-b.secret", "tenant-a")).toBe("tenant-b");
  });

  it("falls back to stored tenant_id when API key does not encode tenant", () => {
    expect(resolveTenantId("plain-key", "tenant-a")).toBe("tenant-a");
  });

  it("uses default tenant when api key exists without tenant and no saved tenant", () => {
    expect(resolveTenantId("plain-key", null)).toBe("default");
  });

  it("returns null when neither api key nor tenant_id is available", () => {
    expect(resolveTenantId(null, null)).toBeNull();
  });
});
