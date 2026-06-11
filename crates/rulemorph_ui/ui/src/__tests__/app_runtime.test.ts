import { describe, expect, it, vi } from "vitest";
import { persistDurationUnit, resolveStoredDurationUnit } from "../app/app_runtime";

function storageWithValue(value: string | null): Storage {
  return {
    getItem: vi.fn(() => value),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
    key: vi.fn(),
    length: value === null ? 0 : 1
  };
}

describe("app runtime helpers", () => {
  it("resolves only ms from storage and falls back to microseconds", () => {
    expect(resolveStoredDurationUnit(storageWithValue("ms"))).toBe("ms");
    expect(resolveStoredDurationUnit(storageWithValue("us"))).toBe("us");
    expect(resolveStoredDurationUnit(storageWithValue("bad"))).toBe("us");
    expect(resolveStoredDurationUnit(storageWithValue(null))).toBe("us");
  });

  it("persists the selected duration unit without changing the storage key", () => {
    const storage = storageWithValue(null);

    persistDurationUnit("ms", storage);
    persistDurationUnit("us", storage);

    expect(storage.setItem).toHaveBeenNthCalledWith(1, "traceDurationUnit", "ms");
    expect(storage.setItem).toHaveBeenNthCalledWith(2, "traceDurationUnit", "us");
  });
});
