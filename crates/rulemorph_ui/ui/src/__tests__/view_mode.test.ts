import { describe, expect, it } from "vitest";
import { shouldResetInitialCenter } from "../view_mode";

describe("shouldResetInitialCenter", () => {
  it("resets when returning from api to trace", () => {
    expect(shouldResetInitialCenter("api", "trace")).toBe(true);
  });

  it("does not reset for other transitions", () => {
    expect(shouldResetInitialCenter("trace", "api")).toBe(false);
    expect(shouldResetInitialCenter("trace", "trace")).toBe(false);
    expect(shouldResetInitialCenter("api", "api")).toBe(false);
  });
});
