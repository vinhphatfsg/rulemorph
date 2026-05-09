import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true
  },
  server: {
    port: 5173
  },
  test: {
    environment: "jsdom",
    exclude: ["node_modules/**", "dist/**", "e2e/**"]
  }
});
