import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// During development the dashboard proxies API calls to a local controller.
export default defineConfig({
  plugins: [react()],
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false },
  server: {
    proxy: {
      "/api": process.env.KEYSHIFT_CONTROLLER ?? "http://localhost:8080",
      "/healthz": process.env.KEYSHIFT_CONTROLLER ?? "http://localhost:8080",
    },
  },
});
