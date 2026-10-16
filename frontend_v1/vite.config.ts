import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const api = process.env.OMNI_API ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: "127.0.0.1",
    port: 5174,  // 5173 is the v2 terminal — both UIs can run side by side
    strictPort: false,
    proxy: {
      "/v1/stream": { target: api, ws: true, changeOrigin: true },
      "/v1": { target: api, changeOrigin: true },
      "/mcp": { target: api, changeOrigin: true },
    },
  },
  build: { target: "es2022", chunkSizeWarningLimit: 800 },
});
