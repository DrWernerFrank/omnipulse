import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// dev proxy: WS + REST to the local backend
export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1", // IPv4 loopback: matches the backend and dodges proxy/IPv6 quirks
    port: 5173,
    proxy: {
      "/api": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/stream": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/ws": { target: "ws://127.0.0.1:8000", ws: true },
    },
  },
});
