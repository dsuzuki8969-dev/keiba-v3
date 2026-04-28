import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

export default defineConfig({
  base: "/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    // v6.1.3: manualChunks を外す（1-byte react-vendor が本番で副作用起こした疑いあり）
    // バンドルサイズ最適化は Vite デフォルト動作に委ねる（code-splitting は自動）。
    chunkSizeWarningLimit: 700,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:5051",
        changeOrigin: true,
      },
      "/output": {
        target: "http://localhost:5051",
        changeOrigin: true,
      },
    },
  },
});
