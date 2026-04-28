/**
 * レスポンシブ スクリーンショット スモークテスト
 *
 * 主要 5 ページを 6 つのビューポート幅で巡回し、
 * fullPage スクリーンショットを撮って目視確認用に保存する。
 *
 * 実行前提:
 *   - @playwright/test がインストール済み (npm i -D @playwright/test)
 *   - ダッシュボードが起動中 (python src/dashboard.py)
 *
 * 実行例:
 *   npx playwright test e2e/responsive-check.spec.ts
 *
 * 環境変数:
 *   E2E_BASE_URL  — 対象 URL (デフォルト: http://127.0.0.1:5051)
 */

import { test } from "@playwright/test";

/** 対象 URL ベース */
const BASE_URL = process.env.E2E_BASE_URL || "http://127.0.0.1:5051";

/** 確認対象ページ */
const PAGES = [
  { name: "home",    path: "/#/home" },
  { name: "today",   path: "/#/today" },
  { name: "results", path: "/#/results" },
  { name: "venue",   path: "/#/venue" },
  { name: "db",      path: "/#/db" },
] as const;

/**
 * 確認するビューポート幅 (px)
 * 設計書 design-system.md「狭幅レイアウト鉄則」検証フロー準拠
 */
const WIDTHS = [320, 375, 520, 768, 1024, 1440] as const;

test.describe("レスポンシブ スクリーンショット スモークテスト", () => {
  for (const page of PAGES) {
    for (const width of WIDTHS) {
      test(`${page.name} @ ${width}px`, async ({ page: pw }) => {
        // ビューポートをテスト幅に設定（高さは固定 900px）
        await pw.setViewportSize({ width, height: 900 });

        // ページ遷移
        await pw.goto(`${BASE_URL}${page.path}`);

        // ネットワーク待機（タイムアウト時は無視して続行）
        await pw.waitForLoadState("networkidle", { timeout: 5000 }).catch(() => {});

        // fullPage スクリーンショット保存
        await pw.screenshot({
          path: `e2e-screenshots/${page.name}-${width}.png`,
          fullPage: true,
        });
      });
    }
  }
});
