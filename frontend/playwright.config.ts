/**
 * Playwright 設定ファイル
 * レスポンシブスモークテスト用 (e2e/responsive-check.spec.ts)
 *
 * 前提: Flask ダッシュボードが http://127.0.0.1:5051 で稼働中
 */
import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  // テストファイルの検索パス
  testDir: "./e2e",

  // 各テストのタイムアウト (ms)
  timeout: 30_000,

  // テスト失敗時のリトライ回数
  retries: 1,

  // 並列実行数 (スクリーンショット取得のみなので制限なし)
  workers: 2,

  // レポーター
  reporter: [["list"], ["html", { open: "never", outputFolder: "playwright-report" }]],

  // スクリーンショット保存先 (e2e-screenshots/ は spec 内で指定)
  use: {
    // ベース URL (spec 内の BASE_URL で上書き可)
    baseURL: process.env.E2E_BASE_URL || "http://127.0.0.1:5051",

    // スクリーンショットはテスト失敗時のみ自動取得 (spec 内で fullPage 取得)
    screenshot: "only-on-failure",

    // ビデオ: 失敗時のみ
    video: "retain-on-failure",

    // アクション待機タイムアウト
    actionTimeout: 10_000,

    // ナビゲーションタイムアウト
    navigationTimeout: 15_000,
  },

  // プロジェクト定義: chromium のみ
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
