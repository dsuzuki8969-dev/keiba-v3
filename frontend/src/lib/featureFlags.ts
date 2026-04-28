/**
 * D-AI Keiba — フィーチャーフラグ管理
 *
 * 使い方:
 *   import { FEATURE_FLAGS } from '@/lib/featureFlags';
 *   if (FEATURE_FLAGS.SHOW_ODDS_ON_RACE_CARD) { ... }
 *
 * マスターレビュー後にフラグを true にして本実装へ移行すること。
 * 本実装確定後はフラグ削除 + サンプルファイルを削除する。
 */

export const FEATURE_FLAGS = {
  /**
   * T-034 サンプル: レースカードにオッズ（人気）を併記表示
   * - PC: 「馬名  勝31.6%  3.2倍 / 1番人気」
   * - モバイル: 「馬名  勝31.6% ① 3.2倍」（①②③ の白丸数字）
   * - 本命◎で人気 5 以上（穴）の場合は金色ハイライト
   * - 最終オッズ未取得時はオッズ部分のみ非表示（"—" 等フォールバック禁止）
   *
   * マスターレビュー後に true 化 → 既存カードへ正式統合
   */
  SHOW_ODDS_ON_RACE_CARD: true,  // T-034 サンプルレビュー用 一時 ON（マスター承認後に戻す or 統合）

  /**
   * Plan-γ Phase 5: 馬カード「絶対指数 / 相対指数」切替トグル
   * - 能力軸のみ切替（他の7軸は影響なし）
   * - LocalStorage "dai-keiba/ability-display-mode" でグローバル設定保存
   * - race_relative_dev が未算出（pred.json 未反映）時は「計算中」表示
   *
   * 本実装（Plan-γ はマスター承認済み正式機能）。USE_HYBRID_SCORING フラグは backend 側で管理。
   */
  USE_RELATIVE_DEV_TOGGLE: true,
} as const;
