/**
 * D-AI Keiba — フィーチャーフラグ管理
 *
 * 使い方:
 *   import { FEATURE_FLAGS } from '@/lib/featureFlags';
 *   if (FEATURE_FLAGS.USE_RELATIVE_DEV_TOGGLE) { ... }
 *
 * T-034 SHOW_ODDS_ON_RACE_CARD は本実装統合済みにより削除（2026-04-28）。
 */

export const FEATURE_FLAGS = {
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
