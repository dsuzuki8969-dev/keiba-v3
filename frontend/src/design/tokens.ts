/**
 * D-Aikeiba Design Tokens (TypeScript mirror)
 * --------------------------------------------------------------------
 * CSS変数 `tokens.css` の型安全コピー。recharts などのライブラリから
 * CSS変数を直接読めない場面で使用する。色のハードコードはここに集約。
 *
 * 注意: tokens.css を編集したら必ずこちらも同期すること。
 */

export const colors = {
  brand: {
    gold: "#d4a853",
    goldLight: "#f0d78c",
    goldDark: "#a97f2b",
    goldSoft: "#f7eacb",
    navy: "#0b1a3a",
    navyLight: "#1e2b52",
    navyDark: "#050c1e",
  },
  semantic: {
    positive: "#10b981",
    negative: "#dc2626",
    warning: "#f59e0b",
    info: "#2563eb",
  },
  confidence: {
    SS: "#16a34a",
    S: "#2563eb",
    A: "#dc2626",
    B: "#1f2937",
    C: "#9ca3af",
    D: "#9ca3af",
  },
  mark: {
    // ◉(鉄板): 赤系で最上位強調 — Phase 2+3 で ◎(本命)の緑と明確に区別
    tekipan: "#dc2626",
    honmei: "#16a34a",
    taikou: "#2563eb",
    tannuke: "#dc2626",
    rendashi: "#7c3aed",
    rendashi2: "#0f172a",
    oana: "#2563eb",
  },
  surface: {
    turf: "#16a34a",
    dirt: "#b45309",
    obstacle: "#7c3aed",
  },
  chart: {
    c1: "#0b1a3a",
    c2: "#2563eb",
    c3: "#d4a853",
    c4: "#7c3aed",
    c5: "#dc2626",
  },
  chartDark: {
    c1: "#d4a853",
    c2: "#60a5fa",
    c3: "#f0d78c",
    c4: "#a78bfa",
    c5: "#f87171",
  },
} as const;

/** グレード（偏差値帯）判定：SS/S/A/B/C/D/E
 * E を新設（2026-04-26 A案 マスター承認）
 * clamp 範囲が -50〜100 に拡張されたため 35 未満を「真の大敗」グレード E に分類
 */
export function gradeFromDev(dev: number | null | undefined): "SS" | "S" | "A" | "B" | "C" | "D" | "E" {
  if (dev == null || Number.isNaN(dev)) return "D";
  if (dev >= 65) return "SS";
  if (dev >= 60) return "S";
  if (dev >= 55) return "A";
  if (dev >= 50) return "B";
  if (dev >= 45) return "C";
  if (dev >= 35) return "D";
  return "E";  // 35 未満（マイナス含む）= 真の大敗
}
