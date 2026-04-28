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
    tekipan: "#16a34a",
    honmei: "#16a34a",
    taikou: "#2563eb",
    tannuke: "#dc2626",
    rendashi: "#7c3aed",
    rendashi2: "#0f172a",
    oana: "#2563eb",
    kiken: "#dc2626",
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

export const typography = {
  fs: {
    "2xs": "0.6875rem",
    xs: "0.75rem",
    sm: "0.8125rem",
    md: "0.875rem",
    base: "1rem",
    lg: "1.125rem",
    xl: "1.25rem",
    "2xl": "1.5rem",
    "3xl": "1.875rem",
    "4xl": "2.25rem",
    "5xl": "3rem",
  },
  lh: {
    tight: 1.15,
    snug: 1.35,
    normal: 1.55,
    relaxed: 1.75,
  },
  fw: {
    regular: 400,
    medium: 500,
    semibold: 600,
    bold: 700,
    extra: 800,
  },
} as const;

export const motion = {
  dur: {
    fast: 120,
    base: 180,
    slow: 320,
  },
  ease: {
    out: "cubic-bezier(0.2, 0.8, 0.2, 1)",
    inOut: "cubic-bezier(0.4, 0.0, 0.2, 1)",
    spring: "cubic-bezier(0.34, 1.56, 0.64, 1)",
  },
} as const;

export const radii = {
  sm: "0.375rem",
  md: "0.5rem",
  lg: "0.625rem",
  xl: "0.875rem",
  "2xl": "1.125rem",
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
