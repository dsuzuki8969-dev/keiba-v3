/**
 * devColors.ts — 偏差値 → ヒートマップ色変換ユーティリティ
 * --------------------------------------------------------------
 * feedback_dev_range.md 準拠（SS≥65 / S 60-65 / A 55-60 / B 50-55 / C 45-50 / D<45）
 * 色覚多様性対応: 背景色 + 文字色の二重符号化 + グレード文字の必ず併記
 * tokens.ts の gradeFromDev を再利用
 */

import { gradeFromDev } from "@/design/tokens";

// E を追加（2026-04-26 A案: clamp -50〜100 拡張・真の大敗グレード）
export type Grade = "SS" | "S" | "A" | "B" | "C" | "D" | "E";

/**
 * 偏差値 → グレード文字列
 * null/undefined は "D" 相当として扱う
 */
export function devToGrade(dev: number | null | undefined): Grade {
  return gradeFromDev(dev);
}

/**
 * 偏差値 → ヒートマップセル色
 * @returns bg（背景用 Tailwind クラス）/ text（文字色クラス）
 * null/undefined の場合はニュートラルグレー
 *
 * 配色方針（WCAG AA コントラスト確保）:
 *   SS: 濃赤背景（#991b1b）+ 白文字
 *   S : 赤橙背景（#c2410c）+ 白文字
 *   A : 橙背景（#b45309）+ 白文字
 *   B : 黄緑背景（#4d7c0f）+ 白文字
 *   C : 灰青背景（#475569）+ 白文字
 *   D : 青紺背景（#1e3a5f）+ 白文字
 *   E : 濃紺背景（#0f172a）+ 白文字（真の大敗・D より濃い暗青）
 *   null: 中立グレー（#e2e8f0）+ 濃グレー文字
 */
export function devToHeatColor(
  dev: number | null | undefined,
): { bg: string; text: string; border?: string } {
  if (dev == null || Number.isNaN(dev)) {
    // データなし: ニュートラルグレー
    return {
      bg: "bg-slate-100 dark:bg-slate-800",
      text: "text-slate-400 dark:text-slate-500",
    };
  }

  const grade = gradeFromDev(dev);

  switch (grade) {
    case "SS":
      // 濃赤: 能力が非常に高い（≥65）
      return {
        bg: "bg-red-800",
        text: "text-white",
        border: "border-red-900",
      };
    case "S":
      // 赤橙: 高い（60-65）
      return {
        bg: "bg-orange-700",
        text: "text-white",
        border: "border-orange-800",
      };
    case "A":
      // 橙: やや高い（55-60）
      return {
        bg: "bg-amber-600",
        text: "text-white",
        border: "border-amber-700",
      };
    case "B":
      // 黄緑: 平均的（50-55）
      return {
        bg: "bg-lime-700",
        text: "text-white",
        border: "border-lime-800",
      };
    case "C":
      // 灰青: やや低い（45-50）
      return {
        bg: "bg-slate-500",
        text: "text-white",
        border: "border-slate-600",
      };
    case "D":
      // 青紺: 低い（35-45）
      return {
        bg: "bg-blue-900",
        text: "text-white",
        border: "border-blue-950",
      };
    case "E":
    default:
      // 最濃紺: 真の大敗（<35・マイナス含む）— D より濃い暗青でヒートマップ上端を強調
      return {
        bg: "bg-slate-900",
        text: "text-blue-300",
        border: "border-slate-950",
      };
  }
}

/**
 * 偏差値グレード → 凡例用色サンプルの背景クラス（inline style 用）
 * devToHeatColor と同期させること
 */
export const GRADE_LEGEND: Array<{ grade: Grade; bgClass: string; label: string }> = [
  { grade: "SS", bgClass: "bg-red-800",     label: "SS（≥65）" },
  { grade: "S",  bgClass: "bg-orange-700",  label: "S（60-）" },
  { grade: "A",  bgClass: "bg-amber-600",   label: "A（55-）" },
  { grade: "B",  bgClass: "bg-lime-700",    label: "B（50-）" },
  { grade: "C",  bgClass: "bg-slate-500",   label: "C（45-）" },
  { grade: "D",  bgClass: "bg-blue-900",    label: "D（35-）" },
  { grade: "E",  bgClass: "bg-slate-900",   label: "E（<35）" },  // 真の大敗（A案）
];
