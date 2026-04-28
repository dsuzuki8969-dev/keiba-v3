/**
 * D-Aikeiba 競馬ドメインの共通ユーティリティ
 * --------------------------------------------------------------
 * Fast Refresh (HMR) の観点から、コンポーネントと混在させたくない
 * 非コンポーネント export（純粋関数・定数）はここに集約する。
 */

/**
 * 印キー → Unicode 記号マップ。
 * バックエンド API (`mark` フィールド) は文字列キーで返ってくるため、
 * 表示用シンボルへの変換が必要。
 */
export const MARK_SYMBOL: Record<string, string> = {
  tekipan:   "◉",
  honmei:    "◎",
  taikou:    "○",
  tannuke:   "▲",
  rendashi:  "△",
  rendashi2: "★",
  oana:      "☆",
  kiken:     "×",
};

/**
 * 勝率ランクマップを計算。
 * 入力: race_no + honmei_win_pct の配列。
 * 出力: race_no → ランク (1 が最高勝率)。
 * 勝率 0 またはnull のレースはランク対象外。
 */
export function computeWinPctRanks(
  races: { race_no: number; honmei_win_pct?: number }[],
): Map<number, number> {
  const ranked = races
    .filter((r) => r.honmei_win_pct != null && r.honmei_win_pct > 0)
    .sort((a, b) => (b.honmei_win_pct ?? 0) - (a.honmei_win_pct ?? 0));
  const map = new Map<number, number>();
  ranked.forEach((r, i) => map.set(r.race_no, i + 1));
  return map;
}

/**
 * PaceFlowChart 用のデータエントリ型。
 */
export interface PaceEntry {
  label: string;
  逃げ: number;
  先行: number;
  差し: number;
  追込: number;
}

/**
 * 脚質別カウント（`{逃げ: 2, 先行: 5, ...}`）から PaceEntry を構築。
 */
export function buildPaceEntry(
  label: string,
  counts: Partial<Record<"逃げ" | "先行" | "差し" | "追込", number>>,
): PaceEntry {
  return {
    label,
    逃げ: counts["逃げ"] ?? 0,
    先行: counts["先行"] ?? 0,
    差し: counts["差し"] ?? 0,
    追込: counts["追込"] ?? 0,
  };
}

/**
 * 印シンボル文字列 → 印キー（逆引き）
 */
export function symbolToMarkKey(sym: string): string {
  switch (sym) {
    case "◉": return "tekipan";
    case "◎": return "honmei";
    case "○": return "taikou";
    case "▲": return "tannuke";
    case "△": return "rendashi";
    case "★": return "rendashi2";
    case "☆": return "oana";
    case "×": return "kiken";
    default:  return sym;
  }
}
