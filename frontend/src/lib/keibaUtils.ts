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
  // 抑え印: 無印1-2人気の救済印 (2026-06-22)
  oshi:      "抑",
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


