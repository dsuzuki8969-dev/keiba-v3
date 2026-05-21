// ランキングスタイル: 1位=緑太字, 2位=青太字, 3位=赤太字, 平均=黒普通, 平均以下=灰色
// v6.1.21: hex ハードコード → design-token CSS 変数参照に統一
//   1位 → --mark-tekipan (緑)   2位 → --mark-taikou (青)   3位 → --mark-tannuke (赤)
//   平均以下 → --muted-foreground
export function rankStyle(val: number, ranked: number[], avg: number): { color: string; fontWeight?: string } {
  if (ranked[0] === val) return { color: "var(--mark-tekipan)", fontWeight: "bold" };
  if (ranked.length > 1 && ranked[1] === val) return { color: "var(--mark-taikou)", fontWeight: "bold" };
  if (ranked.length > 2 && ranked[2] === val) return { color: "var(--mark-tannuke)", fontWeight: "bold" };
  if (val < avg) return { color: "var(--muted-foreground)" };
  return { color: "" };
}

// タイム差の色分け
export function diffColor(diff: number | null | undefined): string {
  if (diff == null || isNaN(diff) || diff === 0) return "";
  if (diff < 0) return "var(--positive)";
  if (diff > 0) return "var(--negative)";
  return "";
}
