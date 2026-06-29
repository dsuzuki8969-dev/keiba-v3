/**
 * JikuConfBadge — 軸馬の信頼度バッジ（T-3）
 * ----------------------------------------------------------------
 * 本命(軸馬度1位)が他馬をどれだけ引き離しているかを「断トツ/優位/拮抗」で表す
 * 相対3段階の自信度。jiku_gap3(本命 - 3位の軸馬度差)を JRA/NAR別に閾値化する。
 *
 * 閾値はリーク無 day-of 実データ(n=1140)の場別3分位で較正(数値=本命の実複勝率):
 *   JRA  断トツ≥21:61.5% / 優位12-21:48.7% / 拮抗<12:37.7%
 *   NAR  断トツ≥17:70.8% / 優位12-17:66.8% / 拮抗<12:63.0%
 * 絶対%表示は識別力不足(place3_prob AUC0.51・S級逆転・4段階以上は非単調)で
 * 看板倒れになるため出さず、相対3段階のみ。表示専用(ML/印/買い目は非汚染)。
 */

export type JikuTier = "断トツ" | "優位" | "拮抗";

/** jiku_gap3 と JRA/NAR から軸馬の信頼度を3段階で返す */
export function jikuConfTier(gap3: number, isJra: boolean): JikuTier {
  const c1 = 12; // 拮抗/優位 境界（実測 JRA12.4 / NAR12.2 ≒ 12）
  const c2 = isJra ? 21 : 17; // 優位/断トツ 境界（実測 JRA20.9 / NAR17.2）
  if (gap3 >= c2) return "断トツ";
  if (gap3 >= c1) return "優位";
  return "拮抗";
}

// JRA10場(中央)の会場名。これ以外はNAR(地方)とみなす。
const JRA_VENUE_NAMES = new Set([
  "札幌", "函館", "福島", "新潟", "東京", "中山", "中京", "京都", "阪神", "小倉",
]);

/** 会場名から JRA(中央) かどうかを判定 */
export function isJraVenue(venue: string | undefined): boolean {
  return JRA_VENUE_NAMES.has(venue || "");
}

/** 軸馬の信頼度バッジ（断トツ=緑 / 優位=青 / 拮抗=琥珀） */
export function JikuConfBadge({ tier, className = "" }: { tier: JikuTier; className?: string }) {
  const cls =
    tier === "断トツ"
      ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"
      : tier === "優位"
        ? "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300"
        : "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300";
  return (
    <span
      className={`text-[10px] font-bold px-1.5 py-0.5 rounded whitespace-nowrap ${cls} ${className}`}
      title="軸馬の信頼度（JRA/NAR別・実複勝率で較正した相対3段階）"
    >
      {tier}
    </span>
  );
}
