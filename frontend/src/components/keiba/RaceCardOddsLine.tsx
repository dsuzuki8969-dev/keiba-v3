/**
 * RaceCardOddsLine.tsx — T-034 本実装（マスター承認済み）
 *
 * レースカードの本命馬行に「オッズ（人気）」を併記するコンポーネント。
 * サンプル隔離（RaceCardOddsLine.sample.tsx）からの正式昇格。
 * featureFlags.ts の SHOW_ODDS_ON_RACE_CARD は削除済み。
 *
 * 表示仕様:
 *   PC:      「ニシノブライアント   勝31.6%   3.2倍 / 1番人気」
 *   モバイル: 「ニシノブライアント   勝31.6% ① 3.2倍」（人気は ①②③ 白丸数字）
 *
 * 穴ハイライト条件:
 *   本命◎/◉ かつ 人気 5 以上（= 市場で低評価） → text-yellow-400 font-bold
 *
 * オッズ未取得時:
 *   odds が null / 0 / undefined の場合はオッズ部分を非表示（フォールバック禁止）
 */

// 人気数字 → 白丸数字に変換（1〜20対応）
const CIRCLE_NUMBERS: Record<number, string> = {
  1: "①", 2: "②", 3: "③", 4: "④", 5: "⑤",
  6: "⑥", 7: "⑦", 8: "⑧", 9: "⑨", 10: "⑩",
  11: "⑪", 12: "⑫", 13: "⑬", 14: "⑭", 15: "⑮",
  16: "⑯", 17: "⑰", 18: "⑱", 19: "⑲", 20: "⑳",
};

/** 人気番号 → 白丸数字（範囲外は通常数字を返す） */
function toCircleNumber(pop: number): string {
  return CIRCLE_NUMBERS[pop] ?? String(pop);
}

export interface RaceCardOddsLineProps {
  /** 馬名 */
  horseName: string;
  /** 本命印（◎ / ◉ 等）— 穴判定に使用 */
  mark?: string;
  /** 勝率（0〜100, %表示前の値。例: 31.6） */
  winPct?: number;
  /** 単勝オッズ（取得済みの場合のみ。0 / null の場合は非表示） */
  odds?: number | null;
  /** 人気順位（取得済みの場合のみ） */
  popularity?: number | null;
  /** モバイル表示モード（true で ①②③ 表記） */
  isMobile?: boolean;
  /** 追加クラス */
  className?: string;
}

/**
 * 穴ハイライト判定
 * 本命◎/◉ かつ 人気 5 以上 → 金色強調
 *
 * [HIGH-2 修正] !popularity は NaN を弾けないため、
 * null/undefined/NaN をそれぞれ明示的にガードする。
 */
function isAnaHighlight(mark?: string, popularity?: number | null): boolean {
  if (!mark) return false;
  // null / undefined / NaN をすべて弾く
  if (popularity == null || isNaN(popularity)) return false;
  if (popularity < 1) return false;
  const isHonmei = mark === "◎" || mark === "◉";
  return isHonmei && popularity >= 5;
}

/**
 * オッズが有効かチェック
 * feedback_no_easy_escape 遵守: 0 / null / undefined はすべて「未取得」扱いで非表示
 */
function hasValidOdds(odds?: number | null): odds is number {
  return odds != null && odds > 0;
}

/**
 * RaceCardOddsLine — 本命馬行（PC版）
 *
 * レイアウト:
 *   [馬名]  [勝XX.X%]  [X.X倍 / X番人気]
 */
export function RaceCardOddsLinePC({
  horseName,
  mark,
  winPct,
  odds,
  popularity,
  className = "",
}: RaceCardOddsLineProps) {
  const showOdds = hasValidOdds(odds);
  const anaClass = isAnaHighlight(mark, popularity)
    // [LOW-1 修正] ライトモードで text-yellow-400 はコントラスト比 1.6:1 (WCAG AA 未達)。
    // dark: では yellow-400 を維持し、ライトモードでは amber-600 を使用する。
    ? "dark:text-yellow-400 text-amber-600 font-bold"
    : "text-muted-foreground";

  return (
    <div
      className={`flex items-center gap-2 flex-wrap text-sm ${className}`}
      data-testid="race-card-odds-line-pc"
    >
      {/* 馬名 */}
      <span className="font-bold text-foreground truncate max-w-[12rem]">
        {horseName}
      </span>

      {/* 勝率
          [MEDIUM-1 修正] winPct > 0 の判定だと予測値 0% の馬が非表示になる。
          undefined / null / NaN のみを除外し、0 は正規値として表示する。 */}
      {winPct !== undefined && winPct !== null && !isNaN(winPct) && (
        <span className="tabular-nums font-semibold text-foreground whitespace-nowrap">
          勝{winPct.toFixed(1)}%
        </span>
      )}

      {/* オッズ / 人気（取得済みの場合のみ表示） */}
      {showOdds && (
        <span className={`tabular-nums whitespace-nowrap ${anaClass}`}>
          {odds.toFixed(1)}倍
          {popularity != null && popularity > 0 && (
            <span className="ml-1 text-xs">/ {popularity}番人気</span>
          )}
        </span>
      )}
    </div>
  );
}

/**
 * RaceCardOddsLine — 本命馬行（モバイル版）
 *
 * レイアウト:
 *   [馬名]  [勝XX.X%]  [① X.X倍]
 *   人気は ①②③ の白丸数字で省スペース表示
 */
export function RaceCardOddsLineMobile({
  horseName,
  mark,
  winPct,
  odds,
  popularity,
  className = "",
}: RaceCardOddsLineProps) {
  const showOdds = hasValidOdds(odds);
  const anaClass = isAnaHighlight(mark, popularity)
    // [LOW-1 修正] ライトモードで text-yellow-400 はコントラスト比 1.6:1 (WCAG AA 未達)。
    // dark: では yellow-400 を維持し、ライトモードでは amber-600 を使用する。
    ? "dark:text-yellow-400 text-amber-600 font-bold"
    : "text-muted-foreground";

  return (
    <div
      className={`flex items-center gap-1.5 flex-wrap text-sm ${className}`}
      data-testid="race-card-odds-line-mobile"
    >
      {/* 馬名 */}
      <span className="font-bold text-foreground truncate max-w-[9rem]">
        {horseName}
      </span>

      {/* 勝率
          [MEDIUM-1 修正] winPct > 0 の判定だと予測値 0% の馬が非表示になる。
          undefined / null / NaN のみを除外し、0 は正規値として表示する。 */}
      {winPct !== undefined && winPct !== null && !isNaN(winPct) && (
        <span className="tabular-nums font-semibold text-foreground whitespace-nowrap">
          勝{winPct.toFixed(1)}%
        </span>
      )}

      {/* 人気（白丸数字） + オッズ（取得済みの場合のみ表示） */}
      {showOdds && (
        <span className={`tabular-nums whitespace-nowrap ${anaClass}`}>
          {popularity != null && popularity > 0 && (
            <span className="mr-0.5">{toCircleNumber(popularity)}</span>
          )}
          {odds.toFixed(1)}倍
        </span>
      )}
    </div>
  );
}

/**
 * RaceCardOddsLine — PC / モバイル自動切替版（デフォルトエクスポート）
 *
 * isMobile prop でレイアウトを切り替える。
 * featureFlags.ts の SHOW_ODDS_ON_RACE_CARD フラグ ON 時に使用。
 */
export default function RaceCardOddsLine(props: RaceCardOddsLineProps) {
  if (props.isMobile) {
    return <RaceCardOddsLineMobile {...props} />;
  }
  return <RaceCardOddsLinePC {...props} />;
}
