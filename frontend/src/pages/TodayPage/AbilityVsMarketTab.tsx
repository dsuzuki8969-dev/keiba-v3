import { memo, useMemo } from "react";
import { WAKU_BG, markCls } from "@/lib/constants";
import { MARK_SYMBOL } from "@/lib/keibaUtils";
import { displayMark } from "@/lib/markDisplay";
import {
  PremiumCard,
  PremiumCardHeader,
  PremiumCardTitle,
  PremiumCardAccent,
} from "@/components/ui/premium/PremiumCard";
import type { HorseData } from "./RaceDetailView";

// ---- 定数 ----
// 乖離マーク判定閾値（実力順位 - 人気順位）
// +2以上: 妙味（実力上位・人気薄）/ -2以下: 危険（実力下位・人気上位）
const DIVERGE_THRESH_GOOD = 2;
const DIVERGE_THRESH_DANGER = -2;

// ハイライトカード最大表示数
const HIGHLIGHT_MAX = 3;

// ---- 型 ----
interface RankedHorse {
  horse: HorseData;
  abilityRank: number;
  popularityRank: number;
  /** 実力順位 - 人気順位（正=妙味、負=危険） */
  divergence: number;
  compositeNorm: number; // 0〜1 正規化済み（バー用）
}

// ---- ユーティリティ ----
/** 印シンボル(文字列)を取得 */
function getMarkSymbol(mark: string | undefined): string {
  if (!mark) return "";
  return displayMark(MARK_SYMBOL[mark] || mark);
}

/** 乖離スコアから表示ラベルを返す */
function divergenceLabel(d: number): { label: string; cls: string } {
  if (d >= DIVERGE_THRESH_GOOD) {
    return { label: "妙味↑", cls: "text-emerald-600 font-bold" };
  }
  if (d <= DIVERGE_THRESH_DANGER) {
    return { label: "危険↓", cls: "text-red-500 font-bold" };
  }
  return { label: "一致", cls: "text-muted-foreground" };
}

// ---- メインコンポーネント ----
interface Props {
  horses: HorseData[];
}

export const AbilityVsMarketTab = memo(function AbilityVsMarketTab({ horses }: Props) {
  // 出走取消馬のみ除外（is_tokusen_kiken=危険視馬は含める・このタブで実力序列と危険ハイライトを表示するため）
  const active = useMemo(
    () => horses.filter((h) => !h.is_scratched),
    [horses],
  );

  // 実力順位・人気順位・正規化値を計算
  const ranked: RankedHorse[] = useMemo(() => {
    if (active.length === 0) return [];

    // composite 降順で実力順位付け
    const sortedByAbility = [...active].sort(
      (a, b) => (b.composite ?? 0) - (a.composite ?? 0),
    );

    // popularity 昇順（人気1位が最高）で人気順位付け
    const sortedByPop = [...active].sort(
      (a, b) => (a.popularity ?? 99) - (b.popularity ?? 99),
    );

    const abilityRankMap = new Map<number, number>();
    sortedByAbility.forEach((h, i) => abilityRankMap.set(h.horse_no, i + 1));

    const popularityRankMap = new Map<number, number>();
    sortedByPop.forEach((h, i) => popularityRankMap.set(h.horse_no, i + 1));

    // composite 正規化
    const compositeValues = active.map((h) => h.composite ?? 0);
    const minC = Math.min(...compositeValues);
    const maxC = Math.max(...compositeValues);
    const range = maxC - minC || 1;

    return sortedByAbility.map((horse) => {
      const ar = abilityRankMap.get(horse.horse_no) ?? 99;
      const pr = popularityRankMap.get(horse.horse_no) ?? 99;
      return {
        horse,
        abilityRank: ar,
        popularityRank: pr,
        // divergence = 人気順位 - 実力順位
        // > 0: 実力順位(ar)小(上位)・人気順位(pr)大(薄い) → 妙味
        // < 0: 実力順位(ar)大(下位)・人気順位(pr)小(上位) → 危険
        divergence: pr - ar,
        compositeNorm: ((horse.composite ?? 0) - minC) / range,
      };
    });
  }, [active]);

  // 妙味ハイライト(divergence が正で大きい順)
  const goodHighlights = useMemo(
    () =>
      [...ranked]
        .filter((r) => r.divergence >= DIVERGE_THRESH_GOOD)
        .sort((a, b) => b.divergence - a.divergence)
        .slice(0, HIGHLIGHT_MAX),
    [ranked],
  );

  // 危険ハイライト(divergence が負で小さい順)
  const dangerHighlights = useMemo(
    () =>
      [...ranked]
        .filter((r) => r.divergence <= DIVERGE_THRESH_DANGER)
        .sort((a, b) => a.divergence - b.divergence)
        .slice(0, HIGHLIGHT_MAX),
    [ranked],
  );

  if (ranked.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-6 text-center">
        データがありません。
      </p>
    );
  }

  return (
    <div className="space-y-4">
      {/* ── 対比ハイライト ── */}
      {(goodHighlights.length > 0 || dangerHighlights.length > 0) && (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          {/* 妙味カード */}
          {goodHighlights.length > 0 && (
            <PremiumCard variant="default" padding="sm">
              <PremiumCardHeader>
                <div className="flex flex-col gap-0.5">
                  <PremiumCardAccent className="text-emerald-600">
                    過小評価(妙味)
                  </PremiumCardAccent>
                  <PremiumCardTitle className="text-base">
                    実力上位・人気薄
                  </PremiumCardTitle>
                </div>
              </PremiumCardHeader>
              <ul className="space-y-2">
                {goodHighlights.map((r) => {
                  const sym = getMarkSymbol(r.horse.mark);
                  const mCls = sym ? markCls(sym) : "";
                  return (
                    <li
                      key={r.horse.horse_no}
                      className="flex items-center gap-2 text-sm"
                    >
                      <span
                        className={`inline-flex w-6 h-6 items-center justify-center rounded-sm text-[11px] font-bold flex-shrink-0 ${WAKU_BG[r.horse.gate_no as number] || "bg-gray-200"}`}
                      >
                        {r.horse.horse_no}
                      </span>
                      {sym && (
                        <span className={`text-base ${mCls} flex-shrink-0`}>
                          {sym}
                        </span>
                      )}
                      <span className="font-bold truncate">{r.horse.horse_name}</span>
                      <span className="ml-auto text-xs text-muted-foreground whitespace-nowrap flex-shrink-0">
                        実力{r.abilityRank}位 / {r.horse.popularity ?? "?"}番人気
                      </span>
                      <span className="text-xs text-emerald-600 font-bold flex-shrink-0">
                        +{r.divergence}
                      </span>
                    </li>
                  );
                })}
              </ul>
              {/* divergence_signal がある馬を補足表示（goodHighlights を再利用） */}
              {goodHighlights
                .filter((r) => r.horse.divergence_signal)
                .map((r) => (
                  <p
                    key={`sig-${r.horse.horse_no}`}
                    className="mt-1.5 text-xs text-muted-foreground"
                  >
                    <span className="font-bold text-foreground">
                      {r.horse.horse_name}
                    </span>
                    : {r.horse.divergence_signal}
                  </p>
                ))}
            </PremiumCard>
          )}

          {/* 危険カード */}
          {dangerHighlights.length > 0 && (
            <PremiumCard variant="default" padding="sm">
              <PremiumCardHeader>
                <div className="flex flex-col gap-0.5">
                  <PremiumCardAccent className="text-red-500">
                    過大評価(危険)
                  </PremiumCardAccent>
                  <PremiumCardTitle className="text-base">
                    実力下位・人気上位
                  </PremiumCardTitle>
                </div>
              </PremiumCardHeader>
              <ul className="space-y-2">
                {dangerHighlights.map((r) => {
                  const sym = getMarkSymbol(r.horse.mark);
                  const mCls = sym ? markCls(sym) : "";
                  return (
                    <li
                      key={r.horse.horse_no}
                      className="flex items-center gap-2 text-sm"
                    >
                      <span
                        className={`inline-flex w-6 h-6 items-center justify-center rounded-sm text-[11px] font-bold flex-shrink-0 ${WAKU_BG[r.horse.gate_no as number] || "bg-gray-200"}`}
                      >
                        {r.horse.horse_no}
                      </span>
                      {sym && (
                        <span className={`text-base ${mCls} flex-shrink-0`}>
                          {sym}
                        </span>
                      )}
                      <span className="font-bold truncate">{r.horse.horse_name}</span>
                      <span className="ml-auto text-xs text-muted-foreground whitespace-nowrap flex-shrink-0">
                        実力{r.abilityRank}位 / {r.horse.popularity ?? "?"}番人気
                      </span>
                      <span className="text-xs text-red-500 font-bold flex-shrink-0">
                        {r.divergence}
                      </span>
                    </li>
                  );
                })}
              </ul>
            </PremiumCard>
          )}
        </div>
      )}

      {/* 乖離なし時のメッセージ */}
      {goodHighlights.length === 0 && dangerHighlights.length === 0 && (
        <PremiumCard variant="soft" padding="sm">
          <p className="text-sm text-muted-foreground text-center">
            実力と市場人気が拮抗しています。乖離馬なし。
          </p>
        </PremiumCard>
      )}

      {/* ── 実力序列リスト ── */}
      <PremiumCard variant="default" padding="sm">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>実力序列</PremiumCardAccent>
            <PremiumCardTitle>composite 降順（市場人気との対比）</PremiumCardTitle>
          </div>
        </PremiumCardHeader>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-xs text-muted-foreground">
                <th className="py-1.5 px-1 text-center w-6">実力</th>
                <th className="py-1.5 px-1 text-center w-6">番</th>
                <th className="py-1.5 px-0.5 text-center w-5">印</th>
                <th className="py-1.5 px-1 text-left">馬名</th>
                <th className="py-1.5 px-1 text-left min-w-[80px]">実力バー</th>
                <th className="py-1.5 px-1 text-right whitespace-nowrap">指数</th>
                <th className="py-1.5 px-1 text-center whitespace-nowrap">人気</th>
                <th className="py-1.5 px-1 text-center whitespace-nowrap">乖離</th>
              </tr>
            </thead>
            <tbody>
              {ranked.map((r) => {
                const sym = getMarkSymbol(r.horse.mark);
                // markCls(sym) で ◉=赤/◎=緑(emerald)/○=青 など他タブと統一
                const mCls = sym ? markCls(sym) : "";
                const { label: divLabel, cls: divCls } = divergenceLabel(
                  r.divergence,
                );
                const barPct = Math.round(r.compositeNorm * 100);
                const composite = r.horse.composite ?? 0;

                return (
                  <tr
                    key={r.horse.horse_no}
                    className="border-b border-border/50 hover:bg-brand-gold/5 transition-colors"
                  >
                    {/* 実力順位 */}
                    <td className="py-1.5 px-1 text-center">
                      <span
                        className={`text-xs font-bold ${
                          r.abilityRank === 1
                            ? "text-emerald-600"
                            : r.abilityRank === 2
                            ? "text-blue-600"
                            : r.abilityRank === 3
                            ? "text-red-600"
                            : "text-muted-foreground"
                        }`}
                      >
                        {r.abilityRank}
                      </span>
                    </td>

                    {/* 馬番（枠色） */}
                    <td className="py-1.5 px-1 text-center">
                      <span
                        className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold ${WAKU_BG[r.horse.gate_no as number] || "bg-gray-200"}`}
                      >
                        {r.horse.horse_no}
                      </span>
                    </td>

                    {/* 印 */}
                    <td className="py-1.5 px-0.5 text-center">
                      {sym && (
                        <span className={`text-base ${mCls}`}>{sym}</span>
                      )}
                    </td>

                    {/* 馬名 */}
                    <td className="py-1.5 px-1 font-bold text-xs whitespace-nowrap">
                      {r.horse.horse_name}
                    </td>

                    {/* 実力バー */}
                    <td className="py-1.5 px-1">
                      <div className="flex items-center gap-1">
                        <div className="flex-1 bg-muted rounded-full h-2 min-w-[60px]">
                          <div
                            className="h-2 rounded-full bg-gradient-to-r from-brand-navy to-brand-navy-light transition-all"
                            style={{ width: `${Math.max(barPct, 4)}%` }}
                          />
                        </div>
                      </div>
                    </td>

                    {/* composite値 */}
                    <td className="py-1.5 px-1 text-right tabular-nums text-xs font-semibold whitespace-nowrap">
                      {composite.toFixed(1)}
                    </td>

                    {/* 市場人気 */}
                    <td className="py-1.5 px-1 text-center">
                      <span className="text-xs text-muted-foreground">
                        {r.horse.popularity != null && r.horse.popularity > 0
                          ? `${r.horse.popularity}番人気`
                          : "—"}
                      </span>
                    </td>

                    {/* 乖離マーク */}
                    <td className="py-1.5 px-1 text-center">
                      <span className={`text-xs whitespace-nowrap ${divCls}`}>
                        {divLabel}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* 凡例 */}
        <div className="mt-2 pt-2 border-t border-border/40 flex flex-wrap gap-3 text-xs text-muted-foreground">
          <span>
            <span className="text-emerald-600 font-bold">妙味↑</span>
            {" "}= 実力上位・人気薄（差{DIVERGE_THRESH_GOOD}以上）
          </span>
          <span>
            <span className="text-red-500 font-bold">危険↓</span>
            {" "}= 実力下位・人気上位（差{Math.abs(DIVERGE_THRESH_DANGER)}以上）
          </span>
          <span className="text-muted-foreground">一致 = その他</span>
          <span className="w-full text-muted-foreground/70 italic">
            ※印は実力評価の序列です（馬券推奨ではありません）
          </span>
        </div>
      </PremiumCard>
    </div>
  );
});
AbilityVsMarketTab.displayName = "AbilityVsMarketTab";
