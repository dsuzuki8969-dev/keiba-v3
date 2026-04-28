import { useMemo, useState, useEffect } from "react";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Target as TargetIcon } from "lucide-react";
import { devGrade, evCls, gradeCls, rankCls, WAKU_BG } from "@/lib/constants";
import { ConfidenceBadge } from "@/components/keiba/ConfidenceBadge";
import type { HorseData } from "./RaceDetailView";

// 狭い画面判定フック（印行の表示制御用）
function useIsNarrow(breakpoint = 1024) {
  const [narrow, setNarrow] = useState(window.innerWidth < breakpoint);
  useEffect(() => {
    const onResize = () => setNarrow(window.innerWidth < breakpoint);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, [breakpoint]);
  return narrow;
}

// モバイルプレビューモード（PC で狭幅表示中）でも narrow 扱いにするため
// useViewMode の isMobile も併用する。
import { useViewMode } from "@/hooks/useViewMode";
function useIsNarrowOrMobile(breakpoint = 1024) {
  const isNarrow = useIsNarrow(breakpoint);
  const { isMobile } = useViewMode();
  return isNarrow || isMobile;
}

interface Props {
  horses: HorseData[];
  race: {
    llm_mark_comment?: string;
    mark_comment_rich?: string;
    [key: string]: unknown;
  };
}

interface MarkDef {
  key: string;
  sym: string;
  label: string;
  aliases: string[];
  color: string;
  bg: string;
  border: string;
}

// 色体系: ◉/◎=緑, ○/☆=青, ▲/×=赤, △=紫, ★=黒
const MARK_ORDER: MarkDef[] = [
  { key: "◉", sym: "◉", label: "鉄板", aliases: ["tekipan"], color: "text-emerald-700", bg: "bg-emerald-50 dark:bg-emerald-950/30", border: "border-l-emerald-200" },
  { key: "◎", sym: "◎", label: "本命", aliases: ["honmei"], color: "text-emerald-700", bg: "bg-emerald-50 dark:bg-emerald-950/30", border: "border-l-emerald-200" },
  { key: "○", sym: "○", label: "対抗", aliases: ["taikou"], color: "text-blue-700", bg: "bg-blue-50 dark:bg-blue-950/30", border: "border-l-blue-200" },
  { key: "▲", sym: "▲", label: "単穴", aliases: ["tannuke"], color: "text-red-700", bg: "bg-red-50 dark:bg-red-950/30", border: "border-l-red-200" },
  { key: "△", sym: "△", label: "連下", aliases: ["rendashi"], color: "text-purple-700", bg: "bg-purple-50 dark:bg-purple-950/30", border: "border-l-purple-200" },
  { key: "★", sym: "★", label: "連下2", aliases: ["rendashi2"], color: "text-foreground", bg: "bg-gray-50 dark:bg-gray-950/30", border: "border-l-gray-200" },
  { key: "☆", sym: "☆", label: "穴", aliases: ["oana"], color: "text-blue-700", bg: "bg-blue-50 dark:bg-blue-950/30", border: "border-l-blue-200" },
  { key: "×", sym: "×", label: "危険", aliases: ["kiken"], color: "text-red-700", bg: "bg-red-50/50 dark:bg-red-950/20", border: "border-l-red-200" },
];

/** 全馬の値から順位・色を計算 */
function rankInfo(vals: number[]) {
  const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
  const ranked = vals
    .map((v, i) => ({ v, i }))
    .sort((a, b) => b.v - a.v);
  const colors: Record<number, string> = {};
  const ranks: Record<number, number> = {};
  ranked.forEach((item, pos) => {
    ranks[item.i] = pos + 1;
    if (pos === 0) colors[item.i] = "text-positive";
    else if (pos === 1) colors[item.i] = "text-info";
    else if (pos === 2) colors[item.i] = "text-negative";
    else if (pos === 3) colors[item.i] = "text-purple-600";
    else if (item.v < avg) colors[item.i] = "text-muted-foreground";
    else colors[item.i] = "text-foreground";
  });
  return { colors, ranks };
}

interface RankColors {
  wpColor: string;
  p2Color: string;
  p3Color: string;
}

/** 能力・展開・適性の順位情報 */
interface IndexRanks {
  abilityRank: number;
  paceRank: number;
  courseRank: number;
}

/** 印の行を描画 */
function MarkRow({ mo, h, rankColors, indexRanks, isNarrow }: { mo: MarkDef; h: HorseData; rankColors?: RankColors; indexRanks?: IndexRanks; isNarrow?: boolean }) {
  const cg = devGrade(h.composite || 0);
  const realOdds = h.odds != null && h.odds > 0;
  const oddsStr = realOdds
    ? `${Number(h.odds).toFixed(1)}倍`
    : h.predicted_tansho_odds
      ? `${h.predicted_tansho_odds.toFixed(1)}倍*`
      : "—";
  const popStr =
    h.popularity != null
      ? `${h.popularity}人気`
      : h.predicted_rank != null
        ? `${h.predicted_rank}位*`
        : "";
  const winP = h.win_prob != null ? (h.win_prob * 100) : null;
  const place2 = h.place2_prob != null ? (h.place2_prob * 100) : null;
  const place3 = h.place3_prob != null ? (h.place3_prob * 100) : null;
  const rawOdds = h.odds ?? h.predicted_tansho_odds ?? 0;
  const ev = (h.win_prob != null && rawOdds > 0)
    ? (h.win_prob * rawOdds)
    : null;

  // --- 狭い画面: 2行レイアウト ---
  if (isNarrow) {
    return (
      <div
        key={`${mo.key}-${h.horse_no}`}
        className={`py-2 px-3 rounded-md border-l-[3px] ${mo.bg} ${mo.border}`}
      >
        {/* 1行目: 印 + 馬番 + 馬名 + グレード・偏差値 */}
        <div className="flex items-center gap-1.5 text-sm">
          <span className={`${mo.color} font-bold text-base leading-none shrink-0`}>{mo.sym}</span>
          <span className="text-[10px] text-muted-foreground shrink-0 w-5">{mo.label}</span>
          <span className={`inline-flex w-4 h-4 items-center justify-center rounded-sm text-[9px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>{h.horse_no}</span>
          <span className="font-bold truncate">{h.horse_name}</span>
          <span className={`ml-auto shrink-0 font-bold ${gradeCls(cg)}`}>
            {cg}・{h.composite?.toFixed(1) || "—"}
          </span>
        </div>
        {/* 2行目: 騎手 + オッズ + 人気 + 勝率 */}
        <div className="flex items-center gap-2 mt-0.5 text-xs pl-7 flex-wrap">
          {h.jockey && <span className="text-foreground whitespace-nowrap">{h.jockey}</span>}
          <span className={`tabular-nums font-bold whitespace-nowrap ${h.popularity != null ? (rankCls(h.popularity) || "text-foreground") : "text-muted-foreground"}`}>{oddsStr}</span>
          <span className={`whitespace-nowrap ${h.popularity != null ? (rankCls(h.popularity) || "text-muted-foreground") : "text-muted-foreground"}`}>{popStr}</span>
          {winP != null && (
            <span className="ml-auto tabular-nums">
              <span className="text-muted-foreground">勝</span> <strong className={rankColors?.wpColor || "text-foreground"}>{winP.toFixed(1)}%</strong>
            </span>
          )}
        </div>
      </div>
    );
  }

  // --- 広い画面: 従来の1行レイアウト ---
  return (
    <div
      key={`${mo.key}-${h.horse_no}`}
      className={`flex items-center gap-3 text-sm py-2 px-3 rounded-md border-l-[3px] ${mo.bg} ${mo.border}`}
    >
      {/* 印記号+ラベル */}
      <div className="flex flex-col items-center w-8 shrink-0">
        <span className={`${mo.color} font-bold text-lg leading-none`}>
          {mo.sym}
        </span>
        <span className="text-[10px] text-muted-foreground">{mo.label}</span>
      </div>
      {/* 馬番+馬名 */}
      <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>{h.horse_no}</span>
      <span className="font-bold min-w-[72px] whitespace-nowrap">{h.horse_name}</span>
      {/* 騎手 */}
      {h.jockey && <span className="text-foreground whitespace-nowrap">{h.jockey}</span>}
      {/* オッズ+人気 — v6.1.21: Japanese char-by-char 改行防止 */}
      <span className={`tabular-nums font-bold whitespace-nowrap ${h.popularity != null ? (rankCls(h.popularity) || "text-foreground") : "text-muted-foreground"}`}>{oddsStr}</span>
      <span className={`font-semibold whitespace-nowrap ${h.popularity != null ? (rankCls(h.popularity) || "text-muted-foreground") : "text-muted-foreground"}`}>{popStr}</span>
      {/* グレード・総合 */}
      <span className={`whitespace-nowrap ${gradeCls(cg)}`}>
        {cg}・{h.composite?.toFixed(1) || "—"}
      </span>
      {/* 能力・展開・適性 */}
      {indexRanks && (
        <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-xs tabular-nums">
          <span><span className="text-muted-foreground">能：</span><span className={gradeCls(devGrade(h.ability_total || 0))}>{devGrade(h.ability_total || 0)}</span><span className="text-muted-foreground ml-0.5">{indexRanks.abilityRank}位</span></span>
          <span><span className="text-muted-foreground">展：</span><span className={gradeCls(devGrade(h.pace_total || 0))}>{devGrade(h.pace_total || 0)}</span><span className="text-muted-foreground ml-0.5">{indexRanks.paceRank}位</span></span>
          <span><span className="text-muted-foreground">適：</span><span className={gradeCls(devGrade(h.course_total || 0))}>{devGrade(h.course_total || 0)}</span><span className="text-muted-foreground ml-0.5">{indexRanks.courseRank}位</span></span>
        </div>
      )}
      {/* 勝率・連対率・複勝率・期待値 */}
      <div className="ml-auto flex items-center gap-3 text-xs tabular-nums text-muted-foreground whitespace-nowrap">
        {winP != null && (
          <span className="inline-flex items-center gap-1">
            勝 <strong className={rankColors?.wpColor || "text-foreground"}>{winP.toFixed(1)}%</strong>
            <div className="w-12 h-2 bg-border rounded-full overflow-hidden">
              <div className="h-full bg-emerald-400 rounded-full transition-all" style={{ width: `${Math.min(winP * 2, 100)}%` }} />
            </div>
          </span>
        )}
        {place2 != null && (
          <span className="inline-flex items-center gap-1">
            連 <strong className={rankColors?.p2Color || "text-foreground"}>{place2.toFixed(1)}%</strong>
            <div className="w-12 h-2 bg-border rounded-full overflow-hidden">
              <div className="h-full bg-blue-400 rounded-full transition-all" style={{ width: `${Math.min(place2, 100)}%` }} />
            </div>
          </span>
        )}
        {place3 != null && (
          <span className="inline-flex items-center gap-1">
            複 <strong className={rankColors?.p3Color || "text-foreground"}>{place3.toFixed(1)}%</strong>
            <div className="w-12 h-2 bg-border rounded-full overflow-hidden">
              <div className="h-full bg-red-400 rounded-full transition-all" style={{ width: `${Math.min(place3, 100)}%` }} />
            </div>
          </span>
        )}
        {ev != null && (
          <span className={evCls(ev)}>
            EV <strong>{ev.toFixed(2)}</strong>
          </span>
        )}
      </div>
    </div>
  );
}

/** 馬の印からMarkDefを取得（印なし→無印用） */
const NO_MARK_DEF: MarkDef = {
  key: "－", sym: "－", label: "無印", aliases: [],
  color: "text-muted-foreground", bg: "bg-muted/20 dark:bg-muted/10", border: "border-l-border",
};

function getMarkDef(mark: string | undefined): MarkDef {
  if (!mark) return NO_MARK_DEF;
  const found = MARK_ORDER.find((mo) => mo.key === mark || mo.aliases.includes(mark));
  return found || NO_MARK_DEF;
}

/** 断層の区切り線 */
function GapSeparator({ gap }: { gap: number }) {
  if (gap >= 5) {
    return (
      <div className="flex items-center gap-2 py-0.5 px-2">
        <div className="flex-1 border-t-2 border-red-400 dark:border-red-500" />
        <span className="text-[11px] font-bold text-red-500 dark:text-red-400 tabular-nums whitespace-nowrap">
          ▼ {gap.toFixed(1)}pt ▼
        </span>
        <div className="flex-1 border-t-2 border-red-400 dark:border-red-500" />
      </div>
    );
  }
  return (
    <div className="flex items-center gap-2 py-0.5 px-2">
      <div className="flex-1 border-t border-dashed border-amber-400 dark:border-amber-500" />
      <span className="text-[10px] text-amber-600 dark:text-amber-400 tabular-nums whitespace-nowrap">
        {gap.toFixed(1)}pt
      </span>
      <div className="flex-1 border-t border-dashed border-amber-400 dark:border-amber-500" />
    </div>
  );
}

export function MarkSummary({ horses, race }: Props) {
  // モバイルプレビューモードでも 2 行レイアウトに切替
  const isNarrow = useIsNarrowOrMobile();

  // 全馬の三連率順位を計算
  const probRanks = useMemo(() => {
    const wp = rankInfo(horses.map((h) => h.win_prob || 0));
    const p2 = rankInfo(horses.map((h) => h.place2_prob || 0));
    const p3 = rankInfo(horses.map((h) => h.place3_prob || 0));
    // horse_no → index マッピング
    const byNo: Record<number, RankColors> = {};
    horses.forEach((h, i) => {
      byNo[h.horse_no] = {
        wpColor: wp.colors[i] || "text-foreground",
        p2Color: p2.colors[i] || "text-foreground",
        p3Color: p3.colors[i] || "text-foreground",
      };
    });
    return byNo;
  }, [horses]);

  // 能力・展開・適性の順位を計算
  const idxRanks = useMemo(() => {
    const aVals = horses.map((h) => h.ability_total || 0);
    const pVals = horses.map((h) => h.pace_total || 0);
    const cVals = horses.map((h) => h.course_total || 0);
    const aRank = rankInfo(aVals);
    const pRank = rankInfo(pVals);
    const cRank = rankInfo(cVals);
    const byNo: Record<number, IndexRanks> = {};
    horses.forEach((h, i) => {
      byNo[h.horse_no] = {
        abilityRank: aRank.ranks[i] || 0,
        paceRank: pRank.ranks[i] || 0,
        courseRank: cRank.ranks[i] || 0,
      };
    });
    return byNo;
  }, [horses]);

  const tokusenHorses = horses.filter((h) => h.is_tokusen);
  const kikenHorses = horses.filter((h) => h.is_tokusen_kiken);

  return (
    <PremiumCard variant="default" padding="md">
      <PremiumCardHeader>
        <div className="flex flex-col gap-0.5">
          <PremiumCardAccent>
            <TargetIcon size={10} className="inline mr-1" />
            Mark Summary
          </PremiumCardAccent>
          <PremiumCardTitle className="text-base flex items-center gap-2">
            印断層分析
            {(() => {
              const _c = (race.overall_confidence || race.confidence) as string | undefined;
              return _c ? (
                <>
                  <span className="text-sm text-muted-foreground font-normal">自信度</span>
                  <ConfidenceBadge rank={_c} className="text-sm px-3 py-1" />
                </>
              ) : null;
            })()}
          </PremiumCardTitle>
        </div>
      </PremiumCardHeader>
      <div className="space-y-1">
        {/* 全馬を総合指数順で表示（断層付き） */}
        {(() => {
          const sorted = [...horses].sort((a, b) => (b.composite || 0) - (a.composite || 0));
          const elements: React.ReactNode[] = [];
          sorted.forEach((h, i) => {
            // 断層チェック（前の馬との差）
            if (i > 0) {
              const prevComp = sorted[i - 1].composite || 0;
              const curComp = h.composite || 0;
              const gap = prevComp - curComp;
              if (gap >= 2.5) {
                elements.push(<GapSeparator key={`gap-${i}`} gap={gap} />);
              }
            }
            const mo = getMarkDef(h.mark);
            elements.push(
              <MarkRow key={`all-${h.horse_no}`} mo={mo} h={h} rankColors={probRanks[h.horse_no]} indexRanks={idxRanks[h.horse_no]} isNarrow={isNarrow} />
            );
          });
          return elements;
        })()}

        {/* 特選穴馬 → ☆穴の行 */}
        {tokusenHorses.length > 0 && (
          <div className="mt-4 space-y-2">
            <h4 className="text-sm font-heading font-bold text-blue-700 dark:text-blue-400 border-b border-blue-200 dark:border-blue-800 pb-1">
              特選穴馬
            </h4>
            {tokusenHorses
              .sort((a, b) => ((b.tokusen_score as number) || 0) - ((a.tokusen_score as number) || 0))
              .map((h) => {
                const oddsStr = h.odds != null && h.odds > 0
                  ? `${Number(h.odds).toFixed(1)}倍`
                  : "—";
                const popStr = h.popularity != null ? `${h.popularity}人気` : "";
                const cg = devGrade(h.composite || 0);
                const comp = h.composite || 0;
                const compRank = horses.filter((o) => (o.composite || 0) > comp).length + 1;

                const reasons: string[] = [];
                const ct = h.course_total || 0;
                if (ct >= 52) reasons.push(`コース適性${ct.toFixed(1)}で高適性`);
                if (comp >= 50 && h.popularity != null && h.popularity > compRank + 1)
                  reasons.push(`総合${comp.toFixed(1)}(${compRank}位)ながら${h.popularity}人気 → 過小評価`);
                if (h.ability_trend === "急上昇" || h.ability_trend === "上昇")
                  reasons.push(`近走${h.ability_trend}で調子上向き`);

                return (
                  <div
                    key={h.horse_no}
                    className="flex flex-col gap-1 text-sm py-2 px-3 rounded-md border-l-[3px] bg-blue-50 dark:bg-blue-950/30 border-l-blue-600"
                  >
                    <div className="flex items-center gap-2">
                      <span className="inline-block px-2 py-0.5 rounded-full text-[11px] font-bold bg-blue-600 text-white">
                        特選穴馬
                      </span>
                      <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>{h.horse_no}</span>
                      <span className="font-semibold">{h.horse_name}</span>
                      <span className="text-muted-foreground text-xs">{oddsStr}</span>
                      <span className="text-muted-foreground text-xs">{popStr}</span>
                      <span className="text-muted-foreground text-xs">
                        {cg}・{comp.toFixed(1)}({compRank}位/{horses.length}頭)
                      </span>
                    </div>
                  </div>
                );
              })}
          </div>
        )}

        {/* 特選危険馬 → ×の行 */}
        {kikenHorses.length > 0 && (
          <div className="mt-4 space-y-2">
            <h4 className="text-sm font-heading font-bold text-red-700 dark:text-red-400 border-b border-red-200 dark:border-red-800 pb-1">
              特選危険馬
            </h4>
            {kikenHorses
              .sort((a, b) => ((b.tokusen_kiken_score as number) || 0) - ((a.tokusen_kiken_score as number) || 0))
              .map((h) => {
                const oddsStr = h.odds != null && h.odds > 0
                  ? `${Number(h.odds).toFixed(1)}倍`
                  : "—";
                const popStr = h.popularity != null ? `${h.popularity}人気` : "";
                const comp = h.composite || 0;
                const compRank = horses.filter((o) => (o.composite || 0) > comp).length + 1;

                const reasons: string[] = [];
                if (h.popularity != null && compRank > h.popularity + 2)
                  reasons.push(`${h.popularity}人気だが総合${compRank}位 → 過大評価`);
                const pastRuns = (h.past_runs || []) as Array<{ finish_pos?: number }>;
                if (pastRuns.length > 0) {
                  const prevFp = pastRuns[0]?.finish_pos;
                  if (prevFp != null && prevFp >= 5)
                    reasons.push(`前走${prevFp}着と凡走`);
                }
                let consec = 0;
                for (const r of pastRuns) {
                  if (r.finish_pos != null && r.finish_pos >= 4) consec++;
                  else break;
                }
                if (consec >= 2)
                  reasons.push(`直近${consec}走連続4着以下`);

                return (
                  <div
                    key={h.horse_no}
                    className="flex flex-col gap-1 text-sm py-2 px-3 rounded-md border-l-[3px] bg-red-50 dark:bg-red-950/30 border-l-red-600"
                  >
                    <div className="flex items-center gap-2">
                      <span className="inline-block px-2 py-0.5 rounded-full text-[11px] font-bold bg-red-600 text-white">
                        特選危険馬
                      </span>
                      <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>{h.horse_no}</span>
                      <span className="font-semibold">{h.horse_name}</span>
                      <span className="text-muted-foreground text-xs">{oddsStr}</span>
                      <span className="text-muted-foreground text-xs">{popStr}</span>
                      <span className="text-muted-foreground text-xs">
                        総合{comp.toFixed(1)}({compRank}位/{horses.length}頭)
                      </span>
                    </div>
                  </div>
                );
              })}
          </div>
        )}

        {/* 印見解 — 廃止 */}

        {/* 買い目 — 廃止 */}
      </div>
    </PremiumCard>
  );
}
