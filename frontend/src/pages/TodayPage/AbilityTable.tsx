import { useState, useMemo } from "react";
import { devGrade, gradeCls, markCls, WAKU_BG } from "@/lib/constants";
import type { HorseData } from "./RaceDetailView";
import type { AbilityEntry } from "@/components/charts/AbilityHeatmap";
import { AbilityHeatmap } from "@/components/charts/AbilityHeatmap";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";

// 印キー → 表示シンボル
// MARK_SYMBOL は Fast Refresh 互換のため @/lib/keibaUtils に移動。
import { MARK_SYMBOL } from "@/lib/keibaUtils";

// グレード文字列 → 推定偏差値
function gradeToApproxDev(grade: string | undefined): number {
  if (!grade || grade === "—") return 50;
  const clean = grade.replace(/[⁺⁻]/g, "");
  const map: Record<string, number> = { SS: 73, S: 66, A: 59, B: 53, C: 47, D: 40 };
  return map[clean] || 50;
}

interface IndexDef {
  label: string;
  key: string;
  getValue: (h: HorseData) => number;
}

const INDEX_DEFS: IndexDef[] = [
  { label: "総合", key: "composite", getValue: (h) => h.composite || 0 },
  { label: "能力", key: "ability", getValue: (h) => h.ability_total || 0 },
  { label: "展開", key: "pace", getValue: (h) => h.pace_total || 0 },
  { label: "適性", key: "course", getValue: (h) => h.course_total || 0 },
  { label: "騎手", key: "jockey", getValue: (h) => h.jockey_dev || gradeToApproxDev(h.jockey_grade) },
  { label: "調教師", key: "trainer", getValue: (h) => h.trainer_dev || gradeToApproxDev(h.trainer_grade) },
  { label: "血統", key: "bloodline", getValue: (h) => h.bloodline_dev || gradeToApproxDev(h.sire_grade) },
  { label: "追切", key: "training", getValue: (h) => h.training_dev ?? 50 },
];

// 順位計算
function calcRanks(horses: HorseData[], key: (h: HorseData) => number): Record<number, number> {
  const ranks: Record<number, number> = {};
  for (let i = 0; i < horses.length; i++) {
    const v = key(horses[i]);
    ranks[horses[i].horse_no] = horses.filter((h) => key(h) > v).length + 1;
  }
  return ranks;
}

// 順位色
function rankCls(rank: number): string {
  if (rank === 1) return "text-emerald-600";
  if (rank === 2) return "text-blue-600";
  if (rank === 3) return "text-red-600";
  return "text-muted-foreground";
}

// ソートキーの型（指数キー + 枠 + 馬番）
type SortKey = string;

interface Props {
  horses: HorseData[];
  isBanei: boolean;
}

export function AbilityTable({ horses, isBanei }: Props) {
  // デフォルト: 馬番順（昇順）
  const [sortKey, setSortKey] = useState<SortKey>("horse_no");
  const [sortAsc, setSortAsc] = useState(true);

  // 総合順 TOP5 をヒートマップ用に抽出（useMemo でメモ化）
  const heatmapEntries: AbilityEntry[] = useMemo(
    () =>
      [...horses]
        .sort((a, b) => (b.composite || 0) - (a.composite || 0))
        .slice(0, 5)
        .map((h) => ({
          horse_no: h.horse_no,
          horse_name: h.horse_name || `#${h.horse_no}`,
          // 生キーをそのまま渡す（表示変換は AbilityHeatmap 側で行う）
          mark: h.mark || "",
          ability_total: h.ability_total ?? null,
          pace_total: isBanei ? null : (h.pace_total ?? null),
          course_total: h.course_total ?? null,
          jockey_dev: h.jockey_dev ?? gradeToApproxDev(h.jockey_grade),
          training_dev: h.training_dev ?? 50,
          bloodline_dev: h.bloodline_dev ?? gradeToApproxDev(h.sire_grade),
        })),
    [horses, isBanei],
  );

  // defs は isBanei のみで決まるのでメモ化（参照安定化）
  const defs = useMemo(
    () => (isBanei ? INDEX_DEFS.filter((d) => d.key !== "pace") : INDEX_DEFS),
    [isBanei],
  );

  // 全指数の順位 — horses/defs 不変ならキャッシュ（O(n²) × 指数数 を回避）
  const allRanks = useMemo(() => {
    const ranks: Record<string, Record<number, number>> = {};
    for (const def of defs) {
      // calcRanks: O(n²) だが 18頭なら問題なし。複数指数ぶんを useMemo でキャッシュ。
      ranks[def.key] = calcRanks(horses, def.getValue);
    }
    return ranks;
  }, [horses, defs]);

  // ソート
  const sorted = [...horses].sort((a, b) => {
    let va: number, vb: number;
    if (sortKey === "gate_no") {
      va = a.gate_no || 0;
      vb = b.gate_no || 0;
    } else if (sortKey === "horse_no") {
      va = a.horse_no || 0;
      vb = b.horse_no || 0;
    } else if (sortKey === "odds") {
      va = a.odds ?? a.predicted_tansho_odds ?? 9999;
      vb = b.odds ?? b.predicted_tansho_odds ?? 9999;
    } else {
      const sortDef = defs.find((d) => d.key === sortKey) || defs[0];
      va = sortDef.getValue(a);
      vb = sortDef.getValue(b);
    }
    return sortAsc ? va - vb : vb - va;
  });

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc);
    } else {
      setSortKey(key);
      // 枠・馬番はデフォルト昇順、指数はデフォルト降順
      setSortAsc(key === "gate_no" || key === "horse_no" || key === "odds");
    }
  };

  const sortArrow = (key: string) => {
    if (sortKey !== key) return "";
    return sortAsc ? " ↑" : " ↓";
  };

  return (
    <div className="space-y-4">
      {/* TOP5 偏差値ヒートマップ（レーダーチャートから置き換え） */}
      {heatmapEntries.length > 0 && (
        <PremiumCard variant="default" padding="md" className="overflow-hidden">
          <PremiumCardHeader>
            <div className="flex flex-col gap-0.5">
              <PremiumCardAccent>Top 5 偏差値ヒートマップ</PremiumCardAccent>
              <PremiumCardTitle>能力プロファイル</PremiumCardTitle>
            </div>
          </PremiumCardHeader>
          <AbilityHeatmap horses={heatmapEntries} isBanei={isBanei} />
        </PremiumCard>
      )}

    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-xs text-muted-foreground">
            <th
              className="py-1.5 px-1 text-center w-6 cursor-pointer hover:text-foreground select-none"
              onClick={() => handleSort("horse_no")}
            >
              番{sortArrow("horse_no")}
            </th>
            <th className="py-1.5 px-0.5 text-center w-5">印</th>
            <th className="py-1.5 px-1 text-left">馬名</th>
            <th
              className="py-1.5 px-1 text-right whitespace-nowrap cursor-pointer hover:text-foreground select-none"
              onClick={() => handleSort("odds")}
            >
              オッズ{sortArrow("odds")}
            </th>
            {defs.map((d) => (
              <th
                key={d.key}
                className="py-1.5 px-1 text-center cursor-pointer hover:text-foreground select-none whitespace-nowrap"
                onClick={() => handleSort(d.key)}
              >
                {d.label}{sortArrow(d.key)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((h) => {
            const realOdds = h.odds != null && h.odds > 0 ? h.odds : null;
            const predOdds = h.predicted_tansho_odds;
            const displayOdds = realOdds ?? predOdds;
            const pop = h.popularity;

            const markSym = h.mark ? MARK_SYMBOL[h.mark] || h.mark : "";
            const mCls = markSym ? markCls(markSym) : "";
            return (
              <tr key={h.horse_no} className="border-b border-border/50 hover:bg-muted/30">
                <td className="py-1.5 px-1 text-center">
                  <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                    {h.horse_no}
                  </span>
                </td>
                <td className="py-1.5 px-0.5 text-center">
                  {markSym && <span className={`text-[15px] ${mCls}`}>{markSym}</span>}
                </td>
                <td className="py-1.5 px-1 font-bold text-xs whitespace-nowrap">{h.horse_name}</td>
                <td className="py-1.5 px-1 text-right tabular-nums whitespace-nowrap">
                  {displayOdds != null ? (
                    <span className="font-semibold">{displayOdds.toFixed(1)}</span>
                  ) : "—"}
                  {pop != null && pop > 0 && (
                    <span className="text-[10px] text-muted-foreground ml-0.5">({pop})</span>
                  )}
                </td>
                {defs.map((d) => {
                  const val = d.getValue(h);
                  const grade = devGrade(val);
                  const rank = allRanks[d.key]?.[h.horse_no] || 0;
                  return (
                    <td key={d.key} className="py-1.5 px-1 text-center whitespace-nowrap">
                      <span className={`${gradeCls(grade)} text-sm`}>{grade}</span>
                      <span className="text-[11px] text-muted-foreground ml-0.5">{val.toFixed(1)}</span>
                      <span className={`text-[11px] ml-0.5 ${rankCls(rank)}`}>({rank})</span>
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
    </div>
  );
}
