import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { BarChart3 } from "lucide-react";
import { displayMark } from "@/lib/markDisplay";
import type { DeviationStatRow, MarkStatRow, DetailedStats } from "@/api/client";

interface Props {
  data: Record<string, unknown>;
  // 全体/JRA/NAR・競馬場 の選択状態は親(ResultsPage)で共有し、上部カードと連動させる
  cat: string;
  setCat: (c: string) => void;
  selectedVenue: string | null;
  setSelectedVenue: (v: string | null) => void;
}

// 偏差値帯の固定表示順（降順・高偏差値を上に）。境界は実データ複勝率カーブで較正(候補D)。
const DEV_BUCKET_ORDER = ["90〜", "82-90", "72-82", "62-72", "53-62", "45-53", "〜45"] as const;

const CAT_TABS = [
  { key: "all", label: "全体" },
  { key: "jra", label: "JRA" },
  { key: "nar", label: "NAR" },
];

function fmtPct(v: number): string {
  return (v ?? 0).toFixed(1) + "%";
}

export function DetailedAnalysis({ data, cat, setCat, selectedVenue, setSelectedVenue }: Props) {

  const catData = (data[cat] || {}) as Record<string, unknown>;
  if (!catData.stats) return null;

  const stats = (catData.stats || {}) as DetailedStats;
  const byVenue = (catData.by_venue || {}) as Record<string, Record<string, unknown>>;
  const byMark = (selectedVenue ? (byVenue[selectedVenue]?.by_mark || {}) : (stats.by_mark || {})) as Record<string, MarkStatRow>;
  // 偏差値帯別: 場選択時はその場の by_deviation、未選択時は全体 stats.by_deviation
  const byDeviation = (selectedVenue ? (byVenue[selectedVenue]?.by_deviation || {}) : (stats.by_deviation || {})) as Record<string, DeviationStatRow>;

  // 競馬場リスト（レース数降順）
  const venueKeys = Object.keys(byVenue).sort(
    (a, b) =>
      Number(byVenue[b].total_races || 0) - Number(byVenue[a].total_races || 0)
  );

  return (
    <PremiumCard variant="default" padding="md">
      <PremiumCardHeader>
        <div className="flex flex-col gap-0.5">
          <PremiumCardAccent>
            <BarChart3 size={10} className="inline mr-1" />
            Analytics
          </PremiumCardAccent>
          <PremiumCardTitle className="text-base">詳細分析</PremiumCardTitle>
        </div>
      </PremiumCardHeader>
      <div className="space-y-4">
        {/* カテゴリタブ — ピル様式（期間セレクタと統一） */}
        <div role="tablist" className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg">
          {CAT_TABS.map((t) => (
            <button
              key={t.key}
              role="tab"
              aria-selected={cat === t.key}
              className={[
                "px-3 text-xs font-semibold rounded-md whitespace-nowrap",
                "min-h-[36px] inline-flex items-center",
                "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
                cat === t.key
                  ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                  : "text-muted-foreground hover:text-foreground hover:bg-background/60",
              ].join(" ")}
              onClick={() => {
                setCat(t.key);
                setSelectedVenue(null);
              }}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* 競馬場チップ — ピル同系統（選択=navy塗り、非選択=チップ枠） */}
        {venueKeys.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {venueKeys.map((v) => {
              const vs = byVenue[v];
              return (
                <button
                  key={v}
                  aria-pressed={selectedVenue === v}
                  onClick={() =>
                    setSelectedVenue(selectedVenue === v ? null : v)
                  }
                  className={[
                    "px-2.5 text-xs font-semibold rounded-md border",
                    "min-h-[36px] inline-flex items-center gap-1",
                    "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
                    selectedVenue === v
                      ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white border-brand-navy shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                      : "border-border text-muted-foreground hover:text-foreground hover:bg-muted",
                  ].join(" ")}
                >
                  {v}
                  <span className={selectedVenue === v ? "text-white/70" : "text-muted-foreground"}>
                    {vs.total_races as number}R
                  </span>
                </button>
              );
            })}
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* 印別成績（単勝ベース） */}
          <div>
            <div className="text-sm font-semibold mb-2">印別成績</div>
            <MarkTable data={byMark} />
          </div>

          {/* 偏差値別 成績 */}
          <div>
            <div className="text-sm font-semibold mb-2">偏差値別 成績</div>
            <DevTable data={byDeviation} />
          </div>
        </div>

      </div>
    </PremiumCard>
  );
}

// 印別テーブル
function MarkTable({ data }: { data: Record<string, MarkStatRow> }) {
  const marks = ["◉", "◎", "○", "▲", "△", "★", "☆"].filter((m) => data[m]);
  if (!marks.length)
    return <p className="text-xs text-muted-foreground">データなし</p>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1 px-1">印</th>
            <th className="text-right py-1 px-1">頭数</th>
            <th className="text-right py-1 px-1">勝率</th>
            <th className="text-right py-1 px-1">連対率</th>
            <th className="text-right py-1 px-1">複勝率</th>
          </tr>
        </thead>
        <tbody>
          {marks.map((m) => {
            const s = data[m];
            return (
              <tr key={m} className="border-b border-border/50 hover:bg-brand-gold/5 transition-colors">
                {/* ☆ は「穴」表示に変換（データキー自体は変更しない） */}
                <td className="py-1 px-1 font-bold">{displayMark(m)}</td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {s.total}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.win_rate)}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.place2_rate)}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.place_rate)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// 偏差値帯別テーブル（印別と同じ列構成: 頭数/勝率/連対率/複勝率）
function DevTable({ data }: { data: Record<string, DeviationStatRow> }) {
  // 固定順（昇順）で存在する bucket のみ表示。データ無し bucket は行スキップ
  const buckets = DEV_BUCKET_ORDER.filter((b) => data[b]);
  if (!buckets.length)
    return <p className="text-xs text-muted-foreground">データなし</p>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1 px-1">偏差値帯</th>
            <th className="text-right py-1 px-1">頭数</th>
            <th className="text-right py-1 px-1">勝率</th>
            <th className="text-right py-1 px-1">連対率</th>
            <th className="text-right py-1 px-1">複勝率</th>
          </tr>
        </thead>
        <tbody>
          {buckets.map((b) => {
            const s = data[b];
            return (
              <tr key={b} className="border-b border-border/50 hover:bg-brand-gold/5 transition-colors">
                <td className="py-1 px-1 font-bold">{b}</td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {s.total}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.win_rate)}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.place2_rate)}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.place_rate)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

