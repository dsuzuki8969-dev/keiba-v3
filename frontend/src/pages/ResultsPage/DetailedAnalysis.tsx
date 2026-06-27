import { useState } from "react";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { BarChart3 } from "lucide-react";

interface Props {
  data: Record<string, unknown>;
}

const CAT_TABS = [
  { key: "all", label: "全体" },
  { key: "jra", label: "JRA" },
  { key: "nar", label: "NAR" },
];

function fmtPct(v: number): string {
  return (v ?? 0).toFixed(1) + "%";
}

export function DetailedAnalysis({ data }: Props) {
  const [cat, setCat] = useState("all");
  const [selectedVenue, setSelectedVenue] = useState<string | null>(null);

  const catData = (data[cat] || {}) as Record<string, unknown>;
  if (!catData.stats) return null;

  const stats = catData.stats as Record<string, unknown>;
  const byVenue = (catData.by_venue || {}) as Record<string, Record<string, unknown>>;
  const byMark = (selectedVenue ? (byVenue[selectedVenue]?.by_mark || {}) : (stats.by_mark || {})) as Record<string, Record<string, number>>;
  const byConf = (selectedVenue ? (byVenue[selectedVenue]?.by_conf || {}) : (stats.by_conf || {})) as Record<string, Record<string, number>>;

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
        {/* カテゴリタブ */}
        <div className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg">
          {CAT_TABS.map((t) => (
            <button
              key={t.key}
              className={`px-3 py-1 text-xs font-semibold rounded-md whitespace-nowrap transition-all ${
                cat === t.key
                  ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                  : "text-muted-foreground hover:text-foreground hover:bg-background/60"
              }`}
              onClick={() => {
                setCat(t.key);
                setSelectedVenue(null);
              }}
            >
              {t.label}
            </button>
          ))}
        </div>

        {/* 競馬場ボタン */}
        {venueKeys.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {venueKeys.map((v) => {
              const vs = byVenue[v];
              return (
                <button
                  key={v}
                  onClick={() =>
                    setSelectedVenue(selectedVenue === v ? null : v)
                  }
                  className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
                    selectedVenue === v
                      ? "bg-primary text-primary-foreground border-primary"
                      : "border-border hover:bg-muted"
                  }`}
                >
                  {v}{" "}
                  <span className="text-muted-foreground">
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

          {/* 自信度別 的中率 */}
          <div>
            <div className="text-sm font-semibold mb-2">自信度別 的中率</div>
            <ConfTable data={byConf} />
          </div>
        </div>

      </div>
    </PremiumCard>
  );
}

// 印別テーブル
function MarkTable({ data }: { data: Record<string, Record<string, number>> }) {
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
                <td className="py-1 px-1 font-bold">{m}</td>
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

// 自信度別テーブル（的中率のみ）
function ConfTable({ data }: { data: Record<string, Record<string, number>> }) {
  const confs = ["SS", "S", "A", "B", "C"].filter((c) => data[c]);
  if (!confs.length)
    return <p className="text-xs text-muted-foreground">データなし</p>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1 px-1">自信度</th>
            <th className="text-right py-1 px-1">購入R</th>
            <th className="text-right py-1 px-1">的中</th>
            <th className="text-right py-1 px-1">的中率</th>
          </tr>
        </thead>
        <tbody>
          {confs.map((c) => {
            const s = data[c];
            return (
              <tr key={c} className="border-b border-border/50 hover:bg-brand-gold/5 transition-colors">
                <td className="py-1 px-1 font-bold">{c}</td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {s.total}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {s.hits}
                </td>
                <td className="text-right py-1 px-1 tabular-nums">
                  {fmtPct(s.hit_rate)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

