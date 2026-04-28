import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { BarChart3 } from "lucide-react";

import type { SanrentanSummaryResponse, SanrentanTopPayout, SanrentanByConfidence } from "@/api/client";

interface Props {
  data: Record<string, unknown>;
  sanrentan?: SanrentanSummaryResponse | null;
}

const CAT_TABS = [
  { key: "all", label: "全体" },
  { key: "jra", label: "JRA" },
  { key: "nar", label: "NAR" },
];

function fmtPct(v: number): string {
  return (v ?? 0).toFixed(1) + "%";
}
function fmtNum(v: number): string {
  return (v ?? 0).toLocaleString();
}
// レース名を短縮（括弧内の条件や長い修飾語を除去）
function shortRaceName(name: string): string {
  return name
    .replace(/\(.*?\)/g, "")   // (3歳)(A2) 等を除去
    .replace(/（.*?）/g, "")   // 全角括弧も除去
    .trim()
    .slice(0, 8);              // 最大8文字
}

export function DetailedAnalysis({ data, sanrentan }: Props) {
  const navigate = useNavigate();
  const [cat, setCat] = useState("all");
  const [selectedVenue, setSelectedVenue] = useState<string | null>(null);

  const catData = (data[cat] || {}) as Record<string, unknown>;
  if (!catData.stats) return null;

  const stats = catData.stats as Record<string, unknown>;
  const byVenue = (catData.by_venue || {}) as Record<string, Record<string, unknown>>;
  const byMark = (selectedVenue ? (byVenue[selectedVenue]?.by_mark || {}) : (stats.by_mark || {})) as Record<string, Record<string, number>>;
  const byConf = (selectedVenue ? (byVenue[selectedVenue]?.by_conf || {}) : (stats.by_conf || {})) as Record<string, Record<string, number>>;
  const top10 = (selectedVenue
    ? ((byVenue[selectedVenue]?.top10_tansho || []) as Record<string, unknown>[])
    : ((data.top10_tansho || []) as Record<string, unknown>[])
  ).filter((x) => {
    if (cat === "jra") return x.is_jra;
    if (cat === "nar") return !x.is_jra;
    return true;
  }).slice(0, 10);

  // （削除）軸馬率 TOP10 → マスター指示 2026-04-22 により「三連単高配当 TOP10」に置き換え
  // 三連単高配当は props.sanrentan.top10_payouts から取得（JRA/NAR/競馬場フィルタは影響しない）

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
        <div className="flex gap-1">
          {CAT_TABS.map((t) => (
            <Button
              key={t.key}
              size="sm"
              variant={cat === t.key ? "default" : "outline"}
              onClick={() => {
                setCat(t.key);
                setSelectedVenue(null);
              }}
            >
              {t.label}
            </Button>
          ))}
        </div>

        {/* 競馬場ボタン */}
        {venueKeys.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {venueKeys.map((v) => {
              const vs = byVenue[v];
              const roi = Number(vs.roi || 0);
              const roiColor =
                roi >= 100
                  ? "text-positive"
                  : roi >= 80
                    ? "text-warning"
                    : "text-negative";
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
                  <span className={`${roiColor}`}>
                    {vs.total_races as number}R / {roi.toFixed(0)}%
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

          {/* 自信度別成績（単勝ベース） */}
          <div>
            <div className="text-sm font-semibold mb-2">自信度別成績（単勝）</div>
            <ConfTable data={byConf} />
          </div>
        </div>

        {/* 三連単フォーメーション: 自信度別 */}
        {sanrentan && sanrentan.by_confidence && sanrentan.by_confidence.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div />
            <div>
              <div className="text-sm font-semibold mb-2">自信度別成績（三連単F）</div>
              <SanrentanConfTable rows={sanrentan.by_confidence} />
            </div>
          </div>
        )}

        {/* TOP10セクション（2列）: 左=単勝高配当 / 右=三連単高配当 */}
        {(top10.length > 0 || (sanrentan?.top10_payouts?.length ?? 0) > 0) && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* 左: 高配当 TOP10 */}
            {top10.length > 0 && (
              <div>
                <div className="text-sm font-semibold mb-2">単勝高配当 TOP10</div>
                <div className="overflow-x-auto">
                  <table className="min-w-[580px] w-full text-xs">
                    <thead>
                      <tr className="border-b border-border">
                        <th className="text-left py-1 px-1">#</th>
                        <th className="text-right py-1 px-1">配当</th>
                        <th className="text-left py-1 px-1">日付</th>
                        <th className="text-left py-1 px-1">場</th>
                        <th className="text-left py-1 px-1">R</th>
                        <th className="text-left py-1 px-1">レース</th>
                        <th className="text-left py-1 px-1">印</th>
                        <th className="text-left py-1 px-1">馬名</th>
                      </tr>
                    </thead>
                    <tbody>
                      {top10.map((x, i) => (
                        <tr
                          key={`${x.date}-${x.venue}-${x.race_no}-${i}`}
                          className="border-b border-border/50 cursor-pointer hover:bg-muted/50 transition-colors"
                          onClick={() => {
                            const d = String(x.date || "");
                            const v = String(x.venue || "");
                            const r = Number(x.race_no || 0);
                            if (d && v && r) {
                              navigate("/today", { state: { date: d, venue: v, raceNo: r } });
                            }
                          }}
                        >
                          <td className="py-1 px-1 text-muted-foreground">{i + 1}</td>
                          <td className="text-right py-1 px-1 font-bold text-warning tabular-nums whitespace-nowrap">
                            {fmtNum(Number(x.payout || 0))}円
                          </td>
                          <td className="py-1 px-1 tabular-nums whitespace-nowrap">{String(x.date || "").slice(5)}</td>
                          <td className="py-1 px-1 whitespace-nowrap">{String(x.venue || "")}</td>
                          <td className="py-1 px-1 tabular-nums">{String(x.race_no || "")}</td>
                          <td className="py-1 px-1 text-muted-foreground max-w-[80px] truncate">{shortRaceName(String(x.race_name || ""))}</td>
                          <td className="py-1 px-1 text-emerald-600 font-semibold">{String(x.marks || x.combo || "")}</td>
                          <td className="py-1 px-1 whitespace-nowrap">{String(x.horse_name || "")}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* 右: 三連単高配当 TOP10 */}
            {sanrentan && sanrentan.top10_payouts && sanrentan.top10_payouts.length > 0 && (
              <div>
                <div className="text-sm font-semibold mb-2">三連単高配当 TOP10</div>
                <SanrentanTopPayoutTable rows={sanrentan.top10_payouts} navigate={navigate} />
              </div>
            )}
          </div>
        )}
      </div>
    </PremiumCard>
  );
}

// 印別テーブル
function MarkTable({ data }: { data: Record<string, Record<string, number>> }) {
  const marks = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"].filter((m) => data[m]);
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
              <tr key={m} className="border-b border-border/50">
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

// 自信度別テーブル
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
            <th className="text-right py-1 px-1">回収率</th>
          </tr>
        </thead>
        <tbody>
          {confs.map((c) => {
            const s = data[c];
            const roi = s.roi ?? 0;
            const roiColor = roi >= 100 ? "text-positive" : "text-negative";
            return (
              <tr key={c} className="border-b border-border/50">
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
                <td
                  className={`text-right py-1 px-1 tabular-nums font-bold ${roiColor}`}
                >
                  {fmtPct(roi)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// 三連単高配当 TOP10 テーブル
function SanrentanTopPayoutTable({
  rows,
  navigate,
}: {
  rows: SanrentanTopPayout[];
  navigate: ReturnType<typeof useNavigate>;
}) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-[480px] w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1 px-1">#</th>
            <th className="text-right py-1 px-1">配当</th>
            <th className="text-left py-1 px-1">日付</th>
            <th className="text-left py-1 px-1">場</th>
            <th className="text-left py-1 px-1">R</th>
            <th className="text-left py-1 px-1">レース</th>
            <th className="text-left py-1 px-1">自信度</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((x, i) => {
            const dateFmt = (x.date || "").length === 8
              ? `${x.date.slice(0, 4)}-${x.date.slice(4, 6)}-${x.date.slice(6, 8)}`
              : String(x.date || "");
            return (
              <tr
                key={`srt-${x.date}-${x.venue}-${x.race_no}-${i}`}
                className="border-b border-border/50 cursor-pointer hover:bg-muted/50 transition-colors"
                onClick={() => {
                  if (x.date && x.venue && x.race_no) {
                    navigate("/today", {
                      state: { date: dateFmt, venue: x.venue, raceNo: x.race_no },
                    });
                  }
                }}
              >
                <td className="py-1 px-1 text-muted-foreground">{i + 1}</td>
                <td className="text-right py-1 px-1 font-bold text-warning tabular-nums whitespace-nowrap">
                  {fmtNum(x.payout)}円
                </td>
                <td className="py-1 px-1 tabular-nums whitespace-nowrap">{dateFmt.slice(5)}</td>
                <td className="py-1 px-1 whitespace-nowrap">{x.venue}</td>
                <td className="py-1 px-1 tabular-nums">{x.race_no}</td>
                <td className="py-1 px-1 text-muted-foreground max-w-[80px] truncate">{shortRaceName(x.race_name)}</td>
                <td className="py-1 px-1 font-semibold">{x.conf}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// 三連単自信度別テーブル
function SanrentanConfTable({ rows }: { rows: SanrentanByConfidence[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-border">
            <th className="text-left py-1 px-2">自信度</th>
            <th className="text-right py-1 px-2">購入R</th>
            <th className="text-right py-1 px-2">的中R</th>
            <th className="text-right py-1 px-2">R的中率</th>
            <th className="text-right py-1 px-2">回収率</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.confidence} className="border-b border-border/50">
              <td className="py-1 px-2 font-bold">{r.confidence}</td>
              <td className="text-right py-1 px-2 tabular-nums">{fmtNum(r.played)}</td>
              <td className="text-right py-1 px-2 tabular-nums">{fmtNum(r.hit)}</td>
              <td className="text-right py-1 px-2 tabular-nums font-bold">{fmtPct(r.hit_rate_pct)}</td>
              <td className={`text-right py-1 px-2 tabular-nums font-bold ${r.roi_pct >= 100 ? "text-positive" : "text-negative"}`}>{fmtPct(r.roi_pct)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
