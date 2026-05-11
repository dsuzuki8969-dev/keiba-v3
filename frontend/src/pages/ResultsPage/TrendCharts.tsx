import { PremiumCard } from "@/components/ui/premium/PremiumCard";

// v6.1.12: Card ラッパ ヘルパー
function ChartCard({
  accentColor,
  title,
  children,
}: {
  accentColor: string;
  title: string;
  children: React.ReactNode;
}) {
  return (
    <PremiumCard variant="default" padding="md" className="overflow-hidden">
      <div className="flex items-center gap-2 mb-2">
        <span
          aria-hidden
          className="inline-block w-1 h-4 rounded-full"
          style={{ background: accentColor }}
        />
        <h3
          className="heading-section text-sm"
          style={{ color: accentColor }}
        >
          {title}
        </h3>
      </div>
      {children}
    </PremiumCard>
  );
}
import {
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  ReferenceLine,
} from "recharts";
import type { HybridSummaryResponse } from "@/api/client";

// v6.1: 共通 Tooltip スタイル（design-tokens 連携）
const tooltipStyle = {
  background: "var(--popover)",
  border: "1px solid var(--border)",
  borderRadius: 8,
  fontSize: 12,
  color: "var(--popover-foreground)",
} as const;

interface Props {
  data: Record<string, unknown>;
  hybrid?: HybridSummaryResponse | null;
}

// チャートカラー
const CHART_COLORS = {
  honmei: "#10b981",         // 緑（◉◎単勝回収率）
  profitPlus: "#10b981",     // 緑（プラス収支・単勝）
  profitMinus: "#ef4444",    // 赤（マイナス収支）
  // M' 戦略 (emerald 系)
  mprime: "#10b981",          // emerald（M' ROI）
  mprimeProfitPlus: "#10b981",
  mprimeProfitMinus: "#ef4444",
};

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}
function fmtYen(v: number): string {
  return (v >= 0 ? "+" : "") + v.toLocaleString() + "円";
}

// M' 戦略 + ◎○単勝 の 4 枚表示
export function TrendCharts({ data, hybrid }: Props) {
  const labels = (data.labels || []) as string[];
  const honmeiRoi = (data.honmei_tansho_roi_cum || []) as number[];
  const monthLabels = (data.monthly_labels || []) as string[];
  const monthProfit = (data.monthly_profit || []) as number[];

  // ──── ◎○単勝 データ ────
  const honmeiRoiData = labels.map((label, i) => ({
    name: label,
    honmei: honmeiRoi[i] ?? 0,
  }));
  const honmeiMonthData = monthLabels.map((label, i) => ({
    name: label,
    profit: monthProfit[i] ?? 0,
  }));

  // ──── M' 月別データ ────
  // monthly は Record<string, MPrimeMonthly> 形式なのでエントリを月順にソート
  const mprimeMonthlyRaw = hybrid?.m_prime_sanrenpuku?.monthly ?? {};
  const mprimeMonthlyData = Object.entries(mprimeMonthlyRaw)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, v]) => ({
      name: month,
      roi: v.roi_pct,
      balance: v.balance,
    }));

  if (!labels.length && !monthLabels.length && mprimeMonthlyData.length === 0) return null;

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {/* ────────── 1: ◎○単勝 ROI推移 ────────── */}
      {honmeiRoiData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.honmei} title="◉◎単勝 回収率推移">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={honmeiRoiData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="honmeiGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={CHART_COLORS.honmei} stopOpacity={0.5} />
                    <stop offset="100%" stopColor={CHART_COLORS.honmei} stopOpacity={0.05} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis
                  dataKey="name"
                  tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
                  tickLine={false}
                  interval="preserveStartEnd"
                  stroke="var(--border)"
                />
                <YAxis
                  tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
                  tickLine={false}
                  tickFormatter={(v) => v + "%"}
                  stroke="var(--border)"
                />
                <Tooltip
                  contentStyle={tooltipStyle}
                  formatter={(v) => [fmtPct(Number(v)), "◉◎単勝回収率"]}
                />
                <ReferenceLine y={100} stroke="var(--brand-gold)" strokeDasharray="4 4" label={{ value: "損益分岐 100%", fill: "var(--brand-gold)", fontSize: 10, position: "insideTopRight" }} />
                <Area
                  type="monotone"
                  dataKey="honmei"
                  stroke={CHART_COLORS.honmei}
                  strokeWidth={2.5}
                  fill="url(#honmeiGrad)"
                  dot={honmeiRoiData.length <= 60}
                  isAnimationActive
                />
              </AreaChart>
            </ResponsiveContainer>
        </ChartCard>
      )}

      {/* ────────── 2: ◎○単勝 月別収支 ────────── */}
      {honmeiMonthData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.honmei} title="◉◎単勝 月別収支">
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={honmeiMonthData}>
                <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.15} />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} />
                <YAxis
                  tick={{ fontSize: 10 }}
                  tickLine={false}
                  tickFormatter={(v) => (v >= 0 ? "+" : "") + v.toLocaleString()}
                />
                <Tooltip formatter={(v) => [fmtYen(Number(v)), "◉◎単勝 収支"]} />
                <Bar dataKey="profit" radius={[4, 4, 0, 0]}>
                  {honmeiMonthData.map((entry, i) => (
                    <Cell
                      key={`tan-${i}`}
                      fill={entry.profit >= 0 ? CHART_COLORS.profitPlus : CHART_COLORS.profitMinus}
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
        </ChartCard>
      )}

      {/* ────────── 3: M' 三連複 月別 ROI推移 (emerald) ────────── */}
      {mprimeMonthlyData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.mprime} title="M' 戦略 月別 ROI 推移">
          <ResponsiveContainer width="100%" height={220}>
            <AreaChart data={mprimeMonthlyData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <defs>
                <linearGradient id="mprimeRoiGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={CHART_COLORS.mprime} stopOpacity={0.5} />
                  <stop offset="100%" stopColor={CHART_COLORS.mprime} stopOpacity={0.05} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
                tickLine={false}
                interval="preserveStartEnd"
                stroke="var(--border)"
              />
              <YAxis
                tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
                tickLine={false}
                tickFormatter={(v) => v + "%"}
                stroke="var(--border)"
              />
              <Tooltip
                contentStyle={tooltipStyle}
                formatter={(v) => [fmtPct(Number(v)), "M' ROI"]}
              />
              <ReferenceLine
                y={100}
                stroke="var(--brand-gold)"
                strokeDasharray="4 4"
                label={{ value: "損益分岐 100%", fill: "var(--brand-gold)", fontSize: 10, position: "insideTopRight" }}
              />
              <Area
                type="monotone"
                dataKey="roi"
                stroke={CHART_COLORS.mprime}
                strokeWidth={2.5}
                fill="url(#mprimeRoiGrad)"
                dot={mprimeMonthlyData.length <= 24}
                isAnimationActive
              />
            </AreaChart>
          </ResponsiveContainer>
        </ChartCard>
      )}

      {/* ────────── 6: M' 三連複 月別純利推移 (emerald / 棒グラフ) ────────── */}
      {mprimeMonthlyData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.mprime} title="M' 戦略 月別純利推移">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={mprimeMonthlyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.15} />
              <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} />
              <YAxis
                tick={{ fontSize: 10 }}
                tickLine={false}
                tickFormatter={(v) => (v >= 0 ? "+" : "") + v.toLocaleString()}
              />
              <Tooltip formatter={(v) => [fmtYen(Number(v)), "M' 純利"]} />
              <Bar dataKey="balance" radius={[4, 4, 0, 0]}>
                {mprimeMonthlyData.map((entry, i) => (
                  <Cell
                    key={`mp-${i}`}
                    fill={entry.balance >= 0 ? CHART_COLORS.mprimeProfitPlus : CHART_COLORS.mprimeProfitMinus}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}
    </div>
  );
}
