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
import type { SanrentanSummaryResponse } from "@/api/client";

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
  sanrentan?: SanrentanSummaryResponse | null;
}

// チャートカラー
const CHART_COLORS = {
  honmei: "#10b981",       // 緑（◉◎単勝回収率）
  sanrentan: "#8b5cf6",    // 紫（三連単F回収率）
  profitPlus: "#10b981",   // 緑（プラス収支・単勝）
  profitMinus: "#ef4444",  // 赤（マイナス収支）
  sanrentanPlus: "#8b5cf6",  // 紫（プラス収支・三連単F）
  sanrentanMinus: "#f59e0b", // オレンジ（マイナス収支・三連単F）
};

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}
function fmtYen(v: number): string {
  return (v >= 0 ? "+" : "") + v.toLocaleString() + "円";
}

// マスター指示 2026-04-22 (案B): 4枚分離表示（2×2グリッド）
// 左上: ◎○単勝 ROI推移 / 右上: 三連単F ROI推移
// 左下: ◎○単勝 月別収支 / 右下: 三連単F 月別収支
export function TrendCharts({ data, sanrentan }: Props) {
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

  // ──── 三連単F データ ────
  const sanrentanMonthly = sanrentan?.monthly || [];
  const sanrentanRoiData = sanrentanMonthly.map((m) => ({
    name: m.month,
    sanrentan: m.cum_roi_pct,
  }));
  const sanrentanMonthData = sanrentanMonthly.map((m) => ({
    name: m.month,
    profit: m.balance,
  }));

  if (!labels.length && !monthLabels.length && sanrentanMonthly.length === 0) return null;

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

      {/* ────────── 2: 三連単F ROI推移 ────────── */}
      {sanrentanRoiData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.sanrentan} title="三連単F 回収率推移">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={sanrentanRoiData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="sanrentanGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={CHART_COLORS.sanrentan} stopOpacity={0.5} />
                    <stop offset="100%" stopColor={CHART_COLORS.sanrentan} stopOpacity={0.05} />
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
                  formatter={(v) => [fmtPct(Number(v)), "三連単F回収率"]}
                />
                <ReferenceLine y={100} stroke="var(--brand-gold)" strokeDasharray="4 4" label={{ value: "損益分岐 100%", fill: "var(--brand-gold)", fontSize: 10, position: "insideTopRight" }} />
                <Area
                  type="monotone"
                  dataKey="sanrentan"
                  stroke={CHART_COLORS.sanrentan}
                  strokeWidth={2.5}
                  fill="url(#sanrentanGrad)"
                  dot={sanrentanRoiData.length <= 60}
                  isAnimationActive
                />
              </AreaChart>
            </ResponsiveContainer>
        </ChartCard>
      )}

      {/* ────────── 3: ◎○単勝 月別収支 ────────── */}
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

      {/* ────────── 4: 三連単F 月別収支 ────────── */}
      {sanrentanMonthData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.sanrentan} title="三連単F 月別収支">
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={sanrentanMonthData}>
                <CartesianGrid strokeDasharray="3 3" stroke="currentColor" opacity={0.15} />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} tickLine={false} />
                <YAxis
                  tick={{ fontSize: 10 }}
                  tickLine={false}
                  tickFormatter={(v) => (v >= 0 ? "+" : "") + v.toLocaleString()}
                />
                <Tooltip formatter={(v) => [fmtYen(Number(v)), "三連単F 収支"]} />
                <Bar dataKey="profit" radius={[4, 4, 0, 0]}>
                  {sanrentanMonthData.map((entry, i) => (
                    <Cell
                      key={`san-${i}`}
                      fill={entry.profit >= 0 ? CHART_COLORS.sanrentanPlus : CHART_COLORS.sanrentanMinus}
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
