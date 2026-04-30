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
import type { SanrentanSummaryResponse, HybridSummaryResponse } from "@/api/client";

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
  hybrid?: HybridSummaryResponse | null;
}

// チャートカラー
const CHART_COLORS = {
  honmei: "#10b981",         // 緑（◉◎単勝回収率）
  sanrentan: "#8b5cf6",      // 紫（三連単F回収率）
  profitPlus: "#10b981",     // 緑（プラス収支・単勝）
  profitMinus: "#ef4444",    // 赤（マイナス収支）
  sanrentanPlus: "#8b5cf6",  // 紫（プラス収支・三連単F）
  sanrentanMinus: "#f59e0b", // オレンジ（マイナス収支・三連単F）
  // 新戦略 (青系)
  hybridSpuku: "#3b82f6",    // 青（三連複動的 ROI）
  hybridTansho: "#22c55e",   // 明緑（単勝 T-4 ROI / 既存◉◎単勝とは別色）
  hybridProfitPlus: "#3b82f6",  // 青（プラス収支・新戦略）
  hybridProfitMinus: "#ef4444", // 赤（マイナス収支）
};

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}
function fmtYen(v: number): string {
  return (v >= 0 ? "+" : "") + v.toLocaleString() + "円";
}

// マスター指示 2026-04-22 (案B) + 2026-04-30 (新戦略追加):
// ◎○単勝 / 三連単F(旧) / 三連複動的(新) / 単勝T-4(新) の 6 枚表示
export function TrendCharts({ data, sanrentan, hybrid }: Props) {
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

  // ──── 新戦略ハイブリッド データ ────
  const spukuMonthly  = hybrid?.sanrenpuku_dynamic?.monthly ?? [];
  const tanshoMonthly = hybrid?.tansho_t4?.monthly ?? [];

  const spukuRoiData = spukuMonthly.map((m) => ({
    name: m.month,
    spuku: m.cum_roi_pct,
  }));
  const tanshoRoiData = tanshoMonthly.map((m) => ({
    name: m.month,
    tansho: m.cum_roi_pct,
  }));

  if (!labels.length && !monthLabels.length && sanrentanMonthly.length === 0
      && spukuMonthly.length === 0 && tanshoMonthly.length === 0) return null;

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
        <ChartCard accentColor={CHART_COLORS.sanrentan} title="三連単F 月別収支 (旧戦略・参考)">
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

      {/* ────────── 5: 三連複動的 ROI推移 (新戦略 / 青系) ────────── */}
      {spukuRoiData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.hybridSpuku} title="三連複動的 回収率推移 (新戦略)">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={spukuRoiData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="spukuGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={CHART_COLORS.hybridSpuku} stopOpacity={0.5} />
                    <stop offset="100%" stopColor={CHART_COLORS.hybridSpuku} stopOpacity={0.05} />
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
                  formatter={(v) => [fmtPct(Number(v)), "三連複動的 ROI"]}
                />
                <ReferenceLine y={100} stroke="var(--brand-gold)" strokeDasharray="4 4" label={{ value: "損益分岐 100%", fill: "var(--brand-gold)", fontSize: 10, position: "insideTopRight" }} />
                <Area
                  type="monotone"
                  dataKey="spuku"
                  stroke={CHART_COLORS.hybridSpuku}
                  strokeWidth={2.5}
                  fill="url(#spukuGrad)"
                  dot={spukuRoiData.length <= 60}
                  isAnimationActive
                />
              </AreaChart>
            </ResponsiveContainer>
        </ChartCard>
      )}

      {/* ────────── 6: 単勝 T-4 ROI推移 (新戦略 / 明緑) ────────── */}
      {tanshoRoiData.length > 0 && (
        <ChartCard accentColor={CHART_COLORS.hybridTansho} title="単勝 T-4 回収率推移 (新戦略)">
            <ResponsiveContainer width="100%" height={220}>
              <AreaChart data={tanshoRoiData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id="tanshoHybridGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={CHART_COLORS.hybridTansho} stopOpacity={0.5} />
                    <stop offset="100%" stopColor={CHART_COLORS.hybridTansho} stopOpacity={0.05} />
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
                  formatter={(v) => [fmtPct(Number(v)), "単勝 T-4 ROI"]}
                />
                <ReferenceLine y={100} stroke="var(--brand-gold)" strokeDasharray="4 4" label={{ value: "損益分岐 100%", fill: "var(--brand-gold)", fontSize: 10, position: "insideTopRight" }} />
                <Area
                  type="monotone"
                  dataKey="tansho"
                  stroke={CHART_COLORS.hybridTansho}
                  strokeWidth={2.5}
                  fill="url(#tanshoHybridGrad)"
                  dot={tanshoRoiData.length <= 60}
                  isAnimationActive
                />
              </AreaChart>
            </ResponsiveContainer>
        </ChartCard>
      )}
    </div>
  );
}
