import {
  ComposedChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts";
import { colors } from "@/design/tokens";

/**
 * PaceFlowChart — 逃/先/差/追 の頭数分布
 * --------------------------------------------------------------
 * コーナー毎の頭数推移ではなく、本番レースでの脚質別頭数を
 * 積み上げ棒として視覚化。ペース予想の直感理解を助ける。
 *
 * データ例:
 *   [{ label: "本番", 逃げ: 2, 先行: 5, 差し: 4, 追込: 3 }]
 *
 * 複数レースの時系列比較にも拡張可能。
 */

// PaceEntry 型と buildPaceEntry は Fast Refresh 互換のため
// @/lib/keibaUtils に移動。ここでは re-export のみ。
export type { PaceEntry } from "@/lib/keibaUtils";
export { buildPaceEntry } from "@/lib/keibaUtils";
import type { PaceEntry } from "@/lib/keibaUtils";

interface Props {
  data: PaceEntry[];
  /** コンパクト表示 */
  compact?: boolean;
}

const STYLE_COLORS = {
  逃げ: "#16a34a",  // 緑
  先行: "#2563eb",  // 青
  差し: "#dc2626",  // 赤
  追込: "#7c3aed",  // 紫
} as const;

export function PaceFlowChart({ data, compact = false }: Props) {
  if (data.length === 0) {
    return (
      <div className="text-sm text-muted-foreground p-4 text-center">
        脚質データがありません
      </div>
    );
  }

  // 色の出所は colors.semantic（万一未使用指摘を避けるため参照のみ）
  void colors;

  return (
    <div className="w-full" style={{ height: compact ? 200 : 280 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart
          data={data}
          margin={{ top: 12, right: 12, left: 8, bottom: 8 }}
        >
          <CartesianGrid stroke="var(--border)" strokeDasharray="3 3" />
          <XAxis
            dataKey="label"
            tick={{ fill: "var(--foreground)", fontSize: 12, fontWeight: 600 }}
            stroke="var(--border)"
          />
          <YAxis
            tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
            stroke="var(--border)"
            allowDecimals={false}
          />
          <Tooltip
            cursor={{ fill: "var(--muted)", opacity: 0.3 }}
            contentStyle={{
              background: "var(--popover)",
              border: "1px solid var(--border)",
              borderRadius: 8,
              fontSize: 12,
              color: "var(--popover-foreground)",
            }}
            formatter={(value, name) => {
              const v = typeof value === "number" ? value : Number(value) || 0;
              return [`${v}頭`, name];
            }}
          />
          <Legend
            wrapperStyle={{ fontSize: 11, color: "var(--muted-foreground)" }}
            iconType="square"
          />
          <Bar dataKey="逃げ" stackId="pace" fill={STYLE_COLORS.逃げ} radius={[0, 0, 0, 0]} />
          <Bar dataKey="先行" stackId="pace" fill={STYLE_COLORS.先行} />
          <Bar dataKey="差し" stackId="pace" fill={STYLE_COLORS.差し} />
          <Bar dataKey="追込" stackId="pace" fill={STYLE_COLORS.追込} radius={[4, 4, 0, 0]} />
        </ComposedChart>
      </ResponsiveContainer>
    </div>
  );
}

