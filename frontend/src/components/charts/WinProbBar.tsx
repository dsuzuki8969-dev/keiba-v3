import {
  BarChart,
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
 * WinProbBar — 勝率/連対率/複勝率 3層スタック横バー
 * --------------------------------------------------------------
 * 全頭を縦に並べ、1頭あたり1本の横バーで
 *   勝率    : 金箔
 *   連対率差: 青
 *   複勝率差: 灰
 * を積む（累積表示なので複勝率 > 連対率 > 勝率 の仕様を崩さない）。
 *
 * 期待データ: p1, p2, p3 は 0〜100 の実数（%）。
 */

export interface WinProbEntry {
  horse_no: number;
  horse_name: string;
  mark?: string;
  p1: number; // 勝率 (%)
  p2: number; // 連対率 (%)
  p3: number; // 複勝率 (%)
}

interface Props {
  horses: WinProbEntry[];
  /** 馬名ラベル最大文字数 */
  nameMax?: number;
  /** コンテナ高さを可変に */
  heightPerRow?: number;
}

export function WinProbBar({ horses, nameMax = 8, heightPerRow = 28 }: Props) {
  if (horses.length === 0) {
    return (
      <div className="text-sm text-muted-foreground p-4 text-center">
        確率データがありません
      </div>
    );
  }

  // 累積ではなく "差分" をスタックすることで p1 < p2 < p3 が成立
  const data = horses.map((h) => {
    const p1 = Math.max(0, h.p1 ?? 0);
    const p2 = Math.max(p1, h.p2 ?? 0);
    const p3 = Math.max(p2, h.p3 ?? 0);
    const label = (h.mark ? `${h.mark} ` : "") + h.horse_name.slice(0, nameMax);
    return {
      label,
      horse_no: h.horse_no,
      p1,               // 勝率
      d2: p2 - p1,      // 連対率 − 勝率
      d3: p3 - p2,      // 複勝率 − 連対率
      total_p3: p3,
    };
  });

  const height = Math.max(180, horses.length * heightPerRow + 40);

  return (
    <div className="w-full" style={{ height }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          layout="vertical"
          data={data}
          margin={{ top: 8, right: 12, left: 8, bottom: 8 }}
          barCategoryGap="18%"
        >
          <CartesianGrid stroke="var(--border)" horizontal={false} strokeDasharray="3 3" />
          <XAxis
            type="number"
            domain={[0, 100]}
            tick={{ fill: "var(--muted-foreground)", fontSize: 10 }}
            stroke="var(--border)"
            unit="%"
          />
          <YAxis
            type="category"
            dataKey="label"
            tick={{ fill: "var(--foreground)", fontSize: 11, fontWeight: 600 }}
            width={110}
            stroke="var(--border)"
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
            formatter={(value, name, entry) => {
              const d = (entry?.payload ?? {}) as { p1?: number; d2?: number; d3?: number };
              const p1 = d.p1 ?? 0;
              const d2 = d.d2 ?? 0;
              const d3 = d.d3 ?? 0;
              if (name === "勝率")    return [`${p1.toFixed(1)}%`, name];
              if (name === "連対率")  return [`${(p1 + d2).toFixed(1)}%`, name];
              if (name === "複勝率")  return [`${(p1 + d2 + d3).toFixed(1)}%`, name];
              const v = typeof value === "number" ? value : Number(value) || 0;
              return [`${v.toFixed(1)}%`, name];
            }}
          />
          <Legend
            wrapperStyle={{ fontSize: 11, color: "var(--muted-foreground)" }}
            iconType="square"
          />
          {/* スタック: 勝率（金）→ 連対率差分（青）→ 複勝率差分（灰） */}
          <Bar dataKey="p1" name="勝率" stackId="p" fill={colors.brand.gold} radius={[4, 0, 0, 4]} />
          <Bar dataKey="d2" name="連対率" stackId="p" fill={colors.chart.c2} />
          <Bar dataKey="d3" name="複勝率" stackId="p" fill="var(--muted)" radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
