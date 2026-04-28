import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  ReferenceArea,
} from "recharts";
import type { DotProps } from "recharts";
import { gradeFromDev } from "@/design/tokens";

// ────────────────────────────────────────────────────────────────
// 型定義
// ────────────────────────────────────────────────────────────────

export interface RunEntry {
  race_date: string;
  venue: string;
  race_name: string;
  grade?: string;
  distance: number;
  surface: string;
  field_count?: number;
  finish_pos?: number;
  jockey_name?: string;
  win_odds?: number;
  finish_time_sec?: number;
  run_dev: number;
}

interface Props {
  runs: RunEntry[];
  horseName?: string;
  isMobile?: boolean;
}

// ────────────────────────────────────────────────────────────────
// 偏差値ゾーン（背景帯）
// ────────────────────────────────────────────────────────────────

const ZONE_BANDS = [
  { min: 65, max: 100, fill: "rgba(224,179,74,0.12)" },  // SS
  { min: 60, max: 65,  fill: "rgba(210,210,100,0.10)" }, // S
  { min: 55, max: 60,  fill: "rgba(147,197,253,0.12)" }, // A
  { min: 50, max: 55,  fill: "rgba(156,163,175,0.10)" }, // B
  { min: 45, max: 50,  fill: "rgba(209,213,219,0.08)" }, // C
  { min: 20, max: 45,  fill: "rgba(245,245,245,0.06)" }, // D
] as const;

// グレード別ドット色（GI=金・GII=青・GIII=緑、他=デフォルト）
function dotColor(grade: string | undefined): string {
  if (!grade) return "#6b7280";
  if (grade === "G1" || grade === "GⅠ") return "#d4a853";
  if (grade === "Jpn1") return "#dc2626";
  if (grade === "G2" || grade === "GⅡ" || grade === "Jpn2") return "#3b82f6";
  if (grade === "G3" || grade === "GⅢ" || grade === "Jpn3") return "#10b981";
  return "#6b7280";
}

// ────────────────────────────────────────────────────────────────
// カスタムドット（重賞は大きく・色付き）
// ────────────────────────────────────────────────────────────────

function CustomDot(props: DotProps & { payload?: RunEntry }) {
  const { cx, cy, payload } = props;
  const grade = payload?.grade ?? "";
  const isGrade = !!(grade && (grade.startsWith("G") || grade.startsWith("Jpn")));
  const color = dotColor(grade);
  const r = isGrade ? 5 : 3;
  if (cx == null || cy == null) return null;
  return (
    <circle
      cx={cx}
      cy={cy}
      r={r}
      fill={color}
      stroke="white"
      strokeWidth={1.5}
    />
  );
}

// ────────────────────────────────────────────────────────────────
// カスタムツールチップ
// ────────────────────────────────────────────────────────────────

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload: RunEntry }>;
}) {
  if (!active || !payload?.length) return null;
  const r = payload[0].payload;
  const grade = gradeFromDev(r.run_dev);
  // A案（2026-04-26）: E グレードを追加（真の大敗 <35）
  const gradeLabel: Record<"SS" | "S" | "A" | "B" | "C" | "D" | "E", string> = {
    SS: "text-amber-500", S: "text-yellow-500",
    A: "text-blue-400", B: "text-gray-400",
    C: "text-gray-400", D: "text-gray-500",
    E: "text-blue-300",  // 真の大敗（マイナス偏差値）= 暗青
  };
  const surf = r.surface === "ダート" ? "ダ" : r.surface === "芝" ? "芝" : (r.surface || "");
  return (
    <div className="bg-popover border border-border rounded-md shadow-lg px-3 py-2 text-[12px] min-w-[160px]">
      <div className="font-bold text-foreground mb-1 flex items-center gap-1">
        {r.race_name}
        {r.grade && (
          <span className={`text-[10px] font-bold ${dotColor(r.grade) === "#d4a853" ? "text-amber-500" : dotColor(r.grade) === "#2563eb" ? "text-blue-500" : "text-emerald-500"}`}>
            {r.grade}
          </span>
        )}
      </div>
      <div className="text-muted-foreground">{r.race_date.slice(2).replace(/-/g,"/")} {r.venue} {surf}{r.distance}m</div>
      <div className="flex justify-between mt-1 gap-3">
        <span>着順 <span className="text-foreground font-bold">{r.finish_pos ?? "—"}</span>/{r.field_count ?? "—"}頭</span>
        <span>
          走破偏差値
          <span className={`ml-1 font-bold ${gradeLabel[grade]}`}>{r.run_dev.toFixed(1)}</span>
          <span className="text-muted-foreground ml-1 text-[10px]">({grade})</span>
        </span>
      </div>
      {r.jockey_name && (
        <div className="text-muted-foreground mt-0.5">騎手: {r.jockey_name}</div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────
// メインコンポーネント
// ────────────────────────────────────────────────────────────────

export function HorseHistoryChart({ runs, horseName, isMobile = false }: Props) {
  // run_dev が null の行は除外し、3戦未満はグラフ非表示
  const validRuns = (runs || []).filter((r) => r.run_dev != null);
  if (validRuns.length < 3) return null;

  const height = isMobile ? 160 : 200;

  // API は古い順（ASC）で返すのでそのまま使う
  const data = validRuns;

  const tickFormatter = (val: string) => val.slice(2).replace(/-/g, "/");

  return (
    <div className="mb-3">
      <div className="text-[11px] font-bold text-muted-foreground mb-1 border-l-[3px] border-amber-500 pl-2">
        走破偏差値推移
        {horseName && <span className="text-foreground ml-1.5">{horseName}</span>}
      </div>
      <div style={{ height }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 4, right: 8, left: -18, bottom: 0 }}>
            {/* 偏差値ゾーン背景帯 */}
            {ZONE_BANDS.map((z) => (
              <ReferenceArea
                key={`zone-${z.min}-${z.max}`}
                y1={z.min}
                y2={z.max}
                fill={z.fill}
                ifOverflow="hidden"
              />
            ))}

            <CartesianGrid strokeDasharray="3 3" stroke="rgba(156,163,175,0.2)" />
            <XAxis
              dataKey="race_date"
              tickFormatter={tickFormatter}
              tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              domain={[20, 100]}
              ticks={[20, 45, 50, 55, 60, 65, 80, 100]}
              tick={{ fontSize: 10, fill: "var(--muted-foreground)" }}
              axisLine={false}
              tickLine={false}
              width={36}
            />
            {/* 偏差値帯境界線 */}
            <ReferenceLine y={65} stroke="rgba(224,179,74,0.5)" strokeDasharray="4 2" label={{ value: "SS", fontSize: 9, fill: "rgba(224,179,74,0.7)", position: "insideTopRight" }} />
            <ReferenceLine y={60} stroke="rgba(210,210,100,0.4)" strokeDasharray="4 2" />
            <ReferenceLine y={55} stroke="rgba(147,197,253,0.4)" strokeDasharray="4 2" />
            <ReferenceLine y={50} stroke="rgba(156,163,175,0.4)" strokeDasharray="4 2" label={{ value: "50", fontSize: 9, fill: "rgba(156,163,175,0.5)", position: "insideTopRight" }} />
            <ReferenceLine y={45} stroke="rgba(209,213,219,0.4)" strokeDasharray="4 2" />

            <Tooltip content={<CustomTooltip />} />

            <Line
              type="monotone"
              dataKey="run_dev"
              stroke="#f59e0b"
              strokeWidth={2}
              dot={<CustomDot />}
              activeDot={{ r: 6, fill: "#f59e0b", stroke: "white", strokeWidth: 2 }}
              connectNulls={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
