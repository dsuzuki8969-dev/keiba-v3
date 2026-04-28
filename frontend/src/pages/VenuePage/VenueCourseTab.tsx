import { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { SurfaceBadge } from "@/components/keiba/SurfaceBadge";
import { BreakdownTable, diffColor } from "@/components/keiba/BreakdownTable";
import { useCourseStats } from "@/api/hooks";
import type { VenueProfileDetail } from "@/api/client";

function CourseDetail({ courseKey }: { courseKey: string }) {
  const { data, isLoading } = useCourseStats(courseKey);
  const [showOther, setShowOther] = useState(false);

  if (isLoading) return <div className="text-sm text-muted-foreground py-2">読み込み中...</div>;
  if (!data) return null;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d = data as any;

  return (
    <PremiumCard variant="default" padding="md" className="space-y-4 mt-2">
      {/* 基本情報 */}
      <div className="flex gap-4 flex-wrap items-baseline text-sm">
        <span>レース数: <strong>{d.race_count || 0}</strong></span>
        <span>出走のべ: <strong>{d.count || 0}</strong></span>
        {d.period?.min_date && d.period?.max_date && (
          <span className="text-xs text-muted-foreground">
            ({d.period.min_date} 〜 {d.period.max_date})
          </span>
        )}
      </div>

      {/* レコード */}
      {d.record && (
        <div className="text-sm">
          レコード: <strong>{d.record.time_str || "—"}</strong>
          <span className="text-xs text-muted-foreground ml-2">
            {d.record.date || ""} {d.record.class_name || ""}
          </span>
        </div>
      )}

      {/* コースの特徴 */}
      {d.course_description && (
        <div className="text-sm text-muted-foreground bg-muted/50 rounded p-2 leading-relaxed">
          <span className="font-semibold text-foreground">コースの特徴:</span>{" "}
          {d.course_description}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* 馬場状態別タイム */}
        {d.condition_diff && Object.keys(d.condition_diff).length > 0 && (
          <div>
            <h4 className="font-bold mb-1 text-sm">馬場状態別タイム（1〜3着平均）</h4>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs text-muted-foreground">
                    <th className="text-left py-1 px-1">馬場</th>
                    <th className="text-right py-1 px-1">件数</th>
                    <th className="text-right py-1 px-1">平均タイム</th>
                    <th className="text-right py-1 px-1">良比較</th>
                  </tr>
                </thead>
                <tbody>
                  {(["良", "稍重", "重", "不良"] as const).map((cond) => {
                    const v = d.condition_diff[cond];
                    if (!v) return null;
                    return (
                      <tr key={cond} className="border-b border-border/50">
                        <td className="py-1 px-1 font-semibold">{cond}</td>
                        <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                        <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                        <td className="text-right py-1 px-1 tabular-nums" style={{ color: diffColor(v.diff) }}>
                          {cond === "良" ? "基準" : v.diff != null ? `${v.diff > 0 ? "+" : ""}${v.diff.toFixed(1)}秒` : "—"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* 季節別タイム */}
        {d.season_diff && Object.keys(d.season_diff).length > 0 && (
          <div>
            <h4 className="font-bold mb-1 text-sm">季節別タイム（良馬場・1〜3着平均）</h4>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs text-muted-foreground">
                    <th className="text-left py-1 px-1">季節</th>
                    <th className="text-right py-1 px-1">件数</th>
                    <th className="text-right py-1 px-1">平均タイム</th>
                    <th className="text-right py-1 px-1">良全体比</th>
                  </tr>
                </thead>
                <tbody>
                  {([
                    ["春", "3〜5月"],
                    ["夏", "6〜8月"],
                    ["秋", "9〜11月"],
                    ["冬", "12〜2月"],
                  ] as const).map(([key, label]) => {
                    const v = d.season_diff[key];
                    if (!v) return null;
                    return (
                      <tr key={key} className="border-b border-border/50">
                        <td className="py-1 px-1 font-semibold">{key}<span className="text-xs text-muted-foreground ml-1">({label})</span></td>
                        <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                        <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                        <td className="text-right py-1 px-1 tabular-nums" style={{ color: diffColor(v.diff) }}>
                          {v.diff != null ? `${v.diff > 0 ? "+" : ""}${v.diff.toFixed(1)}秒` : "—"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      {/* 脚質別成績 */}
      {d.running_style && Object.keys(d.running_style).length > 0 && (
        <div>
          <h4 className="font-bold mb-1 text-sm">脚質別成績</h4>
          <BreakdownTable
            data={d.running_style}
            keyLabel="脚質"
            sortKeys={["逃げ", "先行", "差し", "追込"]}
            showRoi
          />
        </div>
      )}

      {/* 枠番別成績 */}
      {d.gate_bias && Object.keys(d.gate_bias).length > 0 && (
        <div>
          <h4 className="font-bold mb-1 text-sm">枠番別成績</h4>
          <BreakdownTable
            data={d.gate_bias}
            keyLabel="枠番"
            sortKeys={Object.keys(d.gate_bias).sort((a, b) => parseInt(a) - parseInt(b))}
            showRoi
          />
        </div>
      )}

      {/* クラス別成績 */}
      {d.class_avg && Object.keys(d.class_avg).length > 0 && (
        <div>
          <h4 className="font-bold mb-1 text-sm">クラス別成績</h4>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-xs text-muted-foreground">
                  <th className="text-left py-1 px-1">クラス</th>
                  <th className="text-right py-1 px-1">件数</th>
                  <th className="text-right py-1 px-1">平均タイム</th>
                  <th className="text-right py-1 px-1">前3F</th>
                  <th className="text-right py-1 px-1">上り3F</th>
                  <th className="text-right py-1 px-1">ペース</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(
                  d.class_avg as Record<string, { n: number; avg_str: string; first_3f?: number; last_3f?: number; pace_type?: string }>
                ).map(([cls, v]) => {
                  const isOther = cls.includes("その他");
                  if (isOther && v.n > 500 && !showOther) {
                    return (
                      <tr key={cls} className="border-b border-border/50">
                        <td colSpan={6} className="py-1 px-1 text-muted-foreground">
                          <button
                            className="text-xs underline hover:text-foreground"
                            onClick={() => setShowOther(true)}
                          >
                            {cls}（{v.n}件）を表示...
                          </button>
                        </td>
                      </tr>
                    );
                  }
                  return (
                    <tr key={cls} className={`border-b border-border/50 ${isOther ? "text-muted-foreground" : ""}`}>
                      <td className="py-1 px-1 font-semibold">{cls}</td>
                      <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                      <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                      <td className="text-right py-1 px-1 tabular-nums">{v.first_3f?.toFixed(1) ?? "—"}</td>
                      <td className="text-right py-1 px-1 tabular-nums">{v.last_3f?.toFixed(1) ?? "—"}</td>
                      <td className="text-right py-1 px-1">{v.pace_type ?? "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* 平均ペース */}
      {d.pace_avg && (
        <div className="text-sm">
          <span className="font-bold">平均ペース: </span>
          <span className="text-muted-foreground">
            前半3F {d.pace_avg.first_3f?.toFixed(1) || "—"}秒 / 後半3F {d.pace_avg.last_3f?.toFixed(1) || "—"}秒
            {d.pace_avg.pace_type && <span className="ml-2 font-semibold text-foreground">{d.pace_avg.pace_type}</span>}
          </span>
        </div>
      )}
    </PremiumCard>
  );
}

export function VenueCourseTab({ venue }: { venue: VenueProfileDetail }) {
  const [surfaceFilter, setSurfaceFilter] = useState<"all" | "芝" | "ダート">("all");
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const courses = venue.courses.filter(
    (c) => surfaceFilter === "all" || c.surface === surfaceFilter
  );

  return (
    <div className="space-y-3">
      <div className="flex gap-1">
        {(["all", "芝", "ダート"] as const).map((s) => (
          <Button
            key={s}
            variant={surfaceFilter === s ? "default" : "outline"}
            size="sm"
            onClick={() => setSurfaceFilter(s)}
          >
            {s === "all" ? "全て" : s}
          </Button>
        ))}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-muted-foreground">
              <th className="py-2 px-2">馬場</th>
              <th className="py-2 px-2">距離</th>
              <th className="py-2 px-2">方向</th>
              <th className="py-2 px-2">直線</th>
              <th className="py-2 px-2 hidden sm:table-cell">コーナー</th>
              <th className="py-2 px-2 hidden sm:table-cell">初角</th>
              <th className="py-2 px-2 hidden sm:table-cell">坂</th>
              <th className="py-2 px-2 hidden md:table-cell">内外</th>
              <th className="py-2 px-2 hidden md:table-cell">幅員</th>
            </tr>
          </thead>
          <tbody>
            {courses.map((c) => {
              const isOpen = selectedKey === c.course_id;
              return (
                <tr
                  key={c.course_id}
                  className={`border-b cursor-pointer hover:bg-muted/50 transition-colors ${isOpen ? "bg-muted/30" : ""}`}
                  onClick={() => setSelectedKey(isOpen ? null : c.course_id)}
                >
                  <td className="py-2 px-2"><SurfaceBadge surface={c.surface} /></td>
                  <td className="py-2 px-2 font-bold">{c.distance}m</td>
                  <td className="py-2 px-2">{c.direction}</td>
                  <td className="py-2 px-2">{c.straight_m}m</td>
                  <td className="py-2 px-2 hidden sm:table-cell">{c.corner_count} {c.corner_type}</td>
                  <td className="py-2 px-2 hidden sm:table-cell">
                    {c.first_corner}
                    {c.first_corner_m > 0 && (
                      <span className="text-xs text-muted-foreground ml-0.5">({c.first_corner_m}m)</span>
                    )}
                  </td>
                  <td className="py-2 px-2 hidden sm:table-cell">{c.slope_type}</td>
                  <td className="py-2 px-2 hidden md:table-cell">{c.inside_outside}</td>
                  <td className="py-2 px-2 hidden md:table-cell text-muted-foreground">{c.width_m || "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {selectedKey && <CourseDetail courseKey={selectedKey} />}
    </div>
  );
}
