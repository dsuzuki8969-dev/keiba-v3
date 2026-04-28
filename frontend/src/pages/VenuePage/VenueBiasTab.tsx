import { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { BreakdownTable } from "@/components/keiba/BreakdownTable";
import { useVenueBias } from "@/api/hooks";

export function VenueBiasTab({ code }: { code: string }) {
  const { data, isLoading } = useVenueBias(code);
  const [courseFilter, setCourseFilter] = useState<string>("all");

  if (isLoading) return <div className="text-sm text-muted-foreground py-4">読み込み中...</div>;
  if (!data) return <div className="text-sm text-muted-foreground py-4">データなし</div>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d = data as any;

  // コース別データの取得
  const perCourse: Record<string, { surface: string; distance: number; count: number; gate_bias: Record<string, Record<string, number>>; running_style: Record<string, Record<string, number>> }> = d.per_course || {};
  const courseKeys = Object.keys(perCourse).sort((a, b) => {
    const pa = perCourse[a], pb = perCourse[b];
    if (pa.surface !== pb.surface) return pa.surface.localeCompare(pb.surface);
    return pa.distance - pb.distance;
  });

  // 表示対象のデータ
  const gateBias = courseFilter === "all" ? (d.gate_bias || {}) : (perCourse[courseFilter]?.gate_bias || {});
  const runningStyle = courseFilter === "all" ? (d.running_style || {}) : (perCourse[courseFilter]?.running_style || {});

  const surfLabel = (s: string) => s === "芝" ? "text-emerald-600" : "text-amber-700";

  return (
    <div className="space-y-4">
      {/* コース切替 */}
      <div className="flex flex-wrap gap-1">
        <Button
          variant={courseFilter === "all" ? "default" : "outline"}
          size="sm"
          onClick={() => setCourseFilter("all")}
        >
          全コース合計
        </Button>
        {courseKeys.map((k) => {
          const c = perCourse[k];
          return (
            <Button
              key={k}
              variant={courseFilter === k ? "default" : "outline"}
              size="sm"
              onClick={() => setCourseFilter(k)}
            >
              <span className={surfLabel(c.surface)}>{c.surface}</span>
              <span className="ml-0.5">{c.distance}m</span>
            </Button>
          );
        })}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* 枠番別成績 */}
        <PremiumCard variant="default" padding="md" className="space-y-3">
          <h2 className="font-bold text-base">枠番別成績</h2>
          {Object.keys(gateBias).length > 0 ? (
            <BreakdownTable
              data={gateBias}
              keyLabel="枠番"
              sortKeys={Object.keys(gateBias).sort((a, b) => parseInt(a) - parseInt(b))}
              showRoi
            />
          ) : (
            <div className="text-sm text-muted-foreground">データなし</div>
          )}
        </PremiumCard>

        {/* 脚質別成績 */}
        <PremiumCard variant="default" padding="md" className="space-y-3">
          <h2 className="font-bold text-base">脚質別成績</h2>
          {Object.keys(runningStyle).length > 0 ? (
            <BreakdownTable
              data={runningStyle}
              keyLabel="脚質"
              sortKeys={["逃げ", "先行", "差し", "追込"]}
              showRoi
            />
          ) : (
            <div className="text-sm text-muted-foreground">データなし</div>
          )}
        </PremiumCard>
      </div>

      {/* 人気別成績（全コース合計のみ） */}
      {courseFilter === "all" && d.popularity && Object.keys(d.popularity).length > 0 && (
        <PremiumCard variant="default" padding="md" className="space-y-3">
          <h2 className="font-bold text-base">人気別成績</h2>
          <BreakdownTable
            data={d.popularity}
            keyLabel="人気"
            sortKeys={Object.keys(d.popularity).sort((a, b) => {
              const na = a.includes("+") ? 99 : parseInt(a);
              const nb = b.includes("+") ? 99 : parseInt(b);
              return na - nb;
            })}
            showRoi
          />
        </PremiumCard>
      )}
    </div>
  );
}
