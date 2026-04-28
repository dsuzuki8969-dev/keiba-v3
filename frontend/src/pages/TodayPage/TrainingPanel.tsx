import { devGrade, gradeCls, WAKU_BG } from "@/lib/constants";
import { TrainingSection } from "./HorseDiagnosis";
import type { HorseData, TrainingRecord } from "./RaceDetailView";

interface Props {
  horses: HorseData[];
}

export function TrainingPanel({ horses }: Props) {
  const sorted = [...horses].sort((a, b) => (a.horse_no || 0) - (b.horse_no || 0));
  const withTraining = sorted.filter((h) => {
    const recs = (h as Record<string, unknown>).training_records as TrainingRecord[] | undefined;
    return recs && recs.length > 0;
  });

  if (withTraining.length === 0) {
    return <div className="text-sm text-muted-foreground py-4">調教データなし</div>;
  }

  return (
    <div className="space-y-3">
      {withTraining.map((h) => {
        const recs = (h as Record<string, unknown>).training_records as TrainingRecord[];
        const tDev = h.training_dev ?? 50;
        const grade = devGrade(tDev);

        return (
          <div key={h.horse_no} className="border border-border/50 rounded-md p-3">
            <div className="flex items-center gap-2 mb-2">
              <span className={`inline-flex w-7 h-7 items-center justify-center rounded-sm text-sm font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                {h.horse_no}
              </span>
              <span className="text-lg font-bold">{h.horse_name}</span>
              <span className={`text-base font-bold ${gradeCls(grade)}`}>
                調教{grade}
              </span>
              <span className="text-sm text-muted-foreground">({tDev.toFixed(1)})</span>
            </div>
            <TrainingSection records={recs} />
          </div>
        );
      })}
    </div>
  );
}
