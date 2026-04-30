import { useState, lazy, Suspense } from "react";
import { useSearchParams } from "react-router-dom";
import { useResultsSummary, useResultsTrend, useResultsDetailed, useSanrentanSummary, useHybridSummary } from "@/api/hooks";
import { SummaryCards } from "./ResultsPage/SummaryCards";
import { DetailedAnalysis } from "./ResultsPage/DetailedAnalysis";
import { PastPredictions } from "./ResultsPage/PastPredictions";
import { SummaryCardsSkeleton, ChartSkeleton } from "@/components/ui/premium/Skeleton";

// v6.1.22: TrendCharts は recharts を含み重いので遅延ロード。
// Results ページの Summary カードは即時表示され、チャートは裏で読み込まれる。
const TrendCharts = lazy(() =>
  import("./ResultsPage/TrendCharts").then((m) => ({ default: m.TrendCharts })),
);

const YEARS = (() => {
  const cur = new Date().getFullYear();
  const list: string[] = ["all"];
  for (let y = cur; y >= 2024; y--) list.push(String(y));
  return list;
})();

export default function ResultsPage() {
  const [year, setYear] = useState("all");

  // CalendarPage からの遷移: ?date=YYYY-MM-DD クエリパラメータを PastPredictions の初期選択日として渡す
  const [searchParams] = useSearchParams();
  const initialDate = searchParams.get("date") ?? undefined;

  const { data: summary, isLoading: loadingSummary } = useResultsSummary(year);
  const { data: trend } = useResultsTrend(year);
  const { data: detailed } = useResultsDetailed(year);
  // 三連単フォーメーション成績（Phase 3 / マスター指示 2026-04-22）
  const { data: sanrentan } = useSanrentanSummary(year);
  // 新戦略ハイブリッド成績（三連複動的 + 単勝 T-4 / A-NONE 2券種）
  const { data: hybrid } = useHybridSummary(year);

  const summaryData = summary as Record<string, unknown> | undefined;
  const trendData = trend as Record<string, unknown> | undefined;
  const detailedData = detailed as Record<string, unknown> | undefined;
  const sanrentanData = sanrentan ?? null;
  const hybridData = hybrid ?? null;

  return (
    <div className="space-y-4">
      {/* ページヘッダー */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div>
          <div className="section-eyebrow mb-1">Performance Analytics</div>
          <h1 className="heading-display text-2xl border-b border-brand-gold/30 pb-1">成績・実績</h1>
        </div>
        {/* 年タブ — セグメントコントロール風 */}
        <div
          role="tablist"
          aria-label="期間フィルタ"
          className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg shadow-[var(--shadow-xs)] overflow-x-auto"
        >
          {YEARS.map((y) => {
            const active = year === y;
            return (
              <button
                key={y}
                role="tab"
                aria-selected={active}
                onClick={() => setYear(y)}
                className={[
                  "px-3 py-1 text-xs font-semibold rounded-md whitespace-nowrap tnum",
                  "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
                  active
                    ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                    : "text-muted-foreground hover:text-foreground hover:bg-background/60",
                ].join(" ")}
              >
                {y === "all" ? "全期間" : y + "年"}
              </button>
            );
          })}
        </div>
      </div>

      {/* v6.1.6: 読み込み中は skeleton を表示（234 秒級の API が返るまで視覚的フィードバック） */}
      {loadingSummary && <SummaryCardsSkeleton />}

      {/* サマリーカード（上段: 単勝ベース / 中段: 三連単F(旧) / 下段: 新戦略ハイブリッド） */}
      {summaryData && <SummaryCards data={summaryData} sanrentan={sanrentanData} hybrid={hybridData} />}

      {/* データなし */}
      {summaryData && !summaryData.total_races && (
        <p className="text-sm text-muted-foreground py-4 text-center">
          {year === "all" ? "成績データ" : year + "年の成績データ"}はありません
        </p>
      )}

      {/* チャート（単勝+三連単F+新戦略ハイブリッド推移） */}
      {trendData ? (
        <Suspense fallback={<ChartSkeleton count={4} />}>
          <TrendCharts data={trendData} sanrentan={sanrentanData} hybrid={hybridData} />
        </Suspense>
      ) : (
        loadingSummary && <ChartSkeleton count={4} />
      )}

      {/* 詳細分析（左=単勝 / 右=三連単F の並び） */}
      {detailedData && <DetailedAnalysis data={detailedData} sanrentan={sanrentanData} />}

      {/* 過去予想カレンダー（CalendarPage からクエリ ?date= で初期選択日を受け取る） */}
      <PastPredictions initialDate={initialDate} />
    </div>
  );
}
