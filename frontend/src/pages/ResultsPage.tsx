import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useResultsSummary, useResultsDetailed } from "@/api/hooks";
import { SummaryCards } from "./ResultsPage/SummaryCards";
import { DetailedAnalysis } from "./ResultsPage/DetailedAnalysis";
import { PastPredictions } from "./ResultsPage/PastPredictions";
import { SummaryCardsSkeleton } from "@/components/ui/premium/Skeleton";
import { PremiumCard, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { TrendingUp } from "lucide-react";

const YEARS = (() => {
  const cur = new Date().getFullYear();
  const list: string[] = ["all"];
  for (let y = cur; y >= 2024; y--) list.push(String(y));
  return list;
})();

export default function ResultsPage() {
  const [year, setYear] = useState("all");
  // 全体/JRA/NAR・競馬場 の選択を SummaryCards と DetailedAnalysis で共有(カード連動)
  const [cat, setCat] = useState("all");
  const [selectedVenue, setSelectedVenue] = useState<string | null>(null);

  // CalendarPage からの遷移: ?date=YYYY-MM-DD クエリパラメータを PastPredictions の初期選択日として渡す
  const [searchParams] = useSearchParams();
  const initialDate = searchParams.get("date") ?? undefined;

  const { data: summary, isLoading: loadingSummary } = useResultsSummary(year);
  const { data: detailed } = useResultsDetailed(year);

  const summaryData = summary as Record<string, unknown> | undefined;
  const detailedData = detailed as Record<string, unknown> | undefined;

  return (
    <div className="space-y-8 max-w-5xl mx-auto">
      {/* ページヘッダー — AboutPage 統一ヒーロー */}
      <PremiumCard variant="gold" padding="lg">
        <div className="relative overflow-hidden">
          {/* 背景装飾（AboutPage と同じ gradient blob） */}
          <div className="absolute -top-20 -right-20 w-60 h-60 rounded-full bg-gradient-to-br from-brand-gold/10 to-transparent blur-3xl pointer-events-none" />
          <div className="absolute -bottom-16 -left-16 w-48 h-48 rounded-full bg-gradient-to-tr from-blue-500/5 to-transparent blur-2xl pointer-events-none" />

          <div className="relative flex items-center justify-between flex-wrap gap-4">
            <div className="space-y-2">
              <PremiumCardAccent>
                <TrendingUp size={10} className="inline mr-1" />
                Performance Analytics
              </PremiumCardAccent>
              <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight text-foreground leading-tight">
                成績・実績
              </h1>
            </div>
            {/* 年タブ — セグメントコントロール風（既存スタイル維持） */}
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
        </div>
      </PremiumCard>

      {/* v6.1.6: 読み込み中は skeleton を表示（234 秒級の API が返るまで視覚的フィードバック） */}
      {loadingSummary && <SummaryCardsSkeleton />}

      {/* サマリーカード（的中率ヒーロー: 勝率・連対率・複勝率）— cat(全体/JRA/NAR)連動 */}
      {summaryData && <SummaryCards data={summaryData} detailed={detailedData} cat={cat} selectedVenue={selectedVenue} />}

      {/* データなし */}
      {summaryData && !summaryData.total_races && (
        <p className="text-sm text-muted-foreground py-4 text-center">
          {year === "all" ? "成績データ" : year + "年の成績データ"}はありません
        </p>
      )}

      {/* 詳細分析 (単勝ベース) — タブ(全体/JRA/NAR)は上部カードと共有 */}
      {detailedData && <DetailedAnalysis data={detailedData} cat={cat} setCat={setCat} selectedVenue={selectedVenue} setSelectedVenue={setSelectedVenue} />}

      {/* 過去予想カレンダー（CalendarPage からクエリ ?date= で初期選択日を受け取る） */}
      <PastPredictions initialDate={initialDate} />
    </div>
  );
}
