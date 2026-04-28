/**
 * 過去予想カレンダー
 * カレンダーUIで日付を選択→その日の予想を会場別に表示
 */
import { useState, useEffect, useCallback, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/client";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { CalendarDays } from "lucide-react";
import { Button } from "@/components/ui/button";
import { RaceCard, computeWinPctRanks } from "@/components/keiba/RaceCard";
import { RaceDetailView } from "@/pages/TodayPage/RaceDetailView";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { StatsCard } from "@/components/keiba/StatsCard";

// 日次統計の型
interface DailyStat {
  races: number;
  profit: number;
  roi: number;
  win: number;
  place2: number;
  placed: number;
  total: number;
  rate: number;
}

// ツールチップ
function CalendarTooltip({
  stat,
  cellRef,
}: {
  stat: DailyStat;
  cellRef: HTMLElement | null;
}) {
  if (!cellRef) return null;
  const rect = cellRef.getBoundingClientRect();
  const second = stat.place2 - stat.win;
  const third = stat.placed - stat.place2;
  const out = stat.total - stat.placed;
  const profit = stat.profit;
  const profitColor = profit >= 0 ? "text-emerald-400" : "text-red-400";
  const roiColor = stat.roi >= 100 ? "text-emerald-400" : "text-red-400";

  return (
    <div
      className="fixed z-50 pointer-events-none"
      style={{
        left: rect.left + rect.width / 2,
        top: rect.top - 4,
        transform: "translate(-50%, -100%)",
      }}
    >
      <div className="bg-gray-900 text-white text-[11px] rounded-lg px-3 py-2 shadow-xl whitespace-nowrap leading-relaxed">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="text-gray-400">{stat.races}R</span>
          <span className="font-bold">
            <span className="text-emerald-300">{stat.win}</span>
            <span className="text-gray-500">-</span>
            {second}
            <span className="text-gray-500">-</span>
            {third}
            <span className="text-gray-500">-</span>
            <span className="text-gray-500">{out}</span>
          </span>
        </div>
        <div className="flex items-center gap-3">
          <span className={`font-bold ${profitColor}`}>
            {profit >= 0 ? "+" : ""}{profit.toLocaleString()}円
          </span>
          <span className={`${roiColor}`}>
            回収{stat.roi.toFixed(0)}%
          </span>
          <span className="text-gray-400">
            複勝{stat.rate.toFixed(0)}%
          </span>
        </div>
        {/* 吹き出し矢印 */}
        <div className="absolute left-1/2 -translate-x-1/2 top-full w-0 h-0 border-l-[5px] border-l-transparent border-r-[5px] border-r-transparent border-t-[5px] border-t-gray-900" />
      </div>
    </div>
  );
}

// カレンダーコンポーネント
function Calendar({
  year,
  month,
  predDates,
  dailyStats,
  selectedDate,
  onSelect,
  onPrev,
  onNext,
}: {
  year: number;
  month: number;
  predDates: Set<string>;
  dailyStats: Record<string, DailyStat>;
  selectedDate: string | null;
  onSelect: (d: string) => void;
  onPrev: () => void;
  onNext: () => void;
}) {
  const dayNames = ["日", "月", "火", "水", "木", "金", "土"];
  const firstDay = new Date(year, month, 1).getDay();
  const lastDate = new Date(year, month + 1, 0).getDate();
  const [hoverDate, setHoverDate] = useState<string | null>(null);
  // callback ref 方式: hoverRef.current をレンダー中に読む React 19 危険パターンを回避
  const [hoverEl, setHoverEl] = useState<HTMLDivElement | null>(null);

  const cells: { day: number; dateStr: string; hasPred: boolean }[] = [];
  for (let d = 1; d <= lastDate; d++) {
    const ds = `${year}-${String(month + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    cells.push({ day: d, dateStr: ds, hasPred: predDates.has(ds) });
  }

  const hoverStat = hoverDate ? dailyStats[hoverDate] : null;

  return (
    <div>
      {/* ナビゲーション */}
      <div className="flex items-center justify-center gap-3 mb-2">
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onPrev}>
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <span className="text-[15px] font-bold min-w-[120px] text-center">
          {year}年{month + 1}月
        </span>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={onNext}>
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>

      {/* カレンダーグリッド */}
      <div className="grid grid-cols-7 gap-[2px] mb-3">
        {/* 曜日ヘッダー */}
        {dayNames.map((dn, i) => (
          <div
            key={dn}
            className={`text-[11px] font-bold text-center py-1 ${
              i === 0 ? "text-red-400" : i === 6 ? "text-blue-400" : "text-muted-foreground"
            }`}
          >
            {dn}
          </div>
        ))}

        {/* 空セル（月初の曜日調整） */}
        {Array.from({ length: firstDay }, (_, i) => (
          <div key={`e${i}`} className="text-center py-1.5" />
        ))}

        {/* 日付セル */}
        {cells.map(({ day, dateStr, hasPred }) => {
          const isSelected = selectedDate === dateStr;
          const dow = (firstDay + day - 1) % 7;
          const stat = dailyStats[dateStr];
          // 収支インジケータ（小さいドット）
          const dotColor = stat
            ? stat.profit >= 0
              ? "bg-emerald-400"
              : "bg-red-400"
            : "";
          return (
            <div
              key={day}
              ref={hoverDate === dateStr ? setHoverEl : undefined}
              onClick={hasPred ? () => onSelect(dateStr) : undefined}
              onMouseEnter={stat ? () => setHoverDate(dateStr) : undefined}
              onMouseLeave={() => setHoverDate(null)}
              className={`text-center py-1.5 rounded text-[13px] transition-colors relative ${
                isSelected
                  ? "bg-primary text-primary-foreground font-bold"
                  : hasPred
                    ? "bg-muted/50 font-semibold cursor-pointer hover:bg-primary hover:text-primary-foreground"
                    : `text-muted-foreground/40 ${dow === 0 ? "text-red-300/40" : dow === 6 ? "text-blue-300/40" : ""}`
              }`}
            >
              {day}
              {stat && (
                <div className={`absolute bottom-0.5 left-1/2 -translate-x-1/2 w-1 h-1 rounded-full ${dotColor}`} />
              )}
            </div>
          );
        })}
      </div>

      {/* ツールチップ */}
      {hoverStat && <CalendarTooltip stat={hoverStat} cellRef={hoverEl} />}
    </div>
  );
}

// 会場タブ
function VenueTabs({
  venues,
  activeIdx,
  onSelect,
}: {
  venues: string[];
  activeIdx: number;
  onSelect: (i: number) => void;
}) {
  return (
    <div className="flex gap-1 flex-wrap mb-3">
      {venues.map((v, i) => (
        <Button
          key={v}
          size="sm"
          variant={i === activeIdx ? "default" : "outline"}
          className="h-7 text-xs"
          onClick={() => onSelect(i)}
        >
          {v}
        </Button>
      ))}
    </div>
  );
}

// PastPredictions コンポーネント: initialDate は CalendarPage からのクエリ遷移で渡される
export function PastPredictions({ initialDate }: { initialDate?: string }) {
  // initialDate があればその月を初期表示月にし、その日を選択済みにする
  const [calYear, setCalYear] = useState(() => {
    if (initialDate) return parseInt(initialDate.slice(0, 4), 10);
    return new Date().getFullYear();
  });
  const [calMonth, setCalMonth] = useState(() => {
    if (initialDate) return parseInt(initialDate.slice(5, 7), 10) - 1;
    return new Date().getMonth();
  });
  const [selectedDate, setSelectedDate] = useState<string | null>(initialDate ?? null);
  const [venueIdx, setVenueIdx] = useState(0);
  const [selectedRace, setSelectedRace] = useState<{
    venue: string;
    raceNo: number;
  } | null>(null);

  // 予想済み日付一覧を取得
  const { data: datesData } = useQuery({
    queryKey: ["resultsDates"],
    queryFn: () => api.resultsDates(),
    staleTime: 60 * 1000,
  });

  const predDatesSet = useMemo(() => {
    const dates = datesData?.dates || [];
    return new Set(dates);
  }, [datesData]);

  const dailyStats = useMemo(() => {
    return (datesData?.daily_stats || {}) as Record<string, DailyStat>;
  }, [datesData]);

  // 初期カレンダー月を最新データに合わせる
  // 同じ値の場合は setState しないガードを入れて不要な再レンダリングを防ぐ
  useEffect(() => {
    if (!datesData?.dates?.length) return;
    const now = new Date();
    const curYear = now.getFullYear();
    const datesThisYear = datesData.dates.filter((d: string) =>
      d.startsWith(String(curYear))
    );
    if (datesThisYear.length > 0) {
      setCalYear((prev) => (prev === curYear ? prev : curYear));
      setCalMonth((prev) => {
        const m = now.getMonth();
        return prev === m ? prev : m;
      });
    } else {
      // 当年データなし→最新データの年月に移動
      const latest = datesData.dates[0]; // 新しい順
      if (latest) {
        const y = parseInt(latest.substring(0, 4), 10);
        const m = parseInt(latest.substring(5, 7), 10) - 1;
        setCalYear((prev) => (prev === y ? prev : y));
        setCalMonth((prev) => (prev === m ? prev : m));
      }
    }
  }, [datesData]);

  // 選択日の予想データ
  const { data: predData, isLoading: loadingPred } = useQuery({
    queryKey: ["pastPrediction", selectedDate],
    queryFn: () => api.todayPredictions(selectedDate!),
    enabled: !!selectedDate,
    staleTime: 5 * 60 * 1000,
  });

  const venues = predData?.order || Object.keys(predData?.races || {});
  const races =
    (predData?.races as Record<string, RaceItem[]>)?.[venues[venueIdx]] || [];

  const handlePrev = useCallback(() => {
    setCalMonth((m) => {
      if (m <= 0) {
        setCalYear((y) => y - 1);
        return 11;
      }
      return m - 1;
    });
  }, []);

  const handleNext = useCallback(() => {
    setCalMonth((m) => {
      if (m >= 11) {
        setCalYear((y) => y + 1);
        return 0;
      }
      return m + 1;
    });
  }, []);

  const handleDateSelect = useCallback((d: string) => {
    setSelectedDate(d);
    setVenueIdx(0);
    setSelectedRace(null);
  }, []);

  // レース詳細表示中
  if (selectedRace && selectedDate) {
    return (
      <RaceDetailView
        date={selectedDate}
        venue={selectedRace.venue}
        raceNo={selectedRace.raceNo}
        venues={venues}
        onClose={() => setSelectedRace(null)}
        onNavigate={(venue, raceNo) => setSelectedRace({ venue, raceNo })}
      />
    );
  }

  return (
    <PremiumCard variant="default" padding="md">
      <PremiumCardHeader>
        <div className="flex flex-col gap-0.5">
          <PremiumCardAccent>
            <CalendarDays size={10} className="inline mr-1" />
            Past Predictions
          </PremiumCardAccent>
          <PremiumCardTitle className="text-base">過去予想</PremiumCardTitle>
        </div>
      </PremiumCardHeader>

      <Calendar
        year={calYear}
        month={calMonth}
        predDates={predDatesSet}
        dailyStats={dailyStats}
        selectedDate={selectedDate}
        onSelect={handleDateSelect}
        onPrev={handlePrev}
        onNext={handleNext}
      />

      {/* 選択日の予想内容 */}
      {selectedDate && (
        <div>
          <div className="text-[15px] font-bold mt-3 mb-2">
            {selectedDate} の予想
          </div>

          {/* T-031 (2026-04-28): 選択日成績カード追加。過去日は更新ボタン非表示 */}
          <StatsCard
            date={selectedDate}
            title={`${selectedDate} の成績`}
            showRefreshButton={false}
          />

          {loadingPred && (
            <p className="text-sm text-muted-foreground py-4 text-center">
              読み込み中...
            </p>
          )}

          {!loadingPred && venues.length === 0 && (
            <p className="text-sm text-muted-foreground py-4 text-center">
              この日の予想データはありません。
            </p>
          )}

          {!loadingPred && venues.length > 0 && (
            <>
              <VenueTabs
                venues={venues}
                activeIdx={venueIdx}
                onSelect={setVenueIdx}
              />
              <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-3">
                {(() => {
                  const rankMap = computeWinPctRanks(races as unknown as { race_no: number; honmei_win_pct?: number }[]);
                  return races.map((r: RaceItem) => (
                    <RaceCard
                      key={r.race_no}
                      race={r as unknown as Parameters<typeof RaceCard>[0]["race"]}
                      winPctRank={rankMap.get(r.race_no)}
                      onClick={() =>
                        setSelectedRace({
                          venue: venues[venueIdx],
                          raceNo: r.race_no,
                        })
                      }
                    />
                  ));
                })()}
              </div>
            </>
          )}
        </div>
      )}
    </PremiumCard>
  );
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type RaceItem = Record<string, any>;
