import { useState, useCallback, useEffect } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { useTodayPredictions, useHomeInfo } from "@/api/hooks";
import { localDate } from "@/lib/constants";
import { VenueTabs } from "@/components/keiba/VenueTabs";
import { RaceCard, computeWinPctRanks } from "@/components/keiba/RaceCard";
import { RaceDetailView } from "./TodayPage/RaceDetailView";
import { OperationsPanel } from "./TodayPage/OperationsPanel";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

export default function TodayPage() {
  const location = useLocation();
  const navState = location.state as { date?: string; venue?: string; raceNo?: number } | null;

  // CalendarPage からの遷移: ?date=YYYY-MM-DD クエリパラメータを初期日付として受け取る
  const [searchParams] = useSearchParams();
  const queryDate = searchParams.get("date");

  const [date, setDate] = useState(() => queryDate || localDate());
  const [dateInput, setDateInput] = useState(() => queryDate || localDate());
  const [venueIdx, setVenueIdx] = useState(0);
  const [selectedRace, setSelectedRace] = useState<{
    venue: string;
    raceNo: number;
  } | null>(null);

  // クエリパラメータ ?date= が変化したとき（ブラウザ戻る/進む等）に日付を同期
  useEffect(() => {
    if (queryDate) {
      setDate(queryDate);
      setDateInput(queryDate);
      setVenueIdx(0);
      setSelectedRace(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryDate]);

  // HomePageや過去成績からの遷移: state に date/venue/raceNo があればレース詳細を開く
  useEffect(() => {
    if (navState?.venue && navState?.raceNo) {
      if (navState.date) {
        setDate(navState.date);
        setDateInput(navState.date);
      }
      setSelectedRace({ venue: navState.venue, raceNo: navState.raceNo });
    }
    // location.key で遷移ごとに発火させる
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.key]);

  const { data: pred, isLoading, error, refetch } = useTodayPredictions(date);
  const { data: info } = useHomeInfo(date);

  const venues = pred?.order || [];
  const currentVenue = venues[venueIdx] || "";
  const races = (pred?.races as Record<string, RaceSummaryItem[]>)?.[currentVenue] || [];

  // 天気情報
  const weather =
    info?.weather && currentVenue
      ? (info.weather as Record<string, WeatherInfo>)[currentVenue]
      : null;

  // オッズ更新タイムスタンプ
  const oddsTs = pred?.odds_updated_at as string | undefined;

  const handleDateChange = useCallback(() => {
    setDate(dateInput);
    setVenueIdx(0);
    setSelectedRace(null);
  }, [dateInput]);

  const openRace = useCallback(
    (venue: string, raceNo: number) => {
      setSelectedRace({ venue, raceNo });
    },
    []
  );

  const closeDetail = useCallback(() => {
    setSelectedRace(null);
  }, []);

  // レース詳細表示中
  if (selectedRace) {
    return (
      <RaceDetailView
        date={date}
        venue={selectedRace.venue}
        raceNo={selectedRace.raceNo}
        venues={venues}
        venueRaces={pred?.races as Record<string, RaceSummaryItem[]>}
        oddsUpdatedAt={oddsTs}
        onClose={closeDetail}
        onNavigate={openRace}
      />
    );
  }

  return (
    <div className="space-y-4">
      {/* 日付選択バー */}
      <div className="flex items-center gap-3 flex-wrap">
        <Input
          type="date"
          value={dateInput}
          onChange={(e) => setDateInput(e.target.value)}
          className="w-40"
        />
        <Button onClick={handleDateChange} size="sm">
          表示
        </Button>
        <Button onClick={() => refetch()} variant="outline" size="sm">
          更新
        </Button>
        <span className="stat-mono text-sm text-foreground">{date}</span>
        {oddsTs && (
          <span className="text-xs text-muted-foreground ml-auto">
            最終オッズ取得 <span className="stat-mono">{oddsTs.slice(11, 16)}</span>
          </span>
        )}
        {!oddsTs && pred && (
          <span className="text-xs text-muted-foreground ml-auto">
            ※予測オッズのみ
          </span>
        )}
      </div>

      {/* 読み込み中 */}
      {isLoading && (
        <p className="text-sm text-muted-foreground py-8 text-center">
          読み込み中...
        </p>
      )}

      {/* エラー */}
      {error && (
        <p className="text-sm text-destructive py-4">
          エラー: {(error as Error).message}
        </p>
      )}

      {/* データなし */}
      {pred && !venues.length && (
        <p className="text-sm text-muted-foreground py-8 text-center">
          {date} の予想データはありません
        </p>
      )}

      {/* 会場タブ */}
      {venues.length > 0 && (
        <>
          <VenueTabs
            venues={venues}
            activeIndex={venueIdx}
            onChange={setVenueIdx}
          />

          {/* 天気バー */}
          {weather && (
            <div className="flex items-center gap-3 text-sm px-2 py-1.5 bg-muted rounded-md">
              <span className="font-semibold">{currentVenue}</span>
              {weather.condition && <span>{weather.condition}</span>}
              {weather.precip_prob != null && (
                <span className="text-info">降水 {weather.precip_prob}%</span>
              )}
            </div>
          )}

          {/* レースカード一覧 */}
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {(() => {
              const rankMap = computeWinPctRanks(races);
              return races.map((r: RaceSummaryItem) => (
                <RaceCard
                  key={r.race_no}
                  race={r}
                  winPctRank={rankMap.get(r.race_no)}
                  onClick={() => openRace(currentVenue, r.race_no)}
                />
              ));
            })()}
          </div>

          {/* 操作パネル（admin のみ） */}
          <OperationsPanel
            date={date}
            venues={venues}
            onAnalyzeComplete={() => refetch()}
          />
        </>
      )}
    </div>
  );
}

// API応答の型（home.js互換）
interface RaceSummaryItem {
  race_no: number;
  name?: string;
  race_name?: string;
  post_time?: string;
  surface?: string;
  distance?: number;
  head_count?: number;
  grade?: string;
  overall_confidence?: string;
  honmei_name?: string;
  honmei_mark?: string;
  url?: string;
  [key: string]: unknown;
}

interface WeatherInfo {
  condition?: string;
  precip_prob?: number;
}
