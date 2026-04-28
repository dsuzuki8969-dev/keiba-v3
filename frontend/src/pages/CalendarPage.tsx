/**
 * T-038 Phase 3: 開催カレンダーページ
 * kaisai_calendar.json を可視化。月別グリッドで JRA / NAR 開催日を表示。
 * PC: 7列グリッド / モバイル: 縦リスト
 */
import { useState, useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { ChevronLeft, ChevronRight, CalendarDays } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useKaisaiCalendar } from "@/api/hooks";
import { localDate } from "@/lib/constants";

// ─── 型定義 ──────────────────────────────────────────────────────────────────

interface CalendarDayData {
  jra: string[];
  nar: string[];
}

// ─── 日付セル（モバイル用縦リスト行）───────────────────────────────────────

function MobileDayRow({
  dateStr,
  dayData,
  isToday,
  isPast,
  onClick,
}: {
  dateStr: string;
  dayData: CalendarDayData | undefined;
  isToday: boolean;
  isPast: boolean;
  onClick: () => void;
}) {
  const hasAny = (dayData?.jra?.length ?? 0) + (dayData?.nar?.length ?? 0) > 0;
  const d = new Date(dateStr + "T00:00:00");
  const dow = d.getDay();
  const dowLabel = ["日", "月", "火", "水", "木", "金", "土"][dow];
  const dowCls = dow === 0 ? "text-red-500" : dow === 6 ? "text-blue-500" : "text-muted-foreground";

  // aria-label: 「2026年1月4日(月) 中山+京都 川崎+名古屋+佐賀」
  const ariaLabel = useMemo(() => {
    const parts: string[] = [];
    if (dayData?.jra?.length) parts.push(dayData.jra.join("+"));
    if (dayData?.nar?.length) parts.push(dayData.nar.join("+"));
    return `${dateStr}(${dowLabel}) ${parts.join(" ")}`;
  }, [dateStr, dowLabel, dayData]);

  return (
    <button
      className={`w-full flex items-start gap-3 px-3 py-2 border-b border-border/50 text-left transition-colors
        ${hasAny ? "hover:bg-accent/30 cursor-pointer" : "opacity-40 cursor-default"}
        ${isToday ? "bg-primary/10 dark:bg-primary/20" : ""}
      `}
      onClick={hasAny ? onClick : undefined}
      aria-label={ariaLabel}
    >
      {/* 日付 */}
      <div className="flex-shrink-0 w-14">
        <span className={`text-sm font-bold ${dowCls}`}>{dateStr.slice(8)}日</span>
        <span className={`ml-1 text-xs ${dowCls}`}>({dowLabel})</span>
        {isToday && (
          <span className="ml-1 text-[10px] bg-primary text-primary-foreground rounded px-1">今日</span>
        )}
      </div>

      {/* バッジ群 */}
      <div className="flex flex-wrap gap-1 flex-1">
        {!hasAny && (
          <span className="text-xs text-muted-foreground/60">開催なし</span>
        )}
        {(dayData?.jra ?? []).map((v) => (
          <span
            key={`jra-${v}`}
            className="inline-block text-[11px] font-semibold px-1.5 py-0.5 rounded
              bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-200"
          >
            {v}
          </span>
        ))}
        {(dayData?.nar ?? []).map((v) => (
          <span
            key={`nar-${v}`}
            className="inline-block text-[11px] font-semibold px-1.5 py-0.5 rounded
              bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200"
          >
            {v}
          </span>
        ))}
      </div>

      {/* 過去/未来インジケータ */}
      {hasAny && (
        <div className="flex-shrink-0 text-[10px] text-muted-foreground/60 self-center">
          {isPast ? "→成績" : "→予想"}
        </div>
      )}
    </button>
  );
}

// ─── 日付グリッドセル（PC用）────────────────────────────────────────────────

function GridDayCell({
  day,
  dateStr,
  dayData,
  isToday,
  onClick,
}: {
  day: number;
  dateStr: string;
  dayData: CalendarDayData | undefined;
  isToday: boolean;
  onClick: () => void;
}) {
  const hasAny = (dayData?.jra?.length ?? 0) + (dayData?.nar?.length ?? 0) > 0;
  const dow = new Date(dateStr + "T00:00:00").getDay();
  const dateCls =
    dow === 0 ? "text-red-500" : dow === 6 ? "text-blue-500" : "text-foreground";

  // JRA venue 表示: 複数の場合「中山+京都」
  const jraLabel = (dayData?.jra ?? []).join("+");
  // NAR venue 表示
  const narLabel = (dayData?.nar ?? []).join("+");

  const ariaLabel = useMemo(() => {
    const parts: string[] = [];
    if (dayData?.jra?.length) parts.push(dayData.jra.join("+"));
    if (dayData?.nar?.length) parts.push(dayData.nar.join("+"));
    const dowLabel = ["日", "月", "火", "水", "木", "金", "土"][dow];
    return `${dateStr}(${dowLabel}) ${parts.join(" ")}`;
  }, [dateStr, dow, dayData]);

  return (
    <div
      role={hasAny ? "button" : undefined}
      tabIndex={hasAny ? 0 : undefined}
      onClick={hasAny ? onClick : undefined}
      onKeyDown={hasAny ? (e) => { if (e.key === "Enter" || e.key === " ") onClick(); } : undefined}
      aria-label={ariaLabel}
      className={`
        relative min-h-[72px] p-1.5 rounded border transition-colors select-none
        ${hasAny
          ? "cursor-pointer hover:border-primary/60 hover:bg-accent/20"
          : "opacity-40 cursor-default"
        }
        ${isToday
          ? "border-primary bg-primary/10 dark:bg-primary/20"
          : "border-border/40 bg-card"
        }
      `}
    >
      {/* 日付数字 */}
      <div className={`text-[13px] font-bold leading-none mb-1 ${dateCls}`}>
        {day}
        {isToday && (
          <span className="ml-1 text-[9px] bg-primary text-primary-foreground rounded px-1 align-middle">今日</span>
        )}
      </div>

      {/* JRA バッジ */}
      {jraLabel && (
        <div className="mb-0.5">
          <span className="inline-block text-[10px] leading-tight font-semibold px-1 py-0.5 rounded
            bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-200 max-w-full truncate">
            {jraLabel}
          </span>
        </div>
      )}

      {/* NAR バッジ */}
      {narLabel && (
        <div>
          <span className="inline-block text-[10px] leading-tight font-semibold px-1 py-0.5 rounded
            bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200 max-w-full truncate">
            {narLabel}
          </span>
        </div>
      )}
    </div>
  );
}

// ─── メインページ ────────────────────────────────────────────────────────────

export default function CalendarPage() {
  const today = localDate();
  const navigate = useNavigate();
  const { data, isLoading, isError } = useKaisaiCalendar();

  // 表示月の状態（デフォルトは今月）
  const [calYear, setCalYear] = useState(() => new Date().getFullYear());
  const [calMonth, setCalMonth] = useState(() => new Date().getMonth()); // 0-indexed

  // 前月・次月ナビ
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

  // 今月リセット
  const handleReset = useCallback(() => {
    const now = new Date();
    setCalYear(now.getFullYear());
    setCalMonth(now.getMonth());
  }, []);

  // 年選択（2022〜翌年）
  const yearOptions = useMemo(() => {
    const start = data?.period?.start ? parseInt(data.period.start.slice(0, 4)) : 2022;
    const end = data?.period?.end ? parseInt(data.period.end.slice(0, 4)) : new Date().getFullYear() + 1;
    const years: number[] = [];
    for (let y = start; y <= end; y++) years.push(y);
    return years;
  }, [data]);

  // 月内の日付セル計算
  const { firstDow, cells } = useMemo(() => {
    const fd = new Date(calYear, calMonth, 1).getDay();
    const lastDay = new Date(calYear, calMonth + 1, 0).getDate();
    const cs: { day: number; dateStr: string }[] = [];
    for (let d = 1; d <= lastDay; d++) {
      const ds = `${calYear}-${String(calMonth + 1).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
      cs.push({ day: d, dateStr: ds });
    }
    return { firstDow: fd, cells: cs };
  }, [calYear, calMonth]);

  // 日付クリックハンドラ（過去→成績、未来→予想）
  const handleDayClick = useCallback((dateStr: string) => {
    if (dateStr < today) {
      navigate(`/results?date=${dateStr}`);
    } else {
      navigate(`/today?date=${dateStr}`);
    }
  }, [today, navigate]);

  const dayNames = ["日", "月", "火", "水", "木", "金", "土"];
  const monthLabel = `${calYear}年${calMonth + 1}月`;

  return (
    <div className="max-w-5xl mx-auto px-3 py-4 sm:px-6 sm:py-6">
      {/* ページヘッダー */}
      <div className="flex items-center gap-2 mb-4">
        <CalendarDays className="h-5 w-5 text-brand-gold" />
        <h1 className="text-xl font-bold">開催カレンダー</h1>
      </div>

      {/* エラー表示 */}
      {isError && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 px-4 py-3 text-sm text-destructive mb-4">
          カレンダーデータの読み込みに失敗しました。再読み込みしてください。
        </div>
      )}

      {/* ローディング スケルトン */}
      {isLoading && (
        <div className="space-y-2">
          <div className="h-10 bg-muted animate-pulse rounded" />
          <div className="grid grid-cols-7 gap-1">
            {Array.from({ length: 35 }).map((_, i) => (
              <div key={i} className="h-16 bg-muted/60 animate-pulse rounded" />
            ))}
          </div>
        </div>
      )}

      {/* カレンダー本体 */}
      {!isLoading && !isError && data && (
        <>
          {/* 月ナビバー */}
          <div className="flex items-center gap-2 mb-4">
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handlePrev} aria-label="前月">
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <Button variant="outline" size="sm" className="text-sm font-bold min-w-[130px]" onClick={handleReset}>
              {monthLabel}
            </Button>
            <Button variant="ghost" size="icon" className="h-8 w-8" onClick={handleNext} aria-label="次月">
              <ChevronRight className="h-4 w-4" />
            </Button>

            {/* 年セレクト */}
            <select
              value={calYear}
              onChange={(e) => setCalYear(Number(e.target.value))}
              className="ml-2 text-sm border border-border rounded px-2 py-1 bg-background text-foreground"
              aria-label="年を選択"
            >
              {yearOptions.map((y) => (
                <option key={y} value={y}>{y}年</option>
              ))}
            </select>

            {/* 月セレクト */}
            <select
              value={calMonth}
              onChange={(e) => setCalMonth(Number(e.target.value))}
              className="text-sm border border-border rounded px-2 py-1 bg-background text-foreground"
              aria-label="月を選択"
            >
              {Array.from({ length: 12 }, (_, i) => i).map((m) => (
                <option key={m} value={m}>{m + 1}月</option>
              ))}
            </select>
          </div>

          {/* 凡例 */}
          <div className="flex items-center gap-3 mb-3 text-xs text-muted-foreground">
            <div className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-amber-200 dark:bg-amber-800" />
              <span>JRA</span>
            </div>
            <div className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-blue-200 dark:bg-blue-800" />
              <span>NAR</span>
            </div>
            <div className="flex items-center gap-1">
              <span className="inline-block w-3 h-3 rounded bg-border/60" />
              <span>開催なし</span>
            </div>
            <span className="ml-2 text-[11px]">
              ※ 過去日クリック → 成績 / 当日・未来クリック → 予想
            </span>
          </div>

          {/* PC グリッド（768px以上） */}
          <div className="hidden sm:block">
            {/* 曜日ヘッダー */}
            <div className="grid grid-cols-7 gap-1 mb-1">
              {dayNames.map((dn, i) => (
                <div
                  key={dn}
                  className={`text-[12px] font-bold text-center py-1
                    ${i === 0 ? "text-red-500" : i === 6 ? "text-blue-500" : "text-muted-foreground"}`}
                >
                  {dn}
                </div>
              ))}
            </div>

            {/* カレンダーグリッド */}
            <div className="grid grid-cols-7 gap-1">
              {/* 月初空セル（曜日オフセット） */}
              {Array.from({ length: firstDow }, (_, i) => (
                <div key={`empty-${i}`} className="min-h-[72px]" />
              ))}

              {/* 日付セル */}
              {cells.map(({ day, dateStr }) => (
                <GridDayCell
                  key={dateStr}
                  day={day}
                  dateStr={dateStr}
                  dayData={data.days?.[dateStr]}
                  isToday={dateStr === today}
                  onClick={() => handleDayClick(dateStr)}
                />
              ))}
            </div>
          </div>

          {/* モバイル 縦リスト（768px未満） */}
          <div className="sm:hidden rounded-lg border border-border overflow-hidden">
            {/* 月見出し */}
            <div className="bg-muted/50 px-3 py-2 text-sm font-bold text-muted-foreground">
              {monthLabel}
            </div>
            {cells.map(({ dateStr }) => (
              <MobileDayRow
                key={dateStr}
                dateStr={dateStr}
                dayData={data.days?.[dateStr]}
                isToday={dateStr === today}
                isPast={dateStr < today}
                onClick={() => handleDayClick(dateStr)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
