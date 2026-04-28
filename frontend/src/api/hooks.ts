/**
 * D-AI Keiba — TanStack Query フック
 */
import { useQuery, useMutation } from "@tanstack/react-query";
import { api } from "./client";

// キャッシュ時間定数
const MIN_30 = 30 * 60 * 1000;
const HOUR_1 = 60 * 60 * 1000;
const HOUR_2 = 2 * 60 * 60 * 1000;
const HOUR_4 = 4 * 60 * 60 * 1000;
const DAY_1 = 24 * 60 * 60 * 1000;

// 認証モード
export function useAuthMode() {
  return useQuery({
    queryKey: ["auth"],
    queryFn: () => api.authMode(),
    staleTime: Infinity,
  });
}

// ホーム 本日のリアルタイム成績（2分キャッシュ、レース10分後自動更新に合わせて）
export function useHomeTodayStats(date: string) {
  return useQuery({
    queryKey: ["homeTodayStats", date],
    queryFn: () => api.homeTodayStats(date),
    enabled: !!date,
    staleTime: 2 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
    refetchInterval: 2 * 60 * 1000, // 2分ごとに自動再取得
    refetchIntervalInBackground: false,
  });
}

// ホーム情報
export function useHomeInfo(date: string) {
  return useQuery({
    queryKey: ["homeInfo", date],
    queryFn: () => api.homeInfo(date),
    enabled: !!date,
    staleTime: MIN_30,
    gcTime: HOUR_1,
  });
}

// 本日予想
export function useTodayPredictions(date: string, nocache = false) {
  return useQuery({
    queryKey: ["predictions", date],
    queryFn: () => api.todayPredictions(date, nocache),
    staleTime: MIN_30,
    gcTime: HOUR_1,
    enabled: !!date,
  });
}

// レース詳細予想
export function useRacePrediction(date: string, venue: string, raceNo: number) {
  return useQuery({
    queryKey: ["racePrediction", date, venue, raceNo],
    queryFn: () => api.racePrediction(date, venue, raceNo),
    enabled: !!date && !!venue && raceNo > 0,
    staleTime: 10 * 60 * 1000,
    gcTime: MIN_30,
  });
}

// ポートフォリオ
export function usePortfolio() {
  return useQuery({
    queryKey: ["portfolio"],
    queryFn: () => api.portfolio(),
    staleTime: 10 * 60 * 1000,
    gcTime: MIN_30,
  });
}

// 分析ステータス（ポーリング）
export function useAnalyzeStatus(enabled: boolean) {
  return useQuery({
    queryKey: ["analyzeStatus"],
    queryFn: () => api.analyzeStatus(),
    refetchInterval: enabled ? 2000 : false,
    enabled,
  });
}

// オッズ更新ステータス（ポーリング）
export function useOddsUpdateStatus(enabled: boolean) {
  return useQuery({
    queryKey: ["oddsUpdateStatus"],
    queryFn: () => api.oddsUpdateStatus(),
    refetchInterval: enabled ? 2000 : false,
    enabled,
  });
}

// 成績サマリー
export function useResultsSummary(year: string) {
  return useQuery({
    queryKey: ["resultsSummary", year],
    queryFn: () => api.resultsSummary(year),
    enabled: !!year,
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// 三連単フォーメーション成績サマリー（Phase 3）
// マスター指示 2026-04-23: backfill/結果再取得の反映が 1 時間遅れるのは NG。
//   5 分に短縮 + ウィンドウフォーカスで再取得。
export function useSanrentanSummary(year: string) {
  return useQuery({
    queryKey: ["sanrentanSummary", year],
    queryFn: () => api.resultsSanrentanSummary(year),
    enabled: !!year,
    staleTime: 5 * 60 * 1000,
    gcTime: HOUR_1,
    refetchOnWindowFocus: true,
  });
}

// 成績トレンド
export function useResultsTrend(year: string) {
  return useQuery({
    queryKey: ["resultsTrend", year],
    queryFn: () => api.resultsTrend(year),
    enabled: !!year,
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// 成績詳細
export function useResultsDetailed(year: string) {
  return useQuery({
    queryKey: ["resultsDetailed", year],
    queryFn: () => api.resultsDetailed(year),
    enabled: !!year,
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// 結果取得ステータス（ポーリング）
export function useResultsFetchStatus(enabled: boolean) {
  return useQuery({
    queryKey: ["resultsFetchStatus"],
    queryFn: () => api.resultsFetchStatus(),
    refetchInterval: enabled ? 2000 : false,
    enabled,
  });
}

// 騎手/調教師集計
export function useRaceResult(date: string, raceId: string) {
  return useQuery({
    queryKey: ["raceResult", date, raceId],
    queryFn: () => api.raceResult(date, raceId),
    enabled: !!date && !!raceId,
    // 当日結果はレース直後に頻繁に変わるため短めキャッシュ
    // （データ更新ボタン・結果取得完了の invalidateQueries で即時更新されるが、
    //  画面遷移時の軽微な再取得を減らす効果で 30秒のみ保持）
    staleTime: 30 * 1000,      // 30秒
    gcTime: 5 * 60 * 1000,     // 5分
  });
}

export function usePersonnelAgg(queryString: string) {
  return useQuery({
    queryKey: ["personnelAgg", queryString],
    queryFn: () => api.personnelAgg(queryString),
    enabled: !!queryString,
    staleTime: HOUR_2,
    gcTime: HOUR_4,
  });
}

// 当該コース（会場×馬場×距離）で race_log を直接集計した成績
export function usePersonnelAggCourse(queryString: string) {
  return useQuery({
    queryKey: ["personnelAggCourse", queryString],
    queryFn: () => api.personnelAggCourse(queryString),
    enabled: !!queryString,
    staleTime: HOUR_2,
    gcTime: HOUR_4,
  });
}

// コース一覧
export function useCourseList() {
  return useQuery({
    queryKey: ["courseList"],
    queryFn: () => api.courseList(),
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// コース統計
export function useCourseStats(key: string) {
  return useQuery({
    queryKey: ["courseStats", key],
    queryFn: () => api.courseStats(key),
    enabled: !!key,
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// 競馬場プロファイル
export function useVenueProfile(code?: string) {
  return useQuery({
    queryKey: ["venueProfile", code ?? ""],
    queryFn: () => api.venueProfile(code),
    staleTime: DAY_1,
    gcTime: DAY_1,
  });
}

// 競馬場バイアス
export function useVenueBias(code: string) {
  return useQuery({
    queryKey: ["venueBias", code],
    queryFn: () => api.venueBias(code),
    enabled: !!code,
    staleTime: HOUR_2,
    gcTime: HOUR_4,
  });
}

// 特徴量重要度
export function useFeatureImportance() {
  return useQuery({
    queryKey: ["featureImportance"],
    queryFn: () => api.featureImportance(),
    staleTime: Infinity,
  });
}

// データ品質チェック結果（再発防止策 #4）
// 30分キャッシュ。daily_data_quality_check.py の最終実行結果を返す
export function useDataQuality() {
  return useQuery({
    queryKey: ["dataQuality"],
    queryFn: () => api.dataQuality(),
    staleTime: MIN_30,
    gcTime: HOUR_1,
    refetchInterval: MIN_30,
    refetchIntervalInBackground: false,
  });
}

// T-038 開催カレンダーマスタ（1時間キャッシュ）
export interface KaisaiCalendarDay {
  jra: string[];
  nar: string[];
}

export interface KaisaiCalendarData {
  version: string;
  period: { start: string; end: string };
  days: Record<string, KaisaiCalendarDay>;
  error?: string;
}

export function useKaisaiCalendar() {
  return useQuery<KaisaiCalendarData>({
    queryKey: ["kaisai_calendar"],
    queryFn: async () => {
      const r = await fetch("/api/kaisai_calendar");
      if (!r.ok) throw new Error("calendar load failed");
      return r.json();
    },
    staleTime: HOUR_1,  // 1時間キャッシュ（開催カレンダーは頻繁に変わらない）
    gcTime: HOUR_4,
  });
}

// T-039: レースカード的中バッジ用フック
export interface RaceCardHitResult {
  win_hit: boolean | null;        // 単勝 ◎ 的中 (null=結果未取得)
  sanrentan_hit: boolean | null;  // 三連単 F 的中 (null=未対象/結果未取得)
}

export interface RaceCardResultsData {
  date: string;
  results: Record<string, RaceCardHitResult>;
}

/**
 * useRaceCardResults — 指定日の全レースの的中バッジ情報を取得する
 * 30 秒キャッシュ（当日レースの結果更新頻度に合わせる）
 */
export function useRaceCardResults(date: string | null) {
  return useQuery<RaceCardResultsData>({
    queryKey: ["race_card_results", date],
    queryFn: async () => {
      if (!date) return { date: "", results: {} };
      const r = await fetch(`/api/race_card_results?date=${date}`);
      if (!r.ok) throw new Error("race_card_results load failed");
      return r.json() as Promise<RaceCardResultsData>;
    },
    enabled: !!date,
    staleTime: 30 * 1000,  // 30 秒キャッシュ
    gcTime: 2 * 60 * 1000,
  });
}

// LIVE STATS 手動更新（POST /api/force_refresh_today）
// マスター指示 2026-04-27: 「集計 xR / 終了 yR」の遅延解消のため手動更新ボタン追加
export interface ForceRefreshTodayResult {
  status: string;
  date: string;
  fetched: number;
  aggregated: number;
  skipped: number;
  errors: number;
  pending_before: number;
  pending_after: number;
  elapsed_ms: number;
}

export function useForceRefreshToday() {
  return useMutation({
    mutationFn: async (date: string): Promise<ForceRefreshTodayResult> => {
      const res = await fetch("/api/force_refresh_today", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ date }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({})) as { message?: string };
        throw new Error(err?.message || `HTTP ${res.status}`);
      }
      return res.json() as Promise<ForceRefreshTodayResult>;
    },
  });
}
