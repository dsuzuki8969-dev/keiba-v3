/**
 * D-AI Keiba — APIクライアント
 * 既存 api.js の全エンドポイントをTypeScript化
 */

async function get<T>(url: string): Promise<T> {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`API error: ${r.status} ${r.statusText}`);
  return r.json();
}

async function post<T>(url: string, body: unknown): Promise<T> {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`API error: ${r.status} ${r.statusText}`);
  return r.json();
}

export const api = {
  // 認証
  authMode: () => get<{ admin: boolean }>("/api/auth_mode"),

  // ホーム
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  homeInfo: (date: string) => get<Record<string, any>>(`/api/home_info?date=${date}`),
  // 本日のリアルタイム成績
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  homeTodayStats: (date: string) => get<Record<string, any>>(`/api/home/today_stats?date=${date}`),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  todayPredictions: (date: string, nocache = false) =>
    get<Record<string, any>>(
      `/api/today_predictions?date=${date}${nocache ? "&nocache=1" : ""}`
    ),
  shareUrl: (date: string) => get<{ url: string }>(`/api/share_url?date=${date}`),
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  racePrediction: (date: string, venue: string, raceNo: number) =>
    get<Record<string, any>>(
      `/api/race_prediction?date=${date}&venue=${encodeURIComponent(venue)}&race_no=${raceNo}`
    ),
  raceOdds: (body: OddsRequest) => post<OddsResponse>("/api/race_odds", body),
  portfolio: () => get<PortfolioResponse>("/api/portfolio"),

  // 分析実行
  analyze: (body: AnalyzeRequest) => post<{ status: string }>("/api/analyze", body),
  analyzeStatus: () => get<AnalyzeStatusResponse>("/api/analyze_status"),
  analyzeCancel: () => post<{ status: string }>("/api/analyze_cancel", {}),

  // オッズ更新
  oddsUpdate: (body: OddsUpdateRequest) => post<{ status: string }>("/api/odds_update", body),
  oddsUpdateStatus: () => get<OddsUpdateStatusResponse>("/api/odds_update_status"),
  oddsUpdateCancel: () => post<{ status: string }>("/api/odds_update_cancel", {}),
  oddsUnfetchedDates: () => get<{ dates: string[] }>("/api/odds/unfetched_dates"),
  predictionsUnfetchedDates: () => get<{ dates: string[] }>("/api/predictions/unfetched_dates"),

  // 成績
  resultsDates: () => get<{ dates: string[]; daily_stats: Record<string, unknown> }>("/api/results/dates"),
  resultsSummary: (year: string) => get<ResultsSummaryResponse>(`/api/results/summary?year=${year}`),
  resultsSanrentanSummary: (year: string) =>
    get<SanrentanSummaryResponse>(`/api/results/sanrentan_summary?year=${year}`),
  resultsTrend: (year: string) => get<ResultsTrendResponse>(`/api/results/trend?year=${year}`),
  resultsDetailed: (year: string) => get<ResultsDetailedResponse>(`/api/results/detailed?year=${year}`),
  resultsFetch: (body: ResultsFetchRequest) => post<{ status: string }>("/api/results/fetch", body),
  resultsFetchBatch: (body: ResultsFetchBatchRequest) =>
    post<{ status: string }>("/api/results/fetch_batch", body),
  resultsFetchStatus: () => get<ResultsFetchStatusResponse>("/api/results/fetch_status"),
  resultsFetchCancel: () => post<{ status: string }>("/api/results/fetch_cancel", {}),
  unmatchedDates: () => get<{ dates: string[] }>("/api/results/unmatched_dates"),
  genSimpleHtml: (body: GenHtmlRequest) => post<{ url: string }>("/api/generate_simple_html", body),

  // レース結果
  raceResult: (date: string, raceId: string) =>
    get<RaceResultResponse>(`/api/results/race?date=${encodeURIComponent(date)}&race_id=${encodeURIComponent(raceId)}`),

  // データベース
  personnelAgg: (qs: string) => get<PersonnelAggResponse>(`/api/db/personnel_agg?${qs}`),
  personnelAggCourse: (qs: string) => get<PersonnelAggResponse>(`/api/db/personnel_agg_course?${qs}`),
  courseList: () => get<CourseListResponse>("/api/db/course"),
  courseStats: (key: string) =>
    get<CourseStatsResponse>(`/api/db/course_stats?key=${encodeURIComponent(key)}`),

  // DB更新
  dbUpdate: (body: DbUpdateRequest) => post<{ status: string }>("/api/db/update", body),
  dbUpdateStatus: () => get<DbUpdateStatusResponse>("/api/db/update_status"),
  dbUpdateCancel: () => post<{ status: string }>("/api/db/update_cancel", {}),

  // 競馬場研究
  venueProfile: (code?: string) =>
    get<VenueProfileResponse>(code ? `/api/venue/profile?code=${code}` : "/api/venue/profile"),
  venueBias: (code: string) =>
    get<VenueBiasResponse>(`/api/venue/bias?code=${code}`),

  // About
  featureImportance: () => get<FeatureImportanceItem[]>("/api/feature_importance"),

  // データ品質チェック（再発防止策 #4）
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  dataQuality: () => get<Record<string, any>>("/api/data_quality"),
};

// --- 型定義 ---
// APIレスポンスの主要な型（詳細は api/types.ts で管理）

export interface HomeInfoResponse {
  date: string;
  weather: Record<string, unknown>;
  venues: VenueInfo[];
}

export interface VenueInfo {
  venue: string;
  race_count: number;
  surface?: string;
}

export interface TodayPredictionsResponse {
  date: string;
  venues: VenuePrediction[];
  zettai_jiku?: ZettaiJiku[];
  high_conf?: HighConfRace[];
}

export interface VenuePrediction {
  venue: string;
  races: RaceSummary[];
}

export interface RaceSummary {
  race_no: number;
  race_name: string;
  post_time: string;
  surface: string;
  distance: number;
  head_count: number;
  grade?: string;
  confidence?: string;
  honmei_name?: string;
  honmei_number?: number;
}

export interface ZettaiJiku {
  venue: string;
  race_no: number;
  horse_name: string;
  confidence: string;
}

export interface HighConfRace {
  venue: string;
  race_no: number;
  race_name: string;
  confidence: string;
}

export interface RacePredictionResponse {
  race_info: RaceInfo;
  horses: HorseEval[];
  marks: MarkSet;
  betting?: BettingRecommendation;
  narrative?: string;
}

export interface RaceInfo {
  venue: string;
  race_no: number;
  race_name: string;
  post_time: string;
  surface: string;
  distance: number;
  head_count: number;
  grade?: string;
  course_condition?: string;
}

export interface HorseEval {
  number: number;
  name: string;
  jockey: string;
  trainer: string;
  weight?: number;
  age: number;
  sex: string;
  mark?: string;
  win_prob?: number;
  deviation?: number;
  ability_score?: number;
  pace_score?: number;
  course_score?: number;
  overall_score?: number;
  comment?: string;
  past_runs?: PastRun[];
  odds?: number;
  pop_rank?: number;
  ev?: number;
}

export interface PastRun {
  date: string;
  venue: string;
  race_name: string;
  surface: string;
  distance: number;
  finish: number;
  time: string;
  last3f: string;
}

export interface MarkSet {
  tekipan?: number;
  honmei?: number;
  taikou?: number[];
  tannuke?: number[];
  rendashi?: number[];
  oana?: number[];
}

export interface BettingRecommendation {
  tickets: BettingTicket[];
}

export interface BettingTicket {
  type: string;
  formation: unknown;
  confidence: string;
}

export interface OddsRequest {
  race_id?: string;
  date?: string;
  venue?: string;
  race_no?: number;
  /** マスター指示 2026-04-23: fire-and-forget 自動取得モード。
   *  サーバー側で cooldown 管理し、即レスポンス + バックグラウンドで取得。 */
  auto?: boolean;
}

export interface OddsResponse {
  odds: Record<string, unknown>;
  timestamp?: string;
  /** auto=true の場合に返る（fire-and-forget 応答） */
  auto?: boolean;
  started?: boolean;
  skipped?: string;
  remaining?: number;
}

export interface AnalyzeRequest {
  date?: string;
  dates?: string[];
  venues?: string[];
}

export interface AnalyzeStatusResponse {
  running: boolean;
  done?: boolean;
  error?: string;
  progress?: string;
  done_races?: number;
  total_races?: number;
  elapsed_sec?: number;
  current_race?: string;
  log?: string[];
}

export interface OddsUpdateRequest {
  date?: string;
  dates?: string[];
}

export interface OddsUpdateStatusResponse {
  running: boolean;
  done?: boolean;
  error?: string;
  count?: number;
  total?: number;
  current?: number;
  current_race?: string;
  started_at?: number;
  source?: string;
}

export interface ResultsFetchRequest {
  date: string;
}

export interface ResultsFetchBatchRequest {
  dates: string[];
}

export interface ResultsFetchStatusResponse {
  running: boolean;
  done?: boolean;
  completed?: number;
  total?: number;
  current_date?: string;
  progress?: string;
  error?: string;
  elapsed_sec?: number;
  log?: string[];
}

export interface ResultsSummaryResponse {
  year: string;
  summary: Record<string, unknown>;
  cards: SummaryCard[];
}

export interface SanrentanByConfidence {
  confidence: string;
  played: number;
  hit: number;
  hit_rate_pct: number;
  stake: number;
  payback: number;
  roi_pct: number;
}

export interface SanrentanTopPayout {
  payout: number;
  date: string;      // YYYYMMDD
  venue: string;
  race_no: number;
  race_name: string;
  conf: string;
  race_payback: number;
}

export interface SanrentanMonthly {
  month: string;      // "YYYY-MM"
  played: number;
  hit: number;
  stake: number;
  payback: number;
  balance: number;
  roi_pct: number;
  cum_roi_pct: number;
}

export interface SanrentanSummaryResponse {
  period_days: number;
  races_total: number;
  races_played: number;
  races_skipped: number;
  races_hit: number;
  points: number;
  hits: number;
  stake: number;
  payback: number;
  balance: number;
  roi_pct: number;
  race_hit_rate_pct: number;
  point_hit_rate_pct: number;
  max_payout: number;
  date_from: string;
  date_to: string;
  by_confidence: SanrentanByConfidence[];
  top10_payouts: SanrentanTopPayout[];
  monthly: SanrentanMonthly[];
  error?: string;
}

export interface SummaryCard {
  label: string;
  value: string | number;
  sub?: string;
  color?: string;
}

export interface ResultsTrendResponse {
  year: string;
  months: MonthlyTrend[];
}

export interface MonthlyTrend {
  month: string;
  hit_rate: number;
  roi: number;
  profit: number;
  count: number;
}

export interface ResultsDetailedResponse {
  year: string;
  results: DetailedResult[];
}

export interface DetailedResult {
  date: string;
  venue: string;
  race_no: number;
  race_name: string;
  result: string;
  bet_type: string;
  stake: number;
  payout: number;
}

export interface RaceResultEntry {
  horse_no: number;
  finish: number;
  odds: number;
  horse_name?: string;
  jockey?: string;
  mark?: string;
  predicted_rank?: number;
  win_prob?: number;
  gate_no?: number;
  // race_log 補完項目（レース結果画面表示用）
  time?: string;
  margin?: string;
  popularity?: number | null;
  last_3f?: number | null;
  corners?: number[];
  composite?: number;
}

export interface RaceResultPayout {
  combo: string;
  payout: number;
}

export interface RaceResultResponse {
  ok: boolean;
  found?: boolean;
  order?: RaceResultEntry[];
  payouts?: Record<string, RaceResultPayout | RaceResultPayout[]>;
  error?: string;
  // 結果データ再取得待ち（スクレイパーバグで未修復レース）
  data_incomplete?: boolean;
}

export interface PersonnelAggResponse {
  type: string;
  data: PersonnelRow[];
  total: number;
  persons?: PersonnelRow[];
  period?: string;
  [key: string]: unknown;
}

export interface PersonnelRow {
  name: string;
  starts: number;
  wins: number;
  win_rate: number;
  rentai_rate: number;
  fukusho_rate: number;
  avg_odds?: number;
  [key: string]: unknown;
}

export interface CourseListResponse {
  courses: CourseItem[];
  keys?: string[];
  [key: string]: unknown;
}

export interface CourseItem {
  key: string;
  venue: string;
  surface: string;
  distance: number;
  count: number;
}

export interface CourseStatsResponse {
  key: string;
  stats: Record<string, unknown>;
  [key: string]: unknown;
}

export interface GenHtmlRequest {
  date: string;
}

export interface DbUpdateRequest {
  type: string;
  date?: string;
  start_date?: string;
  end_date?: string;
  [key: string]: unknown;
}

export interface DbUpdateStatusResponse {
  running: boolean;
  done?: boolean;
  progress?: string;
  error?: string;
  step?: number;
  total_steps?: number;
  elapsed_sec?: number;
  log?: string[];
  [key: string]: unknown;
}

export interface PortfolioResponse {
  dates: PortfolioDate[];
}

export interface PortfolioDate {
  date: string;
  venues: string[];
  html_ready: boolean;
}

// 競馬場研究
export interface VenueProfileItem {
  venue: string;
  venue_code: string;
  is_jra: boolean;
  has_turf: boolean;
  has_dirt: boolean;
  direction: string;
  avg_straight_m: number;
  max_straight_m: number;
  slope_type: string;
  first_corner_score: number;
  corner_type_dominant: string;
  n_courses: number;
}

export interface VenueProfileDetail extends VenueProfileItem {
  profile: {
    avg_straight_m: number;
    max_straight_m: number;
    slope_type: string;
    first_corner_score: number;
    corner_type_dominant: string;
  };
  composite_weights: Record<string, number>;
  similar_venues: { venue: string; venue_code: string; similarity: number }[];
  courses: {
    course_id: string;
    surface: string;
    distance: number;
    direction: string;
    straight_m: number;
    corner_count: number;
    corner_type: string;
    first_corner: string;
    first_corner_m: number;
    slope_type: string;
    inside_outside: string;
    width_m: string;
  }[];
}

export type VenueProfileResponse = { venues: VenueProfileItem[] } | VenueProfileDetail;

export interface VenueBiasResponse {
  venue_code: string;
  gate_bias: Record<string, Record<string, number>>;
  pace_tendency: Record<string, {
    surface: string;
    distance: number;
    escape_rate: number;
    front_rate: number;
    race_cnt: number;
  }>;
  last3f: Record<string, {
    surface: string;
    distance: number;
    mean: number;
    sigma: number;
    cnt: number;
  }>;
}

export interface FeatureImportanceItem {
  rank: number;
  name: string;
  label?: string;
  desc?: string;
  cat: string;
  pct: number;
}
