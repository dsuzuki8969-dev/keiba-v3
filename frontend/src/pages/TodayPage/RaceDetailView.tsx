import { useState, useCallback, useEffect, useRef } from "react";
import { Button, buttonVariants } from "@/components/ui/button";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { useRacePrediction } from "@/api/hooks";
import { useQueryClient } from "@tanstack/react-query";
import { GradeBadge } from "@/components/keiba/GradeBadge";
import { surfShort } from "@/lib/constants";
import { api } from "@/api/client";
import { TabGroup3Horse } from "./TabGroup3Horse";

// NAR ライブ映像のトラックマップ（レースライブボタン用）
const NAR_LIVE_TRACK_MAP: Record<string, string> = {
  "帯広": "obihiro", "門別": "monbetsu", "盛岡": "morioka", "水沢": "mizusawa",
  "浦和": "urawa", "船橋": "funabashi", "大井": "ooi", "川崎": "kawasaki",
  "金沢": "kanazawa", "笠松": "kasamatsu", "名古屋": "nagoya", "園田": "sonoda",
  "姫路": "himeji", "高知": "kouchi", "佐賀": "saga",
};

interface RaceSummaryItem {
  race_no: number;
  [key: string]: unknown;
}

interface Props {
  date: string;
  venue: string;
  raceNo: number;
  venues: string[];
  venueRaces?: Record<string, RaceSummaryItem[]>;
  oddsUpdatedAt?: string;
  onClose: () => void;
  onNavigate: (venue: string, raceNo: number) => void;
}

export function RaceDetailView({
  date,
  venue,
  raceNo,
  venues,
  venueRaces,
  oddsUpdatedAt,
  onClose,
  onNavigate,
}: Props) {
  const { data, isLoading, error } = useRacePrediction(date, venue, raceNo);
  const queryClient = useQueryClient();
  const [oddsFetching, setOddsFetching] = useState(false);
  const [oddsMsg, setOddsMsg] = useState("");

  // マスター指示 2026-04-23: レース詳細ビュー開いた時に裏でオッズ自動取得（cooldown付）
  // レース結果タブの自動取得は /api/results/race 側で自動判定（発走+10分経過時）
  const autoTriggeredRef = useRef<string>("");
  useEffect(() => {
    const raceId = (data as { race?: { race_id?: string } })?.race?.race_id;
    if (!raceId) return;
    const raceKey = `${date}:${venue}:${raceNo}:${raceId}`;
    if (autoTriggeredRef.current === raceKey) return;
    autoTriggeredRef.current = raceKey;
    // auto モードで POST（fire-and-forget、server 側で cooldown 管理）
    api.raceOdds({
      race_id: raceId, date, venue, race_no: raceNo, auto: true,
    }).catch(() => { /* 失敗しても通常ボタンで再試行可 */ });
  }, [date, venue, raceNo, data]);

  // 現在の会場のレース番号一覧
  const currentRaces = venueRaces?.[venue] || [];
  const maxR = currentRaces.length
    ? Math.max(...currentRaces.map((r) => r.race_no))
    : 12;

  const race = data?.race as RaceDetail | undefined;
  const horses = (race?.horses || []) as HorseData[];
  const sorted = [...horses].sort(
    (a, b) => (a.horse_no || 0) - (b.horse_no || 0)
  );

  // コース情報
  const surf = surfShort(race?.surface || "");
  const courseStr =
    (surf || "") +
    (race?.distance ? race.distance + "m" : "") +
    (race?.direction ? `(${race.direction})` : "");

  const metaParts: string[] = [];
  if (race?.post_time) metaParts.push(`${race.post_time}発走`);
  if (courseStr) metaParts.push(courseStr);
  if (race?.condition) metaParts.push(`馬場:${race.condition}`);
  if (race?.field_count) metaParts.push(`${race.field_count}頭`);

  // ばんえい判定
  const isBanei = race?.is_banei || race?.venue === "帯広";

  // レースライブURL（JRA: 公式トップ / NAR: keiba-lv-st 会場別ライブ）
  const liveUrl = (() => {
    if (!race) return "";
    if (race.is_jra === false) {
      const track = NAR_LIVE_TRACK_MAP[race.venue || ""];
      if (!track) return "";
      return `https://simple.keiba-lv-st.jp/?track=${track}`;
    }
    return "https://www.jra.go.jp/keiba/";
  })();

  // オッズ取得
  const handleFetchOdds = useCallback(async () => {
    if (!race?.race_id) return;
    setOddsFetching(true);
    setOddsMsg("取得中…");
    try {
      const res = await api.raceOdds({
        race_id: race.race_id,
        date,
        venue,
        race_no: raceNo,
      });
      const r = res as unknown as Record<string, unknown>;
      if (!r.ok) {
        setOddsMsg(r.error as string || "オッズ未発売");
        setTimeout(() => setOddsMsg(""), 4000);
        return;
      }
      await queryClient.invalidateQueries({ queryKey: ["racePrediction", date, venue, raceNo] });
      // レース結果タブのキャッシュも無効化（30分キャッシュが効いていたため反映ラグがあった）
      await queryClient.invalidateQueries({ queryKey: ["raceResult", date] });
      // 発走後なら結果も取得されているはず
      const resultFetched = Boolean(r.result_fetched);
      const orderCount = (r.result_order_count as number) || 0;
      if (resultFetched && orderCount > 0) {
        setOddsMsg(`取得完了（オッズ・確率・印・レース結果${orderCount}頭分を更新）`);
      } else {
        setOddsMsg("取得完了（オッズ・確率・印を更新）");
      }
      setTimeout(() => setOddsMsg(""), 4000);
    } catch {
      setOddsMsg("取得失敗");
      setTimeout(() => setOddsMsg(""), 3000);
    } finally {
      setOddsFetching(false);
    }
  }, [race?.race_id, date, venue, raceNo, queryClient]);

  return (
    <div className="space-y-4">
      {/* 戻る + 会場タブ + レース番号タブ（sticky） */}
      <div className="sticky top-[var(--header-h,48px)] z-20 bg-background pb-2 space-y-1.5 -mx-4 px-4 pt-2 border-b border-border/50">
        <div className="flex items-center gap-2 flex-wrap">
          <Button variant="outline" size="sm" onClick={onClose}>
            ← レース一覧に戻る
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleFetchOdds}
            disabled={oddsFetching || !race?.race_id}
            className="bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-100 dark:hover:bg-emerald-900/60 border-emerald-200 dark:border-emerald-800 disabled:opacity-50"
          >
            {oddsFetching ? "更新中…" : "データ更新"}
          </Button>
          {liveUrl && (
            <a
              href={liveUrl}
              target="_blank"
              rel="noopener noreferrer"
              className={buttonVariants({
                variant: "outline",
                size: "sm",
                className:
                  "bg-sky-50 dark:bg-sky-950/40 text-sky-700 dark:text-sky-400 hover:bg-sky-100 dark:hover:bg-sky-900/60 border-sky-200 dark:border-sky-800",
              })}
            >
              レースライブ ↗
            </a>
          )}
          {oddsMsg && (
            <span className="text-xs text-muted-foreground">{oddsMsg}</span>
          )}
        </div>
        {/* 会場タブ */}
        {venues.length > 1 && (
          <div className="flex gap-1 overflow-x-auto">
            {venues.map((v) => (
              <button
                key={v}
                onClick={() => {
                  const vRaces = venueRaces?.[v] || [];
                  const vMax = vRaces.length
                    ? Math.max(...vRaces.map((r) => r.race_no))
                    : 12;
                  onNavigate(v, Math.min(raceNo, vMax));
                }}
                className={`px-3 py-1.5 text-sm font-medium rounded-md whitespace-nowrap transition-colors ${
                  v === venue
                    ? "bg-primary text-primary-foreground"
                    : "bg-secondary text-secondary-foreground hover:bg-muted"
                }`}
              >
                {v}
              </button>
            ))}
          </div>
        )}

        {/* レース番号タブ */}
        <div className="flex gap-1 overflow-x-auto">
          {Array.from({ length: maxR }, (_, i) => i + 1).map((r) => (
            <button
              key={r}
              onClick={() => onNavigate(venue, r)}
              className={`px-3 py-1.5 text-sm font-medium rounded-md whitespace-nowrap transition-colors ${
                r === raceNo
                  ? "bg-primary text-primary-foreground"
                  : "bg-secondary text-secondary-foreground hover:bg-muted"
              }`}
            >
              {r}R
            </button>
          ))}
        </div>
      </div>

      {/* 読み込み中 */}
      {isLoading && (
        <p className="text-sm text-muted-foreground py-8 text-center animate-pulse">
          読み込み中...
        </p>
      )}

      {/* エラー */}
      {error && (
        <p className="text-sm text-destructive py-4">
          エラー: {(error as Error).message}
        </p>
      )}

      {race && (
        <>
          {/* ① レースヘッダー — コンパクトストリップ */}
          <div className="px-1 space-y-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xl font-bold text-brand-gold">
                {race.race_no}R
              </span>
              <span className="text-lg font-bold font-heading">
                {race.race_name || ""}
              </span>
              {race.grade && <GradeBadge grade={race.grade} />}
            </div>
            <div className="flex items-center gap-2 text-sm text-muted-foreground flex-wrap">
              {metaParts.map((p, i) => (
                <span key={i}>
                  {i > 0 && <span className="mx-1 text-border">/</span>}
                  {p}
                </span>
              ))}
              {oddsUpdatedAt ? (
                <span className="ml-auto text-xs">
                  最終オッズ {oddsUpdatedAt.slice(11, 16)}
                </span>
              ) : (
                <span className="ml-auto text-xs text-muted-foreground">
                  ※予測オッズ
                </span>
              )}
            </div>
          </div>

          {/* ── タブ② コース分析（ばんえいのみ） ── */}
          {isBanei && (
            <PremiumCard variant="default" padding="sm" className="space-y-2">
                <div className="flex items-center gap-4 text-sm flex-wrap">
                  <span>
                    水分量: <strong>{race?.water_content != null ? race.water_content.toFixed(1) + "%" : "—"}</strong>
                  </span>
                  <span>
                    斤量: <strong>
                      {(() => {
                        const ws = sorted.map(h => h.weight_kg).filter((w): w is number => w != null && w > 0);
                        if (ws.length === 0) return "—";
                        return `${Math.min(...ws).toFixed(0)}〜${Math.max(...ws).toFixed(0)}kg`;
                      })()}
                    </strong>
                  </span>
                  <span>
                    馬体重: <strong>
                      {(() => {
                        const ws = sorted.map(h => h.horse_weight).filter((w): w is number => w != null && w > 0);
                        if (ws.length === 0) return "—";
                        return `${Math.min(...ws).toFixed(0)}〜${Math.max(...ws).toFixed(0)}kg`;
                      })()}
                    </strong>
                  </span>
                </div>
            </PremiumCard>
          )}
          {/* ペース・予想タイム情報は「展開」タブ（PaceFormation内）に移設 */}

          {/* ── タブ③ 馬分析 ── */}
          <TabGroup3Horse
            horses={sorted}
            race={race}
            isBanei={isBanei}
            raceId={race?.race_id}
            date={date}
            raceNo={raceNo}
          />
        </>
      )}
    </div>
  );
}

// 型定義（APIレスポンス互換）
export interface RaceDetail {
  race_no: number;
  race_name?: string;
  venue?: string;
  post_time?: string;
  surface?: string;
  distance?: number;
  direction?: string;
  condition?: string;
  field_count?: number;
  grade?: string;
  confidence?: string;
  overall_confidence?: string;
  pace_predicted?: string;
  pace_reliability_label?: string;
  estimated_front_3f?: number;
  estimated_last_3f?: number;
  estimated_mid_time?: number;
  predicted_race_time?: number;
  final_formation?: Record<string, number[]>;
  llm_pace_comment?: string;
  pace_comment?: string;
  llm_mark_comment?: string;
  mark_comment_rich?: string;
  horses?: HorseData[];
  tickets?: TicketData[];
  formation_tickets?: TicketData[];
  formation_columns?: { col1?: number[]; col2?: number[]; col3?: number[] };
  bet_decision?: BetDecision;        // 買わない判定 (Phase 1-b)
  tickets_by_mode?: TicketsByMode;   // 3モード (Phase 1-c)
  race_id?: string;
  is_jra?: boolean;
  is_banei?: boolean;
  water_content?: number;
  result_cname?: string;
  shutuba_cname?: string;
  top10_odds?: {
    umaren?: Array<{ combo: number[]; odds: number }>;
    umatan?: Array<{ combo: number[]; odds: number }>;
    sanrenpuku?: Array<{ combo: number[]; odds: number }>;
    sanrentan?: Array<{ combo: number[]; odds: number }>;
  };
  [key: string]: unknown;
}

export interface TrainingRecord {
  date: string;
  venue: string;
  course: string;
  splits: Record<string, number>;
  partner: string;
  position: string;
  rider: string;
  track_condition: string;
  lap_count: string;
  intensity_label: string;
  sigma_from_mean: number | null;
  comment: string;
}

export interface HorseData {
  horse_no: number;
  horse_name: string;
  gate_no?: number;
  sex?: string;
  age?: number;
  weight_kg?: number;
  horse_weight?: number;
  weight_change?: number;
  jockey?: string;
  trainer?: string;
  mark?: string;
  running_style?: string;
  composite?: number;
  ability_total?: number;
  pace_total?: number;
  course_total?: number;
  jockey_change_score?: number;
  win_prob?: number;
  place2_prob?: number;
  place3_prob?: number;
  odds?: number;
  predicted_tansho_odds?: number;
  popularity?: number;
  predicted_rank?: number;
  ability_trend?: string;
  divergence_signal?: string;
  past_runs?: PastRunData[];
  past_3_runs?: PastRunData[];
  horse_comment?: string;
  horse_diagnosis?: string;
  training_records?: TrainingRecord[];
  stable_comment_paraphrased?: string; // LLM パラフレーズ済みコメント（設定済み時はフロント表示で優先）
  jockey_grade?: string;
  trainer_grade?: string;
  sire_grade?: string;
  mgs_grade?: string;
  jockey_dev?: number;
  trainer_dev?: number;
  bloodline_dev?: number;
  training_dev?: number;
  ev?: number; // 期待値 (win_prob × effective_odds)
  ability_grades?: Record<string, string>;
  pace_grades?: Record<string, string>;
  course_grades?: Record<string, string>;
  // Plan-γ Phase 2: 同レース内 ability_total z-score 正規化偏差値（20〜80, 50中心）
  race_relative_dev?: number;
  // Plan-γ Phase 3: ハイブリッド合算指数 = ability_total*(1-β) + race_relative_dev*β
  hybrid_total?: number;
  [key: string]: unknown;
}

export interface PastRunData {
  date?: string;
  venue?: string;
  surface?: string;
  distance?: number;
  condition?: string;
  class?: string;
  jockey?: string;
  weight_kg?: number;
  horse_no?: number;
  field_count?: number;
  finish_pos?: number;
  last_3f?: number;
  last_3f_rank?: number;
  finish_time?: number;
  finish_time_sec?: number;
  margin?: number;
  positions_corners?: string;
  position_4c?: number;
  pace?: string;
  race_level_grade?: string;
  speed_dev_grade?: string;
  speed_dev?: number;
  race_id?: string;
  race_no?: number;
  result_cname?: string;
}

export interface TicketData {
  type: string;
  combo?: number[];
  prob?: number;
  odds?: number;
  ev?: number;
  stake?: number;
  mark_a?: string;
  mark_b?: string;
  mark_c?: string;
  a?: number;
  b?: number;
  c?: number;
  appearance?: number;          // 出現率 %（prob*100 と等価、フロント表示用）
  odds_source?: "real" | "estimated";  // 実オッズ or 推定（Phase 1-a）
  payback_if_hit?: number;      // ヒット時の払戻金（Phase 1-b）
  net_profit_if_hit?: number;   // ヒット時の純利益（Phase 1-b）
  mode?: "accuracy" | "balanced" | "recovery";  // 3モードの識別（Phase 1-c）
  signal?: string;
  is_reference?: boolean;       // 参考ヒモフラグ（買わない時）
  skip_reason?: string;         // low_ev / low_recovery / torigami / confidence_kelly_zero
}

/** 買う/買わない判定 (Phase 1-b) */
export interface BetDecision {
  skip: boolean;
  reason: "low_ev" | "dispersed" | "low_confidence" | "torigami" | null;
  message: string;
  reference_tickets: TicketData[];
}

/** 買い目 (Phase 3: 三連単フォーメーション、後方互換のため旧モードキーも保持) */
export interface TicketsByMode {
  fixed?: TicketData[];      // Phase 3: 三連単フォーメーション
  accuracy?: TicketData[];   // (deprecated) Phase 1-c 互換
  balanced?: TicketData[];   // (deprecated)
  recovery?: TicketData[];   // (deprecated)
  _meta?: {
    skipped?: boolean;
    skip_reason?: string;
    race_ev_ratio?: number;
    candidates_n?: number;
    max_budget?: number;
    format?: string;
  };
}
