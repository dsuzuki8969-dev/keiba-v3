import { useMemo } from "react";
import { useNavigate } from "react-router-dom";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { useTodayPredictions, useHomeInfo } from "@/api/hooks";
import { useLocalDate } from "@/hooks/useLocalDate";
import { useViewMode } from "@/hooks/useViewMode";
import { ConfidenceBadge } from "@/components/keiba/ConfidenceBadge";
import { StatsCard } from "@/components/keiba/StatsCard";
import { JRA_CODES } from "@/lib/constants";
import { OperationsPanel } from "./TodayPage/OperationsPanel";
import { HomePageHero } from "./HomePageHero";
import { DataQualityBanner } from "@/components/keiba/DataQualityBanner";
import { MapPin, Crosshair, Sparkles } from "lucide-react";

// ── TodayStatsPanel は T-031 (2026-04-28) で StatsCard に統合済み ──
// ── 旧 TodayStatsPanel 定義はここから削除 ──

/*
 * 削除済みコード（参照用コメント）:
 *   function TodayStatsPanel({ date }: { date: string }) { ... }
 * → frontend/src/components/keiba/StatsCard.tsx に移管。
 *   呼び出し箇所は <StatsCard date={date} title="本日のリアルタイム成績" showRefreshButton />
 */

// ── 競馬場ロゴ ──
function VenueLogo({ name, isJra }: { name: string; isJra: boolean }) {
  // JRAは全場共通ロゴ、NARは競馬場ごとのロゴ
  // 帯広はSVG（2:1アスペクト比）→ object-cover で正方形に合わせる
  const isBanei = name === "帯広";
  const src = isJra ? "/logos/JRA.jpg" : isBanei ? "/logos/帯広.svg" : `/logos/${name}.jpg`;
  const srcFallback = `/logos/${name}.svg`;
  return (
    <img
      src={src}
      alt={name}
      className={`flex-shrink-0 w-8 h-8 sm:w-12 sm:h-12 md:w-16 md:h-16 rounded-lg ${isBanei ? "object-cover" : "object-contain"}`}
      onError={(e) => {
        const el = e.currentTarget;
        // JPG失敗時はSVGにフォールバック
        if (el.src.endsWith(".jpg") && !isJra) {
          el.src = srcFallback;
          return;
        }
        // SVGも失敗時はイニシャル表示
        el.style.display = "none";
        const div = document.createElement("div");
        div.className = `flex-shrink-0 w-16 h-16 rounded-lg ${isJra ? "bg-primary" : "bg-orange-500"} flex items-center justify-center`;
        div.innerHTML = `<span class="text-white font-bold text-sm">${name.slice(0, 1)}</span>`;
        el.parentNode?.insertBefore(div, el);
      }}
    />
  );
}

export default function HomePage() {
  const date = useLocalDate();
  const { data: pred, isLoading, refetch } = useTodayPredictions(date);
  const { data: info } = useHomeInfo(date);
  const navigate = useNavigate();
  // v6.1.20: モバイル表示時は PIVOT/DARK を 1 カラムに (md: メディアクエリは
  // 画面幅依存で、useViewMode="mobile" 時の max-w-[430px] に追随しないため)
  const { isMobile } = useViewMode();

  const order = (pred?.order || []) as string[];
  const races = (pred?.races || {}) as Record<string, RaceSummary[]>;
  const weather = (info?.weather || {}) as Record<string, WeatherInfo>;
  const venueInfoList = (info?.venues || []) as VenueInfoItem[];
  const venueNames = venueInfoList.map((v) => v.name);

  // 絶対軸 — ◉優先、足りなければ◎で補完して上位5頭（勝率降順）
  // 依存を pred のみにすることで useMemo の安定化を実現（reviewer 指摘 HIGH #3 対応）
  const jikuList = useMemo(() => {
    const _order = (pred?.order || []) as string[];
    const _races = (pred?.races || {}) as Record<string, RaceSummary[]>;
    const tier1: (RaceSummary & { venue: string })[] = [];
    const tier2: (RaceSummary & { venue: string })[] = [];
    for (const v of _order) {
      for (const r of _races[v] || []) {
        if (r.honmei_mark === "◉") tier1.push({ ...r, venue: v });
        else if (r.honmei_mark === "◎") tier2.push({ ...r, venue: v });
      }
    }
    tier1.sort((a, b) => (b.honmei_win_pct || 0) - (a.honmei_win_pct || 0));
    tier2.sort((a, b) => (b.honmei_win_pct || 0) - (a.honmei_win_pct || 0));
    return [...tier1, ...tier2].slice(0, 5);
  }, [pred]);

  // 厳選穴馬 — ☆印 + 高乖離馬（バックエンドで計算済み）
  const anaHorses = (pred?.ana_horses || []).slice(0, 5) as AnaHorse[];

  const goToRace = (venue: string, raceNo: number) => {
    navigate("/today", { state: { venue, raceNo } });
  };

  return (
    <div className="space-y-6 max-w-5xl mx-auto">
      {/* データ品質警告バナー（再発防止策 #4 - 閾値超え時のみ表示） */}
      <DataQualityBanner />

      {/* v6.1 プレミアムヒーロー — 日付＋開催場＋TOP3 */}
      <HomePageHero
        date={date}
        venueCount={order.length}
        venuesLabel={order.join(" / ")}
        races={jikuList.slice(0, 3).map((r) => ({
          venue: r.venue,
          race_no: r.race_no,
          name: r.name || r.race_name,
          post_time: r.post_time,
          grade: r.grade,
          overall_confidence: r.overall_confidence,
          honmei_mark: r.honmei_mark,
          honmei_name: r.honmei_name,
          honmei_no: r.honmei_no,
          honmei_win_pct: r.honmei_win_pct,
          honmei_composite: r.honmei_composite,
          honmei_odds: r.honmei_odds,
        }))}
        onSelect={goToRace}
      />

      {isLoading && (
        <p className="text-sm text-muted-foreground py-8 text-center animate-pulse">
          読み込み中...
        </p>
      )}

      {/* 会場概要カード — v6.1.9 PremiumCard 統一 */}
      {venueInfoList.length > 0 && (
        <PremiumCard variant="default" padding="md">
          <PremiumCardHeader>
            <div className="flex flex-col gap-0.5">
              <PremiumCardAccent>
                <MapPin size={10} className="inline mr-1" />
                <span className="section-eyebrow">Venues</span>
              </PremiumCardAccent>
              <PremiumCardTitle className="text-sm flex items-center gap-2">
                本日の開催競馬場
                <span className="text-xs font-normal text-muted-foreground tnum">
                  {venueInfoList.length}場
                </span>
              </PremiumCardTitle>
            </div>
          </PremiumCardHeader>
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 xl:grid-cols-4 gap-2">
          {venueInfoList.map((v) => {
            const w = weather[v.name] || {};
            const isJra = JRA_CODES.has(v.code);
            const vRaces = (races[v.name] || []).slice().sort(
              (a, b) => (a.post_time || "").localeCompare(b.post_time || "")
            );
            const now = new Date();
            const nowHM =
              String(now.getHours()).padStart(2, "0") + ":" +
              String(now.getMinutes()).padStart(2, "0");
            const nextRace = vRaces.find((r) => (r.post_time || "") > nowHM);
            // 全レース終了の会場はカード非表示（ゾンビ表示防止）
            if (!nextRace) return null;

            return (
              <PremiumCard
                key={v.name}
                variant="default"
                padding="sm"
                interactive
                as="button"
                aria-label={`${v.name} の次レースへ移動`}
                onClick={() => goToRace(v.name, nextRace.race_no)}
                className="text-left stylish-card-hover border border-border/60"
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="space-y-1 min-w-0 flex-1">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="font-bold text-sm whitespace-nowrap">{v.name}</span>
                      <span
                        className={
                          isJra
                            ? "text-[10px] px-1 py-0.5 rounded font-bold bg-gradient-to-br from-brand-navy to-brand-navy-light text-white whitespace-nowrap"
                            : "text-[10px] px-1 py-0.5 rounded font-bold bg-gradient-to-br from-amber-500 to-amber-700 text-white whitespace-nowrap"
                        }
                      >
                        {isJra ? "中央" : "地方"}
                      </span>
                    </div>
                    {/* 天気条件: 未取得の場合もプレースホルダで行高を確保し、会場カード間の行ズレを防ぐ (T-018) */}
                    <div className="text-xs text-muted-foreground min-h-[1rem]">
                      {w.condition || ''}
                    </div>
                    {nextRace && (
                      <>
                        <div className="text-xs flex items-center gap-1 flex-wrap">
                          <span className="font-bold text-foreground tnum">{nextRace.race_no}R</span>
                          <span className="text-muted-foreground tnum">{nextRace.post_time || ""}</span>
                          {nextRace.overall_confidence && (
                            <ConfidenceBadge rank={(nextRace.overall_confidence || "").replace(/\u207a/g, "+")} />
                          )}
                        </div>
                        {nextRace.name && (
                          <div className="text-sm font-medium truncate">{nextRace.name}</div>
                        )}
                      </>
                    )}
                  </div>
                  <VenueLogo name={v.name} isJra={isJra} />
                </div>
              </PremiumCard>
            );
          })}
        </div>
        </PremiumCard>
      )}

      {/* データなし */}
      {!isLoading && !order.length && (
        <p className="text-sm text-muted-foreground py-8 text-center">
          {date} の開催情報はありません
        </p>
      )}

      {/* マスター指示 2026-04-22: 本日のリアルタイム成績（◉◎単勝 + 三連単F） */}
      {/* T-031 (2026-04-28): TodayStatsPanel → StatsCard に統合 */}
      {order.length > 0 && (
        <StatsCard date={date} title="本日のリアルタイム成績" showRefreshButton />
      )}

      <div className={isMobile ? "grid grid-cols-1 gap-4" : "grid grid-cols-1 md:grid-cols-2 gap-4"}>
        {/* 絶対軸（◉印）セクション */}
        {jikuList.length > 0 && (
          <PremiumCard variant="default" padding="md">
            <PremiumCardHeader>
              <div className="flex flex-col gap-0.5">
                <PremiumCardAccent>
                  <Crosshair size={10} className="inline mr-1" />
                  <span className="section-eyebrow">Pivot Horses</span>
                </PremiumCardAccent>
                <PremiumCardTitle className="text-sm flex items-center gap-2">
                  本日の絶対軸
                  <span className="text-xs font-normal text-muted-foreground tnum">TOP 5</span>
                </PremiumCardTitle>
              </div>
            </PremiumCardHeader>
            <div className="space-y-2">
              {jikuList.map((r) => {
                const conf = (r.overall_confidence || "").replace(/\u207a/g, "+");
                const wp = Number(r.honmei_win_pct || 0);
                const rp = Number(r.honmei_rentai_pct || 0);
                const fp = Number(r.honmei_fukusho_pct || 0);
                const comp = Number(r.honmei_composite || 0);
                const gap = Number(r.composite_gap || 0);
                return (
                  <div
                    key={`${r.venue}-${r.race_no}`}
                    className="p-3 rounded-lg bg-muted/50 hover:bg-muted cursor-pointer transition-colors stylish-card-hover border border-border/40"
                    onClick={() => goToRace(r.venue, r.race_no)}
                  >
                    {/* 1行目: 評価 レース 印 馬名 */}
                    <div className="flex items-center gap-2 mb-2 flex-wrap">
                      <ConfidenceBadge rank={conf} />
                      <span className="font-bold text-sm">{r.venue}{r.race_no}R</span>
                      {r.grade && /^(G[123]|Jpn[123]|L|OP|重賞|特別)$/i.test(r.grade) && (
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-bold text-white ${
                          /G1|Jpn1/i.test(r.grade) ? "bg-red-600" : /G2|Jpn2/i.test(r.grade) ? "bg-blue-600" : /G3|Jpn3/i.test(r.grade) ? "bg-green-600" : "bg-orange-500"
                        }`}>{r.grade}</span>
                      )}
                      <span className="font-bold text-foreground">{r.honmei_mark}</span>
                      <span className="text-sm font-semibold">{r.honmei_name || ""}</span>
                    </div>
                    {/* 2行目: オッズ(人気) 総合指数(差) */}
                    <div className="flex items-center gap-x-4 gap-y-1 text-xs ml-8 mb-2 text-muted-foreground flex-wrap">
                      {r.honmei_odds != null && r.honmei_odds > 0 && (
                        <span className="tabular-nums whitespace-nowrap">
                          <span className="stat-mono text-sm text-foreground">{Number(r.honmei_odds).toFixed(1)}</span>
                          <span>倍</span>
                          {r.honmei_popularity != null && (
                            <span className="ml-0.5">({r.honmei_popularity}人気)</span>
                          )}
                        </span>
                      )}
                      {comp > 0 && (
                        <span className="tabular-nums whitespace-nowrap">
                          <span>総合</span>
                          <span className="stat-mono text-sm ml-0.5 text-foreground">{comp.toFixed(1)}</span>
                          {gap > 0 && <span className="ml-0.5">(+{gap.toFixed(1)})</span>}
                        </span>
                      )}
                    </div>
                    {/* 3行目: 勝率 連対率 複勝率 */}
                    <div className="flex items-center gap-x-4 gap-y-1 text-xs ml-8 text-muted-foreground flex-wrap">
                      <span className="tabular-nums whitespace-nowrap">
                        <span>勝</span>
                        <span className="stat-mono text-sm ml-0.5 text-foreground">{wp.toFixed(1)}%</span>
                      </span>
                      <span className="tabular-nums whitespace-nowrap">
                        <span>連</span>
                        <span className="stat-mono text-sm ml-0.5 text-foreground">{rp.toFixed(1)}%</span>
                      </span>
                      <span className="tabular-nums whitespace-nowrap">
                        <span>複</span>
                        <span className="stat-mono text-sm ml-0.5 text-foreground">{fp.toFixed(1)}%</span>
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </PremiumCard>
        )}

        {/* 厳選穴馬 — ☆印 + 高乖離馬 */}
        {anaHorses.length > 0 && (
          <PremiumCard variant="default" padding="md">
            <PremiumCardHeader>
              <div className="flex flex-col gap-0.5">
                <PremiumCardAccent>
                  <Sparkles size={10} className="inline mr-1" />
                  <span className="section-eyebrow">Dark Horses</span>
                </PremiumCardAccent>
                <PremiumCardTitle className="text-sm flex items-center gap-2">
                  本日の厳選穴馬
                  <span className="text-xs font-normal text-muted-foreground tnum">TOP {anaHorses.length}</span>
                </PremiumCardTitle>
              </div>
            </PremiumCardHeader>
            <div className="space-y-2">
              {anaHorses.map((h) => {
                const stars = h.star_rating || 1;
                const starStr = "★".repeat(stars);
                const starCls = stars === 3 ? "text-foreground text-base"
                  : stars === 2 ? "text-foreground/80 text-sm"
                  : "text-foreground/60 text-sm";
                return (
                  <div
                    key={`${h.venue}-${h.race_no}-${h.horse_no}`}
                    className="flex gap-2 p-3 rounded-lg bg-muted/50 hover:bg-muted cursor-pointer transition-colors stylish-card-hover border border-border/40"
                    onClick={() => goToRace(h.venue, h.race_no)}
                  >
                    {/* 左カラム: 星（★3つ分の固定幅） */}
                    <div className={`w-[3em] flex-shrink-0 text-center font-bold pt-0.5 ${starCls}`}>{starStr}</div>
                    {/* 右カラム: 情報3行 */}
                    <div className="flex-1 min-w-0">
                      {/* 1行目: 競馬場+R 印 馬名 */}
                      <div className="flex items-center gap-2 mb-2 flex-wrap">
                        <span className="font-bold text-sm">{h.venue}{h.race_no}R</span>
                        {h.mark && <span className="font-bold text-foreground">{h.mark}</span>}
                        <span className="text-sm font-semibold">{h.horse_name}</span>
                      </div>
                      {/* 2行目: オッズ(人気) 総合指数 */}
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs mb-2 text-muted-foreground flex-wrap">
                        {h.odds > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            <span className="stat-mono text-sm text-foreground">{Number(h.odds).toFixed(1)}</span>
                            <span>倍</span>
                            {h.popularity > 0 && (
                              <span className="ml-0.5">({h.popularity}人気)</span>
                            )}
                          </span>
                        )}
                        {h.composite > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            <span>総合</span>
                            <span className="stat-mono text-sm ml-0.5 text-foreground">{h.composite.toFixed(1)}</span>
                          </span>
                        )}
                      </div>
                      {/* 3行目: 複勝率 妙味 */}
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs text-muted-foreground flex-wrap">
                        <span className="tabular-nums whitespace-nowrap">
                          <span>複勝</span>
                          <span className="stat-mono text-sm ml-0.5 text-foreground">{h.place3_prob.toFixed(1)}%</span>
                        </span>
                        <span className="tabular-nums whitespace-nowrap">
                          <span>妙味</span>
                          <span className="stat-mono text-sm ml-0.5 text-foreground">{h.miryoku.toFixed(2)}</span>
                        </span>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </PremiumCard>
        )}
      </div>

      {/* 各種取得（最下部） */}
      <OperationsPanel
        date={date}
        venues={venueNames}
        onAnalyzeComplete={() => refetch()}
      />
    </div>
  );
}

// ── 型定義 ──
interface RaceSummary {
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
  honmei_win_pct?: number;
  honmei_rentai_pct?: number;
  honmei_fukusho_pct?: number;
  honmei_composite?: number;
  composite_gap?: number;
  honmei_no?: number;
  honmei_odds?: number;
  honmei_popularity?: number;
  bet_others?: {no: number; mark: string}[];
  bet_tan_hit?: number;
  bet_tan_ev?: number;
  bet_umaren_hit?: number;
  bet_umaren_ev?: number;
  bet_umaren_count?: number;
  bet_sanren_hit?: number;
  bet_sanren_ev?: number;
  bet_sanren_count?: number;
  fm_sanren_count?: number;
  fm_sanren_hit?: number;
  fm_sanren_ev?: number;
  [key: string]: unknown;
}

interface AnaHorse {
  venue: string;
  race_no: number;
  race_name: string;
  post_time: string;
  horse_no: number;
  horse_name: string;
  mark: string;
  odds: number;
  popularity: number;
  composite: number;
  win_prob: number;
  place3_prob: number;
  miryoku: number;
  star_rating: number;
  is_star: boolean;
}

interface WeatherInfo {
  condition?: string;
  precip_prob?: number;
}

interface VenueInfoItem {
  name: string;
  code: string;
  race_count?: number;
}
