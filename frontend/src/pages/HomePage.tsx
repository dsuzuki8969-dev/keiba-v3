import { useMemo, useCallback } from "react";
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
import { MapPin, Crosshair, Sparkles, AlertTriangle } from "lucide-react";

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
      loading="lazy"
      decoding="async"
      width={64}
      height={64}
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
    // 絶対軸=断トツ順: 2位との指数差(composite_gap)が大きい順（master指示 2026-06-28）
    tier1.sort((a, b) => (b.composite_gap || 0) - (a.composite_gap || 0));
    tier2.sort((a, b) => (b.composite_gap || 0) - (a.composite_gap || 0));
    return [...tier1, ...tier2].slice(0, 5);
  }, [pred]);

  // 厳選穴馬 — ☆印 + 高乖離馬（バックエンドで計算済み）: 乖離ショーケース「妙味」枠
  const anaHorses = (pred?.ana_horses || []).slice(0, 5) as AnaHorse[];

  // 危険な人気馬 — 人気1〜3位だが実力順位が人気順位より3以上下（過大評価）
  const kikenHorses = (pred?.kiken_horses || []).slice(0, 5) as KikenHorse[];

  // 乖離ショーケース「拮抗・波乱注意」枠 — 上位2頭の軸馬度差(jiku_gap)が小さい本命馬
  // jiku_gap<6 = 上位の軸馬度が拮抗し本命が紛れやすい波乱含みレース（自信度と同一軸）
  // (scripts/analyze_kikko_jiku.py: jiku_gap<6 で◎複勝率が全体比9.6pt低下・単調でtop3_rangeより判別力高い)
  const kikenRaces = useMemo(() => {
    const _order = (pred?.order || []) as string[];
    const _races = (pred?.races || {}) as Record<string, RaceSummary[]>;
    const candidates: (RaceSummary & { venue: string })[] = [];
    for (const v of _order) {
      for (const r of _races[v] || []) {
        const pop = r.honmei_popularity ?? 99;
        const gap = r.jiku_gap ?? 999;
        // 1〜3人気なのに上位2頭の軸馬度差が6未満 = 実力拮抗の「危険な本命」候補
        if (pop <= 3 && gap < 6.0) {
          candidates.push({ ...r, venue: v });
        }
      }
    }
    // jiku_gap 昇順（最も拮抗している順）で上位5件
    candidates.sort((a, b) => (a.jiku_gap ?? 999) - (b.jiku_gap ?? 999));
    return candidates.slice(0, 5);
  }, [pred]);

  const goToRace = useCallback((venue: string, raceNo: number) => {
    navigate("/today", { state: { venue, raceNo } });
  }, [navigate]);

  // ヒーロー用 TOP3 レースリスト — jikuList が変わらない限り再生成しない
  const heroRaces = useMemo(() => jikuList.slice(0, 3).map((r) => ({
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
  })), [jikuList]);

  return (
    <div className="space-y-6 max-w-5xl mx-auto">
      {/* データ品質警告バナー（再発防止策 #4 - 閾値超え時のみ表示） */}
      <DataQualityBanner />

      {/* v6.1 プレミアムヒーロー — 日付＋開催場＋TOP3 */}
      <HomePageHero
        date={date}
        venueCount={order.length}
        venuesLabel={order.join(" / ")}
        races={heroRaces}
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

      {/* 4枚 2x2 グリッド: 絶対軸 / 穴馬 / 危険な人気馬 / 拮抗 */}
      <div className={isMobile ? "grid grid-cols-1 gap-4" : "grid grid-cols-1 md:grid-cols-2 gap-4"}>
        {/* 左上: 絶対軸（◉印） */}
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
                const conf = (r.overall_confidence || "").replace(/⁺/g, "+");
                const wp = Number(r.honmei_win_pct || 0);
                const rp = Number(r.honmei_rentai_pct || 0);
                const fp = Number(r.honmei_fukusho_pct || 0);
                const comp = Number(r.honmei_composite || 0);
                const gap = Number(r.composite_gap || 0);
                return (
                  <div
                    key={`${r.venue}-${r.race_no}`}
                    className="p-3 rounded-lg bg-muted/50 hover:bg-muted cursor-pointer transition-colors stylish-card-hover border border-border/40"
                    role="button"
                    tabIndex={0}
                    onClick={() => goToRace(r.venue, r.race_no)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); goToRace(r.venue, r.race_no); } }}
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

        {/* 右上: 穴馬（妙味・実力上位・人気薄） */}
        {anaHorses.length > 0 && (
          <PremiumCard variant="default" padding="md">
            <PremiumCardHeader>
              <div className="flex flex-col gap-0.5">
                <PremiumCardAccent>
                  <Sparkles size={10} className="inline mr-1" />
                  <span className="section-eyebrow">Dark Horse</span>
                </PremiumCardAccent>
                <PremiumCardTitle className="text-sm flex items-center gap-2">
                  穴馬（妙味）
                  <span className="text-xs font-normal text-muted-foreground">実力上位・過小評価</span>
                </PremiumCardTitle>
              </div>
            </PremiumCardHeader>
            <div className="space-y-2">
              {anaHorses.map((h) => {
                const stars = h.star_rating || 1;
                const starStr = "★".repeat(stars);
                const starCls = stars === 3 ? "text-emerald-500 text-base"
                  : stars === 2 ? "text-emerald-500/70 text-sm"
                  : "text-emerald-500/40 text-sm";
                return (
                  <div
                    key={`${h.venue}-${h.race_no}-${h.horse_no}`}
                    className="flex gap-2 p-3 rounded-lg bg-emerald-50/40 dark:bg-emerald-950/20 hover:bg-emerald-50/70 dark:hover:bg-emerald-950/40 cursor-pointer transition-colors border border-emerald-200/40 dark:border-emerald-800/30"
                    role="button"
                    tabIndex={0}
                    onClick={() => goToRace(h.venue, h.race_no)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); goToRace(h.venue, h.race_no); } }}
                  >
                    {/* 左カラム: 星 */}
                    <div className={`w-[3em] flex-shrink-0 text-center font-bold pt-0.5 ${starCls}`}>{starStr}</div>
                    {/* 右カラム */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                        <span className="font-bold text-sm">{h.venue}{h.race_no}R</span>
                        {h.mark && <span className="font-bold text-foreground">{h.mark}</span>}
                        <span className="text-sm font-semibold">{h.horse_name}</span>
                      </div>
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs mb-1.5 text-muted-foreground flex-wrap">
                        {h.odds > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            <span className="stat-mono text-sm text-foreground">{Number(h.odds).toFixed(1)}</span>倍
                            {h.popularity > 0 && <span className="ml-0.5">({h.popularity}人気)</span>}
                          </span>
                        )}
                        {h.composite > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            総合<span className="stat-mono text-sm ml-0.5 text-foreground">{h.composite.toFixed(1)}</span>
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs text-muted-foreground flex-wrap">
                        <span className="tabular-nums whitespace-nowrap">
                          複勝<span className="stat-mono text-sm ml-0.5 text-emerald-600 dark:text-emerald-400">{h.place3_prob.toFixed(1)}%</span>
                        </span>
                        {/* 穴馬度（新）: ana_do があれば優先、なければ旧miryokuで互換表示 */}
                        {(() => {
                          const score = h.ana_do ?? h.miryoku;
                          const label = h.ana_do != null ? "穴馬度" : "妙味度";
                          const colorCls = score >= 65
                            ? "text-emerald-600 dark:text-emerald-400"
                            : score >= 50
                              ? "text-emerald-500/80 dark:text-emerald-400/80"
                              : "text-emerald-500/60 dark:text-emerald-400/60";
                          return (
                            <span className="tabular-nums whitespace-nowrap">
                              {label}<span className={`stat-mono text-sm ml-0.5 font-bold ${colorCls}`}>{score.toFixed(1)}点</span>
                            </span>
                          );
                        })()}
                        {/* 軸馬度（新）*/}
                        {h.jiku_score != null && (
                          <span className="tabular-nums whitespace-nowrap">
                            軸馬度<span className="stat-mono text-sm ml-0.5 font-bold text-foreground">{h.jiku_score.toFixed(1)}点</span>
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </PremiumCard>
        )}

        {/* 左下: 危険な人気馬（人気上位・実力下位・過大評価） */}
        {kikenHorses.length > 0 && (
          <PremiumCard variant="default" padding="md">
            <PremiumCardHeader>
              <div className="flex flex-col gap-0.5">
                <PremiumCardAccent>
                  <AlertTriangle size={10} className="inline mr-1" />
                  <span className="section-eyebrow">Overrated</span>
                </PremiumCardAccent>
                <PremiumCardTitle className="text-sm flex items-center gap-2">
                  危険な人気馬
                  <span className="text-xs font-normal text-muted-foreground">人気上位・軸馬度低</span>
                </PremiumCardTitle>
              </div>
            </PremiumCardHeader>
            <div className="space-y-2">
              {kikenHorses.map((h) => {
                const jikuRank = h.jiku_rank;
                const pop = h.popularity;
                const overVal = h.over ?? (jikuRank - pop);
                return (
                  <div
                    key={`${h.venue}-${h.race_no}-${h.horse_no}`}
                    className="flex gap-2 p-3 rounded-lg bg-red-50/40 dark:bg-red-950/20 hover:bg-red-50/70 dark:hover:bg-red-950/40 cursor-pointer transition-colors border border-red-200/40 dark:border-red-800/30"
                    role="button"
                    tabIndex={0}
                    onClick={() => goToRace(h.venue, h.race_no)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); goToRace(h.venue, h.race_no); } }}
                  >
                    {/* 左カラム: 警告アイコン */}
                    <div className="w-[3em] flex-shrink-0 text-center pt-0.5">
                      <AlertTriangle size={16} className="text-red-500 inline-block" />
                    </div>
                    {/* 右カラム */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                        <span className="font-bold text-sm">{h.venue}{h.race_no}R</span>
                        {h.mark && <span className="font-bold text-foreground">{h.mark}</span>}
                        <span className="text-sm font-semibold">{h.horse_name}</span>
                      </div>
                      {/* 人気X位なのに軸馬度Y位 — 過大評価の根拠を名指し */}
                      <div className="text-xs text-red-600 dark:text-red-400 font-semibold mb-1.5 tabular-nums">
                        人気{pop}位なのに軸馬度{jikuRank}位 — 過大評価+{overVal}
                      </div>
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs text-muted-foreground flex-wrap">
                        {h.odds > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            <span className="stat-mono text-sm text-foreground">{Number(h.odds).toFixed(1)}</span>倍
                          </span>
                        )}
                        {h.composite > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            総合<span className="stat-mono text-sm ml-0.5 text-foreground">{h.composite.toFixed(1)}</span>
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </PremiumCard>
        )}

        {/* 右下: 拮抗（波乱注意） */}
        {kikenRaces.length > 0 && (
          <PremiumCard variant="default" padding="md">
            <PremiumCardHeader>
              <div className="flex flex-col gap-0.5">
                <PremiumCardAccent>
                  <AlertTriangle size={10} className="inline mr-1" />
                  <span className="section-eyebrow">Close Race</span>
                </PremiumCardAccent>
                <PremiumCardTitle className="text-sm flex items-center gap-2">
                  拮抗（波乱注意）
                  <span className="text-xs font-normal text-muted-foreground">上位2頭が伯仲</span>
                </PremiumCardTitle>
              </div>
            </PremiumCardHeader>
            <div className="space-y-2">
              {kikenRaces.map((r) => {
                const gap = r.jiku_gap ?? 999;
                return (
                  <div
                    key={`${r.venue}-${r.race_no}`}
                    className="flex gap-2 p-3 rounded-lg bg-amber-50/40 dark:bg-amber-950/20 hover:bg-amber-50/70 dark:hover:bg-amber-950/40 cursor-pointer transition-colors border border-amber-200/40 dark:border-amber-800/30"
                    role="button"
                    tabIndex={0}
                    onClick={() => goToRace(r.venue, r.race_no)}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); goToRace(r.venue, r.race_no); } }}
                  >
                    {/* 左カラム: 警告アイコン */}
                    <div className="w-[3em] flex-shrink-0 text-center pt-0.5">
                      <AlertTriangle size={16} className="text-amber-500 inline-block" />
                    </div>
                    {/* 右カラム */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                        <span className="font-bold text-sm">{r.venue}{r.race_no}R</span>
                        {r.honmei_mark && <span className="font-bold text-foreground">{r.honmei_mark}</span>}
                        <span className="text-sm font-semibold">{r.honmei_name || ""}</span>
                      </div>
                      <div className="flex items-center gap-x-4 gap-y-1 text-xs mb-1.5 text-muted-foreground flex-wrap">
                        {(r.honmei_odds ?? 0) > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            <span className="stat-mono text-sm text-foreground">{Number(r.honmei_odds).toFixed(1)}</span>倍
                            {(r.honmei_popularity ?? 0) > 0 && <span className="ml-0.5">({r.honmei_popularity}人気)</span>}
                          </span>
                        )}
                        {(r.honmei_composite ?? 0) > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            総合<span className="stat-mono text-sm ml-0.5 text-foreground">{Number(r.honmei_composite).toFixed(1)}</span>
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-x-3 text-xs text-muted-foreground flex-wrap">
                        <span className="tabular-nums whitespace-nowrap">
                          軸馬度差<span className="stat-mono text-sm ml-0.5 font-bold text-amber-600 dark:text-amber-400">
                            {gap < 999 ? gap.toFixed(1) : "—"}
                          </span>
                        </span>
                        {(r.honmei_fukusho_pct ?? 0) > 0 && (
                          <span className="tabular-nums whitespace-nowrap">
                            複勝<span className="stat-mono text-sm ml-0.5 text-foreground">{Number(r.honmei_fukusho_pct).toFixed(1)}%</span>
                          </span>
                        )}
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
  /** 上位3頭(1位-3位)のcomposite差。拮抗判定用: JRA<8/NAR<6 で波乱注意
   *  (scripts/analyze_kikko_threshold.py: top3差<6pt で◎複勝率が全体比15-20pt低下)
   */
  top3_range?: number;
  /** 本命馬の軸馬度(0-100)。ホームの絶対軸ソートに使用（表示専用・買い目非汚染） */
  honmei_jiku_score?: number;
  /** 本命jiku_score - 2位jiku_score の差。自信度表示バッジに使用（表示専用） */
  jiku_gap?: number;
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
  miryoku: number;       // 旧互換フィールド
  ana_do?: number;       // 新: 穴馬度 0-100
  jiku_score?: number;   // 新: 軸馬度 0-100
  star_rating: number;
  is_star: boolean;
}

// 危険な人気馬: 人気1〜3位だが軸馬度順位が人気順位より3以上下（過大評価）
interface KikenHorse {
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
  jiku_rank: number;     // 軸馬度降順ランク（1=最も軸信頼高）
  over: number;          // jiku_rank - 人気順位（過大評価の大きさ）
  jiku_score?: number;   // 軸馬度スコア 0-100
  divergence_signal: string;
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
