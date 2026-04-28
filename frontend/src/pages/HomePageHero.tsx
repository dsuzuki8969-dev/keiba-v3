/**
 * HomePageHero — トップページ最上部ヒーローセクション（Bloomberg × Linear 強化版）
 * --------------------------------------------------------------
 *  ┌─ 大見出し（金箔グラデの日付）格子パターン背景付き ─┐
 *  │ 開催場 + 開催日の曜日                              │
 *  └────────────────────────────────────────────────────┘
 *
 *  [1位：金グロー大型カード・常時グロー]
 *  [2位：navy-glow 中型・hover グロー]  [3位：default 中型・hover グロー]
 *
 *  強化点（v6.2）:
 *  - タイトルブロックに hero-grid-pattern + 上下金アクセント罫線
 *  - 数字フォント: font-mono tabular-nums（Bloomberg ターミナル感）
 *  - 大数字サイズ 1段 up（large: text-4xl / 通常: text-2xl）
 *  - グループ hover でアイコン scale-110 反応
 *  - 2/3 位カードは neon-card-hover クラスで hover:-translate-y-[2px] 強化
 *  - 全カードに focus-visible:ring-2 ring-brand-gold/40 フォーカスリング
 *  - 1 位カードは gold-pulse 常時アニメで存在感を最大化
 */

import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { MarkBadge } from "@/components/keiba/MarkBadge";
import { ConfidenceBadge } from "@/components/keiba/ConfidenceBadge";
import { GradeBadge } from "@/components/keiba/GradeBadge";
import { Flame, Sparkles, Trophy } from "lucide-react";

export interface HeroRaceItem {
  venue: string;
  race_no: number;
  name?: string;
  post_time?: string;
  grade?: string;
  overall_confidence?: string;
  honmei_mark?: string;
  honmei_name?: string;
  honmei_no?: number;
  honmei_win_pct?: number;
  honmei_composite?: number;
  honmei_odds?: number;
}

interface Props {
  date: string;
  venueCount: number;
  venuesLabel: string;
  races: HeroRaceItem[];
  onSelect: (venue: string, raceNo: number) => void;
}

// 曜日テーブル（日本語）
const WEEKDAYS = ["日", "月", "火", "水", "木", "金", "土"] as const;

function toWeekday(iso: string): string {
  const d = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return "";
  return WEEKDAYS[d.getDay()];
}

function formatDate(iso: string): { y: string; md: string; wd: string } {
  const [y, m, d] = iso.split("-");
  return { y: y ?? "", md: `${Number(m ?? "0")}月${Number(d ?? "0")}日`, wd: toWeekday(iso) };
}

// マークシンボル → MarkBadge キー変換
const toMarkKey = (sym?: string): string => {
  if (!sym) return "";
  if (sym === "◉") return "tekipan";
  if (sym === "◎") return "honmei";
  if (sym === "○") return "taikou";
  if (sym === "▲") return "tannuke";
  if (sym === "△") return "rendashi";
  if (sym === "★") return "rendashi2";
  if (sym === "☆") return "oana";
  return sym;
};

// ─────────────────────────────────────────────────
// ヒーローカード共通コンテンツ
// ─────────────────────────────────────────────────
function CardContent({ r, large = false }: { r: HeroRaceItem; large?: boolean }) {
  const markKey = toMarkKey(r.honmei_mark);
  const wp = Number(r.honmei_win_pct ?? 0);

  return (
    <div className="flex flex-col gap-2">
      {/* 場・R番号 */}
      <div className={`font-bold ${large ? "text-lg" : "text-base"} text-foreground`}>
        {r.venue}{r.race_no}R
      </div>

      {/* レース名（細金下線） */}
      {r.name && (
        <div
          className={`${large ? "text-sm" : "text-xs"} text-muted-foreground truncate border-b pb-1`}
          style={{ borderColor: "rgba(212,168,83,0.30)" }}
        >
          {r.name}
        </div>
      )}

      {/* バッジ行 */}
      <div className="flex items-center gap-1.5 flex-wrap">
        {r.grade && <GradeBadge grade={r.grade} />}
        {r.overall_confidence && (
          <ConfidenceBadge rank={r.overall_confidence} />
        )}
      </div>

      {/* 本命馬情報 */}
      {r.honmei_name && (
        <div className="flex items-center gap-2 flex-wrap mt-1">
          {/* 印バッジ */}
          {markKey && (
            <MarkBadge
              mark={markKey}
              size={large ? "lg" : "md"}
              subtle={!large}
            />
          )}

          {/* 馬名 */}
          <span className={`font-semibold ${large ? "text-base" : "text-sm"} text-foreground truncate max-w-[10rem]`}>
            {r.honmei_name}
          </span>
        </div>
      )}

      {/* 数値行（勝率 + オッズ）— Bloomberg Mono フォント */}
      <div className="flex items-baseline gap-3 flex-wrap">
        {wp > 0 && (
          large ? (
            /* 1位: 大数字・gold-gradient・微発光。font-size を直接指定してレスポンシブ確保 */
            <span
              className="font-mono tabular-nums tracking-tight font-extrabold gold-gradient whitespace-nowrap"
              style={{
                // モバイル小幅で `56.1%` がはみ出さないよう最小値を縮小
                fontSize: "clamp(1.8rem, 4.5vw, 3.5rem)",
                lineHeight: 1,
              }}
            >
              {wp.toFixed(1)}%
            </span>
          ) : (
            /* 2/3位: text-2xl・通常色 */
            <span className="font-mono tabular-nums tracking-tight font-bold text-2xl text-foreground">
              {wp.toFixed(1)}%
            </span>
          )
        )}
        {r.honmei_odds != null && r.honmei_odds > 0 && (
          <span className="font-mono tabular-nums text-sm text-muted-foreground">
            {r.honmei_odds.toFixed(1)} 倍
          </span>
        )}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────
// メインコンポーネント
// ─────────────────────────────────────────────────
export function HomePageHero({ date, venueCount, venuesLabel, races, onSelect }: Props) {
  const top3 = races.slice(0, 3);
  const [first, second, third] = top3;
  const { md, wd, y } = formatDate(date);

  return (
    <section className="space-y-4">
      {/* ── タイトルブロック（金箔グラデ日付・格子パターン背景） ── */}
      <div className="relative overflow-hidden rounded-2xl p-5 sm:p-7 bg-gradient-to-br from-brand-navy via-brand-navy-light to-brand-navy text-header-text shadow-[var(--shadow-lg)]">
        {/* 格子パターンオーバーレイ（Bloomberg ターミナル風・強化版 20% opacity） */}
        <div aria-hidden className="pointer-events-none absolute inset-0 rounded-2xl hero-grid-pattern-strong opacity-100" />

        {/* 微発光の装飾レイヤ */}
        <div
          aria-hidden
          className="pointer-events-none absolute inset-0 rounded-2xl opacity-20"
          style={{
            background:
              "radial-gradient(ellipse at 70% 40%, rgba(212,168,83,0.35) 0%, transparent 65%)",
          }}
        />

        {/* 上部アクセント罫線（金色） */}
        <div
          aria-hidden
          className="pointer-events-none absolute top-0 left-0 right-0 h-px rounded-t-2xl"
          style={{
            background:
              "linear-gradient(90deg, transparent 0%, rgba(212,168,83,0.4) 25%, rgba(240,215,140,0.6) 50%, rgba(212,168,83,0.4) 75%, transparent 100%)",
          }}
        />

        {/* 下部アクセント罫線（金色・薄め） */}
        <div
          aria-hidden
          className="pointer-events-none absolute bottom-0 left-0 right-0 h-px rounded-b-2xl"
          style={{
            background:
              "linear-gradient(90deg, transparent 0%, rgba(212,168,83,0.2) 30%, rgba(212,168,83,0.3) 50%, rgba(212,168,83,0.2) 70%, transparent 100%)",
          }}
        />

        {/* 日付（金箔グラデ大見出し） */}
        <div className="relative flex items-baseline gap-2 flex-wrap">
          <span className="gold-gradient font-extrabold text-3xl sm:text-5xl tracking-tight leading-none">
            {md}
          </span>
          <span className="gold-gradient font-extrabold text-xl sm:text-2xl tracking-wide">
            （{wd}）
          </span>
          <span className="text-header-text/60 text-sm tabular-nums">{y}年</span>
        </div>

        {/* 開催場ラベル */}
        {(venueCount > 0 || venuesLabel) && (
          <div className="relative mt-3 flex items-center gap-2 flex-wrap">
            {venueCount > 0 && (
              <span className="text-header-text/80 font-semibold text-sm">
                {venueCount} 場開催
              </span>
            )}
            {venuesLabel && (
              <span className="text-header-text/60 text-xs truncate">
                {venuesLabel}
              </span>
            )}
          </div>
        )}
      </div>

      {/* ── TOP3 ヒーローカード ── */}
      {top3.length > 0 && (
        <div
          role="list"
          aria-label="本日の注目レース TOP3"
          className="grid grid-cols-1 sm:grid-cols-3 gap-3"
        >
          {/* 1位（金グロー大型・常時 gold-pulse アニメ） */}
          {first && (
            <div role="listitem" className="sm:col-span-1">
              <PremiumCard
                as="button"
                variant="gold"
                padding="md"
                interactive
                aria-label={`筆頭 ${first.venue}${first.race_no}R ${first.name ?? ""}`}
                onClick={() => onSelect(first.venue, first.race_no)}
                className="group w-full text-left h-full gold-pulse-soft sm:backdrop-blur-md focus-ring-gold focus-visible:outline-none"
              >
                {/* ヘッダーラベル */}
                <div
                  className="flex items-center gap-1.5 mb-3 text-brand-gold font-extrabold text-xs tracking-widest uppercase"
                  style={{ textShadow: "0 0 8px rgba(212,168,83,0.4)" }}
                >
                  <Trophy
                    className="w-3.5 h-3.5 transition-transform duration-200 group-hover:scale-110"
                    aria-hidden
                  />
                  筆頭
                </div>
                {/* T-019: large 削除で 3 枚の内部要素サイズを完全統一。
                    筆頭の特別感は variant="gold" + gold-pulse-soft + ラベル「筆頭」で維持 */}
                <CardContent r={first} />
              </PremiumCard>
            </div>
          )}

          {/* 2位（navy-glow 中型・hover 強グロー） */}
          {second && (
            <div role="listitem" className="neon-card-hover">
              <PremiumCard
                as="button"
                variant="navy-glow"
                padding="md"
                interactive
                aria-label={`次点 ${second.venue}${second.race_no}R ${second.name ?? ""}`}
                onClick={() => onSelect(second.venue, second.race_no)}
                className="group w-full text-left h-full sm:backdrop-blur-md focus-ring-gold focus-visible:outline-none hover:shadow-[0_0_0_1px_rgba(96,165,250,0.50),0_16px_36px_-6px_rgba(30,64,175,0.60)]"
              >
                <div className="flex items-center gap-1.5 mb-3 text-blue-400 font-extrabold text-xs tracking-widest uppercase">
                  <Sparkles
                    className="w-3.5 h-3.5 transition-transform duration-200 group-hover:scale-110"
                    aria-hidden
                  />
                  次点
                </div>
                <CardContent r={second} />
              </PremiumCard>
            </div>
          )}

          {/* 3位（default 中型・hover 金グロー） */}
          {third && (
            <div role="listitem" className="neon-card-hover">
              <PremiumCard
                as="button"
                variant="default"
                padding="md"
                interactive
                aria-label={`第3候補 ${third.venue}${third.race_no}R ${third.name ?? ""}`}
                onClick={() => onSelect(third.venue, third.race_no)}
                className="group w-full text-left h-full sm:backdrop-blur-md focus-ring-gold focus-visible:outline-none hover:shadow-[var(--shadow-gold-glow)] hover:border-brand-gold/30"
              >
                <div className="flex items-center gap-1.5 mb-3 text-muted-foreground font-extrabold text-xs tracking-widest uppercase">
                  <Flame
                    className="w-3.5 h-3.5 transition-transform duration-200 group-hover:scale-110"
                    aria-hidden
                  />
                  第3候補
                </div>
                <CardContent r={third} />
              </PremiumCard>
            </div>
          )}
        </div>
      )}
    </section>
  );
}
