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

// マスター指示 2026-06-21: TOP3 ヒーローカード(筆頭/次点/第3候補)を非表示化。
// 日付バナーのみ残すため、カード用の import (PremiumCard/各Badge/icons) は削除。

export interface HeroRaceItem {
  venue: string;
  race_no: number;
  name?: string;
  post_time?: string;
  grade?: string;
  overall_confidence?: string;
  tansho_confidence?: string;
  sanrenpuku_confidence?: string;
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

// ─────────────────────────────────────────────────
// メインコンポーネント（日付バナーのみ。TOP3カードは廃止）
// ─────────────────────────────────────────────────
export function HomePageHero({ date, venueCount, venuesLabel }: Props) {
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
    </section>
  );
}
