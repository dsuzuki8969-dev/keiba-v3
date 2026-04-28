import { useState, useEffect } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { SurfaceBadge } from "./SurfaceBadge";
import { GradeBadge } from "./GradeBadge";
import { ConfidenceBadge } from "./ConfidenceBadge";
import { MarkBadge } from "./MarkBadge";
import { surfShort } from "@/lib/constants";
import { Clock3, Users } from "lucide-react";
// T-034 本実装: オッズ行コンポーネント（マスター承認済み正式統合）
import RaceCardOddsLine from "./RaceCardOddsLine";
import { BREAKPOINTS } from "@/lib/breakpoints";

/**
 * useIsMobile — window幅が md ブレークポイント未満かを検知する hook
 *
 * [HIGH-1 修正] RaceCardOddsLine は isMobile で PC/モバイルを分岐するが、
 * 呼び出し元 RaceCard が prop を渡していなかったため、モバイルでも PC 版固定になっていた。
 * breakpoints.ts の BREAKPOINTS.MD (768px) と Tailwind の md: ブレークポイントを整合させる。
 */
function useIsMobile(breakpoint: number = BREAKPOINTS.MD): boolean {
  const [isMobile, setIsMobile] = useState<boolean>(
    () => typeof window !== "undefined" && window.innerWidth < breakpoint
  );

  useEffect(() => {
    const mq = window.matchMedia(`(max-width: ${breakpoint - 1}px)`);
    const handler = (e: MediaQueryListEvent) => setIsMobile(e.matches);
    // 初期値を確実に同期（SSR→CSR ハイドレーション対策）
    setIsMobile(mq.matches);
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, [breakpoint]);

  return isMobile;
}

/**
 * RaceCard — v6.1 プレミアム情報階層
 * --------------------------------------------------------------
 * 階層:
 *   [Primary]   レース番号 + レース名 + グレード
 *   [Secondary] 距離 / 馬場 / 頭数 / 発走時刻
 *   [Tertiary]  自信度バッジ（右上）
 *   [Accent]    本命印 + 馬名 + 勝率（下段アクセント）
 *
 * 特徴:
 *  - 本命が勝率1位 → 金グロー variant
 *  - 2位 → navy-glow、3位以下 → default
 *  - ホバーで 1px 浮き、影強化
 */

interface RaceData {
  race_no: number;
  name?: string;
  post_time?: string;
  surface?: string;
  distance?: number;
  head_count?: number;
  grade?: string;
  overall_confidence?: string;
  honmei_name?: string;
  honmei_mark?: string;
  honmei_no?: number;
  honmei_composite?: number;
  honmei_win_pct?: number;
  honmei_odds?: number;
  honmei_popularity?: number;
  url?: string;
}

interface Props {
  race: RaceData;
  onClick: () => void;
  /** 競馬場内の勝率ランク (1=最高) */
  winPctRank?: number;
}

/** ランク別 PremiumCard variant */
function cardVariantByRank(rank?: number): "gold" | "navy-glow" | "default" {
  if (rank === 1) return "gold";
  if (rank === 2) return "navy-glow";
  return "default";
}

// computeWinPctRanks は Fast Refresh 互換のため @/lib/keibaUtils に移動。
// 既存の import 互換のため re-export する。
export { computeWinPctRanks } from "@/lib/keibaUtils";

export function RaceCard({ race, onClick, winPctRank }: Props) {
  const conf = (race.overall_confidence || "C").replace(/\u207a/g, "+");
  const surf = surfShort(race.surface || "");
  // [HIGH-1 \u4fee\u6b63] isMobile \u3092\u691c\u77e5\u3057\u3066 RaceCardOddsLine \u306b\u6e21\u3059
  const isMobile = useIsMobile();

  const markKey = race.honmei_mark || "";

  return (
    <PremiumCard
      variant={cardVariantByRank(winPctRank)}
      padding="md"
      interactive
      onClick={onClick}
      className="group space-y-2.5"
      as="button"
    >
      {/* 上段: レース番号 + グレード + 発走時刻（右寄せ） */}
      <div className="flex items-center gap-2">
        <span
          className={[
            "heading-display text-2xl tnum",
            winPctRank === 1 ? "gold-gradient" : "text-primary dark:text-brand-gold",
          ].join(" ")}
        >
          {race.race_no}
          <span className="text-base font-bold ml-0.5">R</span>
        </span>
        {race.grade && <GradeBadge grade={race.grade} />}
        <ConfidenceBadge rank={conf} className="ml-1" />
        {race.post_time && (
          <span className="ml-auto inline-flex items-center gap-1 text-xs font-medium text-muted-foreground tnum">
            <Clock3 size={12} />
            {race.post_time}
          </span>
        )}
      </div>

      {/* レース名（Primary） */}
      <div className="text-[15px] font-bold leading-snug line-clamp-2 text-left">
        {race.name || `${race.race_no}R`}
      </div>

      {/* Secondary メタ情報 */}
      <div className="flex items-center gap-2 text-xs text-left">
        {surf && <SurfaceBadge surface={race.surface || ""} />}
        {race.distance != null && race.distance > 0 && (
          <span className="font-semibold tnum">{race.distance}m</span>
        )}
        {race.head_count != null && race.head_count > 0 && (
          <span className="inline-flex items-center gap-0.5 text-muted-foreground tnum">
            <Users size={11} />
            {race.head_count}
          </span>
        )}
      </div>

      {/* 本命行（Accent） — T-034 本実装: 馬名 + 勝率 + オッズ（人気）を 1 行で表示 */}
      {race.honmei_name && (
        <div className="pt-2 border-t border-border/60">
          <div className="flex items-center gap-2">
            {markKey && <MarkBadge mark={markKey} size="md" subtle={winPctRank !== 1} />}
            {/* [HIGH-1 修正] isMobile を prop として渡す。PC/モバイル分岐が正しく機能する。 */}
            <RaceCardOddsLine
              horseName={race.honmei_name}
              mark={race.honmei_mark}
              winPct={race.honmei_win_pct}
              odds={race.honmei_odds}
              popularity={race.honmei_popularity}
              isMobile={isMobile}
              className="flex-1 min-w-0"
            />
          </div>
        </div>
      )}
    </PremiumCard>
  );
}
