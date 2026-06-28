import { useState, useEffect, memo, useCallback } from "react";
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
import { cn } from "@/lib/utils";
// T-039: 的中バッジ用型（三連複的中バッジ削除後も型定義は残す）
import type { RaceCardHitResult } from "@/api/hooks";

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
 *  - 購入レース → 黒枠 / 三連複的中 → 赤枠（6/21 マスター指示・金/ネイビー枠は廃止）
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
  tansho_confidence?: string;
  sanrenpuku_confidence?: string;
  honmei_name?: string;
  honmei_mark?: string;
  honmei_no?: number;
  honmei_composite?: number;
  honmei_win_pct?: number;
  /** 本命馬の軸馬度（0〜100）— レースカードに表示 */
  honmei_jiku_score?: number;
  honmei_odds?: number;
  honmei_popularity?: number;
  url?: string;
  /** 本命馬◎/◉の確定着順（1〜n 着）。未確定・取得前は null/undefined */
  honmei_chaku?: number | null;
}

interface Props {
  race: RaceData;
  /** memo 最適化のため raceNo を引数に取る形に変更。親側で useCallback 安定参照を渡すこと */
  onOpen: (raceNo: number) => void;
  /** 競馬場内の勝率ランク (1=最高) */
  winPctRank?: number;
  /** T-039: 的中バッジ情報（親 component が useRaceCardResults で取得して渡す） */
  hitResult?: RaceCardHitResult | null;
}

// 6/21 マスター指示: 勝率1位の金枠 / 2位のネイビー枠は廃止。
// カード枠は「購入レース=黒 / 的中=赤」の2状態のみで表現する（下記 frameClass）。

// computeWinPctRanks は @/lib/keibaUtils に移動済。Fast Refresh 互換のため
// このファイルからの re-export は廃止。利用側で `@/lib/keibaUtils` から直接 import すること。

export const RaceCard = memo(function RaceCard({ race, onOpen, winPctRank, hitResult }: Props) {
  const conf = (race.overall_confidence || "C").replace(/⁺/g, "+");
  const surf = surfShort(race.surface || "");
  // [HIGH-1 修正] isMobile を検知して RaceCardOddsLine に渡す
  const isMobile = useIsMobile();
  // memo 効果維持のため race.race_no と onOpen に依存した stable handler
  const handleClick = useCallback(() => onOpen(race.race_no), [onOpen, race.race_no]);

  const markKey = race.honmei_mark || "";

  // hitResult は winPctRank（gold-gradient）以外で現在未使用だが、
  // Props 型の互換性維持のため参照のみ保持（unused lint エラー回避）
  void hitResult;

  // honmei_chaku による枠色分け: 1着=緑枠, 2着=赤枠, 3着=青枠, それ以外=枠なし
  const chakuBorderCls = (() => {
    const c = race.honmei_chaku;
    if (c == null) return "";
    if (c === 1) return "border-2 border-emerald-600 ring-1 ring-emerald-600/40";
    if (c === 2) return "border-2 border-red-600 ring-1 ring-red-600/40";
    if (c === 3) return "border-2 border-blue-600 ring-1 ring-blue-600/40";
    return "";
  })();

  return (
    <PremiumCard
      variant="default"
      padding="md"
      interactive
      onClick={handleClick}
      className={cn("group space-y-2.5", chakuBorderCls)}
      as="button"
    >
      {/* 上段: レース番号 + グレード + 自信度バッジ + 発走時刻（右寄せ） */}
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
        {/* 単勝自信度バッジ（「単」ラベル付き） */}
        {race.tansho_confidence && (
          <ConfidenceBadge rank={(race.tansho_confidence || "").replace(/⁺/g, "+")} label="単" className="ml-1" />
        )}
        {/* fallback: tansho_confidence 未設定時は overall を表示 */}
        {!race.tansho_confidence && (
          <ConfidenceBadge rank={conf} className="ml-1" />
        )}
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

      {/* 本命行（Accent） — 馬名 + 軸馬度 + オッズ（人気）を 1 行で表示 */}
      {race.honmei_name && (
        <div className="pt-2 border-t border-border/60">
          <div className="flex items-center gap-2">
            {markKey && <MarkBadge mark={markKey} size="md" subtle={winPctRank !== 1} />}
            {/* [HIGH-1 修正] isMobile を prop として渡す。PC/モバイル分岐が正しく機能する。 */}
            <RaceCardOddsLine
              horseName={race.honmei_name}
              mark={race.honmei_mark}
              jikuScore={race.honmei_jiku_score}
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
});
RaceCard.displayName = "RaceCard";
