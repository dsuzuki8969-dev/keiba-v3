/**
 * StatsCard — 成績サマリカード（共通コンポーネント）
 *
 * T-031 (2026-04-28): TodayStatsPanel を共通化。
 * - ホームページ: showRefreshButton=true（デフォルト）
 * - 過去成績ページ: showRefreshButton=false（更新ボタン非表示）
 */
import { useCallback, useEffect, useRef, useState } from "react";
import {
  PremiumCard,
  PremiumCardAccent,
  PremiumCardHeader,
  PremiumCardTitle,
} from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { Activity, RefreshCw } from "lucide-react";
import { useHomeTodayStats, useForceRefreshToday } from "@/api/hooks";

export interface StatsCardProps {
  /** 対象日付 (YYYY-MM-DD) */
  date: string;
  /** カードタイトル。デフォルト: "本日のリアルタイム成績" */
  title?: string;
  /** 手動更新ボタンを表示するか。過去日は false にすること。デフォルト: true */
  showRefreshButton?: boolean;
}

export function StatsCard({
  date,
  title = "本日のリアルタイム成績",
  showRefreshButton = true,
}: StatsCardProps) {
  const { data, isLoading, refetch } = useHomeTodayStats(date);
  const { mutate: forceRefresh, isPending: isRefreshing } =
    useForceRefreshToday();

  // 連打防止: 5秒間ボタンを disabled にする
  const [cooldown, setCooldown] = useState(false);
  const cooldownTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // アンマウント時に cooldown タイマーをクリア（setState on unmounted component 防止）
  useEffect(() => {
    return () => {
      if (cooldownTimerRef.current !== null) {
        clearTimeout(cooldownTimerRef.current);
      }
    };
  }, []);

  const handleForceRefresh = useCallback(() => {
    if (isRefreshing || cooldown) return;
    forceRefresh(date, {
      onSuccess: (result) => {
        console.info("LIVE STATS 手動更新完了:", result);
        void refetch();
        alert(
          `更新完了: 取得 ${result.fetched}R, 集計 ${result.aggregated}R` +
            (result.errors > 0 ? ` (エラー ${result.errors}件)` : "")
        );
      },
      onError: (err) => {
        console.error("LIVE STATS 手動更新失敗:", err);
        alert(
          `更新失敗: ${err instanceof Error ? err.message : String(err)}`
        );
      },
      onSettled: () => {
        // 成功・失敗いずれの場合も 5秒間連打防止
        setCooldown(true);
        cooldownTimerRef.current = setTimeout(() => {
          setCooldown(false);
        }, 5000);
      },
    });
  }, [date, forceRefresh, isRefreshing, cooldown, refetch]);

  const d = data as Record<string, unknown> | undefined;
  if (isLoading || !d) return null;
  const found = (d as { found?: boolean }).found;
  if (!found) return null;

  const honmei = (d.honmei || {}) as Record<string, number>;
  const sanrentan = (d.sanrentan || {}) as Record<string, number>;
  const total = honmei.total || 0;
  const win = honmei.win || 0;
  const second = honmei.place2 || 0;
  const third = honmei.place3 || 0;
  const out = honmei.out || 0;
  const winRate = honmei.win_rate || 0;
  const rentai = honmei.place2_rate || 0;
  const tanRoi = honmei.tansho_roi || 0;
  const tanStake = honmei.tansho_stake || 0;
  const tanRet = honmei.tansho_ret || 0;
  const sPlayed = sanrentan.played || 0;
  const sHit = sanrentan.hit || 0;
  const sStake = sanrentan.stake || 0;
  const sRet = sanrentan.payback || 0;
  const sRoi = sanrentan.roi_pct || 0;
  const sBalance = sanrentan.balance || sRet - sStake;
  const lastUpdated = (d as { last_updated?: string }).last_updated || "";
  const resultsPending = (d as { results_pending?: boolean }).results_pending;

  // T-001 (2026-04-25): 3 段表記用メタ情報。reviewer HIGH 対応で typeof ガード追加
  const _num = (key: string): number => {
    const v = (d as Record<string, unknown>)[key];
    return typeof v === "number" && Number.isFinite(v) ? v : 0;
  };
  const totalRaces = _num("total_races");
  const finishedRaces = _num("finished_races");
  const eligibleSanrentan = _num("eligible_for_sanrentan");
  const pendingFetch = _num("pending_fetch");
  const pendingAgeMaxMin = _num("pending_age_max_min");

  // 結果待ち中（レースあるが結果未集計）
  if (resultsPending && total === 0 && sPlayed === 0) {
    return (
      <PremiumCard variant="gold" padding="md">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Activity size={10} className="inline mr-1" />
              <span className="section-eyebrow">Live Stats</span>
            </PremiumCardAccent>
            <span className="heading-section text-sm">{title}</span>
          </div>
          <span className="text-xs text-muted-foreground">
            結果待ち（各レース発走 10 分後に自動更新）
          </span>
        </div>
      </PremiumCard>
    );
  }

  return (
    <PremiumCard variant="gold" padding="md">
      <PremiumCardHeader>
        <div className="flex flex-col gap-0.5">
          <PremiumCardAccent>
            <Activity size={10} className="inline mr-1" />
            <span className="section-eyebrow">Live Stats</span>
          </PremiumCardAccent>
          <PremiumCardTitle className="text-sm">{title}</PremiumCardTitle>
        </div>
        <div className="flex items-center gap-2">
          {/* showRefreshButton=true 時のみ手動更新ボタンを表示（過去日では非表示） */}
          {showRefreshButton && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleForceRefresh}
              disabled={isRefreshing || cooldown}
              aria-label="リアルタイム成績を即座に更新"
              className="h-6 px-2 text-xs"
            >
              <RefreshCw
                size={10}
                className={`mr-1 ${isRefreshing ? "animate-spin" : ""}`}
              />
              {isRefreshing ? "更新中…" : "更新"}
            </Button>
          )}
          {lastUpdated && (
            <span className="text-xs font-normal text-muted-foreground tnum">
              更新 {lastUpdated}
            </span>
          )}
        </div>
      </PremiumCardHeader>

      <div className="space-y-3">
        {/* ◉◎単勝 */}
        <div>
          <div className="text-[11px] text-muted-foreground mb-1">◉◎単勝</div>
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className="stat-mono text-lg">
              <span className="text-positive">{win}</span>
              <span className="text-muted-foreground text-sm">-</span>
              {second}
              <span className="text-muted-foreground text-sm">-</span>
              {third}
              <span className="text-muted-foreground text-sm">-</span>
              <span className="text-muted-foreground">{out}</span>
            </span>
            <span className="text-xs text-muted-foreground tabular-nums">
              勝率{" "}
              <span className="stat-mono text-foreground">
                {winRate.toFixed(1)}%
              </span>
            </span>
            <span className="text-xs text-muted-foreground tabular-nums">
              連対{" "}
              <span className="stat-mono text-foreground">
                {rentai.toFixed(1)}%
              </span>
            </span>
            <span
              className={`text-xs tabular-nums ${
                tanRoi >= 100 ? "text-positive" : "text-muted-foreground"
              }`}
            >
              回収率{" "}
              <span className={tanRoi >= 100 ? "stat-mono-gold" : "stat-mono"}>
                {tanRoi.toFixed(1)}%
              </span>
            </span>
            <span className="text-xs text-muted-foreground tabular-nums">
              {tanStake.toLocaleString()} → {tanRet.toLocaleString()}円
            </span>
          </div>
        </div>

        {/* 三連単F */}
        <div>
          <div className="text-[11px] text-muted-foreground mb-1">三連単F</div>
          <div className="flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <span className="tabular-nums text-sm">
              予想 <span className="text-foreground font-bold">{sPlayed}R</span>
              <span className="text-muted-foreground mx-1">/</span>
              的中{" "}
              <span className="text-positive font-bold">{sHit}R</span>
            </span>
            <span
              className={`text-xs tabular-nums ${
                sBalance >= 0 ? "text-positive" : "text-negative"
              }`}
            >
              収支{" "}
              <span className={sBalance >= 0 ? "stat-mono-gold" : "stat-mono"}>
                {sBalance >= 0 ? "+" : ""}
                {sBalance.toLocaleString()}円
              </span>
            </span>
            <span
              className={`text-xs tabular-nums ${
                sRoi >= 100 ? "text-positive" : "text-muted-foreground"
              }`}
            >
              回収率{" "}
              <span className={sRoi >= 100 ? "stat-mono-gold" : "stat-mono"}>
                {sRoi.toFixed(1)}%
              </span>
            </span>
            <span className="text-xs text-muted-foreground tabular-nums">
              {sStake.toLocaleString()} → {sRet.toLocaleString()}円
            </span>
          </div>

          {/* T-001 (2026-04-25): 3 段表記 — 集計 / 終了 / 対象 を分母として明示 */}
          {(totalRaces > 0 || eligibleSanrentan > 0) && (
            <div className="text-[11px] text-muted-foreground tabular-nums mt-1">
              集計 <span className="text-foreground">{sPlayed}R</span>
              <span className="mx-1">/</span>
              終了 <span className="text-foreground">{finishedRaces}R</span>
              <span className="mx-1">/</span>
              対象 <span className="text-foreground">{eligibleSanrentan}R</span>
              <span className="mx-1">/</span>
              総予想 <span className="text-foreground">{totalRaces}R</span>
            </div>
          )}
        </div>

        {/* T-001: 取り込み遅延警告（発走済みなのに results.json 未取り込みのレースがある） */}
        {pendingFetch > 0 && (
          <div
            role="status"
            aria-live="polite"
            className="text-[11px] tabular-nums text-amber-500 dark:text-amber-400"
          >
            ⚠️ 結果未反映 {pendingFetch.toLocaleString()}R（最古{" "}
            {pendingAgeMaxMin.toLocaleString()}分前発走 — 自動取得待ち）
          </div>
        )}
      </div>
    </PremiumCard>
  );
}
