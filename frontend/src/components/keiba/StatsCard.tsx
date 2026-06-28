/**
 * StatsCard — 成績サマリカード（共通コンポーネント）
 *
 * T-031 (2026-04-28): TodayStatsPanel を共通化。
 * - ホームページ: showRefreshButton=true（デフォルト）
 * - 過去成績ページ: showRefreshButton=false（更新ボタン非表示）
 */
import { useCallback, useEffect, useRef, useState, memo } from "react";
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

export const StatsCard = memo(function StatsCard({
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

  const honmei = (d.honmei ?? {}) as Record<string, number>;
  const total = honmei.total ?? 0;
  const win = honmei.win ?? 0;
  const second = honmei.place2 ?? 0;
  const third = honmei.place3 ?? 0;
  const out = honmei.out ?? 0;
  const winRate = honmei.win_rate ?? 0;
  const rentai = honmei.place2_rate ?? 0;
  const fukusho = honmei.place_rate ?? 0;
  // ◎単勝物差し: tansho_roi / tansho_shushi が存在する場合のみ表示
  const tanshoRoi: number | null = typeof honmei.tansho_roi === "number" && honmei.total > 0
    ? honmei.tansho_roi
    : null;
  const tanshoShushi: number | null = typeof honmei.tansho_shushi === "number" && honmei.total > 0
    ? honmei.tansho_shushi
    : null;
  const lastUpdated = (d as { last_updated?: string }).last_updated ?? "";
  const resultsPending = (d as { results_pending?: boolean }).results_pending;

  // T-001 (2026-04-25): 3 段表記用メタ情報。reviewer HIGH 対応で typeof ガード追加
  const _num = (key: string): number => {
    const v = (d as Record<string, unknown>)[key];
    return typeof v === "number" && Number.isFinite(v) ? v : 0;
  };
  const pendingFetch = _num("pending_fetch");
  const pendingAgeMaxMin = _num("pending_age_max_min");

  // 結果待ち中（レースあるが結果未集計）
  if (resultsPending && total === 0) {
    return (
      <PremiumCard variant="gold" padding="md">
        <div className="flex items-center justify-between gap-2 flex-wrap">
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Activity size={10} className="inline mr-1" />
              <span className="section-eyebrow">Live Stats</span>
            </PremiumCardAccent>
            <span className="heading-section text-base">{title}</span>
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
          <PremiumCardTitle className="text-base">{title}</PremiumCardTitle>
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
        {/* ── ◎本命 的中実績 (主役: 複勝率/連対率/勝率を前面化) ── */}
        <div>
          <div className="text-xs font-semibold text-muted-foreground mb-1.5">◎本命 的中実績</div>

          {/* 上段: 勝率・連対率・複勝率 の3指標を大きく（勝→連→複の順） */}
          <div className="grid grid-cols-3 gap-2 mb-2">
            <div className="bg-muted/40 rounded-lg p-2 text-center">
              <div className="text-[10px] text-muted-foreground font-medium mb-0.5">勝率</div>
              <div className={`stat-mono text-xl font-bold ${winRate >= 30 ? "text-brand-gold" : "text-foreground/70"}`}>
                {winRate.toFixed(1)}<span className="text-sm">%</span>
              </div>
              <div className="text-[9px] text-muted-foreground mt-0.5 tnum">{win}/{total}R</div>
            </div>
            <div className="bg-muted/40 rounded-lg p-2 text-center">
              <div className="text-[10px] text-muted-foreground font-medium mb-0.5">連対率</div>
              <div className={`stat-mono text-xl font-bold ${rentai >= 50 ? "text-emerald-500" : "text-foreground/70"}`}>
                {rentai.toFixed(1)}<span className="text-sm">%</span>
              </div>
              <div className="text-[9px] text-muted-foreground mt-0.5 tnum">{win + second}/{total}R</div>
            </div>
            <div className="bg-muted/40 rounded-lg p-2 text-center">
              <div className="text-[10px] text-muted-foreground font-medium mb-0.5">複勝率</div>
              <div className={`stat-mono text-xl font-bold ${fukusho >= 70 ? "text-emerald-500" : fukusho >= 50 ? "text-foreground" : "text-foreground/70"}`}>
                {fukusho.toFixed(1)}<span className="text-sm">%</span>
              </div>
              <div className="text-[9px] text-muted-foreground mt-0.5 tnum">{win + second + third}/{total}R</div>
            </div>
          </div>

          {/* 下段: 軸馬成績 / 単勝回収率 / 収支 の3カード（◎単勝の物差し） */}
          <div className="grid grid-cols-3 gap-2">
            <div className="bg-muted/40 rounded-lg p-2 text-center">
              <div className="text-[10px] text-muted-foreground font-medium mb-0.5">軸馬成績</div>
              <div className="stat-mono text-base font-bold tabular-nums">
                <span className="text-positive">{win}</span>
                <span className="mx-0.5 text-muted-foreground/50">-</span>
                {second}
                <span className="mx-0.5 text-muted-foreground/50">-</span>
                {third}
                <span className="mx-0.5 text-muted-foreground/50">-</span>
                <span className="text-muted-foreground/50">{out}</span>
              </div>
              <div className="text-[9px] text-muted-foreground mt-0.5">1-2-3-着外</div>
            </div>
            {tanshoRoi !== null ? (
              <div className="bg-muted/40 rounded-lg p-2 text-center">
                <div className="text-[10px] text-muted-foreground font-medium mb-0.5">単勝回収率</div>
                <div className={`stat-mono text-xl font-bold ${tanshoRoi >= 100 ? "text-emerald-500" : "text-foreground/70"}`}>
                  {tanshoRoi.toFixed(1)}<span className="text-sm">%</span>
                </div>
                <div className="text-[9px] text-muted-foreground mt-0.5">◎単勝100円</div>
              </div>
            ) : <div />}
            {tanshoShushi !== null ? (
              <div className="bg-muted/40 rounded-lg p-2 text-center">
                <div className="text-[10px] text-muted-foreground font-medium mb-0.5">収支</div>
                <div className={`stat-mono text-xl font-bold ${tanshoShushi >= 0 ? "text-emerald-500" : "text-foreground/70"}`}>
                  {tanshoShushi >= 0 ? "+" : ""}{tanshoShushi.toLocaleString()}<span className="text-sm">円</span>
                </div>
                <div className="text-[9px] text-muted-foreground mt-0.5">◎単勝100円</div>
              </div>
            ) : <div />}
          </div>
        </div>

        {/* T-001: 取り込み遅延警告（発走済みなのに results.json 未取り込みのレースがある） */}
        {pendingFetch > 0 && (
          <div
            role="status"
            aria-live="polite"
            className="text-xs tabular-nums text-amber-500 dark:text-amber-400"
          >
            ⚠️ 結果未反映 {pendingFetch.toLocaleString()}R（最古{" "}
            {pendingAgeMaxMin.toLocaleString()}分前発走 — 自動取得待ち）
          </div>
        )}
      </div>
    </PremiumCard>
  );
});
StatsCard.displayName = "StatsCard";
