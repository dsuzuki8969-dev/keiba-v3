import { useMemo } from "react";
import { useRaceResult } from "@/api/hooks";
import { WAKU_BG, posCls, markCls, rankCls } from "@/lib/constants";
import type { RaceResultEntry, RaceResultPayout } from "@/api/client";

/** 走破タイム秒を「m:ss.f」形式に変換 (例: 89.8 → "1:29.8" / 65.4 → "1:05.4")
 *  バック API は time_sec で秒数を返すが、表示は分秒形式
 *  null/undefined/NaN は呼出側で「—」表示するため空文字を返す
 */
function formatTime(sec: number | null | undefined): string {
  if (sec == null || Number.isNaN(sec)) return "";
  const m = Math.floor(sec / 60);
  const s = sec - m * 60;
  // 秒部分を 2 桁ゼロ詰め + 小数 1 桁: 89.8 → "1:29.8" / 65.4 → "1:05.4"
  return `${m}:${s.toFixed(1).padStart(4, "0")}`;
}

/** 順位配列を計算（値の降順 or 昇順で 1-based rank を返す）
 *  lowerIsBetter=true なら値が小さいほど高順位（後3F 等タイム系）
 */
function computeRanks(values: Array<number | null | undefined>, lowerIsBetter: boolean): number[] {
  const pairs = values.map((v, i) => ({ v, i }));
  const sorted = pairs
    .filter((p) => p.v != null && !Number.isNaN(p.v as number))
    .sort((a, b) => (lowerIsBetter ? (a.v as number) - (b.v as number) : (b.v as number) - (a.v as number)));
  const ranks: number[] = new Array(values.length).fill(0);
  sorted.forEach((p, idx) => {
    ranks[p.i] = idx + 1;
  });
  return ranks;
}

// ============================================================
// レース結果パネル — 着順＋払戻金をオリジナル表示
// ============================================================

const MARK_SYMBOL: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆", kiken: "×",
};

interface Props {
  date: string;
  raceId: string;
}

export function RaceResultPanel({ date, raceId }: Props) {
  const { data, isLoading, error } = useRaceResult(date, raceId);

  // hooks は条件分岐より前に全て呼ぶ必要がある（React error #310 対策）
  const order = data?.order || [];
  const payouts = data?.payouts || {};

  // 順位色分け用: 総合指数（高い順） / 後3F（低い順）
  const compositeRanks = useMemo(
    () => computeRanks(order.map((o) => (o as any).composite ?? null), false),
    [order],
  );
  const last3fRanks = useMemo(
    () => computeRanks(order.map((o) => (o as any).last_3f ?? null), true),
    [order],
  );

  if (isLoading) return <div className="text-sm text-muted-foreground py-4 text-center">結果を読み込み中...</div>;
  if (error) return <div className="text-sm text-red-500 py-4 text-center">結果の取得に失敗しました</div>;
  if (!data?.found) return <div className="text-sm text-muted-foreground py-4 text-center">レース結果はまだありません</div>;

  // order が空（まだ確定していない）→ 結果取得待ち
  if (order.length === 0) {
    return (
      <div className="text-sm text-muted-foreground py-4 text-center">
        結果取得待ち（レース終了後しばらくお待ちください）
      </div>
    );
  }

  // 通過順カラムを表示するか（少なくとも1馬で通過順がある場合のみ）
  const hasCorners = order.some((o) => Array.isArray((o as any).corners) && (o as any).corners.length > 0);
  const hasLast3F = order.some((o) => (o as any).last_3f != null);

  // 結果データの完全性チェック
  // バック API (`/api/results/race`) のフィールド名:
  //   time_sec (秒数), win_odds (単勝オッズ), popularity (人気)
  // 旧フロント実装は time/odds を期待しており不一致だった (2026-04-28 修正)
  const hasAnyTime = order.some((o) => (o as any).time_sec != null);
  const hasAnyPopularity = order.some((o) => (o as any).popularity != null);
  const hasAnyOdds = order.some((o) => (o as any).win_odds != null);
  const hasAnyPayouts = Object.keys(payouts).length > 0;
  // 全部欠落 → 着順のみ（試合直後で詳細未反映）
  const isFullyPartial = !hasAnyTime && !hasAnyPopularity && !hasAnyOdds && !hasAnyPayouts;
  // 払戻はあるがタイム/人気/単勝が無い → 詳細取得中（スクレイパー制限 or race_log未生成）
  const isDetailsPartial = !isFullyPartial && (!hasAnyTime || !hasAnyPopularity) && hasAnyPayouts;

  return (
    <div className="space-y-5">
      {/* 部分取得バナー */}
      {isFullyPartial && (
        <div className="rounded border border-yellow-400/60 bg-yellow-50 dark:bg-yellow-900/20 px-3 py-2 text-xs text-yellow-800 dark:text-yellow-200">
          ⚠️ 結果データが一部未取得です（着順のみ確定）。タイム・人気・払戻は再取得されるまでお待ちください。
        </div>
      )}
      {isDetailsPartial && (
        <div className="rounded border border-blue-400/60 bg-blue-50 dark:bg-blue-900/20 px-3 py-2 text-xs text-blue-800 dark:text-blue-200">
          ℹ️ 個別馬のタイム・人気・単勝オッズは取得できませんでした（払戻・着順は確定）。夜間の再取得後に反映されます。
        </div>
      )}
      {/* スクレイパーバグで壊れた古いデータ（HTMLキャッシュ無し）：結果データ再取得待ち */}
      {(data as { data_incomplete?: boolean } | undefined)?.data_incomplete && (
        <div className="rounded border-2 border-orange-400/80 bg-orange-50 dark:bg-orange-900/20 px-3 py-2 text-sm text-orange-800 dark:text-orange-200 font-semibold">
          🔧 結果データ再取得待ち — このレースはスクレイパーのバグで人気・オッズが壊れています。単勝オッズは1位馬のみ払戻金から逆算表示、他馬は「—」です。再取得バッチで修復予定。
        </div>
      )}

      {/* 着順テーブル */}
      <div>
        <h4 className="text-sm font-bold text-foreground mb-2 border-l-[3px] border-blue-500 pl-2">着順</h4>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-[11px] text-muted-foreground bg-muted/30">
                <th className="py-1.5 px-1.5 text-center w-9">着</th>
                <th className="py-1.5 px-1.5 text-center w-9">枠</th>
                <th className="py-1.5 px-1.5 text-center w-9">馬番</th>
                <th className="py-1.5 px-1.5 text-left">馬名・騎手</th>
                <th className="py-1.5 px-1.5 text-center w-9">印</th>
                <th className="py-1.5 px-1.5 text-center w-12">総合</th>
                <th className="py-1.5 px-1.5 text-right w-16">タイム</th>
                <th className="py-1.5 px-1.5 text-right w-12">着差</th>
                {hasCorners && <th className="py-1.5 px-1.5 text-center w-20">通過順</th>}
                {hasLast3F && <th className="py-1.5 px-1.5 text-right w-12">後3F</th>}
                <th className="py-1.5 px-1.5 text-right w-12">人気</th>
                <th className="py-1.5 px-1.5 text-right w-16">単勝</th>
              </tr>
            </thead>
            <tbody>
              {order.map((o: RaceResultEntry, i: number) => {
                const markSym = o.mark ? (MARK_SYMBOL[o.mark] || o.mark) : "";
                const mCls = markSym ? markCls(markSym) : "";
                const corners = (o as any).corners as number[] | undefined;
                const last3f = (o as any).last_3f as number | null | undefined;
                // バック API は time_sec (秒数) で返すため formatTime で "m:ss.f" に変換
                const timeSec = (o as any).time_sec as number | null | undefined;
                const margin = (o as any).margin as string | null | undefined;
                const popularity = (o as any).popularity as number | null | undefined;
                // バック API は win_odds で返す (旧フロント実装は odds を期待していて不一致)
                const winOdds = (o as any).win_odds as number | null | undefined;
                const compRank = compositeRanks[i];
                const l3fRank = last3fRanks[i];
                return (
                  <tr key={o.horse_no} className="border-b border-border/30 hover:bg-muted/20">
                    <td className={`py-1.5 px-1.5 text-center font-bold text-[15px] ${posCls(o.finish)}`}>
                      {o.finish}
                    </td>
                    <td className="py-1.5 px-1.5 text-center">
                      {o.gate_no != null && (
                        <span className={`inline-flex w-6 h-6 items-center justify-center rounded-sm text-[11px] font-bold ${WAKU_BG[o.gate_no] || "bg-gray-200"}`}>
                          {o.gate_no}
                        </span>
                      )}
                    </td>
                    <td className="py-1.5 px-1.5 text-center font-bold text-[14px]">{o.horse_no}</td>
                    <td className="py-1.5 px-1.5 text-left whitespace-nowrap">
                      <span className="font-bold text-[14px]">{o.horse_name || `${o.horse_no}番`}</span>
                      {o.jockey && <span className="text-muted-foreground text-[12px] ml-2">{o.jockey}</span>}
                    </td>
                    <td className={`py-1.5 px-1.5 text-center text-[15px] ${mCls}`}>{markSym || "—"}</td>
                    <td className={`py-1.5 px-1.5 text-center tabular-nums text-[12px] ${rankCls(compRank)}`}>
                      {o.composite != null ? o.composite.toFixed(1) : "—"}
                    </td>
                    <td className="py-1.5 px-1.5 text-right tabular-nums text-[12px]">
                      {timeSec != null ? formatTime(timeSec) : "—"}
                    </td>
                    <td className="py-1.5 px-1.5 text-right tabular-nums text-[12px] text-muted-foreground">
                      {margin || "—"}
                    </td>
                    {hasCorners && (
                      <td className="py-1.5 px-1.5 text-center tabular-nums text-[12px]">
                        {corners && corners.length > 0 ? corners.join("-") : "—"}
                      </td>
                    )}
                    {hasLast3F && (
                      <td className={`py-1.5 px-1.5 text-right tabular-nums text-[12px] ${rankCls(l3fRank)}`}>
                        {last3f != null ? last3f.toFixed(1) : "—"}
                      </td>
                    )}
                    <td className="py-1.5 px-1.5 text-right tabular-nums text-[12px]">
                      {popularity != null ? `${popularity}人気` : "—"}
                    </td>
                    <td className="py-1.5 px-1.5 text-right tabular-nums text-[13px] font-semibold">
                      {winOdds != null ? `${winOdds.toFixed(1)}倍` : "—"}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* 払戻金 */}
      <div>
        <h4 className="text-sm font-bold text-foreground mb-2 border-l-[3px] border-emerald-500 pl-2">払戻金</h4>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
          {/* 単勝・複勝 */}
          <PayoutCard title="単勝" data={payouts["単勝"]} />
          <PayoutCard title="複勝" data={payouts["複勝"]} />
          {/* 枠連・馬連 */}
          <PayoutCard title="枠連" data={payouts["枠連"]} />
          <PayoutCard title="馬連" data={payouts["馬連"]} />
          {/* ワイド */}
          <PayoutCard title="ワイド" data={payouts["ワイド"]} />
          {/* 馬単 */}
          <PayoutCard title="馬単" data={payouts["馬単"]} />
          {/* 三連複・三連単 */}
          <PayoutCard title="三連複" data={payouts["三連複"]} />
          <PayoutCard title="三連単" data={payouts["三連単"]} />
        </div>
      </div>
    </div>
  );
}

// 払戻カード
function PayoutCard({ title, data }: { title: string; data?: RaceResultPayout | RaceResultPayout[] }) {
  if (!data) return null;
  const items = Array.isArray(data) ? data : [data];
  if (items.length === 0) return null;

  // 高配当ハイライト
  const highlight = (payout: number): string => {
    if (payout >= 100000) return "text-red-600 font-bold";
    if (payout >= 10000) return "text-orange-600 font-bold";
    if (payout >= 5000) return "text-blue-600 font-bold";
    return "";
  };

  return (
    <div className="bg-muted/30 rounded border border-border/50 px-3 py-2">
      <div className="flex items-center justify-between">
        <span className="text-[11px] font-bold text-muted-foreground">{title}</span>
      </div>
      {items.map((item, i) => (
        <div key={i} className="flex items-center justify-between text-sm mt-0.5">
          <span className="tabular-nums text-muted-foreground text-[12px]">{item.combo}</span>
          <span className={`tabular-nums text-[13px] ${highlight(item.payout)}`}>
            ¥{item.payout.toLocaleString()}
          </span>
        </div>
      ))}
    </div>
  );
}
