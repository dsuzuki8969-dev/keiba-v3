import { PastRunsTable } from "./HorseCardPC";
import { WAKU_BG, markCls } from "@/lib/constants";
import type { HorseData, PastRunData } from "./RaceDetailView";

// ============================================================
// 前三走成績パネル — 全馬の前三走を一覧表示
// ============================================================

const MARK_SYMBOL: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆", kiken: "×",
};

interface Props {
  horses: HorseData[];
}

export function PastRunsPanel({ horses }: Props) {
  // 馬番順に並べ、前三走があるものだけ対象
  const sorted = [...horses].sort((a, b) => (a.horse_no || 0) - (b.horse_no || 0));

  if (sorted.length === 0) {
    return (
      <div className="text-sm text-muted-foreground py-4 text-center">
        馬データがありません
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {sorted.map((h) => {
        const runs = ((h as Record<string, unknown>).past_3_runs as PastRunData[]) || [];
        const markSym = h.mark ? MARK_SYMBOL[h.mark] || h.mark : "";
        const mCls = markSym ? markCls(markSym) : "";
        const gate = h.gate_no || 0;
        // 総合指数と順位（同一値は同順位、値が無い馬は順位計算から除外）
        const comp = h.composite || 0;
        const compRank = comp > 0
          ? horses.filter((o) => (o.composite || 0) > comp).length + 1
          : 0;
        return (
          <div
            key={h.horse_no}
            className="rounded border border-border/60 bg-card/40 p-2 space-y-2"
          >
            {/* 馬ヘッダ */}
            <div className="flex items-center gap-2 flex-wrap">
              {gate > 0 && (
                <span
                  className={`inline-flex w-6 h-6 items-center justify-center rounded-sm text-[11px] font-bold ${
                    WAKU_BG[gate] || "bg-gray-200"
                  }`}
                >
                  {gate}
                </span>
              )}
              <span className="font-bold text-[14px] tabular-nums">{h.horse_no}</span>
              {markSym && (
                <span className={`text-[15px] ${mCls}`}>{markSym}</span>
              )}
              <span className="font-bold text-[14px]">
                {h.horse_name || `${h.horse_no}番`}
              </span>
              {h.jockey && (
                <span className="text-muted-foreground text-[12px]">{h.jockey}</span>
              )}
              {h.odds != null && (
                <span className="text-muted-foreground text-[12px] ml-1">
                  {h.odds.toFixed(1)}倍
                  {h.popularity != null && ` (${h.popularity}人気)`}
                </span>
              )}
              {compRank > 0 && (
                <span className="text-muted-foreground text-[12px] ml-1">
                  総合{comp.toFixed(1)}({compRank}位)
                </span>
              )}
            </div>

            {/* 前三走テーブル */}
            {runs.length > 0 ? (
              <PastRunsTable runs={runs} />
            ) : (
              <div className="text-xs text-muted-foreground pl-2">
                前三走データなし
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
