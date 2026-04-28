import { useState, useMemo, lazy, Suspense } from "react";
import { WAKU_BG } from "@/lib/constants";
import type { HorseData, RaceDetail } from "./RaceDetailView";
import type { WinProbEntry } from "@/components/charts/WinProbBar";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { ChevronDown, ChevronUp } from "lucide-react";

// v6.1.22: recharts WinProbBar を遅延ロードして RaceDetailView 初回バンドル
// から分離。オッズタブを開くまで recharts はロードされない。
const WinProbBar = lazy(() =>
  import("@/components/charts/WinProbBar").then((m) => ({ default: m.WinProbBar })),
);

const MARK_KEY_TO_SYM: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆", kiken: "×",
};

const MARK_COLORS: Record<string, string> = {
  "◉": "text-emerald-600", "◎": "text-emerald-600",
  "○": "text-blue-600", "▲": "text-red-600", "△": "text-purple-600",
  "★": "text-foreground", "☆": "text-blue-600", "×": "text-red-600",
};

// 乖離シグナルの色
function divSignalCls(sig: string | undefined): string {
  if (sig === "S") return "bg-emerald-600 text-white";
  if (sig === "A") return "bg-blue-600 text-white";
  if (sig === "B") return "bg-muted text-foreground";
  if (sig === "C") return "bg-muted text-muted-foreground";
  if (sig === "x") return "bg-red-600 text-white";
  return "bg-muted text-muted-foreground";
}

type SortKey = "odds" | "popularity" | "win_prob" | "place2_prob" | "place3_prob" | "ev" | "horse_no";

interface Props {
  horses: HorseData[];
  race?: RaceDetail;
}

// --- TOP10 オッズテーブル ---
interface Top10Entry {
  combo: number[];
  odds: number;
}

function Top10Table({
  title,
  entries,
  hint,
  horseNoToGate,
}: {
  title: string;
  entries?: Top10Entry[];
  hint: string;
  horseNoToGate: Map<number, number>;
}) {
  if (!entries || entries.length === 0) {
    return (
      <div className="border border-border rounded-md p-3 bg-card">
        <div className="text-base font-bold text-muted-foreground mb-1">{title}</div>
        <div className="text-sm text-muted-foreground">未取得</div>
      </div>
    );
  }
  // ランク別スタイル（1位=金, 2位=銀, 3位=銅）
  const rankStyles: Record<number, { rowCls: string; idxCls: string; oddsCls: string }> = {
    0: {
      rowCls: "bg-gradient-to-r from-amber-100/60 via-transparent to-transparent dark:from-amber-500/10",
      idxCls: "gold-gradient font-extrabold text-base",
      oddsCls: "gold-gradient font-extrabold text-[15px]",
    },
    1: {
      rowCls: "bg-gradient-to-r from-slate-200/50 via-transparent to-transparent dark:from-slate-500/10",
      idxCls: "text-slate-500 dark:text-slate-400 font-extrabold text-base",
      oddsCls: "text-slate-700 dark:text-slate-200 font-bold text-[15px]",
    },
    2: {
      rowCls: "bg-gradient-to-r from-orange-200/40 via-transparent to-transparent dark:from-orange-800/15",
      idxCls: "text-orange-700 dark:text-orange-400 font-extrabold text-base",
      oddsCls: "text-orange-700 dark:text-orange-300 font-bold text-[15px]",
    },
  };
  return (
    <div className="rounded-lg p-3 bg-card border border-border shadow-[var(--shadow-sm)]">
      <div className="flex items-baseline justify-between mb-2">
        <div className="heading-section text-base">{title}</div>
        <div className="text-xs text-muted-foreground">{hint}</div>
      </div>
      <table className="w-full text-sm tnum">
        <thead>
          <tr className="text-xs text-muted-foreground border-b border-border/50">
            <th className="py-1.5 px-1 text-left w-8">#</th>
            <th className="py-1.5 px-1 text-left">組合せ</th>
            <th className="py-1.5 px-1 text-right">オッズ</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e, i) => {
            const rs = rankStyles[i];
            const rowBg = rs?.rowCls ?? "";
            const idxCls = rs?.idxCls ?? "text-muted-foreground";
            const oddsCls = rs?.oddsCls ?? "font-semibold";
            const keyStr = e.combo.join("-") + ":" + e.odds.toFixed(1);
            return (
              <tr
                key={keyStr}
                className={`border-b border-border/30 last:border-b-0 transition-colors ${rowBg}`}
              >
                <td className={`py-1.5 px-1 ${idxCls}`}>{i + 1}</td>
                <td className="py-1.5 px-1">
                  <div className="flex items-center gap-1.5">
                    {e.combo.map((n, idx) => {
                      // n は馬番。枠番に変換して WAKU_BG を引く
                      const gate = horseNoToGate.get(n);
                      const bg = gate != null ? WAKU_BG[gate] : undefined;
                      return (
                        <span
                          key={`${keyStr}-${idx}-${n}`}
                          className={`inline-flex w-7 h-7 items-center justify-center rounded-sm text-sm font-bold shadow-[var(--shadow-xs)] ${bg || "bg-gray-200"}`}
                        >
                          {n}
                        </span>
                      );
                    })}
                  </div>
                </td>
                <td className={`py-1.5 px-1 text-right ${oddsCls}`}>
                  {e.odds.toFixed(1)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function OddsPanel({ horses, race }: Props) {
  const [sortKey, setSortKey] = useState<SortKey>("odds");
  const [sortAsc, setSortAsc] = useState(true);
  // v6.1: 確率分布チャート開閉
  const [showProbChart, setShowProbChart] = useState(true);

  // 確率チャート用データ（勝率降順で上位10頭）— useMemo でメモ化
  const probEntries: WinProbEntry[] = useMemo(
    () =>
      [...horses]
        .filter((h) => (h.win_prob ?? 0) > 0 || (h.place3_prob ?? 0) > 0)
        .sort((a, b) => (b.win_prob ?? 0) - (a.win_prob ?? 0))
        .slice(0, 10)
        .map((h) => ({
          horse_no: h.horse_no,
          horse_name: h.horse_name || `#${h.horse_no}`,
          mark: MARK_KEY_TO_SYM[h.mark || ""] || "",
          p1: (h.win_prob ?? 0) * (h.win_prob && h.win_prob > 1 ? 1 : 100),
          p2: (h.place2_prob ?? 0) * (h.place2_prob && h.place2_prob > 1 ? 1 : 100),
          p3: (h.place3_prob ?? 0) * (h.place3_prob && h.place3_prob > 1 ? 1 : 100),
        })),
    [horses],
  );

  const hasRealOdds = horses.some((h) => h.odds != null && h.odds > 0);
  const top10 = race?.top10_odds;

  // 馬番→枠番マップ（WAKU_BG は枠番でインデックスされるため変換が必要）
  const horseNoToGate = new Map<number, number>();
  horses.forEach((h) => {
    if (h.horse_no != null && h.gate_no != null) {
      horseNoToGate.set(h.horse_no as number, h.gate_no as number);
    }
  });

  const sorted = [...horses].sort((a, b) => {
    let va: number, vb: number;
    switch (sortKey) {
      case "horse_no":
        va = a.horse_no || 0; vb = b.horse_no || 0; break;
      case "odds":
        va = a.odds ?? a.predicted_tansho_odds ?? 9999;
        vb = b.odds ?? b.predicted_tansho_odds ?? 9999; break;
      case "popularity":
        va = a.popularity ?? 999; vb = b.popularity ?? 999; break;
      case "win_prob":
        va = a.win_prob ?? 0; vb = b.win_prob ?? 0; break;
      case "place2_prob":
        va = a.place2_prob ?? 0; vb = b.place2_prob ?? 0; break;
      case "place3_prob":
        va = a.place3_prob ?? 0; vb = b.place3_prob ?? 0; break;
      case "ev":
        va = a.ev ?? 0; vb = b.ev ?? 0; break;
      default:
        va = 0; vb = 0;
    }
    return sortAsc ? va - vb : vb - va;
  });

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc);
    } else {
      setSortKey(key);
      // オッズ・人気・馬番はデフォルト昇順、確率・EVはデフォルト降順
      setSortAsc(key === "odds" || key === "popularity" || key === "horse_no");
    }
  };

  const sortArrow = (key: string) => {
    if (sortKey !== key) return "";
    return sortAsc ? " ↑" : " ↓";
  };

  const thCls = "py-1.5 px-1 text-right cursor-pointer hover:text-foreground select-none whitespace-nowrap";

  return (
    <div className="space-y-4">
      {/* v6.1: 確率分布チャート */}
      {probEntries.length > 0 && (
        <PremiumCard variant="default" padding="md">
          <PremiumCardHeader>
            <div className="flex flex-col gap-0.5">
              <PremiumCardAccent>TOP 10 確率比較</PremiumCardAccent>
              <PremiumCardTitle>勝率 / 連対率 / 複勝率</PremiumCardTitle>
            </div>
            <button
              type="button"
              className="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-md hover:bg-muted transition-colors text-muted-foreground"
              onClick={() => setShowProbChart((v) => !v)}
              aria-label={showProbChart ? "チャートを折りたたむ" : "チャートを展開する"}
            >
              {showProbChart ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
              {showProbChart ? "閉じる" : "開く"}
            </button>
          </PremiumCardHeader>
          {showProbChart && (
            <Suspense fallback={<div className="h-[220px] flex items-center justify-center text-sm text-muted-foreground">チャート読み込み中...</div>}>
              <WinProbBar horses={probEntries} />
            </Suspense>
          )}
        </PremiumCard>
      )}

      {/* 単勝テーブル（既存） */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border text-xs text-muted-foreground">
              <th className="py-1.5 px-1 text-center w-6 cursor-pointer hover:text-foreground select-none" onClick={() => handleSort("horse_no")}>
                番{sortArrow("horse_no")}
              </th>
              <th className="py-1.5 px-1 text-left">馬名</th>
              <th className="py-1.5 px-1 text-center w-6">印</th>
              <th className={thCls} onClick={() => handleSort("odds")}>
                {hasRealOdds ? "単勝" : "予測ｵｯｽﾞ"}{sortArrow("odds")}
              </th>
              <th className={thCls} onClick={() => handleSort("popularity")}>
                人気{sortArrow("popularity")}
              </th>
              <th className={thCls} onClick={() => handleSort("win_prob")}>
                勝率{sortArrow("win_prob")}
              </th>
              <th className={thCls} onClick={() => handleSort("place2_prob")}>
                連対率{sortArrow("place2_prob")}
              </th>
              <th className={thCls} onClick={() => handleSort("place3_prob")}>
                複勝率{sortArrow("place3_prob")}
              </th>
              <th className={thCls} onClick={() => handleSort("ev")}>
                EV{sortArrow("ev")}
              </th>
              <th className="py-1.5 px-1 text-center">乖離</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((h) => {
              const mark = h.mark || "";
              const realOdds = h.odds != null && h.odds > 0 ? h.odds : null;
              const predOdds = h.predicted_tansho_odds;
              const displayOdds = realOdds ?? predOdds;
              const ev = h.ev ?? (h.win_prob && displayOdds ? h.win_prob * displayOdds : null);
              const divSig = h.divergence_signal;

              return (
                <tr key={h.horse_no} className="border-b border-border/50 hover:bg-muted/30">
                  <td className="py-1.5 px-1 text-center">
                    <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                      {h.horse_no}
                    </span>
                  </td>
                  <td className="py-1.5 px-1 font-bold text-xs whitespace-nowrap">{h.horse_name}</td>
                  <td className="py-1.5 px-1 text-center">
                    {mark && <span className={`${MARK_COLORS[mark] || ""} font-bold text-sm leading-none`}>{mark}</span>}
                  </td>
                  <td className="py-1.5 px-1 text-right tabular-nums font-semibold">
                    {displayOdds != null ? displayOdds.toFixed(1) : "—"}
                    {!hasRealOdds && <span className="text-[10px] text-muted-foreground ml-0.5">*</span>}
                  </td>
                  <td className="py-1.5 px-1 text-right tabular-nums">
                    {h.popularity != null ? `${h.popularity}` : "—"}
                  </td>
                  <td className="py-1.5 px-1 text-right tabular-nums">
                    {h.win_prob != null ? `${(h.win_prob * 100).toFixed(1)}%` : "—"}
                  </td>
                  <td className="py-1.5 px-1 text-right tabular-nums">
                    {h.place2_prob != null ? `${(h.place2_prob * 100).toFixed(1)}%` : "—"}
                  </td>
                  <td className="py-1.5 px-1 text-right tabular-nums">
                    {h.place3_prob != null ? `${(h.place3_prob * 100).toFixed(1)}%` : "—"}
                  </td>
                  <td className={`py-1.5 px-1 text-right tabular-nums font-semibold ${ev != null && ev >= 1.0 ? "text-emerald-600" : ""}`}>
                    {ev != null ? ev.toFixed(2) : "—"}
                  </td>
                  <td className="py-1.5 px-1 text-center">
                    {divSig ? (
                      <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold ${divSignalCls(divSig)}`}>
                        {divSig}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">—</span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {!hasRealOdds && (
          <p className="text-xs text-muted-foreground mt-2">* 予測オッズ表示中。「オッズ取得」で実オッズに更新されます。</p>
        )}
      </div>

      {/* TOP10 オッズ（馬連/馬単/三連複/三連単） */}
      {top10 && (
        <div>
          <div className="text-sm font-bold mb-2 text-muted-foreground">
            公式実オッズ TOP10 <span className="text-xs font-normal">（組合せをオッズ昇順で抜粋）</span>
          </div>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
            <Top10Table title="馬連 TOP10" entries={top10.umaren} hint="順不同 2頭" horseNoToGate={horseNoToGate} />
            <Top10Table title="馬単 TOP10" entries={top10.umatan} hint="着順 1着→2着" horseNoToGate={horseNoToGate} />
            <Top10Table title="三連複 TOP10" entries={top10.sanrenpuku} hint="順不同 3頭" horseNoToGate={horseNoToGate} />
            <Top10Table title="三連単 TOP10" entries={top10.sanrentan} hint="着順 1→2→3着" horseNoToGate={horseNoToGate} />
          </div>
        </div>
      )}
    </div>
  );
}
