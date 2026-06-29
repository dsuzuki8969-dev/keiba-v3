/**
 * PC版 出馬表カード（縦カード型）
 *
 * 構造:
 * - 行1: 馬番(枠色) + MY印 + AI印 + 馬名 + [8軸グレード列（横並び）] + 馬体重
 * - 行2: (空白スペーサー)  + [8軸偏差値+順位（横並び）] + 性齢/騎手/斤量 + 単勝オッズ
 * - 行3: 脚質 | 通過順 | 勝率/連対/複勝/EV | 短評 | 人気
 *
 * 各軸列は縦3段（グレード文字 + 偏差値 + 順位）で表示される。
 * アコーディオン展開部（前三走・調教・厩舎コメント）は変更なし。
 */
import { useState, useMemo, useCallback, useEffect, useRef, memo } from "react";
import { createPortal } from "react-dom";
import {
  devGrade,
  gradeCls, rankCls, markCls,
} from "@/lib/constants";
import type { HorseData, PastRunData, TrainingRecord } from "./RaceDetailView";
import { PastRunsTable } from "./PastRunsTable";
import { TrainingSection } from "./HorseDiagnosis";
import { HorseHistoryChart } from "./HorseHistoryChart";
import type { RunEntry } from "./HorseHistoryChart";
import { rankToAxisMark } from "@/lib/horseSummary";
import { parseStableComment } from "@/lib/parseStableComment";
import { ResponsiveAxes } from "@/components/keiba/ResponsiveAxes";
import { useAbilityDisplayMode } from "@/hooks/useAbilityDisplayMode";
import { displayMark } from "@/lib/markDisplay";

// ---------- 定数 ----------

const D_MARK_OPTIONS = ["－", "◉", "◎", "○", "▲", "△", "★", "☆"] as const;

const MARK_SYMBOL: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆",
};

const STYLE_SHORT: Record<string, string> = {
  逃げ: "逃", 先行: "先", 差し: "差", 追込: "追",
};

/** 8 軸定義（表示順） */
const INDEX_DEFS = [
  { label: "総合", key: "comp",  getValue: (h: HorseData) => h.composite ?? 0 },
  { label: "能力", key: "abi",   getValue: (h: HorseData) => h.ability_total ?? 0 },
  { label: "展開", key: "pace",  getValue: (h: HorseData) => h.pace_total ?? 0 },
  { label: "適性", key: "crs",   getValue: (h: HorseData) => h.course_total ?? 0 },
  { label: "騎手", key: "jkd",   getValue: (h: HorseData) => h.jockey_dev ?? 50 },
  { label: "調教師", key: "trd", getValue: (h: HorseData) => h.trainer_dev ?? 50 },
  { label: "血統", key: "bld",   getValue: (h: HorseData) => h.bloodline_dev ?? 50 },
  { label: "追切", key: "trn",   getValue: (h: HorseData) => h.training_dev ?? 0 },
];

// ---------- ヘルパー ----------

function calcRanks(horses: HorseData[], key: (h: HorseData) => number): Record<number, number> {
  const ranks: Record<number, number> = {};
  for (let i = 0; i < horses.length; i++) {
    const v = key(horses[i]);
    ranks[horses[i].horse_no] = horses.filter((h) => key(h) > v).length + 1;
  }
  return ranks;
}

function estimatedCorners(h: HorseData): string {
  const corners = (h as Record<string, unknown>).predicted_corners as string | undefined;
  return corners || "—";
}

/** 枠番スタイル */
function gateColorStyle(gate: number): React.CSSProperties {
  const colors: Record<number, { bg: string; color: string; border?: string }> = {
    1: { bg: "#f0f0f0", color: "#333", border: "1px solid #ccc" },
    2: { bg: "#222", color: "#fff" },
    3: { bg: "#e74c3c", color: "#fff" },
    4: { bg: "#2980b9", color: "#fff" },
    5: { bg: "#f1c40f", color: "#333" },
    6: { bg: "#27ae60", color: "#fff" },
    7: { bg: "#e67e22", color: "#fff" },
    8: { bg: "#e91e9e", color: "#fff" },
  };
  const c = colors[gate] || colors[1];
  return { background: c.bg, color: c.color, border: c.border };
}

/** 脚質チップクラス */
function rsChipCls(style: string): string {
  const s = STYLE_SHORT[style] || style;
  if (s === "逃") return "bg-emerald-50 text-emerald-600 dark:bg-emerald-950/40";
  if (s === "先") return "bg-blue-50 text-blue-600 dark:bg-blue-950/40";
  if (s === "差") return "bg-red-50 text-red-600 dark:bg-red-950/40";
  return "bg-purple-50 text-purple-600 dark:bg-purple-950/40";
}

/** オッズ色 */
function oddsCls(pop: number | null | undefined): string {
  if (pop == null) return "text-muted-foreground";
  return rankCls(pop) || "text-foreground";
}

/** EV 色 */
function evColorCls(ev: number | undefined): string {
  if (ev == null) return "text-muted-foreground";
  if (ev >= 1.20) return "text-emerald-600 font-bold";
  if (ev >= 1.00) return "text-blue-600 font-bold";
  if (ev >= 0.80) return "text-foreground";
  return "text-muted-foreground";
}

/** localStorage D印 */
function dMarkStorageKey(raceId: string): string { return `dmark_${raceId}`; }
function saveDMarks(raceId: string, marks: Record<number, string>) {
  localStorage.setItem(dMarkStorageKey(raceId), JSON.stringify(marks));
}

// ---------- D印セレクター ----------

function DMarkChip({
  horseNo, raceId, dMarks, setDMarks,
}: {
  horseNo: number; raceId: string | undefined;
  dMarks: Record<number, string>; setDMarks: React.Dispatch<React.SetStateAction<Record<number, string>>>;
}) {
  const [pos, setPos] = useState<{ x: number; y: number } | null>(null);
  const popRef = useRef<HTMLDivElement>(null);
  const current = dMarks[horseNo] || "－";
  const isOpen = pos !== null;

  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: MouseEvent) => { if (popRef.current && !popRef.current.contains(e.target as Node)) setPos(null); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) return;
    const handler = () => setPos(null);
    window.addEventListener("scroll", handler, true);
    return () => window.removeEventListener("scroll", handler, true);
  }, [isOpen]);

  const select = useCallback((mark: string) => {
    if (!raceId) return;
    setDMarks((prev) => {
      const updated = { ...prev };
      if (mark === "－") delete updated[horseNo]; else updated[horseNo] = mark;
      saveDMarks(raceId, updated);
      return updated;
    });
    setPos(null);
  }, [horseNo, raceId, setDMarks]);

  const handleClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    if (isOpen) { setPos(null); return; }
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    setPos({ x: rect.left + rect.width / 2, y: rect.bottom + 2 });
  }, [isOpen]);

  return (
    <>
      <span
        className={`cursor-pointer select-none ${current === "－" ? "text-muted-foreground/30" : markCls(current)}`}
        onClick={handleClick}
        title="MY印を変更"
      >
        {current}
      </span>
      {isOpen && createPortal(
        <div ref={popRef} className="fixed z-[9999] bg-popover border border-border rounded-md shadow-lg flex gap-0 py-0.5 px-0.5"
          style={{ left: pos!.x, top: pos!.y, transform: "translateX(-50%)" }}>
          {D_MARK_OPTIONS.map((m) => (
            <button key={m}
              className={`w-6 h-6 flex items-center justify-center rounded text-sm hover:bg-muted transition-colors ${m === current ? "ring-1 ring-primary bg-muted" : ""} ${m === "－" ? "text-muted-foreground" : markCls(m)}`}
              onClick={(e) => { e.stopPropagation(); select(m); }}>
              {m}
            </button>
          ))}
        </div>,
        document.body
      )}
    </>
  );
}

// ---------- カード展開詳細 ----------

function CardDetail({ h }: { h: HorseData }) {
  const runs = (h as Record<string, unknown>).past_3_runs as PastRunData[] || [];
  const trainingRecords = (h as Record<string, unknown>).training_records as TrainingRecord[] || [];
  const trRecs = (h as Record<string, unknown>).training_records as Array<Record<string, unknown>> | undefined;
  const stableComment = trRecs?.[0]?.stable_comment as string || "";
  const stableBullets = trRecs?.[0]?.stable_comment_bullets as string[] | undefined;

  const horseId = (h as Record<string, unknown>).horse_id as string | number | undefined;
  const [histRuns, setHistRuns] = useState<RunEntry[] | null>(null);
  useEffect(() => {
    if (!horseId) return;
    const ctrl = new AbortController();
    fetch(`/api/horse_history/${horseId}?limit=12`, { signal: ctrl.signal })
      .then((r) => r.json())
      .then((d: Record<string, unknown>) => {
        if (d.ok && Array.isArray(d.runs)) setHistRuns(d.runs as RunEntry[]);
      })
      .catch((e) => {
        if (e?.name !== "AbortError" && import.meta.env.DEV) {
          console.warn("horse_history fetch failed:", e);
        }
      });
    return () => ctrl.abort();
  }, [horseId]);

  return (
    <div className="border-t border-border mt-2 pt-3 space-y-3">
      {/* 走破偏差値グラフ（3戦以上でのみ表示） */}
      {histRuns && <HorseHistoryChart runs={histRuns} horseName={h.horse_name} />}
      {/* 前三走 */}
      <PastRunsTable runs={runs} />

      {/* 調教 */}
      <div>
        <div className="text-[11px] font-bold text-muted-foreground mb-1 border-l-[3px] border-blue-500 pl-2">調教</div>
        <TrainingSection records={trainingRecords} />
      </div>

      {/* 厩舎コメント (T-025 2026-04-28: stableBullets/stableComment 両方を parseStableComment で統一処理) */}
      {stableComment && (
        <div>
          <div className="text-[13px] font-bold text-muted-foreground mb-1 border-l-[3px] border-blue-500 pl-2">厩舎コメント</div>
          {(() => {
            // 統一処理: bullets 配列があれば join、無ければ原文 → parseStableComment で prefix/曖昧表現を全部除去
            const inputText = stableBullets && stableBullets.length > 0
              ? stableBullets.join('\n')
              : stableComment;
            const parsed = parseStableComment(inputText);
            if (parsed.length === 0) {
              // パース失敗時は原文表示（最後の砦）
              return (
                <div className="bg-muted/30 p-3 rounded-md border text-[14px] leading-relaxed">
                  {stableComment}
                </div>
              );
            }
            return (
              <ul className="bg-muted/30 p-3 rounded-md border text-[14px] leading-relaxed space-y-1">
                {parsed.map((b, i) => (
                  <li key={i} className="flex gap-1.5 items-start">
                    {/* T-024 (2026-04-28): 箇条書き「・」マーカー追加 */}
                    <span className="text-muted-foreground shrink-0">・</span>
                    <span>{b.text}</span>
                  </li>
                ))}
              </ul>
            );
          })()}
        </div>
      )}
    </div>
  );
}

// ---------- 8軸セル（縦3段: グレード文字 + 偏差値 + 順位） ----------

function AxisCell({
  label, value, rank, isBanei, axisKey,
  isRelativeAbi, hasRelativeData,
}: {
  label: string;
  value: number;
  rank: number;
  isBanei?: boolean;
  axisKey: string;
  /** 相対指数モードで能力軸が表示中かどうか */
  isRelativeAbi?: boolean;
  /** race_relative_dev が存在するかどうか（フォールバック警告用） */
  hasRelativeData?: boolean;
}) {
  // バネイ競馬は展開軸をスキップ
  if (isBanei && axisKey === "pace") return null;

  // 相対モード＋能力軸でデータなし → フォールバック中（警告表示）
  const isFallback = isRelativeAbi && !hasRelativeData;

  const hasVal = value != null && value > 0;
  const g = hasVal ? devGrade(value) : "";
  // データなし（hasVal=false）の場合は印を「−」にする（全頭同率1位で◎固定になる問題を防ぐ）
  const axMark = hasVal ? rankToAxisMark(rank) : "−";

  // 軸ラベル: 相対指数モードの能力軸は「相対」と表示
  const displayLabel = isRelativeAbi ? "相対" : label.slice(0, 2);

  return (
    <div className="flex flex-col items-center flex-1 min-w-0 px-1 py-0.5">
      {/* 軸名ラベル（相対モード時は強調表示） */}
      <span className={`text-[12px] font-semibold leading-none mb-1 ${isRelativeAbi ? "text-sky-500 font-bold" : "text-muted-foreground"}`}>
        {displayLabel}
      </span>
      {/* 印マーク（グレード相当） */}
      <span className={`text-[26px] leading-none font-bold ${axMark === "−" ? "text-muted-foreground/40" : markCls(axMark)}`}>
        {axMark}
      </span>
      {/* 偏差値（グレード文字 + 数値、例: A57 / S67 / B51 / 相対時は R50 等） */}
      <span className={`text-[18px] font-bold tabular-nums leading-none mt-1 ${hasVal ? gradeCls(g) : "text-muted-foreground/30"}`}>
        {isFallback
          ? <span className="text-[12px] text-amber-500">計算中</span>
          : hasVal
            ? <><span className="mr-0.5">{g}</span>{value.toFixed(0)}</>
            : "—"}
      </span>
      {/* 順位 */}
      <span className={`text-[12px] tabular-nums leading-none mt-1 ${hasVal ? rankCls(rank) : "text-muted-foreground/30"}`}>
        {hasVal ? `${rank}位` : ""}
      </span>
    </div>
  );
}

// ---------- 1馬カード（縦カード型） ----------

const HorseCard = memo(function HorseCard({
  h, idxRanks, wpRank, p2Rank, p3Rank,
  raceId, dMarks, setDMarks, hasAnyOdds, isBanei, isRelativeMode,
}: {
  h: HorseData;
  idxRanks: Record<string, Record<number, number>>;
  wpRank: number; p2Rank: number; p3Rank: number;
  raceId?: string;
  dMarks: Record<number, string>;
  setDMarks: React.Dispatch<React.SetStateAction<Record<number, string>>>;
  hasAnyOdds: boolean;
  isBanei?: boolean;
  /** 相対指数表示モードかどうか（トグルからの受け渡し） */
  isRelativeMode: boolean;
}) {
  const [open, setOpen] = useState(false);
  const no = h.horse_no;
  const gate = h.gate_no || 1;
  const mark = h.mark || "－";
  const markSym = displayMark(MARK_SYMBOL[mark] || mark);
  const rsShort = STYLE_SHORT[h.running_style || ""] || h.running_style || "—";
  const corners = estimatedCorners(h);
  // 出走取消判定 (pred.json `is_scratched` フラグ優先 + 旧フォールバック)
  const isScratched = (h as Record<string, unknown>).is_scratched === true
    || (hasAnyOdds && (h.odds == null && h.popularity == null));

  const realOdds = h.odds != null && h.odds > 0;
  const oddsVal = realOdds ? h.odds! : h.predicted_tansho_odds;
  const oddsStr = isScratched
    ? "取消"
    : (oddsVal != null ? oddsVal.toFixed(1) + "倍" + (realOdds ? "" : "*") : "—");
  const popStr = isScratched
    ? ""
    : (h.popularity != null ? h.popularity + "人気" : h.predicted_rank != null ? h.predicted_rank + "位*" : "—");
  const ev = h.ev;
  const wp = ((h.win_prob ?? 0) * 100);
  const p2 = ((h.place2_prob ?? 0) * 100);
  const p3 = ((h.place3_prob ?? 0) * 100);

  // 行アクセント（印別）
  const rowAccent = (() => {
    if (isScratched) return "";
    if (mark === "tekipan")  return "border-brand-gold/70 shadow-[0_0_0_1px_rgba(212,168,83,0.35),0_4px_12px_-4px_rgba(212,168,83,0.35)] bg-gradient-to-r from-amber-50/60 to-transparent dark:from-amber-500/5";
    if (mark === "honmei")   return "border-mark-honmei/50 bg-gradient-to-r from-emerald-50/50 to-transparent dark:from-emerald-500/5";
    if (mark === "taikou")   return "border-mark-taikou/40 bg-gradient-to-r from-blue-50/40 to-transparent dark:from-blue-500/5";
    if (mark === "tannuke")  return "border-mark-tannuke/40 bg-gradient-to-r from-red-50/40 to-transparent dark:from-red-500/5";
    return "border-border";
  })();

  // 性齢・斤量
  const sexAge = `${h.sex || ""}${h.age || ""}`;
  const weightKg = h.weight_kg != null ? `${Number(h.weight_kg).toFixed(1)}kg` : "";

  // 父・母父
  const sire = (h as Record<string, unknown>).sire as string || "";
  const mgs  = (h as Record<string, unknown>).maternal_grandsire as string || "";

  // 馬体重（前走比）
  const horseWeightNode = h.horse_weight != null ? (
    <span className="inline-flex items-baseline gap-0.5 whitespace-nowrap tabular-nums">
      <span className="text-foreground">{Number(h.horse_weight).toFixed(0)}kg</span>
      {h.weight_change != null && (
        <span className={`font-semibold ${
          h.weight_change > 0
            ? "text-red-500 dark:text-red-400"
            : h.weight_change < 0
              ? "text-blue-500 dark:text-blue-400"
              : "text-muted-foreground"
        }`}>
          ({h.weight_change > 0 ? "+" : h.weight_change === 0 ? "±" : ""}{h.weight_change})
        </span>
      )}
    </span>
  ) : null;

  return (
    <div
      className={`bg-card border rounded-md transition-all hover:border-brand-gold/60 hover:shadow-[0_0_8px_-2px_rgba(212,168,83,0.3)] ${rowAccent} ${open ? "shadow-md" : ""} ${isScratched ? "opacity-40" : ""}`}
    >
      {/* ======== クリックで展開するヘッダ部（縦3段構成） ======== */}
      <div
        className="cursor-pointer px-2 py-1.5 flex flex-col gap-1"
        role="button"
        tabIndex={0}
        aria-expanded={open}
        onClick={() => setOpen(!open)}
        onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setOpen(!open); } }}
      >
        {/* 【1】馬名行: 馬番 + MY印 + AI印 + 馬名 + 騎手 + オッズ(人気) + 脚質チップ + 通過順 */}
        <div className="flex items-center gap-1">
          <span
            className="w-6 h-6 rounded flex items-center justify-center text-xs font-bold shrink-0"
            style={gateColorStyle(gate)}
          >
            {no}
          </span>
          <span className="w-5 flex items-center justify-center shrink-0">
            <DMarkChip horseNo={no} raceId={raceId} dMarks={dMarks} setDMarks={setDMarks} />
          </span>
          <span className={`w-5 flex items-center justify-center shrink-0 text-[13px] font-bold ${markSym === "－" ? "text-muted-foreground/30" : markCls(markSym)}`}>
            {markSym}
          </span>
          <span className={`font-bold text-[15px] min-w-0 truncate leading-tight ${isScratched ? "line-through text-muted-foreground" : ""}`}>{h.horse_name}</span>
          {/* 騎手・当日オッズ・脚質チップ・通過順（馬名の直後に配置） */}
          <span className="shrink-0 flex items-center gap-1.5 whitespace-nowrap">
            <span className="font-bold text-[13px] text-foreground">{h.jockey || "—"}</span>
            <span className={`text-[14px] tabular-nums font-bold ${isScratched ? "text-red-500 dark:text-red-400" : oddsCls(h.popularity)}`}>{oddsStr}</span>
            <span className={`text-[12px] ${h.popularity != null ? rankCls(h.popularity) || "text-muted-foreground" : "text-muted-foreground"}`}>({popStr})</span>
            {/* 脚質チップ（rsChipCls 色） */}
            {rsShort !== "—" && (
              <span className={`px-1.5 py-0 rounded text-[12px] font-bold ${rsChipCls(h.running_style || "")}`}>{rsShort}</span>
            )}
            {/* 通過順 */}
            {corners !== "—" && (
              <span className="text-[12px] tabular-nums text-muted-foreground">{corners}</span>
            )}
            {/* 前日想定オッズ + 当日GAP（assumed_odds が有効値のときのみ表示） */}
            {h.assumed_odds != null && h.assumed_odds > 0 && (
              <>
                <span className="text-muted-foreground text-[12px]">前日想定</span>
                <span className="tabular-nums text-muted-foreground text-[12px]">
                  {h.assumed_odds.toFixed(1)}倍{h.assumed_popularity != null ? `(${h.assumed_popularity}人気)` : ""}
                </span>
                {h.odds != null && h.odds > 0 && (() => {
                  const gap = (h.odds - h.assumed_odds) / h.assumed_odds * 100;
                  if (Math.abs(gap) < 3) {
                    return <span className="text-muted-foreground tabular-nums text-[12px]">±0%</span>;
                  } else if (gap < 0) {
                    return <span className="tabular-nums text-emerald-600 dark:text-emerald-400 font-semibold text-[12px]">↓{Math.abs(gap).toFixed(0)}%</span>;
                  } else {
                    return <span className="tabular-nums text-muted-foreground text-[12px]">↑{gap.toFixed(0)}%</span>;
                  }
                })()}
              </>
            )}
          </span>
        </div>

        {/* 【2】強さ指標行（軸馬度を主役・内訳を小さく） */}
        <div className="flex items-center flex-wrap gap-x-2 gap-y-0.5">
          {/* 軸馬度バッジ（主役・大） */}
          {h.jiku_score != null && (
            <span className="inline-flex items-baseline gap-1 px-2 py-0.5 rounded bg-emerald-100 dark:bg-emerald-900/40">
              <span className="text-[12px] font-semibold text-emerald-700 dark:text-emerald-300">軸馬度</span>
              <span className="text-[22px] font-medium tabular-nums text-emerald-700 dark:text-emerald-300 leading-none">{h.jiku_score.toFixed(1)}</span>
            </span>
          )}
          {/* 穴馬度バッジ（主役・大・ana_do != null なら0.0でも常時表示） */}
          {h.ana_do != null && (
            <span className="inline-flex items-baseline gap-1 px-2 py-0.5 rounded bg-amber-100 dark:bg-amber-900/40">
              <span className="text-[12px] font-semibold text-amber-700 dark:text-amber-300">穴馬度</span>
              <span className="text-[22px] font-medium tabular-nums text-amber-700 dark:text-amber-300 leading-none">{h.ana_do.toFixed(1)}</span>
            </span>
          )}
          {/* 縦区切り（バッジと内訳確率の間） */}
          {(h.jiku_score != null || h.ana_do != null) && (
            <span className="text-muted-foreground/40 select-none">｜</span>
          )}
          {/* 勝率（内訳） */}
          <span className="flex items-baseline gap-0.5 shrink-0">
            <span className="text-muted-foreground text-[11px]">勝</span>
            <span className={`tabular-nums text-[13px] font-semibold ${rankCls(wpRank)}`}>{wp.toFixed(1)}%</span>
            <span className={`text-[11px] ${rankCls(wpRank)}`}>({wpRank}位)</span>
          </span>
          {/* 連対率（内訳） */}
          <span className="flex items-baseline gap-0.5 shrink-0">
            <span className="text-muted-foreground text-[11px]">連</span>
            <span className={`tabular-nums text-[13px] font-semibold ${rankCls(p2Rank)}`}>{p2.toFixed(1)}%</span>
            <span className={`text-[11px] ${rankCls(p2Rank)}`}>({p2Rank}位)</span>
          </span>
          {/* 複勝率（内訳） */}
          <span className="flex items-baseline gap-0.5 shrink-0">
            <span className="text-muted-foreground text-[11px]">複</span>
            <span className={`tabular-nums text-[13px] font-semibold ${rankCls(p3Rank)}`}>{p3.toFixed(1)}%</span>
            <span className={`text-[11px] ${rankCls(p3Rank)}`}>({p3Rank}位)</span>
          </span>
          {/* EV（内訳） */}
          <span className="flex items-baseline gap-0.5 shrink-0">
            <span className="text-muted-foreground text-[11px]">EV</span>
            <span className={`tabular-nums text-[13px] font-semibold ${ev != null ? evColorCls(ev) : "text-muted-foreground"}`}>
              {ev != null ? ev.toFixed(2) : "—"}
            </span>
          </span>
        </div>

        {/* 【3】左右2カラム: 左=性齢父母...道悪 / 右=8ファクター */}
        <div className="flex flex-col md:flex-row gap-2 md:gap-3 md:items-stretch">

        {/* 左カラム: 性齢斤量馬体重/父/母父/厩舎+騎手/通過順+オッズ/前日想定/道悪 */}
        <div className="w-full md:w-[300px] shrink-0">
          <div className="ml-0 space-y-px">
            {/* 行A1: 性齢・斤量・馬体重（全幅・折り返しなし） */}
            <div className="flex items-baseline gap-1.5 text-[12px] whitespace-nowrap">
              <span className="text-foreground">{sexAge}</span>
              {weightKg && <span className="text-muted-foreground">{weightKg}</span>}
              {horseWeightNode && (
                <span className="text-[12px] text-muted-foreground whitespace-nowrap">馬体重 {horseWeightNode}</span>
              )}
            </div>

            {/* 行A2: 父のみ */}
            <div className="text-[12px] text-muted-foreground">
              父 <span className="text-foreground">{sire || "—"}</span>
            </div>

            {/* 行B: 母父 */}
            <div className="text-[12px] text-muted-foreground">
              母父 <span className="text-foreground">{mgs || "—"}</span>
            </div>

            {/* 行C: 厩舎のみ（騎手は馬名行へ移動） */}
            <div className="text-[12px] text-muted-foreground truncate">
              {h.trainer || "—"}
            </div>

            {/* 行E: 道悪着度数（全馬表示・未経験は「経験なし」） */}
            <div className="flex items-center gap-1.5 text-[11px] mt-0.5">
              <span className="text-muted-foreground">道悪</span>
              {h.baba_record && h.baba_record.bad_n > 0 ? (
                <>
                  {/* 着度数: 1-2-3-着外（XX走：複勝率XX.X%） */}
                  <span className={`tabular-nums font-semibold ${
                    h.baba_record.bad_p3 != null && h.baba_record.good_p3 != null && h.baba_record.bad_p3 - h.baba_record.good_p3 >= 10
                      ? "text-emerald-600 dark:text-emerald-400"
                      : h.baba_record.bad_p3 != null && h.baba_record.good_p3 != null && h.baba_record.bad_p3 - h.baba_record.good_p3 <= -10
                        ? "text-muted-foreground"
                        : "text-foreground"
                  }`}>
                    {h.baba_record.bad_1 ?? 0}-{h.baba_record.bad_2 ?? 0}-{h.baba_record.bad_3 ?? 0}-{h.baba_record.bad_other ?? 0}
                  </span>
                  <span className="text-muted-foreground">
                    ({h.baba_record.bad_n}走
                    {h.baba_record.bad_p3 != null
                      ? `：複勝率${h.baba_record.bad_p3.toFixed(1)}%`
                      : "：複勝率—"}
                    )
                  </span>
                  {h.baba_record.bad_p3 != null && h.baba_record.good_p3 != null && h.baba_record.bad_p3 - h.baba_record.good_p3 >= 10 && (
                    <span className="inline-flex items-center px-1 py-0 rounded bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-400 text-[10px] font-bold border border-emerald-300 dark:border-emerald-700">
                      道悪◎
                    </span>
                  )}
                </>
              ) : (
                <span className="text-[11px] text-muted-foreground">経験なし</span>
              )}
            </div>
          </div>
        </div>

        {/* 右カラム: 8軸のみ */}
        <div className="flex flex-col flex-1 min-w-0">
          {/* 8軸（馬名行の上端から始まる） */}
          <ResponsiveAxes count={8}>
            {INDEX_DEFS.map((def) => {
              // 相対指数モード: 能力軸(abi)のみ race_relative_dev を使う
              // race_relative_dev が null/undefined の場合は絶対指数にフォールバック（警告あり）
              let displayValue = def.getValue(h);
              if (isRelativeMode && def.key === "abi") {
                const rrd = h.race_relative_dev;
                if (rrd != null) {
                  displayValue = rrd;
                }
                // rrd が null/undefined の場合は絶対指数のまま（フォールバック禁止: 警告のみ）
              }
              return (
                <AxisCell
                  key={def.key}
                  label={def.label}
                  value={displayValue}
                  rank={idxRanks[def.key]?.[no] || 99}
                  isBanei={isBanei}
                  axisKey={def.key}
                  isRelativeAbi={isRelativeMode && def.key === "abi"}
                  hasRelativeData={h.race_relative_dev != null}
                />
              );
            })}
          </ResponsiveAxes>
        </div>
        </div>{/* 【3】左右2カラム 終わり */}
      </div>{/* ヘッダー 終わり */}

      {/* ======== アコーディオン展開詳細 ======== */}
      {open && (
        <div className="px-2 pb-2">
          <CardDetail h={h} />
        </div>
      )}
    </div>
  );
});
HorseCard.displayName = "HorseCard";

// ---------- メインコンポーネント ----------

interface Props {
  horses: HorseData[];
  raceId?: string;
  isBanei?: boolean;
  dMarks: Record<number, string>;
  setDMarks: React.Dispatch<React.SetStateAction<Record<number, string>>>;
}

export function HorseCardPC({ horses, raceId, isBanei, dMarks, setDMarks }: Props) {
  // 全指数の順位を事前計算
  const idxRanks = useMemo(() => {
    const result: Record<string, Record<number, number>> = {};
    for (const def of INDEX_DEFS) {
      result[def.key] = calcRanks(horses, def.getValue);
    }
    return result;
  }, [horses]);

  const wpRanks = useMemo(() => calcRanks(horses, (h) => h.win_prob ?? 0), [horses]);
  const p2Ranks = useMemo(() => calcRanks(horses, (h) => h.place2_prob ?? 0), [horses]);
  const p3Ranks = useMemo(() => calcRanks(horses, (h) => h.place3_prob ?? 0), [horses]);

  // レース全体でオッズが1頭でもあるか（全馬nullならオッズ未取得＝取消判定しない）
  const hasAnyOdds = useMemo(
    () => horses.some((h) => h.odds != null || h.popularity != null),
    [horses],
  );

  // Plan-γ Phase 5: 絶対指数/相対指数 切替トグル（グローバルモード）
  const { mode, toggle } = useAbilityDisplayMode();
  const isRelativeMode = mode === "relative";

  // 相対指数データが1頭でも存在するか（トグルの有効化判定）
  const hasAnyRelativeData = useMemo(
    () => horses.some((h) => h.race_relative_dev != null),
    [horses],
  );

  return (
    <div className="space-y-1.5">
      {/* 絶対指数 / 相対指数 切替トグル（能力軸のみ影響） */}
      <div className="flex items-center justify-end gap-2 px-1 py-0.5">
        <span className="text-[12px] text-muted-foreground">能力軸:</span>
        <button
          type="button"
          role="switch"
          aria-checked={isRelativeMode}
          aria-label="絶対指数 / 相対指数 切替"
          disabled={!hasAnyRelativeData}
          onClick={toggle}
          className={[
            "inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-[12px] font-semibold",
            "border transition-colors select-none",
            isRelativeMode
              ? "bg-sky-100 dark:bg-sky-900/40 border-sky-400 text-sky-700 dark:text-sky-300"
              : "bg-muted border-border text-muted-foreground hover:bg-muted/80",
            !hasAnyRelativeData ? "opacity-40 cursor-not-allowed" : "cursor-pointer",
          ].join(" ")}
          title={hasAnyRelativeData ? "絶対指数/相対指数を切替" : "相対指数データが未算出です"}
        >
          {/* トグルインジケーター */}
          <span className={[
            "inline-block w-3.5 h-3.5 rounded-full border border-current transition-colors",
            isRelativeMode ? "bg-sky-500" : "bg-muted-foreground/30",
          ].join(" ")} />
          {isRelativeMode ? "相対指数" : "絶対指数"}
        </button>
      </div>
      {horses.map((h) => (
        <HorseCard
          key={h.horse_no}
          h={h}
          idxRanks={idxRanks}
          wpRank={wpRanks[h.horse_no]}
          p2Rank={p2Ranks[h.horse_no]}
          p3Rank={p3Ranks[h.horse_no]}
          raceId={raceId}
          isBanei={isBanei}
          dMarks={dMarks}
          setDMarks={setDMarks}
          hasAnyOdds={hasAnyOdds}
          isRelativeMode={isRelativeMode}
        />
      ))}
    </div>
  );
}
