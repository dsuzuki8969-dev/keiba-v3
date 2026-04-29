/**
 * モバイル版 出馬表カード（完全縦並びレイアウト）
 *
 * 8軸を縦1行ずつ表示し、印と指数が必ず同行に揃う。
 * 横スクロールなし。アコーディオン展開部は変更なし。
 */
import { useState, useRef, useEffect, useMemo } from "react";
import {
  devGrade, gradeCls, posCls, markCls, rankCls, WAKU_BG, pastRunResultUrl,
} from "@/lib/constants";
import type { HorseData, PastRunData, TrainingRecord } from "./RaceDetailView";
import { TrainingSection } from "./HorseDiagnosis";
import { HorseHistoryChart } from "./HorseHistoryChart";
import type { RunEntry } from "./HorseHistoryChart";
import { generateHorseSummary, rankToAxisMark } from "@/lib/horseSummary";
import { parseStableComment } from "@/lib/parseStableComment";
import { useAbilityDisplayMode } from "@/hooks/useAbilityDisplayMode";

interface Props {
  horses: HorseData[];
  isBanei?: boolean;
  dMarks?: Record<number, string>;
  onDMarkSelect?: (horseNo: number, mark: string) => void;
}

// D印の選択肢
const D_MARK_OPTIONS = ["－", "◉", "◎", "○", "▲", "△", "★", "☆", "×"] as const;

// 印シンボルマップ
const MARK_SYMBOL: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆", kiken: "×",
};

// 8 軸定義（PC と統一）
const INDEX_DEFS = [
  { label: "総合", key: "comp",  getValue: (h: HorseData) => h.composite || 0 },
  { label: "能力", key: "abi",   getValue: (h: HorseData) => h.ability_total || 0 },
  { label: "展開", key: "pace",  getValue: (h: HorseData) => h.pace_total || 0 },
  { label: "適性", key: "crs",   getValue: (h: HorseData) => h.course_total || 0 },
  { label: "騎手", key: "jkd",   getValue: (h: HorseData) => h.jockey_dev || 50 },
  { label: "調教師", key: "trd", getValue: (h: HorseData) => h.trainer_dev || 50 },
  { label: "血統", key: "bld",   getValue: (h: HorseData) => h.bloodline_dev || 50 },
  { label: "追切", key: "trn",   getValue: (h: HorseData) => h.training_dev ?? 0 },
];

// 脚質短縮マップ
const STYLE_SHORT: Record<string, string> = {
  "逃げ": "逃", "先行": "先", "差し": "差", "追込": "追",
};

// 順位計算
function calcRanks(horses: HorseData[], key: (h: HorseData) => number): Record<number, number> {
  const ranks: Record<number, number> = {};
  for (let i = 0; i < horses.length; i++) {
    const v = key(horses[i]);
    ranks[horses[i].horse_no] = horses.filter((h) => key(h) > v).length + 1;
  }
  return ranks;
}

// 馬場状態カラー
function condCls(cond: string | undefined): string {
  if (cond === "重" || cond === "不良") return "text-blue-600";
  return "";
}

// タイム表示
function fmtTime(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return "—";
  const m = Math.floor(sec / 60);
  const s = sec - m * 60;
  return m > 0 ? `${m}:${s.toFixed(1).padStart(4, "0")}` : s.toFixed(1);
}

// 脚質色
function rsColorCls(style: string): string {
  const s = STYLE_SHORT[style] || style;
  if (s === "逃") return "text-emerald-600 font-bold";
  if (s === "先") return "text-blue-600 font-bold";
  if (s === "差") return "text-red-600 font-bold";
  if (s === "追") return "text-purple-600 font-bold";
  return "";
}

// 印マーク別に行背景グラデ（PC と統一）
function markRowAccent(mark: string | undefined): string {
  if (!mark) return "";
  if (mark === "tekipan")  return "bg-gradient-to-r from-amber-50/80 via-amber-50/20 to-transparent dark:from-amber-500/10 dark:via-amber-500/3";
  if (mark === "honmei")   return "bg-gradient-to-r from-emerald-50/70 via-emerald-50/15 to-transparent dark:from-emerald-500/10";
  if (mark === "taikou")   return "bg-gradient-to-r from-blue-50/60 via-blue-50/15 to-transparent dark:from-blue-500/8";
  if (mark === "tannuke")  return "bg-gradient-to-r from-red-50/60 via-red-50/15 to-transparent dark:from-red-500/8";
  return "";
}

// ---------- D印ポップオーバー（モバイル用、既存実装維持） ----------
function DMarkBadge({
  horseNo, dMarks, onSelect,
}: {
  horseNo: number;
  dMarks?: Record<number, string>;
  onSelect?: (horseNo: number, mark: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const dm = dMarks?.[horseNo] || "－";

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  return (
    <span ref={ref} className="relative">
      <span
        className={`text-base leading-none cursor-pointer select-none ${dm === "－" ? "text-muted-foreground/30" : markCls(dm)}`}
        onClick={(e) => { e.stopPropagation(); setOpen((v) => !v); }}
      >
        {dm}
      </span>
      {open && (
        <div className="absolute left-0 top-full z-50 mt-1 bg-popover border border-border rounded-lg shadow-[var(--shadow-lg)] flex gap-0.5 py-1 px-1 whitespace-nowrap">
          {D_MARK_OPTIONS.map((m) => (
            <button
              key={m}
              className={`min-w-[40px] min-h-[40px] flex items-center justify-center rounded text-lg hover:bg-muted transition-colors ${
                m === dm ? "ring-2 ring-brand-gold bg-muted" : ""
              } ${m === "－" ? "text-muted-foreground" : markCls(m)}`}
              onClick={(e) => { e.stopPropagation(); onSelect?.(horseNo, m); setOpen(false); }}
              aria-label={`D印を ${m} に設定`}
            >
              {m}
            </button>
          ))}
        </div>
      )}
    </span>
  );
}

// ---------- 前三走カード（展開時、既存実装維持） ----------
function PastRunsMini({ runs }: { runs: PastRunData[] }) {
  if (!runs || runs.length === 0) return null;
  return (
    <div>
      <div className="text-[11px] font-bold text-muted-foreground mb-1 border-l-[3px] border-blue-500 pl-1.5">前三走成績</div>
      <div className="space-y-1.5">
        {runs.map((r, i) => {
          const dateShort = r.date ? r.date.slice(2).replace(/-/g, "/") : "—";
          const url = pastRunResultUrl(r.race_id, r.date, r.venue, r.result_cname, r.race_no);
          const surf = r.surface === "ダート" ? "ダ" : r.surface === "芝" ? "芝" : (r.surface || "");
          const dist = r.distance || 0;
          const cond = r.condition || "";
          const cls = r.class || "";
          const fc = r.field_count || 0;
          const fp = r.finish_pos;
          const pop = (r as Record<string, unknown>).popularity as number | undefined;
          const jk = r.jockey || ""; // slice 削除: CSS truncate で制御
          const isBaneiRun = r.venue === "帯広";
          const corners = isBaneiRun ? "" : (r.positions_corners || "");
          const l3f = r.last_3f;
          const l3fRank = r.last_3f_rank;
          const l3fCls = l3fRank === 1 ? "text-emerald-600 font-bold" : l3fRank === 2 ? "text-blue-600 font-bold" : l3fRank === 3 ? "text-red-600 font-bold" : "";
          const ft = r.finish_time ?? r.finish_time_sec;
          const margin = r.margin;
          const devVal = r.speed_dev;
          // A案（2026-04-26）: clamp が -50 まで拡張されたためマイナスも数値表示
          // isFloorClamped は撤回。null のみ「—」表示
          const devGrd = devVal != null ? devGrade(devVal) : "";

          return (
            <div key={r.race_id ?? `${r.date}-${r.venue}-${r.race_no}-${i}`} className="bg-muted/30 rounded border border-border/50 px-2 py-1.5">
              <div className="flex items-center justify-between text-[12px]">
                <div className="flex items-center gap-1.5">
                  {url ? (
                    <a href={url} target="_blank" rel="noopener noreferrer"
                      className="text-blue-600 dark:text-blue-400 underline shrink-0">{dateShort}</a>
                  ) : (
                    <span className="shrink-0">{dateShort}</span>
                  )}
                  <span className="shrink-0">{r.venue || "—"}</span>
                  <span className="shrink-0">
                    {surf}{dist > 0 ? dist : ""}
                    <span className={`ml-0.5 ${condCls(cond)}`}>{cond}</span>
                  </span>
                </div>
                <span className={`shrink-0 font-bold text-[13px] ml-2 ${posCls(fp)}`}>
                  {fp != null && fp > 0 ? `${fp}着` : "—"}/{fc || "?"}
                  {pop != null && (
                    <span className="text-muted-foreground font-normal text-[11px] ml-0.5">({pop}人気)</span>
                  )}
                </span>
              </div>
              {cls && (
                <div className="text-[11px] text-muted-foreground mt-0.5 truncate">{cls}</div>
              )}
              <div className="flex items-center gap-2 text-[11px] mt-0.5 text-muted-foreground">
                <span className="tabular-nums">
                  {devVal != null ? (
                    // 偏差値を数値表示（マイナス含む）。null のみ「—」
                    <>
                      {devVal.toFixed(1)}
                      <span className={`ml-0.5 font-bold ${gradeCls(devGrd)}`}>({devGrd})</span>
                    </>
                  ) : "—"}
                </span>
                <span className="text-border">|</span>
                <span className="text-foreground max-w-[6.5em] truncate inline-block align-bottom" title={jk || "—"}>{jk || "—"}</span>
                <span className="text-border">|</span>
                <span className="tabular-nums">{corners || "—"}</span>
                <span className="text-border">|</span>
                <span className={`tabular-nums ${l3fCls}`}>
                  {l3f != null && l3f > 0 ? l3f.toFixed(1) : "—"}
                </span>
                <span className="text-border">|</span>
                <span className="tabular-nums text-foreground">{fmtTime(ft)}</span>
                <span className="tabular-nums">
                  {margin == null || fp === 1
                    ? "—"
                    : (margin > 0 ? `+${margin.toFixed(1)}` : margin.toFixed(1))}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------- 展開時の詳細パネル（既存実装維持） ----------
function CardDetail({ h }: { h: HorseData }) {
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

  const sire = (h as Record<string, unknown>).sire as string || "";
  const mgs = (h as Record<string, unknown>).maternal_grandsire as string || "";
  const sireGrade = (h as Record<string, unknown>).sire_grade as string || "";
  const mgsGrade = (h as Record<string, unknown>).mgs_grade as string || "";
  const jockeyGrade = h.jockey_grade || "";
  const trainerGrade = h.trainer_grade || "";
  const pastRuns = (h as Record<string, unknown>).past_3_runs as PastRunData[] || [];

  const sireTotal = (h as Record<string, unknown>).sire_total as number | undefined;
  const mgsTotal = (h as Record<string, unknown>).mgs_total as number | undefined;
  const sireGrd = sireGrade || (sireTotal != null && sireTotal > 0 ? devGrade(sireTotal) : "");
  const mgsGrd = mgsGrade || (mgsTotal != null && mgsTotal > 0 ? devGrade(mgsTotal) : "");

  const withGrade = (name: string, grade: string) => {
    if (!grade || grade === "—") return <>{name || "—"}</>;
    return <>{name || "—"}(<span className={gradeCls(grade)}>{grade}</span>)</>;
  };

  return (
    <div className="px-3 pb-3 space-y-2">
      {/* 騎手・調教師 */}
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-[12px]">
        <span>
          <span className="text-muted-foreground">騎手：</span>
          <strong>{withGrade(h.jockey || "", jockeyGrade)}</strong>
        </span>
        <span>
          <span className="text-muted-foreground">調教師：</span>
          <strong>{withGrade(h.trainer || "", trainerGrade)}</strong>
        </span>
      </div>
      {/* 父・母父 */}
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 text-[12px]">
        <span>
          <span className="text-muted-foreground">父：</span>
          <strong>{withGrade(sire, sireGrd)}</strong>
        </span>
        <span>
          <span className="text-muted-foreground">母父：</span>
          <strong>{withGrade(mgs, mgsGrd)}</strong>
        </span>
      </div>

      {/* 走破偏差値グラフ */}
      {histRuns && <HorseHistoryChart runs={histRuns} horseName={h.horse_name} isMobile />}
      {/* 前三走 */}
      <PastRunsMini runs={pastRuns} />

      {/* 調教偏差値グレード */}
      {(() => {
        const trnDev = (h as Record<string, unknown>).training_dev as number | undefined;
        if (!trnDev || trnDev <= 0) return null;
        const trnGrade = devGrade(trnDev);
        return (
          <div className="text-[12px]">
            <span className="text-muted-foreground">調教：</span>
            <span className={`font-bold ${gradeCls(trnGrade)}`}>{trnGrade}</span>
            <span className="text-muted-foreground ml-1 tabular-nums">{trnDev.toFixed(1)}</span>
          </div>
        );
      })()}

      {/* 調教 */}
      <TrainingSection records={(h as Record<string, unknown>).training_records as TrainingRecord[] || []} />

      {/* 厩舎コメント */}
      {(() => {
        const trRecs = (h as Record<string, unknown>).training_records as Array<Record<string, unknown>> | undefined;
        const stableComment = trRecs?.[0]?.stable_comment as string || "";
        const stableBullets = trRecs?.[0]?.stable_comment_bullets as string[] | undefined;
        if (!stableComment) return null;
        return (
          <div className="bg-muted/40 p-2 rounded-md text-[12px] leading-relaxed">
            <div className="font-bold text-muted-foreground mb-1">【厩舎コメント】</div>
            {(() => {
              // T-025 (2026-04-28): stableBullets / stableComment 両方を parseStableComment で統一処理
              // バックエンド paraphrase cache の prefix 残存・曖昧表現も全部除去
              const inputText = stableBullets && stableBullets.length > 0
                ? stableBullets.join('\n')
                : stableComment;
              const parsed = parseStableComment(inputText);
              if (parsed.length === 0) return <span>{stableComment}</span>;
              return (
                <ul className="space-y-0.5">
                  {parsed.map((b, i) => (
                    <li key={i} className="flex gap-1 items-start">
                      <span className="text-muted-foreground shrink-0">・</span>
                      <span>{b.text}</span>
                    </li>
                  ))}
                </ul>
              );
            })()}
          </div>
        );
      })()}
    </div>
  );
}

// ---------- メインコンポーネント ----------
export function HorseCardMobile({ horses, isBanei, dMarks, onDMarkSelect }: Props) {
  const [openNo, setOpenNo] = useState<number | null>(null);

  // 8軸の順位を全頭で事前計算
  const idxRanks: Record<string, Record<number, number>> = {};
  for (const def of INDEX_DEFS) {
    idxRanks[def.key] = calcRanks(horses, def.getValue);
  }
  const wpRanks = calcRanks(horses, (h) => h.win_prob || 0);
  const p2Ranks = calcRanks(horses, (h) => h.place2_prob || 0);
  const p3Ranks = calcRanks(horses, (h) => h.place3_prob || 0);

  // Plan-γ Phase 5: 絶対指数/相対指��� 切替トグル（グローバルモード・PC版と共有）
  const { mode, toggle } = useAbilityDisplayMode();
  const isRelativeMode = mode === "relative";

  // 相対指数データが1頭でも存在するか（トグルの有効化判定）
  const hasAnyRelativeData = useMemo(
    () => horses.some((h) => h.race_relative_dev != null),
    [horses],
  );

  return (
    <div className="divide-y divide-border">
      {/* 絶対指数 / 相対指数 切替トグル（能力軸のみ影響） */}
      <div className="flex items-center justify-end gap-2 px-2 py-1.5 bg-muted/30">
        <span className="text-[11px] text-muted-foreground">能力軸:</span>
        <button
          type="button"
          role="switch"
          aria-checked={isRelativeMode}
          aria-label="絶対指数 / 相対指数 切替"
          disabled={!hasAnyRelativeData}
          onClick={toggle}
          className={[
            "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[11px] font-semibold",
            "border transition-colors select-none",
            isRelativeMode
              ? "bg-sky-100 dark:bg-sky-900/40 border-sky-400 text-sky-700 dark:text-sky-300"
              : "bg-muted border-border text-muted-foreground",
            !hasAnyRelativeData ? "opacity-40 cursor-not-allowed" : "cursor-pointer",
          ].join(" ")}
          title={hasAnyRelativeData ? "絶対指数/相対指数を切替" : "相対指数データが���算出です"}
        >
          <span className={[
            "inline-block w-3 h-3 rounded-full border border-current transition-colors",
            isRelativeMode ? "bg-sky-500" : "bg-muted-foreground/30",
          ].join(" ")} />
          {isRelativeMode ? "相対" : "絶対"}
        </button>
      </div>
      {horses.map((h) => {
        const isOpen = openNo === h.horse_no;
        // h.mark には "tekipan" 等のキー or 既に "◎" 等のシンボルが入っている。
        // PC 版と同じ fallback 順序で MARK_SYMBOL → 元値 → "－" の順で解決。
        const markSym = MARK_SYMBOL[h.mark || ""] || h.mark || "－";
        const rowAccent = markRowAccent(h.mark);
        const isTekipan = h.mark === "tekipan";

        // 出走取消判定 (pred.json `is_scratched` フラグ優先)
        const isScratched = (h as Record<string, unknown>).is_scratched === true;

        // オッズ表示
        const realOdds = h.odds != null && h.odds > 0;
        const oddsStr = isScratched
          ? "取消"
          : realOdds
            ? `${Number(h.odds).toFixed(1)}倍`
            : h.predicted_tansho_odds != null
              ? `${h.predicted_tansho_odds.toFixed(1)}倍*`
              : "—";

        // 人気表示
        const popStr = isScratched
          ? ""
          : h.popularity != null && h.popularity > 0
            ? `${h.popularity}人気`
            : h.predicted_rank != null
              ? `${h.predicted_rank}位*`
              : "—";

        // 脚質短縮
        const runStyle = STYLE_SHORT[h.running_style || ""] || h.running_style || "—";
        const corners = (h as Record<string, unknown>).predicted_corners as string || "";
        const summary = generateHorseSummary(h);

        const wp = (h.win_prob || 0) * 100;
        const p2 = (h.place2_prob || 0) * 100;
        const p3 = (h.place3_prob || 0) * 100;
        const wpRank = wpRanks[h.horse_no];
        const p2Rank = p2Ranks[h.horse_no];
        const p3Rank = p3Ranks[h.horse_no];

        // 馬体重
        const horseWeightNode = h.horse_weight != null ? (
          <span className="flex items-baseline gap-0.5 shrink-0" aria-label="馬体重">
            <span className="tabular-nums text-foreground">{Number(h.horse_weight).toFixed(0)}kg</span>
            {h.weight_change != null && (
              <span className={`tabular-nums font-semibold ${
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
            key={h.horse_no}
            className={[
              isTekipan ? "shadow-[inset_2px_0_0_var(--brand-gold)]" : "",
            ].filter(Boolean).join(" ")}
          >
            {/* タップ可能なカード本体 */}
            <div
              className={`px-2 py-2 cursor-pointer active:bg-muted/50 transition-colors ${rowAccent}`}
              onClick={() => setOpenNo(isOpen ? null : h.horse_no)}
            >
              {/* ---- 行1: 馬番 | MY印 | AI印 | 馬名 | 単勝オッズ ---- */}
              <div className="flex items-center gap-1">
                {/* 馬番（枠色） */}
                <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                  {h.horse_no}
                </span>

                {/* MY印 */}
                <span className="shrink-0">
                  <DMarkBadge horseNo={h.horse_no} dMarks={dMarks} onSelect={onDMarkSelect} />
                </span>

                {/* AI印（総合） */}
                <span className={`text-sm leading-none shrink-0 font-bold ${markSym === "－" ? "text-muted-foreground/30" : markCls(markSym)}`}>
                  {markSym}
                </span>

                {/* 馬名 */}
                <span className={`text-sm font-bold truncate flex-1 ml-1 min-w-0 ${isScratched ? "line-through text-muted-foreground" : ""}`}>{h.horse_name}</span>

                {/* 単勝オッズ (取消時は赤字「取消」) */}
                <span className={`shrink-0 text-[12px] font-bold tabular-nums ml-1 ${isScratched ? "text-red-500 dark:text-red-400" : (h.popularity != null && h.popularity > 0 ? (rankCls(h.popularity) || "text-foreground") : "text-foreground")}`}>
                  {oddsStr}
                </span>
              </div>

              {/* ---- 行2: 性齢 | 騎手 | 斤量 | 人気 ---- */}
              <div className="flex items-center gap-1 mt-0.5 text-[11px]">
                <span className="text-muted-foreground whitespace-nowrap">
                  {h.sex || ""}{h.age || ""}
                </span>
                <span className="font-bold truncate flex-1 min-w-0">{h.jockey || "—"}</span>
                <span className="text-muted-foreground whitespace-nowrap">
                  {h.weight_kg != null ? `${Number(h.weight_kg).toFixed(1)}kg` : ""}
                </span>
                <span className={`ml-auto shrink-0 ${h.popularity != null && h.popularity > 0 ? (rankCls(h.popularity) || "text-muted-foreground") : "text-muted-foreground"}`}>
                  {popStr}
                </span>
              </div>

              {/* ---- 行3: 馬体重（取得済み時のみ） ---- */}
              {horseWeightNode && (
                <div className="flex items-center mt-0.5 text-[11px]">
                  {horseWeightNode}
                </div>
              )}

              {/* ---- セパレータ ---- */}
              <div className="border-t border-border/40 mt-1.5 mb-1" />

              {/* ---- 8軸縦並びセクション（1軸1行・印と指数が同行） ---- */}
              <div className="space-y-0.5">
                {INDEX_DEFS.map((def) => {
                  // バネイ競馬は展開軸をスキップ
                  if (isBanei && def.key === "pace") return null;

                  // Plan-γ Phase 5: 相対指数モード時は能力軸(abi)を race_relative_dev に差し替え
                  let val = def.getValue(h);
                  const isRelativeAbi = isRelativeMode && def.key === "abi";
                  const hasRelativeData = h.race_relative_dev != null;
                  const isFallback = isRelativeAbi && !hasRelativeData;
                  if (isRelativeAbi && hasRelativeData) {
                    val = h.race_relative_dev as number;
                  }

                  const rank = idxRanks[def.key]?.[h.horse_no] || 0;
                  const hasVal = val != null && val > 0;
                  // データなし（hasVal=false）の場合は印を「−」にする（全頭同率1位で◎固定になる問題を防ぐ）
                  const axMark = hasVal ? rankToAxisMark(rank) : "−";
                  const g = devGrade(val);

                  // 軸ラベル: 相対指数モードの能力軸は「相対」と表示
                  const displayLabel = isRelativeAbi ? "相対" : def.label;

                  return (
                    <div
                      key={def.key}
                      className="grid items-center gap-1 text-xs"
                      // 余白詰め: 軸名 44px / 印 18px / 指数 1fr / 順位 44px
                      style={{ gridTemplateColumns: "44px 18px 1fr 44px" }}
                    >
                      {/* 軸名（左寄せ・相対指数モード時は強調） */}
                      <span className={`text-[11px] truncate ${isRelativeAbi ? "text-sky-500 font-bold" : "text-muted-foreground"}`}>
                        {displayLabel}
                      </span>

                      {/* 印（中央・色付き） */}
                      <span className={`text-center text-base leading-none font-bold ${axMark === "−" ? "text-muted-foreground/30" : markCls(axMark)}`}>
                        {axMark}
                      </span>

                      {/* 指数（右寄せ・グレード文字+数値、例: A57 / S67） */}
                      <span className={`text-right font-mono tabular-nums font-bold ${hasVal ? gradeCls(g) : "text-muted-foreground/30"}`}>
                        {isFallback
                          ? <span className="text-[10px] text-amber-500 font-normal">計算中</span>
                          : hasVal
                            ? <><span className="mr-0.5">{g}</span>{val.toFixed(1)}</>
                            : "—"}
                      </span>

                      {/* 順位（小文字・muted） */}
                      <span className="text-right text-[10px] text-muted-foreground tabular-nums whitespace-nowrap">
                        {hasVal ? `(${rank}位)` : ""}
                      </span>
                    </div>
                  );
                })}
              </div>

              {/* ---- セパレータ ---- */}
              <div className="border-t border-border/40 mt-1.5 mb-1" />

              {/* ---- 下部: 脚質 | 通過順 | 三連率 | EV | 短評 ---- */}
              <div className="flex items-center flex-wrap gap-x-2 gap-y-0.5 text-[11px]">
                {/* 脚質 */}
                <span className={`shrink-0 font-bold ${rsColorCls(h.running_style || "")}`}>
                  {runStyle}
                </span>

                {/* 通過順（バネイは非表示） */}
                {!isBanei && corners && (
                  <span className="text-muted-foreground tabular-nums shrink-0">{corners}</span>
                )}

                {/* 三連率 */}
                <span className="flex items-baseline gap-0.5 shrink-0">
                  <span className="text-muted-foreground">勝</span>
                  <span className={`tabular-nums font-semibold ${rankCls(wpRank)}`}>{wp.toFixed(1)}%</span>
                </span>
                <span className="flex items-baseline gap-0.5 shrink-0">
                  <span className="text-muted-foreground">連</span>
                  <span className={`tabular-nums font-semibold ${rankCls(p2Rank)}`}>{p2.toFixed(1)}%</span>
                </span>
                <span className="flex items-baseline gap-0.5 shrink-0">
                  <span className="text-muted-foreground">複</span>
                  <span className={`tabular-nums font-semibold ${rankCls(p3Rank)}`}>{p3.toFixed(1)}%</span>
                </span>

                {/* EV */}
                {h.ev != null && (
                  <span className={`tabular-nums shrink-0 ${
                    h.ev >= 1.2 ? "text-emerald-600 font-bold" :
                    h.ev >= 1.0 ? "text-blue-600 font-bold" :
                    "text-muted-foreground"
                  }`}>
                    EV {h.ev.toFixed(2)}
                  </span>
                )}

                {/* 短評 */}
                {summary && (
                  <span className="text-muted-foreground italic truncate flex-1 min-w-0">
                    ★ {summary}
                  </span>
                )}
              </div>
            </div>

            {/* 展開時の詳細パネル（変更なし） */}
            {isOpen && (
              <div className="bg-muted/20 border-t border-border/50">
                <CardDetail h={h} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
