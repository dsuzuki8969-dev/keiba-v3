import React, { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { circledNum, devGrade, pastRunResultUrl } from "@/lib/constants";
import { paraphraseTrainingComment } from "@/lib/paraphrase";
import { parseStableComment } from "@/lib/parseStableComment";
import type { HorseData, PastRunData, TrainingRecord } from "./RaceDetailView";

interface Props {
  horses: HorseData[];
}

// 枠番背景色
const WAKU_BG: Record<number, string> = {
  1: "bg-white text-black border border-gray-300",
  2: "bg-black text-white",
  3: "bg-red-600 text-white",
  4: "bg-blue-600 text-white",
  5: "bg-yellow-400 text-black",
  6: "bg-green-600 text-white",
  7: "bg-orange-500 text-white",
  8: "bg-pink-400 text-white",
};

// 印スタイル（◉/◎=緑, ○/☆=青, ▲/×=赤, △=紫, ★=黒）
const MARK_COLORS: Record<string, string> = {
  "◉": "text-emerald-600",
  "◎": "text-emerald-600",
  "○": "text-blue-600",
  "▲": "text-red-600",
  "△": "text-purple-600",
  "★": "text-foreground",
  "☆": "text-blue-600",
  "×": "text-red-600",
};

// 脚質短縮
const STYLE_SHORT: Record<string, string> = {
  逃げ: "逃", 先行: "先", 差し: "差", 追込: "追",
};

// グレード色（6段階: SS=緑, S=青, A=赤, B=黒, C=灰, D=薄灰）
function gradeCls(g: string): string {
  if (!g || g === "—") return "text-muted-foreground";
  if (g === "SS") return "text-emerald-600 font-bold";
  if (g === "S") return "text-blue-600 font-bold";
  if (g === "A") return "text-red-600 font-bold";
  if (g === "B") return "text-foreground font-bold";
  if (g === "C") return "text-muted-foreground";
  return "text-muted-foreground/60";
}

// 順位色: 1位=緑, 2位=青, 3位=赤, 4位=紫, 他=黒
function rankCls(rank: number): string {
  if (rank === 1) return "text-emerald-600 font-bold";
  if (rank === 2) return "text-blue-600 font-bold";
  if (rank === 3) return "text-red-600 font-bold";
  if (rank === 4) return "text-purple-600 font-bold";
  return "";
}

// 順位計算（同値は同順位）
function calcRanks(horses: HorseData[], key: (h: HorseData) => number): Record<number, number> {
  const ranks: Record<number, number> = {};
  for (let i = 0; i < horses.length; i++) {
    const v = key(horses[i]);
    ranks[horses[i].horse_no] = horses.filter((h) => key(h) > v).length + 1;
  }
  return ranks;
}

// グレード文字列 → 推定偏差値（dev値がない既存データ用フォールバック）
function gradeToApproxDev(grade: string | undefined): number {
  if (!grade || grade === "—") return 50; // データなし→中央値（レーダーチャート崩壊防止）
  const clean = grade.replace(/[⁺⁻]/g, "");
  const map: Record<string, number> = {
    SS: 73, S: 66, A: 59, B: 53, C: 47, D: 40,
  };
  return map[clean] || 0;
}

// 指数の定義
interface IndexDef {
  label: string;
  key: string;
  getValue: (h: HorseData) => number;
}
const INDEX_DEFS: IndexDef[] = [
  { label: "総合", key: "composite", getValue: (h) => h.composite || 0 },
  { label: "能力", key: "ability", getValue: (h) => h.ability_total || 0 },
  { label: "展開", key: "pace", getValue: (h) => h.pace_total || 0 },
  { label: "適性", key: "course", getValue: (h) => h.course_total || 0 },
  { label: "騎手", key: "jockey", getValue: (h) => h.jockey_dev || gradeToApproxDev(h.jockey_grade) },
  { label: "調教師", key: "trainer", getValue: (h) => h.trainer_dev || gradeToApproxDev(h.trainer_grade) },
  { label: "血統", key: "bloodline", getValue: (h) => h.bloodline_dev || gradeToApproxDev(h.sire_grade) },
  { label: "追切", key: "training", getValue: (h) => h.training_dev ?? 50 },
];

// レーダーチャート用の軸（総合は除く）
const RADAR_AXES_ALL = INDEX_DEFS.slice(1); // 能力/展開/適性/騎手/調教師/血統/調教
const RADAR_AXES_BANEI = RADAR_AXES_ALL.filter((ax) => ax.key !== "pace"); // ばんえい: 展開除外

// 指数内訳の定義
interface BreakdownItem { label: string; value: number | string | null; unit?: string; }
function getBreakdown(key: string, h: HorseData): BreakdownItem[] {
  const g = (k: string) => (h as Record<string, unknown>)[k];
  const n = (k: string) => g(k) as number | null;
  const fmt = (v: number | null | undefined, d = 1) => v != null ? Number(v).toFixed(d) : "—";
  const fmtSign = (v: number | null | undefined, d = 1) => v != null ? (v >= 0 ? `+${Number(v).toFixed(d)}` : Number(v).toFixed(d)) : "—";
  switch (key) {
    case "composite": {
      // 総合 = 各指数 × 重み + 補正
      const items: BreakdownItem[] = [
        { label: "能力", value: fmt(h.ability_total), unit: `× 重み` },
        { label: "展開", value: fmt(h.pace_total), unit: `× 重み` },
        { label: "適性", value: fmt(h.course_total), unit: `× 重み` },
        { label: "騎手", value: fmt(h.jockey_dev), unit: `× 重み` },
        { label: "調教師", value: fmt(h.trainer_dev), unit: `× 重み` },
        { label: "血統", value: fmt(h.bloodline_dev), unit: `× 重み` },
        { label: "追切", value: fmt(n("training_dev")), unit: `好調→能力展開乗算` },
      ];
      const oc = n("odds_consistency_adj");
      if (oc != null && oc !== 0) items.push({ label: "オッズ整合", value: fmtSign(oc), unit: "pt" });
      const mc = n("ml_composite_adj");
      if (mc != null && mc !== 0) items.push({ label: "ML補正", value: fmtSign(mc), unit: "pt" });
      return items;
    }
    case "ability": return [
      { label: "最高(MAX)", value: fmt(n("ability_max")) },
      { label: "平均(WA)", value: fmt(n("ability_wa")) },
      { label: "α値", value: fmt(n("ability_alpha"), 2) },
      { label: "トレンド", value: (g("ability_trend") as string) || "—" },
      { label: "信頼度", value: (g("ability_reliability") as string) || "—" },
      { label: "クラス補正", value: fmtSign(n("ability_class_adj")), unit: "pt" },
      { label: "血統補正", value: fmtSign(n("ability_bloodline_adj")), unit: "pt" },
    ];
    case "pace": return [
      { label: "ベース", value: fmt(n("pace_base")) },
      { label: "末脚評価", value: fmtSign(n("pace_last3f_eval")), unit: "pt" },
      { label: "位置取り", value: fmtSign(n("pace_position_balance")), unit: "pt" },
      { label: "枠順バイアス", value: fmtSign(n("pace_gate_bias")), unit: "pt" },
      { label: "脚質相性", value: fmtSign(n("pace_course_style_bias")), unit: "pt" },
      { label: "騎手展開", value: fmtSign(n("pace_jockey")), unit: "pt" },
      { label: "軌跡方向", value: fmtSign(n("pace_trajectory")), unit: "pt" },
      { label: "通過順補正", value: fmtSign(n("pace_corner_adj")), unit: "pt" },
      { label: "推定脚質", value: (g("running_style") as string) || "—" },
    ];
    case "course": return [
      { label: "コース実績", value: fmtSign(n("course_record")), unit: "pt" },
      { label: "競馬場適性", value: fmtSign(n("course_venue_apt")), unit: "pt" },
      { label: "適性レベル", value: (g("course_venue_level") as string) || "—" },
      { label: "騎手コース", value: fmtSign(n("course_jockey")), unit: "pt" },
    ];
    case "jockey": {
      const jd = (g("jockey_detail_grades") || {}) as Record<string, string>;
      const items: BreakdownItem[] = [
        { label: "偏差値", value: fmt(h.jockey_dev) },
        { label: "基準50.0→", value: fmtSign(h.jockey_dev != null ? h.jockey_dev - 50 : null), unit: "pt" },
      ];
      const jLabels: Record<string, string> = { venue: "競馬場", similar_venue: "類似場", surface: "コース", distance: "距離", same_cond: "同条件", style: "脚質", gate: "枠" };
      for (const [k, l] of Object.entries(jLabels)) if (jd[k] && jd[k] !== "—") items.push({ label: l, value: jd[k] });
      return items;
    }
    case "trainer": {
      const td = (g("trainer_detail_grades") || {}) as Record<string, string>;
      const items: BreakdownItem[] = [
        { label: "偏差値", value: fmt(h.trainer_dev) },
        { label: "基準50.0→", value: fmtSign(h.trainer_dev != null ? h.trainer_dev - 50 : null), unit: "pt" },
      ];
      const tLabels: Record<string, string> = { venue: "競馬場", similar_venue: "類似場", surface: "コース", distance: "距離", same_cond: "同条件", style: "脚質", gate: "枠" };
      for (const [k, l] of Object.entries(tLabels)) if (td[k] && td[k] !== "—") items.push({ label: l, value: td[k] });
      return items;
    }
    case "bloodline": {
      const bd = (g("bloodline_detail_grades") || {}) as Record<string, string>;
      const items: BreakdownItem[] = [
        { label: "偏差値", value: fmt(h.bloodline_dev) },
        { label: "基準50.0→", value: fmtSign(h.bloodline_dev != null ? h.bloodline_dev - 50 : null), unit: "pt" },
        { label: "父", value: h.sire_grade || "—" },
        { label: "母父", value: h.mgs_grade || "—" },
      ];
      const bLabels: Record<string, string> = { venue: "競馬場", surface: "コース", distance: "距離", same_cond: "同条件", style: "脚質" };
      for (const [k, l] of Object.entries(bLabels)) if (bd[k] && bd[k] !== "—") items.push({ label: l, value: bd[k] });
      return items;
    }
    case "training": {
      const trDev = n("training_dev");
      return [
        { label: "偏差値", value: fmt(trDev) },
        { label: "基準50.0→", value: fmtSign(trDev != null ? trDev - 50 : null), unit: "pt" },
      ];
    }
    default: return [];
  }
}

// SVGレーダーチャート（白背景・灰グリッド・緑塗り）
// グリッドは 30/40/50/60/70/80 の6段階（ラベル付き）
function RadarChart({ h, isBanei }: { h: HorseData; isBanei?: boolean }) {
  const RADAR_AXES = isBanei ? RADAR_AXES_BANEI : RADAR_AXES_ALL;
  const cx = 160, cy = 160, R = 110;
  const N = RADAR_AXES.length;
  // 視覚レンジ: 20（中心）→ 100（外縁）で100スケールに対応
  const VMIN = 20, VMAX = 100;

  const point = (i: number, r: number): [number, number] => {
    const angle = (Math.PI * 2 * i) / N - Math.PI / 2;
    return [cx + r * Math.cos(angle), cy + r * Math.sin(angle)];
  };

  // 6段階グリッド
  // 8段階グリッド: 20(中心)→100(外縁)、10刻み
  const gridLevels = [1/8, 2/8, 3/8, 4/8, 5/8, 6/8, 7/8, 1.0];
  const gridValues = [30, 40, 50, 60, 70, 80, 90, 100];
  const gridPaths = gridLevels.map((lv) => {
    const pts = Array.from({ length: N }, (_, i) => point(i, R * lv));
    return pts.map((p, i) => `${i === 0 ? "M" : "L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ") + " Z";
  });

  const values = RADAR_AXES.map((ax) => {
    const raw = ax.getValue(h);
    // 20以下=0%, 80以上=100%
    return Math.max(0, Math.min(1, (raw - VMIN) / (VMAX - VMIN)));
  });
  const dataPts = values.map((v, i) => point(i, R * v));
  const dataPath = dataPts.map((p, i) => `${i === 0 ? "M" : "L"}${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ") + " Z";

  const labelPts = Array.from({ length: N }, (_, i) => point(i, R + 38));

  return (
    <svg viewBox="0 0 320 320" className="w-[280px] h-[280px] shrink-0">
      {/* 白背景 */}
      <rect x="0" y="0" width="320" height="320" fill="white" rx="4" />
      {/* グリッド線（灰色） */}
      {gridPaths.map((d, i) => (
        <path key={i} d={d} fill="none" stroke="#bbb" strokeWidth={i === gridLevels.length - 1 ? 1.5 : 0.8} />
      ))}
      {/* 軸線（灰色） */}
      {Array.from({ length: N }, (_, i) => {
        const [ex, ey] = point(i, R);
        return <line key={i} x1={cx} y1={cy} x2={ex} y2={ey} stroke="#bbb" strokeWidth={0.8} />;
      })}
      {/* グリッド数値ラベル（右上軸に沿って） */}
      {gridLevels.map((lv, i) => {
        const [gx, gy] = point(0, R * lv); // 上軸（12時方向）
        return (
          <text key={i} x={gx + 14} y={gy + 2} fontSize="9" fill="#999" textAnchor="start" dominantBaseline="middle">
            {gridValues[i]}
          </text>
        );
      })}
      {/* データ領域（緑） */}
      <path d={dataPath} fill="rgba(34, 197, 94, 0.35)" stroke="#16a34a" strokeWidth={2.5} strokeLinejoin="round" />
      {/* データ点 */}
      {dataPts.map((p, i) => (
        <circle key={i} cx={p[0]} cy={p[1]} r={4} fill="#16a34a" stroke="white" strokeWidth={1.5} />
      ))}
      {/* 軸ラベル + 数値 */}
      {RADAR_AXES.map((ax, i) => {
        const [lx, ly] = labelPts[i];
        const val = ax.getValue(h);
        return (
          <g key={i}>
            <text x={lx} y={ly - 6} textAnchor="middle" dominantBaseline="middle" fontSize="12" fontWeight="bold" fill="#333">
              {ax.label}
            </text>
            <text x={lx} y={ly + 9} textAnchor="middle" dominantBaseline="middle" fontSize="11" fill="#666">
              {val > 0 ? val.toFixed(1) : "—"}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

// タイム表示: 秒→分:秒.小数
function fmtTime(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return "—";
  const m = Math.floor(sec / 60);
  const s = sec - m * 60;
  return m > 0 ? `${m}:${s.toFixed(1).padStart(4, "0")}` : s.toFixed(1);
}

// 着順色
function posCls(pos: number | undefined): string {
  if (pos === 1) return "text-red-600 font-bold";
  if (pos === 2) return "text-blue-600 font-bold";
  if (pos === 3) return "text-emerald-600 font-bold";
  return "";
}

// 馬場状態色
function condCls(cond: string | undefined): string {
  if (cond === "重" || cond === "不良") return "text-blue-600";
  return "";
}

// 前三走テーブル
function PastRunsTable({ runs }: { runs: PastRunData[] }) {
  if (!runs || runs.length === 0) return null;
  return (
    <div>
      <div className="text-[13px] font-bold text-muted-foreground mb-1">前三走成績</div>
      <div className="overflow-x-auto">
        <table className="text-[13px] border-collapse w-full min-w-[600px]">
          <thead>
            <tr className="text-[10px] text-muted-foreground border-b border-border bg-muted/40">
              <th className="text-left py-1 px-1.5 font-normal">日付</th>
              <th className="text-left py-1 px-1.5 font-normal">場</th>
              <th className="text-left py-1 px-1.5 font-normal">コース</th>
              <th className="text-left py-1 px-1.5 font-normal">クラス</th>
              <th className="text-center py-1 px-1.5 font-normal">着/頭</th>
              <th className="text-center py-1 px-1.5 font-normal">偏差値</th>
              <th className="text-left py-1 px-1.5 font-normal">騎手</th>
              <th className="text-center py-1 px-1.5 font-normal">通過</th>
              <th className="text-right py-1 px-1.5 font-normal">上3F</th>
              <th className="text-right py-1 px-1.5 font-normal">タイム</th>
              <th className="text-right py-1 px-1.5 font-normal">着差</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((r, i) => {
              const dateShort = r.date ? r.date.slice(2).replace(/-/g, "/") : "—";
              const surf = r.surface === "ダート" ? "ダ" : r.surface === "芝" ? "芝" : (r.surface || "");
              const dist = r.distance || 0;
              const cond = r.condition || "";
              const cls = r.class || "";
              const fc = r.field_count || 0;
              const fp = r.finish_pos;
              const jk = (r.jockey || "").slice(0, 5);
              const corners = r.positions_corners || "";
              const l3f = r.last_3f;
              const l3fRank = r.last_3f_rank;
              const l3fCls = l3fRank === 1 ? "text-emerald-600 font-bold" : l3fRank === 2 ? "text-blue-600 font-bold" : l3fRank === 3 ? "text-red-600 font-bold" : "";
              const ft = r.finish_time ?? r.finish_time_sec;
              const margin = r.margin;
              const devVal = r.speed_dev;
              // A案（2026-04-26）: clamp が -50 まで拡張されたためマイナスも数値表示
              // isFloorClamped は撤回。null のみ「—」表示
              const dg = devVal != null ? devGrade(devVal) : "—";

              return (
                <tr key={i} className="border-b border-border/30 hover:bg-muted/20">
                  <td className="py-1 px-1.5 whitespace-nowrap">
                    {(r.race_id || r.result_cname || r.race_no) ? (
                      <a href={pastRunResultUrl(r.race_id, r.date, r.venue, r.result_cname, r.race_no)}
                        target="_blank" rel="noopener noreferrer"
                        className="text-blue-600 hover:underline"
                        title="レース結果を見る">{dateShort}</a>
                    ) : dateShort}
                  </td>
                  <td className="py-1 px-1.5 whitespace-nowrap">{r.venue || "—"}</td>
                  <td className="py-1 px-1.5 whitespace-nowrap">
                    {surf}{dist > 0 ? dist : ""}
                    <span className={`ml-0.5 ${condCls(cond)}`}>{cond}</span>
                  </td>
                  <td className="py-1 px-1.5 max-w-[120px] truncate" title={cls}>{cls || "—"}</td>
                  <td className={`text-center py-1 px-1.5 ${posCls(fp)}`}>
                    {fp != null ? `${fp}着` : "—"}/{fc || "?"}
                  </td>
                  <td className="text-center py-1 px-1.5 whitespace-nowrap tabular-nums">
                    {devVal != null ? (
                      // 偏差値を数値表示（マイナス含む）。null のみ「—」
                      <>
                        {devVal.toFixed(1)}
                        <span className={`ml-0.5 ${gradeCls(dg)}`}>({dg})</span>
                      </>
                    ) : "—"}
                  </td>
                  <td className="py-1 px-1.5 whitespace-nowrap">{jk || "—"}</td>
                  <td className="text-center py-1 px-1.5 text-[11px]">{corners || "—"}</td>
                  <td className={`text-right py-1 px-1.5 tabular-nums ${l3fCls}`}>{l3f != null ? l3f.toFixed(1) : "—"}</td>
                  <td className="text-right py-1 px-1.5 tabular-nums">{fmtTime(ft)}</td>
                  <td className="text-right py-1 px-1.5 tabular-nums">
                    {margin != null && Math.abs(margin) < 15
                      ? (margin >= 0 ? `+${margin.toFixed(1)}` : margin.toFixed(1))
                      : "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// 調教強度ラベルの色（中立化: 主観を避け一律の見た目で表示）
// 「一杯に追う」「強めに追う」等の強度判定は本来それ自体で評価すべき事象ではないため、
// 追い方ラベルでの色塗りは撤廃。時計（スプリット秒数）の絶対値でのみ好悪を表現する。
function getIntensityCls(_label: string): string {
  return "text-foreground";
}

// 時計の優秀さ判定
// 「素晴らしい時計」= 緑太字 / 「惜しい時計（+0.5秒以内）」= 青太字
// コース種別（坂路 vs 芝・ウッドチップ等）で閾値を切り替える
// 坂路表記: 栗東=「栗坂」, 美浦=「美坂」, フル表記「坂路」すべて対応
function isSlopeCourse(location: string): boolean {
  if (!location) return false;
  return location.includes("坂");
}
// 絶対値閾値（秒）— マスター指定
const TIME_THRESHOLDS_FLAT: Record<string, number> = {
  "1F": 12.0,
  "3F": 36.5,
  "4F": 50.0,
  "5F": 65.0,
};
const TIME_THRESHOLDS_SLOPE: Record<string, number> = {
  "1F": 12.5,
  "3F": 37.5,
  "4F": 52.0,
};
// 緑太字 / 青太字 / 通常 の 3 段階を返す
function getSplitCls(label: string, val: number, location: string): string {
  const table = isSlopeCourse(location) ? TIME_THRESHOLDS_SLOPE : TIME_THRESHOLDS_FLAT;
  const excellent = table[label];
  if (excellent == null) return "";
  if (val <= excellent) return "text-emerald-600 font-bold";
  if (val <= excellent + 0.5) return "text-blue-600 font-bold";
  return "";
}

// メートル→ハロン変換ラベル
const METER_TO_F: Record<string, string> = {
  "1200": "6F", "1000": "5F", "800": "4F", "600": "3F", "400": "2F", "200": "1F",
};
// splitsを距離降順に並べて取得（メートルキー/ハロンキー両対応）
function getSplitEntries(splits: Record<string, number>): { label: string; val: number }[] {
  const entries: { dist: number; label: string; val: number }[] = [];
  for (const [k, v] of Object.entries(splits)) {
    if (v == null) continue;
    const numKey = String(k);
    // ハロンキー ("5F" 等)
    const fMatch = numKey.match(/^(\d+)F$/);
    if (fMatch) {
      entries.push({ dist: parseInt(fMatch[1]) * 200, label: numKey, val: v });
      continue;
    }
    // メートルキー (1000, 800 等)
    const dist = parseInt(numKey);
    if (!isNaN(dist) && dist > 0) {
      entries.push({ dist, label: METER_TO_F[numKey] || `${dist}m`, val: v });
    }
  }
  // 距離降順（長い方から短い方へ）
  entries.sort((a, b) => b.dist - a.dist);
  return entries;
}

// 調教セクション（競馬ブック形式完全トレース）
export function TrainingSection({ records }: { records: TrainingRecord[] }) {
  if (!records || records.length === 0) return null;

  // 最初のレコードのコメントを総合評価として表示（著作権対応でパラフレーズ）
  // マスター指示 (2026-04-30):
  //   - 「強度に記載がなければ何も書かない」 → 完コピ回避 + LLM 幻覚回避が真意
  //   - paraphrase.ts の SAFE_MAP で辞書未登録時は空 "" を返す → 既に達成
  //   - 強度ラベル不問で records[0].comment があれば paraphrase 試行 (元データあれば書く)
  const summaryComment = paraphraseTrainingComment(records[0]?.comment || "");

  return (
    <div>
      {/* 総合評価コメント（見出しは重複のため省略） */}
      {summaryComment && (
        <div className="text-base mb-1 text-foreground">{summaryComment}</div>
      )}
      {/* 各レコード（競馬ブック形式: ヘッダー行 → タイム行 → 併せ馬コメント） */}
      <div>
        {records.map((rec, i) => {
          let dateShort = rec.date || "—";
          if (dateShort.includes("-") && dateShort.length >= 10) {
            const p = dateShort.split("-");
            dateShort = `${parseInt(p[1])}/${parseInt(p[2])}`;
          }
          const location = rec.venue || rec.course || "";
          const trackCond = rec.track_condition || "";
          const rider = rec.rider || "";
          const splitEntries = getSplitEntries(rec.splits);
          const intensityCls = getIntensityCls(rec.intensity_label);
          // 強度ラベル: "通常" は非表示
          const intensityText = rec.intensity_label === "通常" ? "" : rec.intensity_label;
          // 2件目以降のコメントは併せ馬情報（レコード直下に表示）
          // ※併せ馬コメントは馬名入りの事実情報が多いためパラフレーズはせず、
          //   純粋短評のみ辞書置換（馬名入りはそのまま表示）
          const pairCommentRaw = i > 0 ? rec.comment || "" : "";
          const pairComment = pairCommentRaw
            ? paraphraseTrainingComment(pairCommentRaw)
            : "";

          // lap_count の整形: 「1回」「［５］」「［６］」のような周回/折返しのみ採用。
          // DB ソースによっては「7F 98.0」のような 1400m 通過タイム文字列が混入するため非表示。
          // （splits と重複するノイズ情報で、マスター要望により除去）
          const rawLap = rec.lap_count || "";
          const cleanLap = /^\s*\d+F\s/.test(rawLap) ? "" : rawLap;
          // タイム行を出すかどうか（splits が 1つ以上 or 整形後 lap がある場合のみ）
          const hasTimeRow = splitEntries.length > 0 || !!cleanLap;

          return (
            <div
              key={i}
              className={i > 0 ? "border-t border-border/70 leading-tight" : "leading-tight"}
            >
              {/* ヘッダー行: 乗り手 日付 コース 馬場 ... 強度ラベル */}
              <div className="flex items-baseline text-base">
                <span className="text-sm text-muted-foreground shrink-0 w-[60px]">
                  {i === 0 ? "(前回)" : rider || ""}
                </span>
                <span className="shrink-0 whitespace-nowrap w-[44px] text-right mr-1.5">
                  {dateShort}
                </span>
                <span className="shrink-0 whitespace-nowrap mr-1" title={location}>
                  {location}
                </span>
                {trackCond && (
                  <span className="shrink-0 whitespace-nowrap mr-1">{trackCond}</span>
                )}
                <span className={`ml-auto shrink-0 text-base ${intensityCls}`}>
                  {intensityText}
                </span>
              </div>
              {/* タイム行: スプリットをタブ形式で表示 + 周回数
                  ・絶対値閾値クリア → 緑太字（素晴らしい）
                  ・閾値 +0.5秒以内   → 青太字（惜しい）
                  ・閾値対象外 or 遅い → 通常（末尾1Fだけは従来通り太字）
                  ※ splits も lap も無ければ行自体を描画せず詰める */}
              {hasTimeRow && (
                <div className="flex items-baseline text-base tabular-nums pl-[60px]">
                  {splitEntries.map((s, j) => {
                    const evalCls = getSplitCls(s.label, s.val, location);
                    // 評価クラス優先。該当なしなら末尾1Fだけ太字（従来挙動維持）
                    const baseBold = j === splitEntries.length - 1 ? "font-bold" : "";
                    const cls = evalCls || baseBold;
                    return (
                      <span key={j} className={`w-[56px] text-right ${cls}`} title={`${s.label} ${s.val.toFixed(1)}秒`}>
                        {s.val.toFixed(1)}
                      </span>
                    );
                  })}
                  {cleanLap && (
                    <span className="w-[40px] text-right text-muted-foreground ml-1">{cleanLap}</span>
                  )}
                </div>
              )}
              {/* 併せ馬コメント（2件目以降、コメントがある場合のみ） */}
              {pairComment && (
                <div className="text-[15px] text-muted-foreground pl-[60px]">
                  {pairComment}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

export function HorseDiagnosis({ horses }: Props) {
  const [openSet, setOpenSet] = useState<Set<number>>(new Set());

  const sorted = [...horses].sort((a, b) => (a.horse_no || 0) - (b.horse_no || 0));

  // 全頭診断は常に表示（短評廃止後もデータがあれば表示）
  if (sorted.length === 0) return null;

  // 全指数の順位を事前計算
  const allRanks: Record<string, Record<number, number>> = {};
  for (const def of INDEX_DEFS) {
    allRanks[def.key] = calcRanks(sorted, def.getValue);
  }

  const wpRanks = calcRanks(sorted, (h) => h.win_prob || 0);
  const p2Ranks = calcRanks(sorted, (h) => h.place2_prob || 0);
  const p3Ranks = calcRanks(sorted, (h) => h.place3_prob || 0);

  const toggle = (no: number) => {
    setOpenSet((prev) => {
      const next = new Set(prev);
      if (next.has(no)) next.delete(no);
      else next.add(no);
      return next;
    });
  };

  return (
    <PremiumCard variant="default" padding="none">
      <div className="px-4 pt-3 pb-1 flex items-center justify-between">
        <h3 className="heading-section text-base">全頭診断</h3>
        <span className="text-xs text-muted-foreground">クリックで詳細展開</span>
      </div>
      <div className="p-0">
        {sorted.map((h) => {
          const no = h.horse_no;
          const isOpen = openSet.has(no);
          const mark = h.mark || "";
          const markColor = MARK_COLORS[mark] || "";

          // オッズ表示
          const realOdds = h.odds != null && h.odds > 0;
          const oddsVal = realOdds ? h.odds : h.predicted_tansho_odds;
          const oddsStr = oddsVal != null ? `${oddsVal.toFixed(1)}` : "—";
          const popStr = h.popularity != null ? `(${h.popularity}人気)` : "";

          return (
            <div key={no} className="border-t">
              {/* ===== サマリー行 ===== */}
              <div
                className="px-3 py-2 cursor-pointer hover:bg-muted/50 select-none flex items-center gap-1.5"
                onClick={() => toggle(no)}
              >
                <span className={`w-5 h-5 flex items-center justify-center rounded-sm text-[11px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                  {h.gate_no as number}
                </span>
                <span className="text-sm font-bold w-5 text-center shrink-0">{circledNum(no)}</span>
                {mark && (
                  <span className={`${markColor} font-bold text-lg min-w-[18px] text-center leading-none`}>{mark}</span>
                )}
                <span className="font-bold text-sm truncate max-w-[7em]">{h.horse_name}</span>
                <span className="text-[11px] text-muted-foreground shrink-0">{h.sex}{h.age}</span>
                <span className="text-[11px] text-muted-foreground shrink-0">
                  {h.weight_kg != null ? `${Number(h.weight_kg).toFixed(0)}kg` : ""}
                </span>
                <span className="text-[11px] font-semibold shrink-0">
                  {STYLE_SHORT[h.running_style || ""] || h.running_style || ""}
                </span>
                <span className="text-[11px] shrink-0">
                  {oddsStr}倍{popStr}
                </span>
                <span className="ml-auto text-muted-foreground text-xs shrink-0">{isOpen ? "▲" : "▼"}</span>
              </div>

              {/* ===== 展開部分 ===== */}
              {isOpen && (
                <DetailBody
                  h={h}
                  allRanks={allRanks}
                  wpRank={wpRanks[no]}
                  p2Rank={p2Ranks[no]}
                  p3Rank={p3Ranks[no]}
                />
              )}
            </div>
          );
        })}
      </div>
    </PremiumCard>
  );
}

// 指数内訳パネル（HorseTableからも使用）
export function IndexBreakdown({ indexKey, h }: { indexKey: string; h: HorseData }) {
  const items = getBreakdown(indexKey, h);
  if (items.length === 0) return null;
  return (
    <tr>
      <td colSpan={4} className="p-0">
        <div className="bg-muted/60 px-4 py-1.5 text-[11px] grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-0.5">
          {items.map((it, i) => (
            <div key={i} className="flex justify-between">
              <span className="text-muted-foreground">{it.label}</span>
              <span className="font-mono tabular-nums">{it.value}{it.unit ? <span className="text-muted-foreground ml-0.5">{it.unit}</span> : ""}</span>
            </div>
          ))}
        </div>
      </td>
    </tr>
  );
}

// 詳細展開パネル
function DetailBody({ h, allRanks, wpRank, p2Rank, p3Rank }: {
  h: HorseData;
  allRanks: Record<string, Record<number, number>>;
  wpRank: number;
  p2Rank: number;
  p3Rank: number;
}) {
  const no = h.horse_no;
  const [expandedIdx, setExpandedIdx] = useState<string | null>(null);

  const sire = (h as Record<string, unknown>).sire as string || "";
  const mgs = (h as Record<string, unknown>).maternal_grandsire as string || "";

  const wp = ((h.win_prob || 0) * 100).toFixed(1);
  const p2 = ((h.place2_prob || 0) * 100).toFixed(1);
  const p3 = ((h.place3_prob || 0) * 100).toFixed(1);

  return (
    <div className="px-4 pb-4 bg-muted/30 space-y-2">
      {/* サブ行: 騎手・調教師・父・母父 */}
      <div className="flex flex-wrap gap-x-4 gap-y-0.5 text-[13px] pt-2">
        <span><span className="text-muted-foreground">騎手：</span><strong>{h.jockey || "—"}</strong></span>
        <span><span className="text-muted-foreground">調教師：</span><strong>{h.trainer || "—"}</strong></span>
        <span><span className="text-muted-foreground">父：</span><strong>{sire || "—"}</strong></span>
        <span><span className="text-muted-foreground">母父：</span><strong>{mgs || "—"}</strong></span>
      </div>

      {/* 確率行 */}
      <div className="flex gap-3 text-[13px]">
        <span>
          <span className="text-muted-foreground">勝率</span>
          <strong className={rankCls(wpRank)}> {wp}%</strong>
          <span className="text-[11px] text-muted-foreground">({wpRank}位)</span>
        </span>
        <span>
          <span className="text-muted-foreground">連対率</span>
          <strong className={rankCls(p2Rank)}> {p2}%</strong>
          <span className="text-[11px] text-muted-foreground">({p2Rank}位)</span>
        </span>
        <span>
          <span className="text-muted-foreground">複勝率</span>
          <strong className={rankCls(p3Rank)}> {p3}%</strong>
          <span className="text-[11px] text-muted-foreground">({p3Rank}位)</span>
        </span>
      </div>

      {/* 指数テーブル + レーダーチャート（スマホ:縦並び / PC:横並び） */}
      <div className="flex flex-col sm:flex-row gap-3 items-start">
        {/* 指数テーブル */}
        <table className="text-[13px] border-collapse shrink-0 w-full sm:w-auto">
          <thead>
            <tr className="text-[11px] text-muted-foreground border-b border-border">
              <th className="text-left py-1 pr-4 font-normal">指数</th>
              <th className="text-center py-1 px-3 font-normal">評価</th>
              <th className="text-right py-1 px-3 font-normal">数値</th>
              <th className="text-right py-1 pl-3 font-normal">順位</th>
            </tr>
          </thead>
          <tbody>
            {INDEX_DEFS.map((def) => {
              const val = def.getValue(h);
              const grade = devGrade(val);
              const rank = allRanks[def.key]?.[no] || 0;
              const isTotal = def.key === "composite";
              const isExpanded = expandedIdx === def.key;
              return (
                <React.Fragment key={def.key}>
                  <tr
                    className={`border-b border-border/30 cursor-pointer hover:bg-blue-50 ${isTotal ? "bg-muted/50" : ""} ${isExpanded ? "bg-blue-50/70" : ""}`}
                    onClick={() => setExpandedIdx(isExpanded ? null : def.key)}
                    title="クリックで内訳表示"
                  >
                    <td className={`py-1 pr-4 ${isTotal ? "font-bold" : ""}`}>
                      <span className="text-[9px] text-muted-foreground mr-0.5">{isExpanded ? "▼" : "▶"}</span>
                      {def.label}
                    </td>
                    <td className={`text-center py-1 px-3 ${gradeCls(grade)}`}>{val > 0 ? grade : "—"}</td>
                    <td className="text-right py-1 px-3 tabular-nums">{val > 0 ? val.toFixed(1) : "—"}</td>
                    <td className={`text-right py-1 pl-3 ${rankCls(rank)}`}>{val > 0 ? `${rank}位` : "—"}</td>
                  </tr>
                  {isExpanded && <IndexBreakdown indexKey={def.key} h={h} />}
                </React.Fragment>
              );
            })}
          </tbody>
        </table>

        {/* レーダーチャート（スマホでは中央寄せ） */}
        <div className="self-center sm:self-start">
          <RadarChart h={h} />
        </div>
      </div>

      {/* 前三走成績 */}
      <PastRunsTable runs={(h as Record<string, unknown>).past_3_runs as PastRunData[] || []} />

      {/* 調教 */}
      <TrainingSection records={(h as Record<string, unknown>).training_records as TrainingRecord[] || []} />

      {/* 厩舎コメント（競馬ブック厩舎の話ページ。LLMパラフレーズ済み bullets 優先、なければフロント側パーサで箇条書き化） */}
      {(() => {
        const trRecs = (h as Record<string, unknown>).training_records as Array<Record<string, unknown>> | undefined;
        const stableComment = trRecs?.[0]?.stable_comment as string || "";
        const stableBullets = trRecs?.[0]?.stable_comment_bullets as string[] | undefined;
        if (!stableComment) return null;
        return (
          <div className="bg-background p-3 rounded-md border text-[13px] leading-relaxed">
            <div className="font-bold text-muted-foreground mb-1">【厩舎コメント】</div>
            {(() => {
              // T-025 (2026-04-28): stableBullets / stableComment 両方を parseStableComment で統一処理
              const inputText = stableBullets && stableBullets.length > 0
                ? stableBullets.join('\n')
                : stableComment;
              const parsed = parseStableComment(inputText);
              if (parsed.length === 0) {
                // パース結果が0件のみ原文表示（極短文等）
                return <span>{stableComment}</span>;
              }
              return (
                <ul className="space-y-1">
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
        );
      })()}
    </div>
  );
}
