import { useMemo } from "react";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { CourseMapSVG } from "./CourseMapSVG";
import { useCourseStats } from "@/api/hooks";
import { VENUE_NAME_TO_CODE, fmtRaceTime, confColorClass } from "@/lib/constants";
import type { HorseData } from "./RaceDetailView";

// 枠番背景色（HorseDiagnosis.tsx と同じ）
const WAKU_BG: Record<number, string> = {
  1: "bg-white text-black border border-gray-400",
  2: "bg-black text-white",
  3: "bg-red-600 text-white",
  4: "bg-blue-600 text-white",
  5: "bg-yellow-400 text-black",
  6: "bg-green-600 text-white",
  7: "bg-orange-500 text-white",
  8: "bg-pink-400 text-white",
};

// 枠番を算出（馬番→枠番）
function gateGroup(gateNo: number | undefined, horseNo: number, fieldCount: number): number {
  if (gateNo && gateNo >= 1 && gateNo <= 8) return gateNo;
  if (fieldCount <= 8) return horseNo;
  return Math.min(8, Math.ceil((horseNo / fieldCount) * 8));
}

// --- 型定義 ---

interface HorsePosition {
  horseNo: number;
  horseName: string;
  gate: number;
  x: number; // 0-100 (%) — 横方向: 前後位置
  y: number; // 0-100 (%) — 縦方向: 内外位置
  stretch?: "big" | "normal" | "small" | "fading";
  front3fSec?: number;   // 予測前半3F（秒）
  startDiffSec?: number; // 前半600mでの先頭との秒差
  cruxDiffSec?: number;  // 最終コーナーでの先頭との秒差
  last3fSec?: number;    // 予測上がり3F（秒）
  finishDiffSec?: number; // ゴール前での先頭との予測走破タイム差（秒）
  last3fRank?: number;    // 予測上がり3F順位（1=最速）
  posChange?: "up" | "down" | "same"; // 最終コーナー: 前半600mからの位置変化
}

type TabMode = "course" | "start" | "crux" | "finish";

interface RaceInfo {
  final_formation?: Record<string, number[]>;
  field_count?: number;
  direction?: string; // "右" | "左"
  surface?: string;   // "芝" | "ダート"
  distance?: number;  // レース距離（m）
  inside_outside?: string; // 内外回り判定 "外" | "内" | "" — Phase 1B/C で追加
  first_corner_m?: number; // スタート〜初角距離（m）
  // 残り600m地点データ（コース構造による展開図調整）
  l3f_straight_pct?: number;  // 残り600mの直線比率 (0.0-1.0)
  l3f_corners?: number;       // 残り600mのコーナー数
  l3f_elevation?: number;     // 残り600m区間の高低差(m)
  l3f_hill_start?: number;    // ゴール前何mから急坂
  straight_m?: number;        // 直線距離(m)
}

// --- 走行ライン推定 ---
type RunningLine = "inner" | "mid-inner" | "middle" | "mid-outer" | "outer";

// グリッド定数
const GRID_STEP = 0.15; // 秒 = 一馬身差
const CONTAINER_H = 340; // px固定
const Y_MIN = 12; // 内ラチ下 (%)
const Y_MAX = 88; // 外ラチ上 (%)

// ゾーン⇔インデックス変換（5等分: 内/内中/中/中外/外）
const LINE_INDEX: Record<RunningLine, number> = {
  "inner": 0, "mid-inner": 1, "middle": 2, "mid-outer": 3, "outer": 4,
};

// --- グリッドユーティリティ ---

function calcGridX(
  snappedDiff: number,
  maxDiff: number,
  direction: string,
): number {
  const xStart = 10;
  const xEnd = 92;
  const ratio = maxDiff > 0 ? snappedDiff / maxDiff : 0;
  // 右回り: 先頭=左端(ゴール方向←), 左回り: 先頭=右端(ゴール方向→)
  return direction === "右"
    ? xStart + ratio * (xEnd - xStart)
    : xEnd - ratio * (xEnd - xStart);
}

function zoneToY(zoneIndex: number): number {
  return Y_MIN + (zoneIndex + 0.5) * (Y_MAX - Y_MIN) / 5;
}

interface GridCell {
  col: number;       // X軸: スナップ後秒差 / GRID_STEP (整数)
  row: number;       // Y軸: ゾーンインデックス (0-4)
  idx: number;       // 元の配列インデックス
}

function resolveCollisions(cells: GridCell[], horseNos?: number[]): GridCell[] {
  // 同一col内で衝突する馬群をまとめて処理
  const colGroups = new Map<number, GridCell[]>();
  for (const cell of cells) {
    const group = colGroups.get(cell.col) || [];
    group.push(cell);
    colGroups.set(cell.col, group);
  }
  for (const [, group] of colGroups) {
    if (group.length <= 1) continue;
    // gate順ソート（馬番若い=内側）
    group.sort((a, b) => {
      const noA = horseNos ? horseNos[a.idx] : a.idx;
      const noB = horseNos ? horseNos[b.idx] : b.idx;
      return noA - noB;
    });
    // 希望rowの中央値を基準に配置
    const avgRow = group.reduce((s, c) => s + c.row, 0) / group.length;
    const startRow = Math.max(0, Math.min(4 - group.length + 1, Math.round(avgRow - (group.length - 1) / 2)));
    for (let i = 0; i < group.length; i++) {
      group[i].row = Math.min(4, startRow + i);
    }
  }

  // 衝突チェック（異なるcol間で同じマスになるケース対応）
  const occupied = new Map<string, number>();
  const pending: GridCell[] = [];
  for (const cell of cells) {
    const key = `${cell.col},${cell.row}`;
    if (!occupied.has(key)) {
      occupied.set(key, cell.idx);
    } else {
      pending.push(cell);
    }
  }
  for (const cell of pending) {
    let placed = false;
    for (let offset = 1; offset <= 4 && !placed; offset++) {
      for (const dir of [1, -1]) {
        const newRow = cell.row + offset * dir;
        if (newRow < 0 || newRow > 4) continue;
        const key = `${cell.col},${newRow}`;
        if (!occupied.has(key)) {
          cell.row = newRow;
          occupied.set(key, cell.idx);
          placed = true;
          break;
        }
      }
    }
    // 全5ゾーン満杯 → col方向にずらす
    if (!placed) {
      for (const colOff of [1, -1, 2, -2]) {
        for (let r = 0; r <= 4; r++) {
          const key = `${cell.col + colOff},${r}`;
          if (!occupied.has(key)) {
            cell.col += colOff;
            cell.row = r;
            occupied.set(key, cell.idx);
            placed = true;
            break;
          }
        }
        if (placed) break;
      }
    }
    if (!placed) {
      const key = `${cell.col},${cell.row}`;
      occupied.set(key, cell.idx);
    }
  }
  return cells;
}

// ====================================================================
// 前半600m地点のコース取り推定
// コースの構造（600mが1角の前か後か）で全く違う隊列になる
// ====================================================================
function estimateRunningLineStart(
  horse: HorseData,
  _N: number,
  firstCornerM: number,
  raceDist: number,
): RunningLine {
  const style = horse.running_style || "";
  const gate = horse.gate_no ?? 4;
  // 600m地点がコーナーのどこにあるか判定
  // firstCornerM: スタート〜1角距離(m)
  const is600mBeforeCorner = firstCornerM > 500; // 京都芝1600(712m), 京都芝1800(912m)等
  const is600mNearCorner = firstCornerM > 400 && firstCornerM <= 500;
  // 大半のコースは firstCornerM < 400 → 600m地点は1角通過後

  if (is600mBeforeCorner) {
    // ── 600mがコーナー前: まだ直線を走っている ──
    // 枠順の影響が非常に大きい。内枠は内、外枠は外のまま
    // 逃げ馬はハナを主張するが、コーナーがまだ先なので無理に内に切り込まない
    if (style === "逃げ") {
      // 逃げはどの枠でも前に行くが、内外位置は枠なり
      if (gate <= 3) return "inner";
      if (gate <= 5) return "mid-inner";
      return "middle"; // 外枠の逃げはまだ内に入れていない
    }
    if (style === "先行") {
      if (gate <= 2) return "inner";
      if (gate <= 4) return "mid-inner";
      if (gate <= 6) return "middle";
      return "mid-outer";
    }
    // 差し・追込: 枠なり
    if (gate <= 2) return "inner";
    if (gate <= 4) return "mid-inner";
    if (gate <= 6) return "middle";
    if (gate <= 7) return "mid-outer";
    return "outer";
  }

  if (is600mNearCorner) {
    // ── 600mがコーナー付近: 各馬がポジションを争っている最中 ──
    // 逃げ・先行は内に切り込もうとしているが、まだ完全には入り切れていない
    if (style === "逃げ") {
      if (gate <= 4) return "inner";
      return "mid-inner"; // 外枠逃げはまだ内に入り切れていない
    }
    if (style === "先行") {
      if (gate <= 3) return "inner";
      if (gate <= 5) return "mid-inner";
      return "middle";
    }
    // 差し・追込: 枠なりだが、やや内寄りに
    if (gate <= 2) return "inner";
    if (gate <= 4) return "mid-inner";
    if (gate <= 6) return "middle";
    return "mid-outer";
  }

  // ── 600mがコーナー後（大半のコース）: 隊列がほぼ確定 ──
  // 逃げ・先行は内ラチ沿いに入り込んでいる
  // 差し・追込は枠なりだが、距離が長いほど内目に寄れる
  const isLongDist = raceDist >= 1800;
  const isMidDist = raceDist >= 1400;

  if (style === "逃げ") {
    // 逃げ馬はコーナー通過後なので内ラチ沿いに入っている
    if (gate <= 6 || isLongDist) return "inner";
    return "mid-inner"; // 大外枠の短距離逃げのみ内に入り切れない
  }
  if (style === "先行") {
    if (gate <= 3 || isLongDist) return "inner";
    if (gate <= 5 || isMidDist) return "mid-inner";
    return "middle";
  }
  // 差し・追込: 後方で脚を溜めている
  if (gate <= 2) return "inner";
  if (gate <= 4) return "mid-inner";
  if (gate <= 6) return "middle";
  return gate <= 7 ? "mid-outer" : "outer";
}

// ====================================================================
// 最終コーナーのコース取り推定
// 前半600mからの位置変化 + 脚質 + 枠で判断
// 逃げ・先行は内、差し・追込は枠なり〜やや外を回る
// マクリ型（後方から前進中）は外を回る
// ====================================================================
function estimateRunningLineCrux(
  horse: HorseData,
  _N: number,
  advance: number, // 正=前進（マクリ）、負=後退（バテ）
): RunningLine {
  const style = horse.running_style || "";
  const gate = horse.gate_no ?? 4;

  // 逃げ: 最終コーナーでも内ラチ沿い
  if (style === "逃げ") return "inner";

  // 先行: 内〜内中。バテていても位置は変わらない（下がるだけ）
  if (style === "先行") {
    return gate <= 3 ? "inner" : "mid-inner";
  }

  // 差し・追込: 位置変化量で判断
  // マクリ型（大きく前進中）→ 外を回って捲っている
  if (advance > 0.30 && gate >= 5) return "outer";       // 外枠マクリ → 大外
  if (advance > 0.30 && gate >= 3) return "mid-outer";   // 中枠マクリ → 外目
  if (advance > 0.20 && gate >= 6) return "mid-outer";   // 外枠やや前進 → 外目

  // 枠なり: 内枠の差し馬は内で我慢、外枠は外
  if (gate <= 2) return "inner";             // 内枠 → インで脚を溜める
  if (gate <= 4) return "mid-inner";         // 内中枠 → 内目で待機
  if (gate <= 6) return "middle";            // 中枠 → 中団
  if (gate <= 7) return "mid-outer";         // 外枠 → 外目
  return "outer";                            // 大外枠 → 大外
}

// ====================================================================
// 直線のコース取り推定
// 最終コーナーから直線に入ると馬群がバラける
// 追込馬は外に持ち出す、逃げ馬は内ラチ沿い
// 伸びる馬は外から交わす、バテる馬は馬群に沈む
// ====================================================================
function estimateRunningLineFinish(
  horse: HorseData,
  _N: number,
  advance: number,
  stretchLevel?: "big" | "normal" | "small" | "fading",
): RunningLine {
  const style = horse.running_style || "";
  const gate = horse.gate_no ?? 4;
  const isBigStretch = stretchLevel === "big";
  const isNormalStretch = stretchLevel === "normal";

  // 逃げ: 内ラチ沿いを粘る。バテても内のまま（外に持ち出す余裕がない）
  if (style === "逃げ") return "inner";

  // 先行: 基本は内〜内中で粘る
  if (style === "先行") {
    if (stretchLevel === "fading") return gate <= 3 ? "inner" : "mid-inner";
    if (isBigStretch && gate <= 3) return "inner"; // 内で抜け出す
    if (isBigStretch) return "mid-inner";
    return gate <= 3 ? "inner" : "mid-inner";
  }

  // 差し・追込: 伸び具合 + マクリ傾向 + 枠で判断
  // マクリ型 → 外を伸びてくる
  if (advance > 0.25) {
    return isBigStretch ? "mid-outer" : "outer";
  }

  // 内枠差し → イン突き（前が開けば内を突く）
  if (gate <= 2) {
    return isBigStretch ? "inner" : "mid-inner";
  }

  // 中枠差し → 馬群の間を割る
  if (gate <= 5) {
    if (isBigStretch) return "mid-inner";
    if (isNormalStretch) return "middle";
    return "middle";
  }

  // 外枠差し・追込 → 外から交わしに行く
  if (isBigStretch) return "mid-outer";
  if (isNormalStretch) return "mid-outer";
  return "outer";
}

// --- 座標変換 ---

// 伸び矢印のマップを計算（バテ馬=fading も含む）
function calcStretchMap(
  horses: HorseData[],
  mode: TabMode,
): Record<number, "big" | "normal" | "small" | "fading"> {
  const map: Record<number, "big" | "normal" | "small" | "fading"> = {};
  if (mode === "start") return map;

  // 各馬のコーナー通過順を解析
  const cornerData = horses.map(h => {
    const pc = (h as Record<string, unknown>).predicted_corners as string | undefined;
    const ranks = pc ? pc.split("-").map(Number).filter(n => !isNaN(n)) : [];
    return { h, ranks };
  });

  // ゴール前タブの位置（推定走破タイム）順位を計算
  const finishRanks: Record<number, number> = {};
  const totalTimes = horses.map(h => {
    const tt = (h as Record<string, unknown>).pace_estimated_total_time as number | undefined;
    if (tt != null && tt > 0) return { no: h.horse_no, sec: tt };
    const f3f = (h as Record<string, unknown>).pace_estimated_front3f as number | undefined;
    const mid = (h as Record<string, unknown>).pace_estimated_mid_sec as number | undefined;
    const l3f = (h as Record<string, unknown>).pace_estimated_last3f as number | undefined;
    return { no: h.horse_no, sec: (f3f ?? 0) + (mid ?? 0) + (l3f ?? 0) };
  }).sort((a, b) => a.sec - b.sec);
  totalTimes.forEach((x, i) => { finishRanks[x.no] = i; });

  for (const h of horses) {
    const cd = cornerData.find(c => c.h.horse_no === h.horse_no);

    if (mode === "crux") {
      // 最終コーナータブ: 1角→4角の変化で判定
      const pos_init = ((h as Record<string, unknown>).position_1c as number | undefined)
        ?? ((h as Record<string, unknown>).position_initial as number | undefined);
      const pos_4c = (h as Record<string, unknown>).pace_estimated_pos4c as number | undefined;
      if (pos_init != null && pos_4c != null) {
        const initRank = pos_init * horses.length;
        const cruxPos = pos_4c;
        const change = initRank - cruxPos;
        if (change >= 3) map[h.horse_no] = "big";
        else if (change >= 1) map[h.horse_no] = "normal";
        else if (change <= -2) map[h.horse_no] = "fading";
        else map[h.horse_no] = "small";
      }
    } else {
      // 直線タブ: 4角→ゴール前の変化で判定
      // 4角順位
      const corner4Rank = cd && cd.ranks.length > 0 ? cd.ranks[cd.ranks.length - 1] - 1 : horses.length / 2;
      const fRank = finishRanks[h.horse_no] ?? horses.length / 2;
      const posChange = corner4Rank - fRank; // 正=前に出た
      if (posChange >= 3) map[h.horse_no] = "big";
      else if (posChange >= 1) map[h.horse_no] = "normal";
      else if (posChange <= -2) map[h.horse_no] = "fading";
      else map[h.horse_no] = "small";
    }
  }
  return map;
}

// ====================================================================
// メイン位置計算 — 各タブで全く異なるロジック
// ====================================================================
function calcPositions(
  horses: HorseData[],
  mode: TabMode,
  race: RaceInfo,
): HorsePosition[] {
  const N = horses.length;
  if (N === 0) return [];

  const direction = race.direction || "右";
  const fieldCount = race.field_count || N;
  const firstCornerM = race.first_corner_m ?? 300;
  const raceDist = race.distance ?? 1600;

  // 伸び矢印（先に計算 — Y推定にも使う）
  const stretchMap = calcStretchMap(horses, mode);

  // 各馬のコーナー通過順を解析
  const cornerData = horses.map(h => {
    const pc = (h as Record<string, unknown>).predicted_corners as string | undefined;
    const ranks = pc ? pc.split("-").map(Number).filter(n => !isNaN(n)) : [];
    return { h, ranks };
  });
  const hasCorners = cornerData.some(c => c.ranks.length > 0);

  // セクションタイムデータ
  const sectionData = horses.map(h => {
    const f3f = (h as Record<string, unknown>).pace_estimated_front3f as number | undefined;
    const mid = (h as Record<string, unknown>).pace_estimated_mid_sec as number | undefined;
    const l3f = (h as Record<string, unknown>).pace_estimated_last3f as number | undefined;
    const tt = (h as Record<string, unknown>).pace_estimated_total_time as number | undefined;
    return {
      h,
      front: f3f ?? null,
      mid: mid ?? null,
      last3f: l3f ?? null,
      total: tt ?? (f3f != null ? (f3f + (mid ?? 0) + (l3f ?? 0)) : null),
    };
  });

  // 1角→4角の前進量（最終コーナー/直線で使用）
  const advanceMap = new Map<number, number>();
  for (const h of horses) {
    const posInit = ((h as Record<string, unknown>).position_1c as number | undefined)
      ?? ((h as Record<string, unknown>).position_initial as number | undefined)
      ?? 0.5;
    const pos4c = (h as Record<string, unknown>).pace_estimated_pos4c as number | undefined;
    const posCrux = pos4c != null && N > 1
      ? Math.max(0, Math.min(1, (pos4c - 1) / (N - 1)))
      : 0.5;
    advanceMap.set(h.horse_no, posInit - posCrux); // 正=前進（マクリ）
  }

  // 各馬の前後位置 rawPos (0=先頭, 1=最後方)
  let rawPositions: { h: HorseData; pos: number }[];

  // ================================================================
  // 前半600m: predicted_corners の1角位置を優先（カード表示と一致させる）
  // フォールバック: front3f → position_1c
  // ================================================================
  if (mode === "start") {
    if (hasCorners && N >= 2) {
      // predicted_corners の1角位置（カードの通過順と同じデータ）
      rawPositions = cornerData.map(c => ({
        h: c.h,
        pos: c.ranks.length > 0 ? (c.ranks[0] - 1) / Math.max(N - 1, 1) : 0.5,
      }));
    } else {
      // コーナー通過順がない場合: front3fタイムで順位付け
      const f3fData = horses.map(h => ({
        h,
        f3f: (h as Record<string, unknown>).pace_estimated_front3f as number | undefined,
      }));
      const validF3f = f3fData.filter(x => x.f3f != null);

      if (validF3f.length >= 2) {
        const sorted = [...validF3f].sort((a, b) => a.f3f! - b.f3f!);
        const f3fRankMap = new Map<number, number>();
        sorted.forEach((x, i) => { f3fRankMap.set(x.h.horse_no, i); });
        rawPositions = f3fData.map(x => ({
          h: x.h,
          pos: x.f3f != null
            ? (f3fRankMap.get(x.h.horse_no) ?? N / 2) / Math.max(N - 1, 1)
            : 0.5,
        }));
      } else {
        rawPositions = horses.map(h => ({
          h,
          pos: ((h as Record<string, unknown>).position_1c as number | undefined)
            ?? ((h as Record<string, unknown>).position_initial as number | undefined)
            ?? 0.5,
        }));
      }
    }
  }

  // ================================================================
  // 最終コーナー: 4角位置のみ（上がり3Fブレンドなし）
  // 最終コーナー時点で上がり3Fはまだ始まっていない
  // （残600mは4角の74〜400m手前）
  // ================================================================
  else if (mode === "crux") {
    if (hasCorners && N >= 2) {
      rawPositions = cornerData.map(c => ({
        h: c.h,
        pos: c.ranks.length > 0
          ? (c.ranks[c.ranks.length - 1] - 1) / Math.max(N - 1, 1)
          : 0.5,
      }));
    } else {
      // フォールバック: 道中累積タイム
      const cumTimes = sectionData.map(x => ({
        h: x.h,
        sec: x.front != null ? x.front + (x.mid ?? 0) : null,
      }));
      const validCum = cumTimes.filter(x => x.sec != null);
      const minCum = validCum.length ? Math.min(...validCum.map(x => x.sec!)) : 0;
      const maxCum = validCum.length ? Math.max(...validCum.map(x => x.sec!)) : 0;
      const cumRange = maxCum - minCum || 1;
      rawPositions = cumTimes.map(x => ({
        h: x.h,
        pos: x.sec != null ? (x.sec - minCum) / cumRange : 0.5,
      }));
    }
  }

  // ================================================================
  // 直線: 4角位置 + 上がり3Fランクのブレンド
  // コース構造で比率を動的調整
  // ================================================================
  else {
    // コース構造でブレンド比率調整
    const l3fPct = race.l3f_straight_pct ?? (race.straight_m ? Math.min(1.0, race.straight_m / 600) : 0.55);
    // 短直線(浦和200m:0.33) → 4角重視(0.60)
    // 長直線(東京526m:0.88) → 上がり重視(0.22)
    const CORNER_WEIGHT = Math.max(0.20, Math.min(0.65, 0.65 - (l3fPct - 0.33) * (0.45 / 0.55)));

    if (hasCorners && N >= 2) {
      // 4角位置
      const corner4Pos = cornerData.map(c => ({
        h: c.h,
        pos: c.ranks.length > 0 ? (c.ranks[c.ranks.length - 1] - 1) / Math.max(N - 1, 1) : 0.5,
      }));
      // 上がり3Fランク
      const l3fData = horses.map(h => ({
        h,
        l3f: (h as Record<string, unknown>).pace_estimated_last3f as number | undefined,
      }));
      const validL3f = l3fData.filter(x => x.l3f != null).sort((a, b) => a.l3f! - b.l3f!);
      const l3fRankMap = new Map<number, number>();
      validL3f.forEach((x, i) => { l3fRankMap.set(x.h.horse_no, i); });
      rawPositions = corner4Pos.map(c => {
        const l3fRank = l3fRankMap.get(c.h.horse_no);
        const l3fNorm = l3fRank != null ? l3fRank / Math.max(validL3f.length - 1, 1) : 0.5;
        return { h: c.h, pos: CORNER_WEIGHT * c.pos + (1 - CORNER_WEIGHT) * l3fNorm };
      });
    } else {
      // フォールバック: 走破タイム
      const validTotal = sectionData.filter(x => x.total != null);
      const minTotal = validTotal.length ? Math.min(...validTotal.map(x => x.total!)) : 0;
      const maxTotal = validTotal.length ? Math.max(...validTotal.map(x => x.total!)) : 0;
      const globalRange = maxTotal - minTotal || 1;
      rawPositions = sectionData.map(x => ({
        h: x.h,
        pos: x.total != null ? (x.total - minTotal) / globalRange : 0.5,
      }));
    }
  }

  // ソート
  rawPositions.sort((a, b) => a.pos - b.pos);

  // 走行ラインを事前計算（モード別に異なるロジック）
  const preCalc = rawPositions.map(item => {
    const adv = advanceMap.get(item.h.horse_no) ?? 0;
    let line: RunningLine;
    if (mode === "start") {
      line = estimateRunningLineStart(item.h, N, firstCornerM, raceDist);
    } else if (mode === "crux") {
      line = estimateRunningLineCrux(item.h, N, adv);
    } else {
      line = estimateRunningLineFinish(item.h, N, adv, stretchMap[item.h.horse_no]);
    }
    return { ...item, line, advance: adv };
  });

  // タイム差計算
  const timeDiffs: { startDiff: number; cruxDiff: number; finishDiff: number }[] = [];
  const allF3f = horses
    .map(hh => (hh as Record<string, unknown>).pace_estimated_front3f as number | undefined)
    .filter((v): v is number => v != null);
  const minF3f = allF3f.length ? Math.min(...allF3f) : 0;
  const allCums = horses
    .map(hh => {
      const ff = (hh as Record<string, unknown>).pace_estimated_front3f as number | undefined;
      const mm = (hh as Record<string, unknown>).pace_estimated_mid_sec as number | undefined;
      return (ff != null && mm != null) ? ff + mm : null;
    })
    .filter((v): v is number => v != null);
  const minCum = allCums.length ? Math.min(...allCums) : 0;

  // 推定走破タイム（直線タブ用）
  const allTotalTimes = horses
    .map(hh => (hh as Record<string, unknown>).pace_estimated_total_time as number | undefined)
    .filter((v): v is number => v != null && v > 0);
  const minTotalTime = allTotalTimes.length ? Math.min(...allTotalTimes) : 0;
  const maxTotalTime = allTotalTimes.length ? Math.max(...allTotalTimes) : 0;
  const maxTotalDiff = maxTotalTime - minTotalTime;

  for (const item of preCalc) {
    const h = item.h;
    const myF3f = (h as Record<string, unknown>).pace_estimated_front3f as number | undefined;
    const myMid = (h as Record<string, unknown>).pace_estimated_mid_sec as number | undefined;
    const myL3f = (h as Record<string, unknown>).pace_estimated_last3f as number | undefined;
    const myTotal = (h as Record<string, unknown>).pace_estimated_total_time as number | undefined;

    let startDiff = 0;
    let cruxDiff = 0;
    let finishDiff = 0;

    // 前半600m: front3f差
    if (myF3f != null) {
      startDiff = Math.round((myF3f - minF3f) * 10) / 10;
    }
    // 最終コーナー: 累積タイム差
    if (myF3f != null && myMid != null) {
      cruxDiff = Math.round((myF3f + myMid - minCum) * 10) / 10;
    }
    // 直線: 推定走破タイム差
    if (myTotal != null && myTotal > 0) {
      finishDiff = Math.round(Math.max(0, myTotal - minTotalTime) * 10) / 10;
    } else if (myF3f != null && myMid != null && myL3f != null) {
      const allL3fVals = horses
        .map(hh => (hh as Record<string, unknown>).pace_estimated_last3f as number | undefined)
        .filter((v): v is number => v != null);
      const minL3f = allL3fVals.length ? Math.min(...allL3fVals) : 0;
      const gap = (myF3f + myMid - minCum) + (myL3f - minL3f);
      finishDiff = Math.round(Math.max(0, gap) * 10) / 10;
    } else {
      finishDiff = Math.round(maxTotalDiff * 10) / 10 || 2.0;
    }
    timeDiffs.push({ startDiff, cruxDiff, finishDiff });
  }

  // モード別の秒差を取得
  const getDiff = (idx: number): number => {
    const td = timeDiffs[idx];
    if (mode === "start") return td.startDiff;
    if (mode === "crux") return td.cruxDiff;
    return td.finishDiff;
  };

  // 秒差が大きすぎる場合はステップを自動拡大
  const maxRawDiff = Math.max(...preCalc.map((_, i) => getDiff(i)), 0);
  const effectiveStep = maxRawDiff > 2.0 ? GRID_STEP * 2 : GRID_STEP;

  // グリッドセルに変換
  const gridCells: GridCell[] = preCalc.map((item, i) => {
    const diff = getDiff(i);
    const col = Math.round(diff / effectiveStep);
    const row = LINE_INDEX[item.line];
    return { col, row, idx: i };
  });

  // 衝突回避
  const horseNos = preCalc.map(item => item.h.horse_no);
  resolveCollisions(gridCells, horseNos);

  // 最大col値
  const maxCol = Math.max(...gridCells.map(c => c.col), 1);
  const maxSnappedDiff = maxCol * effectiveStep;

  // 座標に変換
  const coords = gridCells.map(cell => ({
    x: calcGridX(cell.col * effectiveStep, maxSnappedDiff, direction),
    y: zoneToY(cell.row),
  }));

  // 位置変化マップ（最終コーナー用）
  // 1角順位→4角順位の変化
  const posChangeMap = new Map<number, "up" | "down" | "same">();
  if (mode === "crux") {
    for (const h of horses) {
      const cd = cornerData.find(c => c.h.horse_no === h.horse_no);
      if (cd && cd.ranks.length >= 2) {
        const firstRank = cd.ranks[0];
        const lastRank = cd.ranks[cd.ranks.length - 1];
        const diff = firstRank - lastRank; // 正=順位が上がった（前進）
        if (diff >= 2) posChangeMap.set(h.horse_no, "up");
        else if (diff <= -2) posChangeMap.set(h.horse_no, "down");
        else posChangeMap.set(h.horse_no, "same");
      }
    }
  }

  const result: HorsePosition[] = [];

  for (let idx = 0; idx < preCalc.length; idx++) {
    const item = preCalc[idx];
    const { h } = item;
    const gate = gateGroup(h.gate_no, h.horse_no, fieldCount);
    const x = coords[idx].x;
    const y = coords[idx].y;
    const td = timeDiffs[idx];
    const myF3f = (h as Record<string, unknown>).pace_estimated_front3f as number | undefined;
    const myL3f = (h as Record<string, unknown>).pace_estimated_last3f as number | undefined;

    result.push({
      horseNo: h.horse_no,
      horseName: h.horse_name,
      gate,
      x,
      y,
      stretch: stretchMap[h.horse_no],
      front3fSec: myF3f,
      startDiffSec: mode === "start" ? td.startDiff : undefined,
      cruxDiffSec: mode === "crux" ? td.cruxDiff : undefined,
      last3fSec: myL3f,
      finishDiffSec: mode === "finish" ? td.finishDiff : undefined,
      last3fRank: undefined, // 後で計算
      posChange: posChangeMap.get(h.horse_no),
    });
  }

  // 上がり3F順位を計算
  const l3fEntries = result
    .filter(r => r.last3fSec != null)
    .sort((a, b) => a.last3fSec! - b.last3fSec!);
  l3fEntries.forEach((r, i) => { r.last3fRank = i + 1; });

  // ゴール前: 先頭馬を+0.0にする
  if (mode === "finish") {
    const finishDiffs = result.filter(r => r.finishDiffSec != null).map(r => r.finishDiffSec!);
    const minFinish = finishDiffs.length ? Math.min(...finishDiffs) : 0;
    for (const r of result) {
      if (r.finishDiffSec != null) {
        r.finishDiffSec = Math.round((r.finishDiffSec - minFinish) * 10) / 10;
      }
    }
  }

  return result;
}

// --- 伸び矢印 / バテ矢印 ---
function StretchArrow({
  level,
  direction,
}: {
  level?: "big" | "normal" | "small" | "fading";
  direction: string;
}) {
  if (!level || level === "small") return null;
  const isRight = direction === "右";
  const goalArrow = isRight ? "←" : "→";
  const fadeArrow = isRight ? "→" : "←";
  const cfg = {
    big: { text: `${goalArrow}${goalArrow}`, cls: "text-emerald-500 font-bold text-sm" },
    normal: { text: goalArrow, cls: "text-blue-500 text-xs" },
    fading: { text: `${fadeArrow}${fadeArrow}`, cls: "text-red-500 font-bold text-sm" },
    small: { text: "", cls: "" },
  };
  const c = cfg[level];
  return <span className={`leading-none ${c.cls}`}>{c.text}</span>;
}

// 印→リングカラー（ゴール前タブで使用）
// 印リング色（◉/◎=緑, ○/☆=青, ▲/×=赤, △=紫, ★=黒）
const MARK_RING: Record<string, string> = {
  "◉": "ring-2 ring-emerald-500",
  "◎": "ring-2 ring-emerald-500",
  "○": "ring-2 ring-blue-500",
  "▲": "ring-2 ring-red-500",
  "△": "ring-2 ring-purple-500",
  "★": "ring-2 ring-gray-600",
  "☆": "ring-2 ring-blue-400",
  "×": "ring-2 ring-red-400",
};

// --- 位置変化インジケータ ---
function PosChangeIndicator({ change }: { change?: "up" | "down" | "same" }) {
  if (!change || change === "same") return null;
  if (change === "up") {
    return <span className="text-[9px] text-emerald-500 font-bold leading-none">▲押上</span>;
  }
  return <span className="text-[9px] text-red-500 font-bold leading-none">▼後退</span>;
}

// --- 馬マーカー ---
function HorseMarker({
  pos,
  direction,
  showStretch,
  mode,
  mark,
}: {
  pos: HorsePosition;
  direction: string;
  showStretch: boolean;
  mode: TabMode;
  mark?: string;
}) {
  const bg = WAKU_BG[pos.gate] || "bg-gray-500 text-white";
  const markRing = mode === "finish" && mark ? (MARK_RING[mark] || "ring-1 ring-background/80") : "ring-1 ring-background/80";
  const shortName = pos.horseName.length > 3
    ? pos.horseName.slice(0, 3)
    : pos.horseName;
  const isLeftTurn = direction === "左";

  // 上がり3F色分け（1位=緑, 2位=青, 3位=赤）
  const last3fColor = pos.last3fRank === 1 ? "text-emerald-500 font-bold"
    : pos.last3fRank === 2 ? "text-blue-500 font-bold"
    : pos.last3fRank === 3 ? "text-red-500 font-bold"
    : "text-foreground/50";

  // タイム情報テキスト
  let timeLabel = "";
  let subLabel = "";
  let subCls = "text-foreground/50";
  if (mode === "start") {
    // 前半600m: 先頭との秒差
    if (pos.startDiffSec != null && pos.startDiffSec > 0) {
      timeLabel = `+${pos.startDiffSec.toFixed(1)}`;
    }
  } else if (mode === "crux") {
    // 最終コーナー: 先頭との秒差
    if (pos.cruxDiffSec != null && pos.cruxDiffSec > 0) {
      timeLabel = `+${pos.cruxDiffSec.toFixed(1)}`;
    }
  } else if (mode === "finish") {
    // 直線: 上がり3F
    if (pos.last3fSec != null) {
      subLabel = `上${pos.last3fSec.toFixed(1)}`;
      subCls = last3fColor;
    }
  }

  return (
    <div
      className="absolute flex flex-col items-center gap-0 transition-all duration-300"
      style={{
        left: `${pos.x}%`,
        top: `${pos.y}%`,
        transform: "translate(-50%, -50%)",
      }}
    >
      <div className="flex items-center gap-1">
        {showStretch && !isLeftTurn && (
          <StretchArrow level={pos.stretch} direction={direction} />
        )}
        <div className="relative">
          <div
            className={`w-7 h-7 rounded-full flex items-center justify-center text-[13px] font-bold ${bg} shadow-md ${markRing}`}
          >
            {pos.horseNo}
          </div>
          {mode === "finish" && mark && mark !== "—" && mark !== "-" && (
            <span className="absolute -top-2 -right-2 text-[9px] font-bold text-foreground/80 bg-background/90 rounded px-0.5 leading-tight">
              {mark}
            </span>
          )}
        </div>
        {showStretch && isLeftTurn && (
          <StretchArrow level={pos.stretch} direction={direction} />
        )}
      </div>
      <span className="text-[12px] text-foreground/80 whitespace-nowrap leading-tight font-bold">
        {shortName}
      </span>
      {/* 最終コーナー: 位置変化インジケータ */}
      {mode === "crux" && <PosChangeIndicator change={pos.posChange} />}
      {timeLabel && (
        <span className="text-[11px] text-foreground/60 whitespace-nowrap leading-tight font-medium">
          {timeLabel}
        </span>
      )}
      {subLabel && (
        <span className={`text-[11px] whitespace-nowrap leading-tight ${subCls}`}>
          {subLabel}
        </span>
      )}
    </div>
  );
}

// --- トラックビュー ---
function TrackView({
  horses,
  mode,
  race,
}: {
  horses: HorseData[];
  mode: TabMode;
  race: RaceInfo;
}) {
  const positions = useMemo(
    () => calcPositions(horses, mode, race),
    [horses, mode, race],
  );

  const direction = race.direction || "右";
  const surface = race.surface || "芝";
  const isDirt = surface === "ダート";
  const showStretch = mode === "finish";

  // 馬場背景色
  const trackBg = isDirt
    ? "bg-gradient-to-b from-amber-900/20 via-amber-800/10 to-amber-900/20 dark:from-amber-800/25 dark:via-amber-700/10 dark:to-amber-800/25"
    : "bg-gradient-to-b from-emerald-900/20 via-emerald-800/10 to-emerald-900/20 dark:from-emerald-800/25 dark:via-emerald-700/10 dark:to-emerald-800/25";

  const isRight = direction === "右";

  // グリッド線用: 最大秒差を算出
  const maxDiff = useMemo(() => {
    const diffs = positions.map(p => {
      if (mode === "start") return p.startDiffSec ?? 0;
      if (mode === "crux") return p.cruxDiffSec ?? 0;
      return p.finishDiffSec ?? 0;
    });
    return Math.max(...diffs, 0);
  }, [positions, mode]);

  const effectiveStep = maxDiff > 2.0 ? GRID_STEP * 2 : GRID_STEP;
  const labelInterval = effectiveStep * 2;

  // ゾーンラベル
  const zoneLabels = ["内", "内中", "中", "中外", "外"] as const;

  // タブ説明テキスト
  const _hasCorners = horses.some(h => {
    const pc = (h as Record<string, unknown>).predicted_corners as string | undefined;
    return pc ? pc.split("-").some(s => !isNaN(Number(s)) && s !== "") : false;
  });
  const modeDescription = mode === "start"
    ? (_hasCorners ? `序盤の隊列` : `600m地点の隊列`)
    : mode === "crux"
    ? "最終コーナーの隊列"
    : "直線の隊列";

  return (
    <div
      className="relative w-full rounded-lg overflow-hidden border border-border/50 mt-2"
      style={{ height: CONTAINER_H }}
    >
      {/* 馬場背景 */}
      <div className={`absolute inset-0 ${trackBg}`} />

      {/* タブ説明 */}
      <div className="absolute top-1 left-1/2 -translate-x-1/2 text-[9px] text-foreground/30 font-medium z-10">
        {modeDescription}
      </div>

      {/* 内ラチ（白実線） */}
      <div
        className="absolute left-[6%] right-[3%] h-[2px] bg-white/60 dark:bg-white/35"
        style={{ top: `${Y_MIN}%` }}
      />
      {/* 外ラチ（白実線） */}
      <div
        className="absolute left-[6%] right-[3%] h-[2px] bg-white/60 dark:bg-white/35"
        style={{ top: `${Y_MAX}%` }}
      />

      {/* ゾーン境界線（4本） */}
      {[1, 2, 3, 4].map(i => {
        const yPct = Y_MIN + i * (Y_MAX - Y_MIN) / 5;
        return (
          <div key={`zone-${i}`}
            className="absolute left-[6%] right-[3%] border-t border-dashed border-foreground/8"
            style={{ top: `${yPct}%` }}
          />
        );
      })}

      {/* ゾーンラベル（左端に5つ） */}
      {zoneLabels.map((label, i) => {
        const yPct = zoneToY(i);
        return (
          <div key={label}
            className="absolute left-[0.5%] text-[9px] text-foreground/40 font-medium"
            style={{ top: `${yPct}%`, transform: "translateY(-50%)" }}
          >
            {label}
          </div>
        );
      })}

      {/* ゴール方向矢印 */}
      <div className={`absolute text-[10px] text-foreground/40 ${isRight ? "left-[7%]" : "right-[3%]"}`}
        style={{ top: `${Y_MAX + 3}%` }}
      >
        {isRight ? "← ゴール方向" : "ゴール方向 →"}
      </div>

      {/* タイムグリッド線 */}
      {maxDiff > 0.05 && (() => {
        const lines: { sec: number; xPct: number; showLabel: boolean }[] = [];
        const maxSnapped = Math.ceil(maxDiff / effectiveStep) * effectiveStep;
        for (let sec = effectiveStep; sec <= maxSnapped + 0.01; sec += effectiveStep) {
          const xPct = calcGridX(sec, maxSnapped, direction);
          const isLabel = Math.abs(sec % labelInterval) < 0.01
            || Math.abs(sec % labelInterval - labelInterval) < 0.01;
          lines.push({ sec, xPct, showLabel: isLabel });
        }
        return lines.map(({ sec, xPct, showLabel }) => (
          <div key={sec.toFixed(2)}
            className={`absolute border-l ${
              showLabel ? "border-dashed border-foreground/12" : "border-dotted border-foreground/6"
            }`}
            style={{ left: `${xPct}%`, top: `${Y_MIN}%`, bottom: `${100 - Y_MAX}%` }}
          >
            {showLabel && (
              <span className="absolute -top-3 left-1/2 -translate-x-1/2 text-[8px] text-foreground/25 whitespace-nowrap">
                +{sec.toFixed(1)}s
              </span>
            )}
          </div>
        ));
      })()}

      {/* 馬マーカー */}
      {positions.map(pos => {
        const hd = horses.find(h => h.horse_no === pos.horseNo);
        return (
          <HorseMarker
            key={pos.horseNo}
            pos={pos}
            direction={direction}
            showStretch={showStretch}
            mode={mode}
            mark={hd?.mark}
          />
        );
      })}
    </div>
  );
}

// --- 凡例 ---
function Legend({ direction }: { direction: string }) {
  const isRight = direction === "右";
  const goalArrow = isRight ? "←" : "→";
  const fadeArrow = isRight ? "→" : "←";
  return (
    <div className="flex items-center gap-3 text-[10px] text-muted-foreground mt-1.5 flex-wrap">
      <span>
        <span className="text-emerald-500 font-bold">{goalArrow}{goalArrow}</span> 大きく伸びる
      </span>
      <span>
        <span className="text-blue-500">{goalArrow}</span> 伸びる
      </span>
      <span>
        <span className="text-red-500 font-bold">{fadeArrow}{fadeArrow}</span> バテる
      </span>
      <span>
        <span className="text-emerald-500 font-bold text-[9px]">▲</span>押上
        <span className="text-red-500 font-bold text-[9px] ml-1">▼</span>後退
      </span>
      <span>1マス=一馬身(0.15秒)</span>
    </div>
  );
}

// --- コースバイアス表示 ---
function CourseBias({ venue, surface, distance }: { venue: string; surface?: string; distance?: number }) {
  const venueCode = VENUE_NAME_TO_CODE[venue] || "";
  const courseKey = venueCode && distance ? `${venueCode}_${surface || "ダート"}_${distance}` : "";
  const { data } = useCourseStats(courseKey);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d = data as any;
  if (!d) return null;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rs: Record<string, any> = d.running_style || {};
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const gb: Record<string, any> = d.gate_bias || {};
  const styles = ["逃げ", "先行", "差し", "追込"] as const;
  const getN = (v: any) => v?.total ?? v?.runs ?? 0;
  const hasRS = styles.some(s => getN(rs[s]) > 0);

  // 枠バイアス: 1〜8枠個別
  const gateKeys = ["1", "2", "3", "4", "5", "6", "7", "8"];
  const hasGate = gateKeys.some(k => getN(gb[k]) > 0);

  if (!hasRS && !hasGate) return null;

  // 脚質の最大複勝率（バー幅算出用）
  const maxP3 = Math.max(...styles.map(s => rs[s]?.place3_rate || 0), 1);

  return (
    <div className="space-y-2">
      {hasRS && (
        <div>
          <div className="flex items-center gap-2 mb-1.5">
            <span className="text-muted-foreground text-xs font-semibold">脚質バイアス</span>
            <span className="text-[10px] text-muted-foreground">({styles.reduce((s, k) => s + getN(rs[k]), 0)}件)</span>
          </div>
          <div className="space-y-1">
            {styles.map(s => {
              const v = rs[s];
              if (!v || getN(v) === 0) return null;
              const barW = Math.round((v.place3_rate / maxP3) * 100);
              const isTop = v.place3_rate === maxP3;
              return (
                <div key={s} className="flex items-center gap-1.5 text-xs">
                  <span className={`w-8 shrink-0 font-bold ${isTop ? "text-emerald-600" : ""}`}>{s}</span>
                  <div className="flex-1 h-3 bg-muted rounded-full overflow-hidden">
                    <div
                      className={`h-full rounded-full ${isTop ? "bg-emerald-500/70" : "bg-blue-400/50"}`}
                      style={{ width: `${barW}%` }}
                    />
                  </div>
                  <span className="w-20 text-right tabular-nums text-muted-foreground">
                    勝<span className={`font-semibold ${isTop ? "text-emerald-600" : "text-foreground"}`}>{v.win_rate.toFixed(1)}</span>%
                    複<span className={`font-semibold ${isTop ? "text-emerald-600" : "text-foreground"}`}>{v.place3_rate.toFixed(1)}</span>%
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      )}
      {hasGate && (() => {
        const maxGateP3 = Math.max(...gateKeys.map(k => gb[k]?.place3_rate || 0), 1);
        return (
          <div>
            <span className="text-muted-foreground text-xs font-semibold">枠バイアス</span>
            <div className="grid grid-cols-2 gap-x-3 gap-y-1 mt-1.5">
              {gateKeys.map(k => {
                const v = gb[k];
                if (!v || getN(v) === 0) return null;
                const barW = Math.round((v.place3_rate / maxGateP3) * 100);
                const isBest = v.place3_rate === maxGateP3;
                return (
                  <div key={k} className="flex items-center gap-1.5 text-xs">
                    <span className={`w-5 h-5 rounded-sm flex items-center justify-center text-[11px] font-bold shrink-0 ${WAKU_BG[Number(k)] || ""}`}>{k}</span>
                    <div className="flex-1 h-2.5 bg-muted rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full ${isBest ? "bg-emerald-500/70" : "bg-blue-400/40"}`}
                        style={{ width: `${barW}%` }}
                      />
                    </div>
                    <span className={`w-12 text-right tabular-nums shrink-0 ${isBest ? "text-emerald-600 font-bold" : "text-muted-foreground"}`}>
                      {v.win_rate.toFixed(1)}%
                    </span>
                  </div>
                );
              })}
            </div>
          </div>
        );
      })()}
    </div>
  );
}

// --- コース詳細ビュー ---
// 距離×芝/ダート別レース傾向ラベル（案A: 距離固有特性を露出）
function distanceTendency(surface: string | undefined, distance: number | undefined): {
  category: string;     // 短距離/マイル/中距離/長距離
  pace: string;         // ハイ/ミドル/スロー
  style: string;        // 有利脚質
  key: string;          // 勝負ポイント
  color: string;        // 表示色
  paceColor: string;    // ペース色
} | null {
  if (!distance || distance < 600) return null;
  const isDirt = surface === "ダート";

  if (isDirt) {
    // ダート: 砂で前止まり気味、基本ハイペース、スタミナ色濃い
    if (distance <= 1200) {
      return { category: "ダート短距離", pace: "ハイ", style: "先行有利（逃げ残り多）",
        key: "スピード勝負", color: "text-red-500", paceColor: "text-red-500" };
    }
    if (distance <= 1600) {
      return { category: "ダートマイル", pace: "ミドル〜ハイ", style: "先行〜好位",
        key: "スタミナ+先行力", color: "text-amber-500", paceColor: "text-amber-500" };
    }
    if (distance <= 1900) {
      return { category: "ダート中距離", pace: "ミドル", style: "先行〜差し",
        key: "本格スタミナ", color: "text-blue-500", paceColor: "text-blue-500" };
    }
    return { category: "ダート長距離", pace: "スロー〜ミドル", style: "差し届く",
      key: "消耗戦の我慢", color: "text-purple-500", paceColor: "text-blue-500" };
  }

  // 芝
  if (distance <= 1200) {
    return { category: "芝短距離（スプリント）", pace: "ハイ", style: "逃げ・先行有利",
      key: "純スピード勝負", color: "text-red-500", paceColor: "text-red-500" };
  }
  if (distance <= 1400) {
    return { category: "芝1400m級", pace: "ハイ〜ミドル", style: "先行＋好位差し",
      key: "スピード持続力", color: "text-amber-500", paceColor: "text-red-500" };
  }
  if (distance <= 1700) {
    return { category: "芝マイル", pace: "ミドル", style: "好位差し有利",
      key: "バランス＋瞬発力", color: "text-emerald-500", paceColor: "text-amber-500" };
  }
  if (distance <= 2100) {
    return { category: "芝中距離", pace: "ミドル〜スロー", style: "差し届きやすい",
      key: "持続力＋瞬発力", color: "text-blue-500", paceColor: "text-amber-500" };
  }
  if (distance <= 2500) {
    return { category: "芝クラシック距離", pace: "スロー〜ミドル", style: "差し・追込可",
      key: "瞬発力勝負", color: "text-blue-500", paceColor: "text-emerald-500" };
  }
  return { category: "芝長距離（スタミナ）", pace: "スロー", style: "後方一気も届く",
    key: "スタミナ＋瞬発力", color: "text-purple-500", paceColor: "text-emerald-500" };
}

function CourseDetailView({ race, venue }: { race: RaceInfo; venue: string }) {
  const straightM = race.straight_m || 0;
  // 1角までの距離（データがない場合は直線距離から推定）
  const firstCornerM = race.first_corner_m || (straightM > 0 ? Math.round(straightM * 0.8) : 0);

  // 距離特性（距離・芝/ダート別） — 案A: 距離で変わる部分を明示
  const dt = distanceTendency(race.surface, race.distance);

  // 坂ラベル
  const slopeLabel = (() => {
    const e = race.l3f_elevation;
    if (e == null) return "平坦";
    if (e >= 2.0) return "急坂";
    if (e >= 0.5) return "軽坂";
    return "平坦";
  })();
  const slopeColor = (() => {
    const e = race.l3f_elevation || 0;
    if (e >= 2.0) return "text-red-500";
    if (e >= 0.5) return "text-amber-500";
    return "text-emerald-500";
  })();

  // ラスト3F開始位置ラベル（見取り図のコーナー番号と対応）
  const l3fCorners = race.l3f_corners || 0;
  const l3fCornerLabel = l3fCorners === 0
    ? "直線"
    : `${4 - l3fCorners}角手前`;
  const cornerDesc = l3fCorners >= 2
    ? "コーナー途中から加速（器用さ必要）"
    : l3fCorners === 1
      ? "コーナー1つ→直線勝負"
      : "直線のみ（純粋なスピード勝負）";
  const cornerColor = l3fCorners >= 2 ? "text-amber-500" : l3fCorners === 1 ? "text-blue-500" : "text-emerald-500";

  // 直線の長さ評価
  const straightEval = straightM >= 500 ? "長い（差し有利）" : straightM >= 350 ? "標準" : "短い（先行有利）";
  const straightColor = straightM >= 500 ? "text-blue-500" : straightM >= 350 ? "text-foreground" : "text-red-500";

  // 1角までの距離評価
  const fcEval = firstCornerM >= 400 ? "長い（隊列落ち着く）" : firstCornerM >= 250 ? "標準" : "短い（先行争い激化）";
  const fcColor = firstCornerM >= 400 ? "text-emerald-500" : firstCornerM >= 250 ? "text-foreground" : "text-red-500";

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 py-3 px-2">
      {/* 左: SVGコース図 */}
      <div className="flex items-center justify-center">
        <CourseMapSVG race={race} venue={venue} />
      </div>
      {/* 右: コース情報 */}
      <div className="space-y-3 text-sm">
        {/* 距離特性（距離×芝ダート別） — このレース固有の傾向 */}
        {dt && (
          <div className="rounded-md border border-border/60 bg-muted/30 px-2 py-1.5 space-y-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-[11px] text-muted-foreground">距離特性</span>
              <span className={`font-bold text-xs ${dt.color}`}>{dt.category}</span>
              {race.distance != null && (
                <span className="text-[11px] text-muted-foreground">({race.distance}m)</span>
              )}
            </div>
            <div className="flex items-center gap-3 text-[11px] flex-wrap">
              <span className="text-muted-foreground">
                ペース <span className={`font-bold ${dt.paceColor}`}>{dt.pace}</span>
              </span>
              <span className="text-muted-foreground">
                脚質 <span className="font-bold text-foreground">{dt.style}</span>
              </span>
              <span className="text-muted-foreground">
                要素 <span className="font-bold text-foreground">{dt.key}</span>
              </span>
            </div>
          </div>
        )}
        {/* 直線距離 */}
        {straightM > 0 && (
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-16 shrink-0 text-xs">直線</span>
            <span className={`font-bold ${straightColor}`}>{straightM}m</span>
            <span className="text-xs text-muted-foreground">{straightEval}</span>
          </div>
        )}
        {/* 1角まで */}
        {firstCornerM > 0 && (
          <div className="flex items-center gap-2">
            <span className="text-muted-foreground w-16 shrink-0 text-xs">1角まで</span>
            <span className={`font-bold ${fcColor}`}>{firstCornerM}m</span>
            <span className="text-xs text-muted-foreground">{fcEval}</span>
          </div>
        )}
        {/* ラスト3F開始位置 */}
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-16 shrink-0 text-xs">ラスト3F</span>
          <span className={`font-bold ${cornerColor}`}>{l3fCornerLabel}</span>
          <span className="text-xs text-muted-foreground">{cornerDesc}</span>
        </div>
        {/* 直線（坂情報統合） */}
        <div className="flex items-center gap-2">
          <span className="text-muted-foreground w-16 shrink-0 text-xs">直線</span>
          <span className={`font-bold ${slopeColor}`}>{slopeLabel}</span>
          {race.l3f_elevation != null && race.l3f_elevation > 0 && (
            <span className="text-xs text-muted-foreground">高低差 {race.l3f_elevation.toFixed(1)}m</span>
          )}
        </div>
        {/* コースバイアス */}
        <CourseBias venue={venue} surface={race.surface} distance={race.distance} />
      </div>
    </div>
  );
}

// --- メインコンポーネント ---
export default function PaceFormation({
  horses,
  race,
}: {
  horses: HorseData[];
  race: RaceInfo & Record<string, unknown>;
}) {
  // 出走取消馬を除外
  const activeHorses = horses.filter(h => {
    const hAny = h as Record<string, unknown>;
    if (hAny.is_scratched) return false;
    const hasOddsRace = horses.some(hh => (hh as Record<string, unknown>).odds != null);
    if (hasOddsRace && hAny.odds == null && hAny.popularity == null) return false;
    return true;
  });

  const venue = (race.venue as string) || "";
  const direction = (race.direction as string) || "右";
  const surface = (race.surface as string) || "芝";
  const raceInfo: RaceInfo = {
    final_formation: race.final_formation,
    field_count: race.field_count || activeHorses.length,
    direction,
    surface,
    inside_outside: (race.inside_outside as string | undefined) || "",
    distance: race.distance as number | undefined,
    first_corner_m: race.first_corner_m as number | undefined,
    straight_m: race.straight_m as number | undefined,
    l3f_straight_pct: race.l3f_straight_pct as number | undefined,
    l3f_corners: race.l3f_corners as number | undefined,
    l3f_elevation: race.l3f_elevation as number | undefined,
    l3f_hill_start: race.l3f_hill_start as number | undefined,
  };

  // ── ペース・予想タイム ヘッダー情報 ──
  const paceLabels: Record<string, string> = { H: "ハイ", M: "ミドル", S: "スロー" };
  const paceCode = (race.pace_predicted as string) || "";
  const paceJa = paceLabels[paceCode] || "";
  const paceLabel = paceCode ? `${paceCode} ${paceJa}` : "—";
  const f3f = (race.estimated_front_3f as number | undefined)?.toFixed(1) || "—";
  const l3f = (race.estimated_last_3f as number | undefined)?.toFixed(1) || "—";
  const midTime = (race.estimated_mid_time as number | undefined)?.toFixed(1) || "—";
  const raceTimeStr = fmtRaceTime(race.predicted_race_time as number | undefined);
  const _conf = ((race.overall_confidence || race.confidence) as string) || "C";
  const reliLabel = (race.pace_reliability_label as string) || _conf || "—";
  const confCol = confColorClass(_conf);

  return (
    <div className="space-y-2">
      {/* ヘッダー: ペース / 展開自信度 / 予想タイム / ラップ */}
      <div className="flex items-center gap-4 text-sm flex-wrap px-1 py-2 border-b border-border/50">
        <span>
          ペース: <strong>{paceLabel}</strong>
        </span>
        <span className="text-muted-foreground">
          展開自信度 <strong className={confCol}>{reliLabel}</strong>
        </span>
        <span className="text-muted-foreground">|</span>
        <span>
          予想 <strong>{raceTimeStr}</strong>
        </span>
        <span>
          前半3F <strong>{f3f}</strong>
        </span>
        {midTime !== "—" && midTime !== "0.0" && (
          <span>
            道中 <strong>{midTime}</strong>
          </span>
        )}
        <span>
          後半3F <strong>{l3f}</strong>
        </span>
      </div>

      {/* ビジュアル展開予測 */}
      <Tabs defaultValue="course">
        <TabsList className="w-full">
          <TabsTrigger value="course">コース詳細</TabsTrigger>
          <TabsTrigger value="start">前半600m</TabsTrigger>
          <TabsTrigger value="crux">最終コーナー</TabsTrigger>
          <TabsTrigger value="finish">直 線</TabsTrigger>
        </TabsList>
        <TabsContent value="course">
          <CourseDetailView race={raceInfo} venue={venue} />
        </TabsContent>
        <TabsContent value="start">
          <TrackView horses={activeHorses} mode="start" race={raceInfo} />
        </TabsContent>
        <TabsContent value="crux">
          <TrackView horses={activeHorses} mode="crux" race={raceInfo} />
        </TabsContent>
        <TabsContent value="finish">
          <TrackView horses={activeHorses} mode="finish" race={raceInfo} />
        </TabsContent>
        <Legend direction={direction} />
        <TabsList className="w-full">
          <TabsTrigger value="course">コース詳細</TabsTrigger>
          <TabsTrigger value="start">前半600m</TabsTrigger>
          <TabsTrigger value="crux">最終コーナー</TabsTrigger>
          <TabsTrigger value="finish">直 線</TabsTrigger>
        </TabsList>
      </Tabs>
    </div>
  );
}
