/**
 * D-AI 独自短評生成ロジック
 * 競馬ブック風キャッチフレーズを HorseData のデータから自動生成する純粋関数。
 * バックエンド変更なし・フロント派生のみ。
 */

import type { HorseData } from "@/pages/TodayPage/RaceDetailView";

/**
 * 前走偏差値デルタを計算（直近2走差）
 * past_3_runs[0] = 最新走、[1] = 1走前
 */
function calcRunDevDelta(horse: HorseData): number | null {
  const runs = (horse as Record<string, unknown>).past_3_runs as Array<Record<string, unknown>> | undefined;
  if (!runs || runs.length < 2) return null;
  const latest = runs[0]?.speed_dev as number | undefined;
  const prev = runs[1]?.speed_dev as number | undefined;
  if (latest == null || prev == null || latest <= 0 || prev <= 0) return null;
  return latest - prev;
}

/**
 * 短評生成（優先度順に最初に該当した1件を返す）
 * 優先度: 総合 → 軸別（騎手/調教/展開/血統/適性） → 人気/前走
 */
export function generateHorseSummary(horse: HorseData): string {
  const composite = horse.composite || 0;
  const abilityTotal = horse.ability_total || 0;
  const paceTotal = horse.pace_total || 0;
  const courseTotal = horse.course_total || 0;
  const jockeyDev = horse.jockey_dev || 0;
  const trainerDev = horse.trainer_dev || 0;
  const bloodlineDev = horse.bloodline_dev || 0;
  const trainingDev = horse.training_dev || 0;
  const popularity = horse.popularity ?? horse.predicted_rank;

  // 1. 総合 SS (≥65) → 「実力上位」
  if (composite >= 65) return "実力上位";

  // 2. 総合 S (≥60) → 「中心格」
  if (composite >= 60) return "中心格";

  // 3. 能力 S 以上 (≥60) → 「能力上位」
  if (abilityTotal >= 60) return "能力上位";

  // 4. 騎手 SS/S (≥60) → 「鞍上強力」
  if (jockeyDev >= 60) return "鞍上強力";

  // 5. 調教 SS/S (≥60) → 「直前気配良し」
  if (trainingDev >= 60) return "直前気配良し";

  // 6. 展開 SS/S (≥60) → 「展開向く」
  if (paceTotal >= 60) return "展開向く";

  // 7. 血統 SS/S (≥60) → 「血統適性高」
  if (bloodlineDev >= 60) return "血統適性高";

  // 8. 適性 SS/S (≥60) → 「コース合う」
  if (courseTotal >= 60) return "コース合う";

  // 9. 調教師 SS/S (≥60) → 「厩舎好調」
  if (trainerDev >= 60) return "厩舎好調";

  // 10. 人気薄（8人気以下）かつ総合 A 以上 (≥55) → 「人気以上」
  if (popularity != null && popularity >= 8 && composite >= 55) return "人気以上";

  // 11. 人気上位（3人気以内）かつ総合 C 以下 (<45) → 「危険人気」
  if (popularity != null && popularity <= 3 && composite < 45 && composite > 0) return "危険人気";

  // 12. 前走偏差値急上昇 (delta > 5) → 「前走収穫あり」
  const delta = calcRunDevDelta(horse);
  if (delta != null && delta > 5) return "前走収穫あり";

  // 13. 前走偏差値急下落 (delta < -5) → 「前走凡走」
  if (delta != null && delta < -5) return "前走凡走";

  return "";
}

/**
 * 8 軸印付与（全頭中の指数順位で印シンボルを返す）
 *
 * ★軸別印で使えるのは ◎○▲△★ の 5 種類のみ★
 * （◉/☆/× は総合印専用。feedback_marks.md ルール準拠）
 *
 * 1位 = ◎  2位 = ○  3位 = ▲  4位 = △  5位 = ★  6位以下 = −
 */
export function rankToAxisMark(rank: number): string {
  switch (rank) {
    case 1: return "◎";
    case 2: return "○";
    case 3: return "▲";
    case 4: return "△";
    case 5: return "★";
    default: return "−";
  }
}
