/**
 * AbilityHeatmap — TOP5 偏差値ヒートマップ表
 * --------------------------------------------------------------
 * レーダーチャート（AbilityRadar）の後継コンポーネント。
 * 6軸（能力/展開/適性/騎手/調教/血統）× TOP5 の表形式で可視化。
 * isBanei=true の場合は「展開」列をスキップ。
 *
 * 各セル: グレード文字（大・bold）+ 偏差値（数値）+ 順位（小）
 * モバイル対応: <640px では overflow-x-auto で横スクロール許容
 */

import { useMemo } from "react";
import { MARK_SYMBOL } from "@/lib/keibaUtils";
import { devToGrade, devToHeatColor, GRADE_LEGEND } from "@/lib/devColors";

// AbilityRadar.tsx から移植したインターフェイス（互換性維持）
export interface AbilityEntry {
  horse_no: number;
  horse_name: string;
  mark?: string;
  ability_total?: number | null;
  pace_total?: number | null;
  course_total?: number | null;
  jockey_dev?: number | null;
  training_dev?: number | null;
  bloodline_dev?: number | null;
}

interface AxisDef {
  key: keyof AbilityEntry;
  label: string;
  /** true の場合はバネイ競馬でスキップ */
  skipOnBanei?: boolean;
}

// 6軸定義
const AXES: AxisDef[] = [
  { key: "ability_total",  label: "能力" },
  { key: "pace_total",     label: "展開", skipOnBanei: true },
  { key: "course_total",   label: "適性" },
  { key: "jockey_dev",     label: "騎手" },
  { key: "training_dev",   label: "調教" },
  { key: "bloodline_dev",  label: "血統" },
];

interface Props {
  horses: AbilityEntry[];
  /** 最大表示頭数（既定 5） */
  maxHorses?: number;
  /** バネイ競馬モード: true なら「展開」列をスキップ */
  isBanei?: boolean;
}

export function AbilityHeatmap({ horses, maxHorses = 5, isBanei = false }: Props) {
  // picked を useMemo で先にメモ化し参照を安定させる
  const picked = useMemo(() => horses.slice(0, maxHorses), [horses, maxHorses]);

  // バネイ時は展開列を除外
  const axes = useMemo(
    () => AXES.filter((a) => !(isBanei && a.skipOnBanei)),
    [isBanei],
  );

  // 各軸の全馬偏差値から順位を計算（horses 全体 vs picked 内どちらで計算するか）
  // TOP5 内での相対順位を表示する（レーダー時代と同等）
  const rankMaps = useMemo(() => {
    const maps: Record<string, Record<number, number>> = {};
    for (const axis of axes) {
      const vals = picked.map((h) => {
        const v = h[axis.key] as number | null | undefined;
        return { horse_no: h.horse_no, v: v ?? null };
      });
      const ranks: Record<number, number> = {};
      for (const entry of vals) {
        if (entry.v == null) {
          ranks[entry.horse_no] = 0; // データなし
        } else {
          // 自分より大きい値の数 + 1 = 順位
          ranks[entry.horse_no] =
            vals.filter((x) => x.v != null && x.v > (entry.v as number)).length + 1;
        }
      }
      maps[axis.key] = ranks;
    }
    return maps;
  }, [picked, axes]);

  if (picked.length === 0) {
    return (
      <div className="text-sm text-muted-foreground p-4 text-center">
        偏差値データがありません
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* メインテーブル: モバイルで横スクロール許容 */}
      <div className="overflow-x-auto">
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr>
              {/* 印・馬名列ヘッダ */}
              <th className="text-left py-1.5 px-1.5 text-muted-foreground font-medium whitespace-nowrap border-b border-border sticky left-0 bg-background z-10">
                馬名
              </th>
              {/* 各軸ヘッダ */}
              {axes.map((axis) => (
                <th
                  key={axis.key}
                  className="text-center py-1.5 px-1 text-muted-foreground font-medium whitespace-nowrap border-b border-border min-w-[52px]"
                >
                  {axis.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {picked.map((horse, rowIdx) => {
              // 生キーからシンボルへ変換（AbilityTable 側では生キーのまま渡す）
              const markSym = horse.mark
                ? MARK_SYMBOL[horse.mark] ?? horse.mark
                : "";
              // 行ストライプクラス（sticky td でも透けないよう直接適用）
              const rowBgCls = rowIdx % 2 === 0 ? "bg-background" : "bg-muted/20";
              return (
                <tr
                  key={horse.horse_no}
                  className={rowBgCls}
                >
                  {/* 馬名列（印 + 馬名） — sticky 側に bg クラスを直接付与してスクロール時の透け防止 */}
                  <td className={`py-1.5 px-1.5 whitespace-nowrap sticky left-0 z-10 border-b border-border/30 ${rowBgCls}`}
                  >
                    <div className="flex items-center gap-1">
                      {markSym && (
                        <span className="text-sm font-bold">{markSym}</span>
                      )}
                      <span className="font-semibold text-foreground truncate max-w-[80px] sm:max-w-[120px]">
                        {horse.horse_name}
                      </span>
                    </div>
                  </td>
                  {/* 各軸セル */}
                  {axes.map((axis) => {
                    const rawVal = horse[axis.key] as number | null | undefined;
                    const val = rawVal != null && !Number.isNaN(rawVal) ? rawVal : null;
                    const colors = devToHeatColor(val);
                    const rank = val != null ? rankMaps[axis.key]?.[horse.horse_no] : 0;

                    return (
                      <td
                        key={axis.key}
                        className={`py-1 px-0.5 text-center border-b border-border/20 ${colors.bg} ${colors.text}`}
                      >
                        {val != null ? (
                          <div className="flex flex-col items-center leading-tight">
                            {/* 1行目: グレード文字（大・bold） */}
                            <span className="text-sm font-bold leading-none">
                              {devToGrade(val)}
                            </span>
                            {/* 2行目: 偏差値（tabular-nums） */}
                            <span className="text-[10px] tabular-nums leading-none mt-0.5 opacity-90">
                              {val.toFixed(1)}
                            </span>
                            {/* 3行目: 順位 */}
                            {rank != null && rank > 0 && (
                              <span className="text-[9px] leading-none mt-0.5 opacity-75">
                                ({rank}位)
                              </span>
                            )}
                          </div>
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* 凡例（足元）: SS/S/A/B/C/D の色サンプル */}
      <div className="flex flex-wrap items-center gap-x-2 gap-y-1 px-1">
        {GRADE_LEGEND.map((item) => (
          <div key={item.grade} className="flex items-center gap-1">
            <span
              className={`inline-block w-3.5 h-3.5 rounded-sm ${item.bgClass}`}
              aria-hidden="true"
            />
            <span className="text-[10px] text-muted-foreground">{item.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
