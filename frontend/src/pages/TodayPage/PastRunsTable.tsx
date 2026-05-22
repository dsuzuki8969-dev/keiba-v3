/**
 * 前三走テーブルコンポーネント
 *
 * HorseCardPC.tsx から切り出し。PastRunsPanel / HorseCardPC 両方から利用。
 * バンドル最適化: HorseCardPC の重い依存 (HorseDiagnosis, recharts 等) を
 * RaceDetailView チャンクに巻き込まないための分離。
 */
import { devGrade, gradeCls, posCls, pastRunResultUrl } from "@/lib/constants";
import type { PastRunData } from "./RaceDetailView";

/** タイム表示 (このファイル内でのみ使用) */
function fmtTime(sec: number | null | undefined): string {
  if (sec == null || sec <= 0) return "—";
  const m = Math.floor(sec / 60);
  const s = sec - m * 60;
  return m > 0 ? `${m}:${s.toFixed(1).padStart(4, "0")}` : s.toFixed(1);
}

/** 馬場状態カラー (このファイル内でのみ使用) */
function condCls(cond: string | undefined): string {
  if (cond === "重" || cond === "不良") return "text-blue-600";
  return "";
}

export function PastRunsTable({ runs }: { runs: PastRunData[] }) {
  if (!runs || runs.length === 0) return null;
  return (
    <div>
      <div className="text-[13px] font-bold text-muted-foreground mb-1 border-l-[3px] border-blue-500 pl-2">前三走成績</div>
      <div className="overflow-x-auto">
        <table className="text-[14px] border-collapse w-full min-w-[720px]">
          <thead>
            <tr className="text-[12px] text-muted-foreground border-b border-border bg-muted/40">
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
              const jk = r.jockey || ""; // slice 削除: CSS truncate で制御
              const isBaneiRun = r.venue === "帯広";
              const corners = r.positions_corners || "";
              const passStr = isBaneiRun ? "" : corners;
              const l3f = r.last_3f;
              const l3fRank = r.last_3f_rank;
              const l3fCls = l3fRank === 1 ? "text-emerald-600 font-bold" : l3fRank === 2 ? "text-blue-600 font-bold" : l3fRank === 3 ? "text-red-600 font-bold" : "";
              const ft = r.finish_time ?? r.finish_time_sec;
              const margin = r.margin;
              const devVal = r.speed_dev;
              // A案（2026-04-26）: clamp が -50 まで拡張されたためマイナスも数値表示
              // isFloorClamped は撤回。null のみ「—」表示
              return (
                <tr key={r.race_id ?? `${r.date}-${r.venue}-${r.race_no}-${i}`} className="border-b border-border/30 hover:bg-brand-gold/5">
                  <td className="py-1 px-1.5 whitespace-nowrap">
                    {(() => {
                      const url = pastRunResultUrl(r.race_id, r.date, r.venue, r.result_cname, r.race_no);
                      return url ? (
                        <a href={url} target="_blank" rel="noopener noreferrer"
                          className="text-blue-600 dark:text-blue-400 hover:underline underline"
                          title="レース結果を見る">{dateShort}</a>
                      ) : dateShort;
                    })()}
                  </td>
                  <td className="py-1 px-1.5 whitespace-nowrap">{r.venue || "—"}</td>
                  <td className="py-1 px-1.5 whitespace-nowrap">
                    {surf}{dist > 0 ? dist : ""}
                    <span className={`ml-0.5 ${condCls(cond)}`}>{cond}</span>
                  </td>
                  <td className="py-1 px-1.5 max-w-[120px] truncate" title={cls}>{cls || "—"}</td>
                  <td className={`text-center py-1 px-1.5 ${posCls(fp)}`}>
                    {fp != null && fp > 0 ? `${fp}着` : "—"}/{fc || "?"}
                    {(r as Record<string, unknown>).popularity != null && (
                      <span className="text-muted-foreground ml-0.5 text-[11px]">({String((r as Record<string, unknown>).popularity)}人気)</span>
                    )}
                  </td>
                  <td className="text-center py-1 px-1.5 whitespace-nowrap tabular-nums">
                    {devVal != null ? (
                      // 偏差値を数値表示（マイナス含む）。null のみ「—」
                      <>
                        {devVal.toFixed(1)}
                        <span className={`ml-0.5 ${gradeCls(devGrade(devVal))}`}>({devGrade(devVal)})</span>
                      </>
                    ) : "—"}
                  </td>
                  <td className="py-1 px-1.5 max-w-[6.5em] truncate" title={jk || "—"}>{jk || "—"}</td>
                  <td className="text-center py-1 px-1.5 text-[13px] tabular-nums">{passStr || "—"}</td>
                  <td className={`text-right py-1 px-1.5 tabular-nums ${l3fCls}`}>{l3f != null && l3f > 0 ? l3f.toFixed(1) : "—"}</td>
                  <td className="text-right py-1 px-1.5 tabular-nums">{fmtTime(ft)}</td>
                  <td className="text-right py-1 px-1.5 tabular-nums">
                    {/* 着差: 1着 0.0 でも +0.0 で表示。異常値（取消等）は — */}
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
