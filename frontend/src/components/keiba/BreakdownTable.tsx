import { useMemo } from "react";

// ランキングスタイル: 1位=緑太字, 2位=青太字, 3位=赤太字, 平均=黒普通, 平均以下=灰色
// v6.1.21: hex ハードコード → design-token CSS 変数参照に統一
//   1位 → --mark-tekipan (緑)   2位 → --mark-taikou (青)   3位 → --mark-tannuke (赤)
//   平均以下 → --muted-foreground
export function rankStyle(val: number, ranked: number[], avg: number): { color: string; fontWeight?: string } {
  if (ranked[0] === val) return { color: "var(--mark-tekipan)", fontWeight: "bold" };
  if (ranked.length > 1 && ranked[1] === val) return { color: "var(--mark-taikou)", fontWeight: "bold" };
  if (ranked.length > 2 && ranked[2] === val) return { color: "var(--mark-tannuke)", fontWeight: "bold" };
  if (val < avg) return { color: "var(--muted-foreground)" };
  return { color: "" };
}

// タイム差の色分け
export function diffColor(diff: number | null | undefined): string {
  if (diff == null || isNaN(diff) || diff === 0) return "";
  if (diff < 0) return "var(--positive)";
  if (diff > 0) return "var(--negative)";
  return "";
}

// 汎用ブレイクダウンテーブル（脚質別/枠番別等）— ランキング色分け + 回収率対応
export function BreakdownTable({
  data,
  keyLabel,
  sortKeys,
  showRoi,
}: {
  data: Record<string, Record<string, number>>;
  keyLabel: string;
  sortKeys?: string[];
  showRoi?: boolean;
}) {
  const entries = useMemo(() => {
    if (sortKeys) {
      const seen = new Set<string>();
      const result: [string, Record<string, number>][] = [];
      for (const k of sortKeys) {
        if (data[k]) { result.push([k, data[k]]); seen.add(k); }
      }
      Object.entries(data).forEach(([k, v]) => {
        if (!seen.has(k)) result.push([k, v]);
      });
      return result;
    }
    return Object.entries(data).sort(
      (a, b) => (b[1].total || b[1].runs || 0) - (a[1].total || a[1].runs || 0)
    );
  }, [data, sortKeys]);

  // 各列のランキング・平均を事前計算
  const { wrRanked, p2rRanked, p3rRanked, roiRanked, wrAvg, p2rAvg, p3rAvg, roiAvg } = useMemo(() => {
    const vals: { wr: number; p2r: number; p3r: number; roi: number }[] = [];
    for (const [, st] of entries) {
      const t = st.total || st.runs || 0;
      if (!t) continue;
      const wr = st.win_rate != null ? +st.win_rate : ((st.win || 0) / t) * 100;
      const p2r = st.place2_rate != null ? +st.place2_rate : ((st.place2 || 0) / t) * 100;
      const p3r = st.place3_rate != null ? +st.place3_rate : ((st.place3 || 0) / t) * 100;
      vals.push({ wr, p2r, p3r, roi: st.roi ?? 0 });
    }
    const sorted = (arr: number[]) => [...arr].sort((a, b) => b - a);
    const avg = (arr: number[]) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
    const wrs = vals.map(v => v.wr);
    const p2rs = vals.map(v => v.p2r);
    const p3rs = vals.map(v => v.p3r);
    const rois = vals.map(v => v.roi);
    return {
      wrRanked: sorted(wrs), p2rRanked: sorted(p2rs), p3rRanked: sorted(p3rs), roiRanked: sorted(rois),
      wrAvg: avg(wrs), p2rAvg: avg(p2rs), p3rAvg: avg(p3rs), roiAvg: avg(rois),
    };
  }, [entries]);

  if (!entries.length) return <p className="text-xs text-muted-foreground">データなし</p>;

  const hasRoiData = showRoi && entries.some(([, st]) => st.roi != null && st.roi > 0);

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-xs text-muted-foreground">
            <th className="text-left py-1 px-1">{keyLabel}</th>
            <th className="text-right py-1 px-1">出走</th>
            <th className="text-right py-1 px-1">成績</th>
            <th className="text-right py-1 px-1">勝率</th>
            <th className="text-right py-1 px-1">連対率</th>
            <th className="text-right py-1 px-1">複勝率</th>
            {hasRoiData && <th className="text-right py-1 px-1">単回率</th>}
          </tr>
        </thead>
        <tbody>
          {entries.map(([key, st]) => {
            const t = st.total || st.runs || 0;
            const wrVal = t ? (st.win_rate != null ? +st.win_rate : ((st.win || 0) / t) * 100) : 0;
            const p2rVal = t ? (st.place2_rate != null ? +st.place2_rate : ((st.place2 || 0) / t) * 100) : 0;
            const p3rVal = t ? (st.place3_rate != null ? +st.place3_rate : ((st.place3 || 0) / t) * 100) : 0;
            const wr = t ? wrVal.toFixed(1) + "%" : "—";
            const p2r = t ? p2rVal.toFixed(1) + "%" : "—";
            const p3r = t ? p3rVal.toFixed(1) + "%" : "—";
            const lose = t - (st.place3 || 0);
            const record = `${st.win || 0}-${(st.place2 || 0) - (st.win || 0)}-${(st.place3 || 0) - (st.place2 || 0)}-${Math.max(0, lose)}`;
            const roi = st.roi;
            const wrStyle = t ? rankStyle(wrVal, wrRanked, wrAvg) : {};
            const p2rStyle = t ? rankStyle(p2rVal, p2rRanked, p2rAvg) : {};
            const p3rStyle = t ? rankStyle(p3rVal, p3rRanked, p3rAvg) : {};
            const roiStyle = hasRoiData && roi != null ? rankStyle(roi, roiRanked, roiAvg) : {};
            return (
              <tr key={key} className="border-b border-border/50">
                <td className="py-1 px-1 font-semibold">{key}</td>
                <td className="text-right py-1 px-1 tabular-nums">{t}</td>
                <td className="text-right py-1 px-1 text-muted-foreground tabular-nums text-xs">{record}</td>
                <td className="text-right py-1 px-1 tabular-nums" style={wrStyle}>{wr}</td>
                <td className="text-right py-1 px-1 tabular-nums" style={p2rStyle}>{p2r}</td>
                <td className="text-right py-1 px-1 tabular-nums" style={p3rStyle}>{p3r}</td>
                {hasRoiData && (
                  <td className="text-right py-1 px-1 tabular-nums" style={roiStyle}>
                    {roi != null ? `${roi.toFixed(1)}%` : "—"}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
