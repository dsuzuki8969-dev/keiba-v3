import { useState, useMemo } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { useCourseList, useCourseStats } from "@/api/hooks";
import { VENUE_MAP } from "@/lib/constants";
import { BreakdownTable, diffColor } from "@/components/keiba/BreakdownTable";

const VENUE_INFO: Record<string, { name: string; surface: string; desc: string }> = {
  "01": { name: "福島", surface: "芝・ダート", desc: "右回り小回り。直線292m。先行有利。" },
  "02": { name: "新潟", surface: "芝・ダート", desc: "外回り芝は直線659mとJRA最長。直線1000mコースあり。" },
  "03": { name: "札幌", surface: "芝・ダート", desc: "右回り大回り。北海道の洋芝コース。坂なし、直線266m。" },
  "04": { name: "函館", surface: "芝・ダート", desc: "右回り小回り。直線262mとJRA最短クラス。" },
  "05": { name: "東京", surface: "芝・ダート", desc: "左回り大回り。芝直線525.9m。差し・追込有効。" },
  "06": { name: "中山", surface: "芝・ダート", desc: "右回り。直線310m。ゴール前急坂（2m）。先行有利。" },
  "07": { name: "中京", surface: "芝・ダート", desc: "左回り大回り。芝直線412.5m。坂あり。" },
  "08": { name: "京都", surface: "芝・ダート", desc: "右回り内外回り。3〜4コーナーの下り坂が特徴。" },
  "09": { name: "阪神", surface: "芝・ダート", desc: "右回り内外回り。芝直線473m（外回り）急坂あり。" },
  "10": { name: "小倉", surface: "芝・ダート", desc: "右回り。直線293m。先行有利。" },
  "30": { name: "門別", surface: "ダート", desc: "右回り。直線330m。北海道の地方競馬。" },
  "35": { name: "盛岡", surface: "芝・ダート", desc: "右回り。地方で芝コースを持つ数少ない競馬場。" },
  "36": { name: "水沢", surface: "ダート", desc: "右回り。直線220m。小回りで先行有利。" },
  "42": { name: "浦和", surface: "ダート", desc: "右回り。直線220m。南関東最小。先行有利。" },
  "43": { name: "船橋", surface: "ダート", desc: "右回り。直線308m。南関東の主要場。" },
  "44": { name: "大井", surface: "ダート", desc: "右回り内外回り。直線386m〜400m。南関東最大。" },
  "45": { name: "川崎", surface: "ダート", desc: "右回り。直線300m。南関東の主要場。" },
  "46": { name: "金沢", surface: "ダート", desc: "右回り。直線236m。先行有利。" },
  "47": { name: "笠松", surface: "ダート", desc: "右回り。直線235m。東海地方の地方競馬。" },
  "48": { name: "名古屋", surface: "ダート", desc: "右回り。直線240m。先行有利。" },
  "49": { name: "園田", surface: "ダート", desc: "右回り。直線215m。関西最大の地方競馬場。" },
  "50": { name: "園田", surface: "ダート", desc: "右回り。直線215m。" },
  "51": { name: "姫路", surface: "ダート", desc: "右回り。直線218m。" },
  "54": { name: "高知", surface: "ダート", desc: "右回り。直線200m。四国唯一の地方競馬場。" },
  "55": { name: "佐賀", surface: "ダート", desc: "右回り。直線295m。九州唯一の競馬場。" },
  "65": { name: "帯広(ばんえい)", surface: "ばんえい", desc: "直線200m。ばんえい競馬。" },
};

const JRA_CODES = ["03", "04", "01", "02", "05", "06", "07", "08", "09", "10"];
const NAR_CODES = ["30", "35", "36", "42", "43", "44", "45", "46", "47", "48", "49", "50", "51", "54", "55", "65"];

export function CourseExplorer() {
  const [region, setRegion] = useState<"JRA" | "NAR">("JRA");
  const [selectedCourse, setSelectedCourse] = useState<string | null>(null);

  const { data: courseData } = useCourseList();

  // コースキーを競馬場別に整理
  const venueMap = useMemo(() => {
    const keys = (courseData?.keys || []) as string[];
    const m: Record<string, string[]> = {};
    for (const k of keys) {
      const vc = k.split("_")[0];
      if (!m[vc]) m[vc] = [];
      m[vc].push(k);
    }
    return m;
  }, [courseData]);

  const codes = region === "JRA" ? JRA_CODES : NAR_CODES;

  return (
    <div className="space-y-3">
      {/* リージョン切替 */}
      <div className="flex gap-1">
        <Button
          size="sm"
          variant={region === "JRA" ? "default" : "outline"}
          onClick={() => { setRegion("JRA"); setSelectedCourse(null); }}
        >
          JRA ({JRA_CODES.length}場)
        </Button>
        <Button
          size="sm"
          variant={region === "NAR" ? "default" : "outline"}
          onClick={() => { setRegion("NAR"); setSelectedCourse(null); }}
        >
          NAR ({NAR_CODES.length}場)
        </Button>
      </div>

      {/* 競馬場カード */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 gap-3">
        {codes.map((vc) => {
          const info = VENUE_INFO[vc] || { name: vc, surface: "", desc: "" };
          const courseKeys = (venueMap[vc] || []).sort((a, b) => {
            const [, sa, da] = a.split("_");
            const [, sb, db] = b.split("_");
            if (sa !== sb) return sa < sb ? -1 : 1;
            return parseInt(da) - parseInt(db);
          });
          const hasCourse = courseKeys.length > 0;

          return (
            <PremiumCard
              key={vc}
              variant="default"
              padding="sm"
              className={hasCourse ? "space-y-2" : "opacity-50 space-y-2"}
            >
              <div className="flex items-center gap-2">
                <span className="font-bold text-sm">{info.name}</span>
                <span className="text-xs text-muted-foreground">{info.surface}</span>
              </div>
              <p className="text-xs text-muted-foreground leading-relaxed">{info.desc}</p>
              {hasCourse ? (
                <div className="flex flex-wrap gap-1">
                  {courseKeys.map((k) => {
                    const [, sf, ds] = k.split("_");
                    return (
                      <button
                        key={k}
                        onClick={() => setSelectedCourse(k)}
                        className={`text-xs px-2 py-0.5 rounded transition-colors ${
                          selectedCourse === k
                            ? "bg-primary text-primary-foreground"
                            : sf === "芝"
                              ? "bg-turf/10 text-turf hover:bg-turf/20"
                              : sf === "障害"
                                ? "bg-purple-100 text-purple-700 hover:bg-purple-200"
                                : "bg-dirt/10 text-dirt hover:bg-dirt/20"
                        }`}
                      >
                        {sf} {ds}m
                      </button>
                    );
                  })}
                </div>
              ) : (
                <span className="text-xs text-warning">統計未収集</span>
              )}
            </PremiumCard>
          );
        })}
      </div>

      {/* コース詳細モーダル */}
      {selectedCourse && (
        <CourseDetailModal
          courseKey={selectedCourse}
          onClose={() => setSelectedCourse(null)}
        />
      )}
    </div>
  );
}

// コース詳細モーダル — 旧UIと同等の情報量
function CourseDetailModal({ courseKey, onClose }: { courseKey: string; onClose: () => void }) {
  const { data: courseStats, isLoading } = useCourseStats(courseKey);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d = courseStats as any;

  const [vc, sf, ds] = courseKey.split("_");
  const vn = VENUE_MAP[vc] || vc;

  // ⑦ クラス別「その他」折りたたみ
  const [showOther, setShowOther] = useState(false);

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-8 px-4" onClick={onClose}>
      <div className="fixed inset-0 bg-black/40" />
      <div
        className="relative bg-background border border-border rounded-lg shadow-xl w-full max-w-3xl max-h-[85vh] overflow-y-auto p-4 space-y-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* ヘッダー */}
        <div className="flex items-center justify-between">
          <h3 className="text-base font-bold">
            {vn} {sf || ""} {ds || ""}m コース詳細
          </h3>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground text-lg px-2">
            ✕
          </button>
        </div>

        {isLoading && (
          <p className="text-sm text-muted-foreground animate-pulse py-4 text-center">
            読み込み中...
          </p>
        )}

        {d && !isLoading && (
          <div className="space-y-4 text-sm">
            {/* ① 基本情報 + データ取得期間 */}
            <div className="flex gap-4 flex-wrap items-baseline">
              <span>レース数: <strong>{d.race_count || 0}</strong></span>
              <span>出走のべ: <strong>{d.count || 0}</strong></span>
              {d.period?.min_date && d.period?.max_date && (
                <span className="text-xs text-muted-foreground">
                  ({d.period.min_date} 〜 {d.period.max_date})
                </span>
              )}
            </div>

            {/* レコード */}
            {d.record && (
              <div>
                レコード: <strong>{d.record.time_str || "—"}</strong>
                <span className="text-xs text-muted-foreground ml-2">
                  {d.record.date || ""} {d.record.class_name || ""}
                </span>
              </div>
            )}

            {/* ④ コースの特徴（②平均タイムと③ペース傾向の代わり） */}
            {d.course_description && (
              <div className="text-sm text-muted-foreground bg-muted/50 rounded p-2 leading-relaxed">
                <span className="font-semibold text-foreground">コースの特徴:</span>{" "}
                {d.course_description}
              </div>
            )}

            {/* 馬場状態別タイム差 */}
            {d.condition_diff && Object.keys(d.condition_diff).length > 0 && (
              <div>
                <h4 className="font-bold mb-1">馬場状態別タイム（1〜3着平均）</h4>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-xs text-muted-foreground">
                        <th className="text-left py-1 px-1">馬場</th>
                        <th className="text-right py-1 px-1">件数</th>
                        <th className="text-right py-1 px-1">平均タイム</th>
                        <th className="text-right py-1 px-1">良比較</th>
                      </tr>
                    </thead>
                    <tbody>
                      {(["良", "稍重", "重", "不良"] as const).map((cond) => {
                        const v = (d.condition_diff as Record<string, { n: number; avg_str: string; diff: number | null }>)[cond];
                        if (!v) return null;
                        return (
                          <tr key={cond} className="border-b border-border/50">
                            <td className="py-1 px-1 font-semibold">{cond}</td>
                            <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                            <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                            <td className="text-right py-1 px-1 tabular-nums" style={{ color: diffColor(v.diff) }}>
                              {cond === "良" ? "基準" : v.diff != null ? `${v.diff > 0 ? "+" : ""}${v.diff.toFixed(1)}秒` : "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* 季節別タイム差（良馬場基準） */}
            {d.season_diff && Object.keys(d.season_diff).length > 0 && (
              <div>
                <h4 className="font-bold mb-1">季節別タイム（良馬場・1〜3着平均）</h4>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-xs text-muted-foreground">
                        <th className="text-left py-1 px-1">季節</th>
                        <th className="text-right py-1 px-1">件数</th>
                        <th className="text-right py-1 px-1">平均タイム</th>
                        <th className="text-right py-1 px-1">良全体比</th>
                      </tr>
                    </thead>
                    <tbody>
                      {([
                        ["春", "3〜5月"],
                        ["夏", "6〜8月"],
                        ["秋", "9〜11月"],
                        ["冬", "12〜2月"],
                      ] as const).map(([key, label]) => {
                        const v = (d.season_diff as Record<string, { n: number; avg_str: string; diff: number | null }>)[key];
                        if (!v) return null;
                        return (
                          <tr key={key} className="border-b border-border/50">
                            <td className="py-1 px-1 font-semibold">{key}<span className="text-xs text-muted-foreground ml-1">({label})</span></td>
                            <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                            <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                            <td className="text-right py-1 px-1 tabular-nums" style={{ color: diffColor(v.diff) }}>
                              {v.diff != null ? `${v.diff > 0 ? "+" : ""}${v.diff.toFixed(1)}秒` : "—"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* 脚質別成績（⑤ 回収率列追加） */}
            {d.running_style && Object.keys(d.running_style).length > 0 && (
              <div>
                <h4 className="font-bold mb-1">脚質別成績</h4>
                <BreakdownTable
                  data={d.running_style}
                  keyLabel="脚質"
                  sortKeys={["逃げ", "先行", "差し", "追込"]}
                  showRoi
                />
              </div>
            )}

            {/* 枠番別成績（昇順ソート + 回収率列追加） */}
            {d.gate_bias && Object.keys(d.gate_bias).length > 0 && (
              <div>
                <h4 className="font-bold mb-1">枠番別成績</h4>
                <BreakdownTable
                  data={d.gate_bias}
                  keyLabel="枠番"
                  sortKeys={Object.keys(d.gate_bias).sort((a, b) => parseInt(a) - parseInt(b))}
                  showRoi
                />
              </div>
            )}

            {/* ⑦ クラス別成績（ペース情報統合） */}
            {d.class_avg && Object.keys(d.class_avg).length > 0 && (
              <div>
                <h4 className="font-bold mb-1">クラス別成績</h4>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-border text-xs text-muted-foreground">
                        <th className="text-left py-1 px-1">クラス</th>
                        <th className="text-right py-1 px-1">件数</th>
                        <th className="text-right py-1 px-1">平均タイム</th>
                        <th className="text-right py-1 px-1">前3F</th>
                        <th className="text-right py-1 px-1">上り3F</th>
                        <th className="text-right py-1 px-1">ペース</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(
                        d.class_avg as Record<string, { n: number; avg_str: string; first_3f?: number; last_3f?: number; pace_type?: string }>
                      ).map(([cls, v]) => {
                        const isOther = cls.includes("その他");
                        // 「その他」は件数が多い場合折りたたみ
                        if (isOther && v.n > 500 && !showOther) {
                          return (
                            <tr key={cls} className="border-b border-border/50">
                              <td colSpan={6} className="py-1 px-1 text-muted-foreground">
                                <button
                                  className="text-xs underline hover:text-foreground"
                                  onClick={() => setShowOther(true)}
                                >
                                  {cls}（{v.n}件）を表示...
                                </button>
                              </td>
                            </tr>
                          );
                        }
                        return (
                          <tr key={cls} className={`border-b border-border/50 ${isOther ? "text-muted-foreground" : ""}`}>
                            <td className="py-1 px-1 font-semibold">{cls}</td>
                            <td className="text-right py-1 px-1 tabular-nums">{v.n}</td>
                            <td className="text-right py-1 px-1 tabular-nums font-semibold">{v.avg_str}</td>
                            <td className="text-right py-1 px-1 tabular-nums">{v.first_3f?.toFixed(1) ?? "—"}</td>
                            <td className="text-right py-1 px-1 tabular-nums">{v.last_3f?.toFixed(1) ?? "—"}</td>
                            <td className="text-right py-1 px-1">{v.pace_type ?? "—"}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

// BreakdownTable, rankStyle, diffColor は共有コンポーネントに移動済み
