import { useState, useMemo } from "react";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { useFeatureImportance } from "@/api/hooks";
import type { FeatureImportanceItem } from "@/api/client";
import { Cpu, BarChart3 } from "lucide-react";

// カテゴリのスタイル定義（7因子: 能力/展開/適性/騎手/調教師/血統/調教）
const CAT_STYLES: Record<string, { bg: string; text: string; border: string }> = {
  能力:   { bg: "bg-emerald-50", text: "text-emerald-700", border: "border-emerald-300" },
  展開:   { bg: "bg-blue-50",    text: "text-blue-700",    border: "border-blue-300" },
  適性:   { bg: "bg-teal-50",    text: "text-teal-700",    border: "border-teal-300" },
  騎手:   { bg: "bg-orange-50",  text: "text-orange-700",  border: "border-orange-300" },
  調教師: { bg: "bg-purple-50",  text: "text-purple-700",  border: "border-purple-300" },
  血統:   { bg: "bg-yellow-50",  text: "text-yellow-700",  border: "border-yellow-300" },
  調教:   { bg: "bg-rose-50",    text: "text-rose-700",    border: "border-rose-300" },
};

const BAR_COLORS: Record<string, string> = {
  能力: "bg-emerald-500",
  展開: "bg-blue-500",
  適性: "bg-teal-500",
  騎手: "bg-orange-500",
  調教師: "bg-purple-500",
  血統: "bg-yellow-500",
  調教: "bg-rose-500",
};

export default function AboutPage() {
  const { data, isLoading, error } = useFeatureImportance();
  const [filterCat, setFilterCat] = useState("all");
  const [search, setSearch] = useState("");

  // カテゴリ別カウント
  const catCounts = useMemo(() => {
    if (!data) return {};
    const counts: Record<string, number> = {};
    for (const d of data) {
      counts[d.cat] = (counts[d.cat] || 0) + 1;
    }
    return counts;
  }, [data]);

  // フィルタ適用
  const filtered = useMemo(() => {
    if (!data) return [];
    const q = search.toLowerCase();
    return data.filter(
      (d: FeatureImportanceItem) =>
        (filterCat === "all" || d.cat === filterCat) &&
        (!q ||
          (d.label || d.name).toLowerCase().includes(q) ||
          (d.desc || "").toLowerCase().includes(q))
    );
  }, [data, filterCat, search]);

  const maxPct = data?.[0]?.pct ?? 1;

  return (
    <div className="space-y-6 max-w-5xl mx-auto">
      {/* システム概要 */}
      <PremiumCard variant="default" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Cpu size={10} className="inline mr-1" />
              <span className="section-eyebrow">System</span>
            </PremiumCardAccent>
            <PremiumCardTitle className="border-b border-brand-gold/30 pb-1">D-AI Keiba 予想システム</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div>
          <div className="space-y-3 text-sm text-muted-foreground leading-relaxed">
            <p>
              LightGBM分割モデル（42モデル）とPyTorch Neural Rankerの
              アンサンブルを中核とした競馬予想システムです。
              人気・オッズの影響を完全に排除し、純粋なデータ分析のみで
              JRA中央競馬およびNAR地方競馬の全レースを予想します。
            </p>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
              <div className="space-y-1.5">
                <h4 className="section-eyebrow mb-1">分析エンジン</h4>
                <ul className="text-xs space-y-0.5 list-disc list-inside">
                  <li>LightGBM 42分割モデル（会場25・面距離8・汎用4・確率3・ランカー1・ばんえい1）</li>
                  <li>PyTorch Neural Ranker（順位学習）</li>
                  <li>ML合議制による印判定（win_prob × composite）</li>
                  <li>105特徴量による多角的評価</li>
                </ul>
              </div>
              <div className="space-y-1.5">
                <h4 className="section-eyebrow mb-1">7因子コンポジット</h4>
                <ul className="text-xs space-y-0.5 list-disc list-inside">
                  <li>能力値（走破タイム・着差・調子偏差値）</li>
                  <li>展開予測（ペース・脚質・位置取り）</li>
                  <li>コース適性（実績・形状・騎手コース相性）</li>
                  <li>騎手・調教師・血統評価</li>
                  <li>調教（追切タイム・仕上がり・厩舎評価）</li>
                </ul>
              </div>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 pt-1">
              <div className="space-y-1.5">
                <h4 className="section-eyebrow mb-1">自信度 v5（市場フリー）</h4>
                <ul className="text-xs space-y-0.5 list-disc list-inside">
                  <li>6信号スコア（composite gap・ML一致・多因子一致 等）</li>
                  <li>市場評価（人気・オッズ）は一切不使用</li>
                  <li>JRA/NAR別パーセンタイル閾値で SS〜D 判定</li>
                  <li>SS/S は win_prob・gap ゲートで厳格フィルタ</li>
                </ul>
              </div>
              <div className="space-y-1.5">
                <h4 className="section-eyebrow mb-1">評価スケール</h4>
                <ul className="text-xs space-y-0.5 list-disc list-inside">
                  <li>全項目 20〜100 の偏差値スケールで統一</li>
                  <li>SS（≥65）S（≥60）A（≥55）B（≥50）C（≥45）D（&lt;45）</li>
                  <li>人気順位・オッズによる補正は一切なし</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </PremiumCard>

      {/* 特徴量重要度 */}
      <PremiumCard variant="default" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <BarChart3 size={10} className="inline mr-1" />
              <span className="section-eyebrow">Feature Importance</span>
            </PremiumCardAccent>
            <PremiumCardTitle>特徴量重要度</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div>
          {isLoading && (
            <p className="text-sm text-muted-foreground">読み込み中...</p>
          )}
          {error && (
            <p className="text-sm text-destructive">
              読み込み失敗: {(error as Error).message}
            </p>
          )}
          {data && (
            <div className="space-y-3">
              {/* カテゴリフィルター */}
              <div className="flex gap-1.5 flex-wrap items-center">
                <span className="text-xs text-muted-foreground mr-1">カテゴリ:</span>
                <button
                  onClick={() => setFilterCat("all")}
                  className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                    filterCat === "all"
                      ? "bg-primary text-primary-foreground border-primary"
                      : "bg-secondary text-secondary-foreground border-border hover:bg-muted"
                  }`}
                >
                  全て({data.length})
                </button>
                {Object.keys(CAT_STYLES).map((cat) =>
                  catCounts[cat] ? (
                    <button
                      key={cat}
                      onClick={() => setFilterCat(cat)}
                      className={`px-2.5 py-1 text-xs rounded-full border transition-colors ${
                        filterCat === cat
                          ? `${CAT_STYLES[cat].bg} ${CAT_STYLES[cat].text} ${CAT_STYLES[cat].border}`
                          : `bg-secondary text-secondary-foreground ${CAT_STYLES[cat].border} hover:${CAT_STYLES[cat].bg}`
                      }`}
                    >
                      {cat}({catCounts[cat]})
                    </button>
                  ) : null
                )}
              </div>

              {/* 検索 */}
              <Input
                type="text"
                placeholder="特徴量名・説明で検索..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="w-full"
              />

              {/* テーブル */}
              <div className="max-h-[540px] overflow-y-auto border rounded-md">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-muted">
                    <tr>
                      <th className="text-right px-2 py-1.5 section-eyebrow w-10">
                        #
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow">
                        特徴量名
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow hidden sm:table-cell">
                        説明
                      </th>
                      <th className="text-center px-2 py-1.5 section-eyebrow w-20">
                        カテゴリ
                      </th>
                      <th className="text-left px-2 py-1.5 section-eyebrow w-40">
                        寄与度
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {filtered.map((d: FeatureImportanceItem) => {
                      const barW = Math.max(1, Math.round((d.pct / maxPct) * 100));
                      const pctStr = d.pct >= 0.01 ? `${d.pct.toFixed(2)}%` : "<0.01%";
                      const cs = CAT_STYLES[d.cat] ?? CAT_STYLES.コース;
                      const barColor = BAR_COLORS[d.cat] ?? "bg-gray-400";
                      return (
                        <tr key={d.rank} className="border-t hover:bg-brand-gold/5 transition-colors">
                          <td className="text-right px-2 py-1 stat-mono text-xs text-muted-foreground">
                            {d.rank}
                          </td>
                          <td className="px-2 py-1 text-xs font-semibold whitespace-nowrap">
                            {d.label || d.name}
                          </td>
                          <td className="px-2 py-1 text-xs text-muted-foreground leading-snug hidden sm:table-cell">
                            {d.desc || ""}
                          </td>
                          <td className="px-2 py-1 text-center">
                            <Badge
                              variant="outline"
                              className={`text-[10px] ${cs.bg} ${cs.text} ${cs.border}`}
                            >
                              {d.cat}
                            </Badge>
                          </td>
                          <td className="px-2 py-1">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2 bg-muted rounded-full overflow-hidden">
                                <div
                                  className={`h-full ${barColor} rounded-full`}
                                  style={{ width: `${barW}%` }}
                                />
                              </div>
                              <span className="stat-mono text-[10px] text-muted-foreground w-12 text-right">
                                {pctStr}
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </PremiumCard>
    </div>
  );
}
