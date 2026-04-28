import { useNavigate } from "react-router-dom";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import type { VenueProfileDetail } from "@/api/client";

// 競馬場の特徴テキスト（CourseExplorerのVENUE_INFOと同等）
const VENUE_DESC: Record<string, string> = {
  "01": "右回り小回り。直線292m。先行有利。",
  "02": "外回り芝は直線659mとJRA最長。直線1000mコースあり。",
  "03": "右回り大回り。北海道の洋芝コース。坂なし、直線266m。",
  "04": "右回り小回り。直線262mとJRA最短クラス。",
  "05": "左回り大回り。芝直線525.9m。差し・追込有効。",
  "06": "右回り。直線310m。ゴール前急坂（2m）。先行有利。",
  "07": "左回り大回り。芝直線412.5m。坂あり。",
  "08": "右回り内外回り。3〜4コーナーの下り坂が特徴。",
  "09": "右回り内外回り。芝直線473m（外回り）急坂あり。",
  "10": "右回り。直線293m。先行有利。",
  "30": "右回り。直線330m。北海道の地方競馬。",
  "35": "右回り。地方で芝コースを持つ数少ない競馬場。",
  "36": "右回り。直線220m。小回りで先行有利。",
  "42": "右回り。直線220m。南関東最小。先行有利。",
  "43": "右回り。直線308m。南関東の主要場。",
  "44": "右回り内外回り。直線386m〜400m。南関東最大。",
  "45": "右回り。直線300m。南関東の主要場。",
  "46": "右回り。直線236m。先行有利。",
  "47": "右回り。直線235m。東海地方の地方競馬。",
  "48": "右回り。直線240m。先行有利。",
  "49": "右回り。直線215m。関西最大の地方競馬場。",
  "50": "右回り。直線215m。",
  "51": "右回り。直線218m。",
  "54": "右回り。直線200m。四国唯一の地方競馬場。",
  "55": "右回り。直線295m。九州唯一の競馬場。",
};

// composite重みのラベルと色
const WEIGHT_LABELS: { key: string; label: string; color: string }[] = [
  { key: "ability", label: "能力", color: "bg-emerald-500" },
  { key: "pace", label: "展開", color: "bg-blue-500" },
  { key: "course", label: "適性", color: "bg-amber-500" },
  { key: "jockey", label: "騎手", color: "bg-purple-500" },
  { key: "trainer", label: "調教師", color: "bg-red-500" },
  { key: "bloodline", label: "血統", color: "bg-pink-400" },
];

export function VenueProfileTab({ venue }: { venue: VenueProfileDetail }) {
  const navigate = useNavigate();

  // コースのサマリー集計
  const turfCourses = venue.courses.filter(c => c.surface === "芝");
  const dirtCourses = venue.courses.filter(c => c.surface === "ダート");
  const turfDistances = turfCourses.map(c => c.distance).sort((a, b) => a - b);
  const dirtDistances = dirtCourses.map(c => c.distance).sort((a, b) => a - b);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {/* 基本情報 */}
      <PremiumCard variant="default" padding="md" className="space-y-3">
        <h2 className="font-bold text-base">基本情報</h2>
        <div className="grid grid-cols-2 gap-2 text-sm">
          <div>
            <span className="text-muted-foreground">場コード</span>
            <span className="ml-2 font-semibold">{venue.venue_code}</span>
          </div>
          <div>
            <span className="text-muted-foreground">回り</span>
            <span className="ml-2 font-semibold">{venue.direction}</span>
          </div>
          <div>
            <span className="text-muted-foreground">坂</span>
            <span className="ml-2 font-semibold">{venue.profile.slope_type}</span>
          </div>
          <div>
            <span className="text-muted-foreground">コース数</span>
            <span className="ml-2 font-semibold">{venue.n_courses}</span>
          </div>
          <div>
            <span className="text-muted-foreground">馬場</span>
            <span className="ml-2 font-semibold">
              {venue.has_turf && <span className="text-emerald-600">芝</span>}
              {venue.has_turf && venue.has_dirt && " / "}
              {venue.has_dirt && <span className="text-amber-700">ダート</span>}
            </span>
          </div>
          <div>
            <span className="text-muted-foreground">直線</span>
            <span className="ml-2 font-semibold">{venue.profile.max_straight_m}m</span>
          </div>
        </div>
        {/* 競馬場の特徴テキスト */}
        {VENUE_DESC[venue.venue_code] && (
          <div className="text-sm text-muted-foreground bg-muted/50 rounded p-2 leading-relaxed">
            {VENUE_DESC[venue.venue_code]}
          </div>
        )}
      </PremiumCard>

      {/* コース一覧サマリー */}
      <PremiumCard variant="default" padding="md" className="space-y-3">
        <h2 className="font-bold text-base">コース一覧</h2>
        {turfCourses.length > 0 && (
          <div className="text-sm">
            <span className="text-emerald-600 font-semibold">芝</span>
            <span className="ml-2 text-muted-foreground">{turfCourses.length}コース</span>
            <div className="mt-1 flex flex-wrap gap-1">
              {turfDistances.map(d => (
                <span key={`turf-${d}`} className="text-xs px-2 py-0.5 rounded bg-emerald-50 text-emerald-700">{d}m</span>
              ))}
            </div>
          </div>
        )}
        {dirtCourses.length > 0 && (
          <div className="text-sm">
            <span className="text-amber-700 font-semibold">ダート</span>
            <span className="ml-2 text-muted-foreground">{dirtCourses.length}コース</span>
            <div className="mt-1 flex flex-wrap gap-1">
              {dirtDistances.map(d => (
                <span key={`dirt-${d}`} className="text-xs px-2 py-0.5 rounded bg-amber-50 text-amber-700">{d}m</span>
              ))}
            </div>
          </div>
        )}
      </PremiumCard>

      {/* composite重み */}
      <PremiumCard variant="default" padding="md" className="space-y-3">
        <h2 className="font-bold text-base">総合評価の重み配分</h2>
        {/* スタックバー */}
        <div className="flex h-6 rounded-full overflow-hidden">
          {WEIGHT_LABELS.map((w) => {
            const pct = (venue.composite_weights[w.key] || 0) * 100;
            return pct > 0 ? (
              <div
                key={w.key}
                className={`${w.color} relative group`}
                style={{ width: `${pct}%` }}
                title={`${w.label}: ${pct.toFixed(1)}%`}
              />
            ) : null;
          })}
        </div>
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm">
          {WEIGHT_LABELS.map((w) => {
            const pct = (venue.composite_weights[w.key] || 0) * 100;
            return (
              <div key={w.key} className="flex items-center gap-1.5">
                <div className={`w-3 h-3 rounded-sm ${w.color}`} />
                <span>{w.label}</span>
                <span className="font-bold">{pct.toFixed(1)}%</span>
              </div>
            );
          })}
        </div>
      </PremiumCard>

      {/* 類似競馬場 */}
      <PremiumCard variant="default" padding="md" className="space-y-3">
        <h2 className="font-bold text-base">類似競馬場 TOP5</h2>
        <div className="space-y-2">
          {venue.similar_venues.map((sv, i) => (
            <div
              key={sv.venue_code}
              className="flex items-center gap-3 cursor-pointer hover:bg-muted/50 rounded-md p-1.5 -mx-1.5 transition-colors"
              onClick={() => navigate(`/venue/${sv.venue_code}`)}
            >
              <span className="w-5 text-center text-sm font-bold text-muted-foreground">{i + 1}</span>
              <span className="font-semibold text-sm min-w-[3rem]">{sv.venue}</span>
              <div className="flex-1 h-2 rounded-full bg-muted overflow-hidden">
                <div
                  className="h-full rounded-full bg-blue-500 transition-all"
                  style={{ width: `${sv.similarity * 100}%` }}
                />
              </div>
              <span className="text-sm font-bold tabular-nums w-12 text-right">{(sv.similarity * 100).toFixed(0)}%</span>
            </div>
          ))}
        </div>
      </PremiumCard>
    </div>
  );
}
