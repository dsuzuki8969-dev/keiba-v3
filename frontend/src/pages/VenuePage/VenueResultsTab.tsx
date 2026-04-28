import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { useResultsDetailed } from "@/api/hooks";

function fmtPct(v: number): string {
  return (v ?? 0).toFixed(1) + "%";
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function MarkTable({ byMark }: { byMark: Record<string, any> }) {
  const markOrder = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"];
  const marks = markOrder.filter((m) => byMark[m]);

  if (marks.length === 0) return <div className="text-sm text-muted-foreground">印別データなし</div>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b text-left text-muted-foreground">
            <th className="py-1.5 px-2">印</th>
            <th className="py-1.5 px-2 text-right">出走</th>
            <th className="py-1.5 px-2 text-right">勝率</th>
            <th className="py-1.5 px-2 text-right">連対率</th>
            <th className="py-1.5 px-2 text-right">複勝率</th>
            <th className="py-1.5 px-2 text-right">回収率</th>
          </tr>
        </thead>
        <tbody>
          {marks.map((m) => {
            const s = byMark[m];
            return (
              <tr key={m} className="border-b">
                <td className="py-1.5 px-2 font-bold text-base">{m}</td>
                <td className="py-1.5 px-2 text-right tabular-nums">{s.total || 0}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-bold text-emerald-600">{fmtPct(s.win_rate)}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-semibold text-blue-600">{fmtPct(s.place2_rate ?? s.rentai_rate)}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-semibold">{fmtPct(s.place_rate ?? s.place3_rate ?? s.fukusho_rate)}</td>
                <td className={`py-1.5 px-2 text-right tabular-nums font-bold ${(s.tansho_roi ?? s.roi ?? 0) >= 100 ? "text-emerald-600" : ""}`}>
                  {(s.tansho_roi ?? s.roi ?? 0).toFixed(0)}%
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function ConfTable({ byConf }: { byConf: Record<string, any> }) {
  const confOrder = ["SS", "S", "A", "B", "C", "D", "F"];
  const confs = confOrder.filter((c) => byConf[c]);

  if (confs.length === 0) return <div className="text-sm text-muted-foreground">自信度別データなし</div>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b text-left text-muted-foreground">
            <th className="py-1.5 px-2">自信度</th>
            <th className="py-1.5 px-2 text-right">出走</th>
            <th className="py-1.5 px-2 text-right">勝率</th>
            <th className="py-1.5 px-2 text-right">複勝率</th>
            <th className="py-1.5 px-2 text-right">回収率</th>
          </tr>
        </thead>
        <tbody>
          {confs.map((c) => {
            const s = byConf[c];
            return (
              <tr key={c} className="border-b">
                <td className="py-1.5 px-2 font-bold">{c}</td>
                <td className="py-1.5 px-2 text-right tabular-nums">{s.total || 0}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-bold text-emerald-600">{fmtPct(s.win_rate)}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-semibold">{fmtPct(s.place_rate ?? s.place3_rate ?? s.fukusho_rate)}</td>
                <td className={`py-1.5 px-2 text-right tabular-nums font-bold ${(s.tansho_roi ?? s.roi ?? 0) >= 100 ? "text-emerald-600" : ""}`}>
                  {(s.tansho_roi ?? s.roi ?? 0).toFixed(0)}%
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function VenueResultsTab({ venueName }: { venueName: string }) {
  const { data, isLoading } = useResultsDetailed("all");

  if (isLoading) return <div className="text-sm text-muted-foreground py-4">読み込み中...</div>;
  if (!data) return <div className="text-sm text-muted-foreground py-4">データなし</div>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const d = data as any;
  // all カテゴリの by_venue からこの場のデータを取得
  const allData = d.all || {};
  const byVenue = allData.by_venue || {};
  const venueData = byVenue[venueName] || {};

  if (!venueData.total_races) {
    return <div className="text-sm text-muted-foreground py-4">{venueName}の成績データがありません</div>;
  }

  const byMark = venueData.by_mark || {};
  const byConf = venueData.by_conf || {};

  return (
    <div className="space-y-4">
      {/* サマリー */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <PremiumCard variant="default" padding="sm" className="text-center">
          <div className="text-xs text-muted-foreground">レース数</div>
          <div className="text-xl font-bold">{venueData.total_races || 0}</div>
        </PremiumCard>
        <PremiumCard variant="default" padding="sm" className="text-center">
          <div className="text-xs text-muted-foreground">的中率</div>
          <div className="text-xl font-bold text-emerald-600">{fmtPct(venueData.hit_rate ?? 0)}</div>
        </PremiumCard>
        <PremiumCard variant="default" padding="sm" className="text-center">
          <div className="text-xs text-muted-foreground">回収率</div>
          <div className={`text-xl font-bold ${(venueData.roi ?? 0) >= 100 ? "text-emerald-600" : "text-red-600"}`}>
            {(venueData.roi ?? 0).toFixed(0)}%
          </div>
        </PremiumCard>
        <PremiumCard variant="default" padding="sm" className="text-center">
          <div className="text-xs text-muted-foreground">収支</div>
          <div className={`text-xl font-bold ${(venueData.profit ?? 0) >= 0 ? "text-emerald-600" : "text-red-600"}`}>
            {(venueData.profit ?? 0) >= 0 ? "+" : ""}{(venueData.profit ?? 0).toLocaleString()}
          </div>
        </PremiumCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* 印別成績 */}
        <PremiumCard variant="default" padding="md" className="space-y-2">
          <h2 className="font-bold text-base">印別成績</h2>
          <MarkTable byMark={byMark} />
        </PremiumCard>

        {/* 自信度別成績 */}
        <PremiumCard variant="default" padding="md" className="space-y-2">
          <h2 className="font-bold text-base">自信度別成績</h2>
          <ConfTable byConf={byConf} />
        </PremiumCard>
      </div>
    </div>
  );
}
