import { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { usePersonnelAgg } from "@/api/hooks";

const TYPES = [
  { key: "jockey", label: "騎手" },
  { key: "trainer", label: "調教師" },
  { key: "sire", label: "種牡馬" },
  { key: "bms", label: "母父" },
] as const;

type PersonType = (typeof TYPES)[number]["key"];

function RankingTable({ type, code }: { type: PersonType; code: string }) {
  const qs = `type=${type}&venue=${code}&sort=total&limit=30`;
  const { data, isLoading } = usePersonnelAgg(qs);

  if (isLoading) return <div className="text-sm text-muted-foreground py-4">読み込み中...</div>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const persons: any[] = data?.persons || data?.data || [];

  if (persons.length === 0) {
    return <div className="text-sm text-muted-foreground py-4">データなし</div>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b text-left text-muted-foreground">
            <th className="py-1.5 px-2 w-8">#</th>
            <th className="py-1.5 px-2">名前</th>
            <th className="py-1.5 px-2 text-right">出走</th>
            <th className="py-1.5 px-2 text-right">勝率</th>
            <th className="py-1.5 px-2 text-right">連対率</th>
            <th className="py-1.5 px-2 text-right">複勝率</th>
            <th className="py-1.5 px-2 text-right">回収率</th>
          </tr>
        </thead>
        <tbody>
          {persons.map((p, i) => {
            const roi = p.roi ?? 0;
            return (
              <tr key={p.name || i} className="border-b hover:bg-muted/30">
                <td className="py-1.5 px-2 text-muted-foreground font-medium">{i + 1}</td>
                <td className="py-1.5 px-2 font-semibold">{p.name}</td>
                <td className="py-1.5 px-2 text-right tabular-nums">{p.starts ?? p.total ?? 0}</td>
                <td className="py-1.5 px-2 text-right tabular-nums font-bold text-emerald-600">
                  {(p.win_rate ?? 0).toFixed(1)}%
                </td>
                <td className="py-1.5 px-2 text-right tabular-nums font-semibold text-blue-600">
                  {(p.place2_rate ?? p.rentai_rate ?? 0).toFixed(1)}%
                </td>
                <td className="py-1.5 px-2 text-right tabular-nums font-semibold">
                  {(p.place3_rate ?? p.fukusho_rate ?? 0).toFixed(1)}%
                </td>
                <td className={`py-1.5 px-2 text-right tabular-nums font-bold ${roi >= 100 ? "text-emerald-600" : ""}`}>
                  {roi.toFixed(0)}%
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function VenueRankingTab({ code, venueName }: { code: string; venueName: string }) {
  const [type, setType] = useState<PersonType>("jockey");

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-2">
        <span className="text-sm font-medium text-muted-foreground">{venueName}での成績</span>
        <div className="flex gap-1">
          {TYPES.map((t) => (
            <Button
              key={t.key}
              variant={type === t.key ? "default" : "outline"}
              size="sm"
              onClick={() => setType(t.key)}
            >
              {t.label}
            </Button>
          ))}
        </div>
      </div>

      <PremiumCard variant="default" padding="md">
        <RankingTable type={type} code={code} />
      </PremiumCard>
    </div>
  );
}
