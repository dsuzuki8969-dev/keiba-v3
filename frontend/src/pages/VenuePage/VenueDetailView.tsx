import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { useVenueProfile } from "@/api/hooks";
import type { VenueProfileDetail } from "@/api/client";
import { VenueProfileTab } from "./VenueProfileTab";
import { VenueCourseTab } from "./VenueCourseTab";
import { VenueBiasTab } from "./VenueBiasTab";
import { VenueRankingTab } from "./VenueRankingTab";
import { VenueResultsTab } from "./VenueResultsTab";

const TABS = [
  { key: "profile", label: "概要" },
  { key: "course", label: "コース" },
  { key: "bias", label: "バイアス" },
  { key: "ranking", label: "ランキング" },
  { key: "results", label: "成績" },
] as const;

type TabKey = (typeof TABS)[number]["key"];

export function VenueDetailView({ code }: { code: string }) {
  const { data, isLoading } = useVenueProfile(code);
  const [tab, setTab] = useState<TabKey>("profile");
  const navigate = useNavigate();

  if (isLoading) {
    return <div className="flex items-center justify-center py-20 text-sm text-muted-foreground">読み込み中...</div>;
  }

  // 一覧レスポンスではなく詳細レスポンスかチェック
  const venue = (data && "venue" in data && typeof data.venue === "string") ? data as VenueProfileDetail : null;
  if (!venue) {
    return <div className="py-10 text-center text-muted-foreground">競馬場データが見つかりません</div>;
  }

  return (
    <div className="space-y-4">
      {/* ヘッダー */}
      <div className="flex items-center gap-3">
        <Button variant="ghost" size="sm" onClick={() => navigate("/venue")}>
          ← 一覧
        </Button>
        <h1 className="text-xl font-bold">{venue.venue}競馬場</h1>
        <Badge variant={venue.is_jra ? "default" : "secondary"}>
          {venue.is_jra ? "JRA" : "NAR"}
        </Badge>
      </div>

      {/* タブ */}
      <div className="flex gap-1 overflow-x-auto border-b">
        {TABS.map((t) => (
          <button
            key={t.key}
            className={`px-3 py-2 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
              tab === t.key
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground"
            }`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* タブコンテンツ */}
      {tab === "profile" && <VenueProfileTab venue={venue} />}
      {tab === "course" && <VenueCourseTab venue={venue} />}
      {tab === "bias" && <VenueBiasTab code={code} />}
      {tab === "ranking" && <VenueRankingTab code={code} venueName={venue.venue} />}
      {tab === "results" && <VenueResultsTab venueName={venue.venue} />}
    </div>
  );
}
