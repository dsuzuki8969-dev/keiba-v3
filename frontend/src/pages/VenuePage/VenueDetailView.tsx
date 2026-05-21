import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { useVenueProfile } from "@/api/hooks";
import type { VenueProfileDetail } from "@/api/client";
import { VenueProfileTab } from "./VenueProfileTab";
import { VenueCourseTab } from "./VenueCourseTab";
import { VenueBiasTab } from "./VenueBiasTab";
import { VenueRankingTab } from "./VenueRankingTab";
import { VenueResultsTab } from "./VenueResultsTab";
import { MapPin } from "lucide-react";

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
    <div className="space-y-8 max-w-5xl mx-auto">
      {/* ヘッダー */}
      <PremiumCard variant="gold" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <MapPin size={10} className="inline mr-1" />
              <span className="section-eyebrow">Venue Detail</span>
            </PremiumCardAccent>
            <div className="flex items-center gap-3">
              <PremiumCardTitle>{venue.venue}競馬場</PremiumCardTitle>
              <Badge
                className={
                  venue.is_jra
                    ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white border border-brand-navy-light/60 text-[10px]"
                    : "bg-gradient-to-br from-amber-500 to-amber-700 text-white border border-amber-400/60 text-[10px]"
                }
              >
                {venue.is_jra ? "JRA" : "NAR"}
              </Badge>
            </div>
          </div>
        </PremiumCardHeader>
        <div className="flex items-center gap-2 mt-4">
          <Button variant="outline" size="sm" onClick={() => navigate("/venue")}>
            ← 一覧に戻る
          </Button>
        </div>
      </PremiumCard>

      {/* タブ */}
      <div role="tablist" className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg overflow-x-auto">
        {TABS.map((t) => (
          <button
            key={t.key}
            role="tab"
            aria-selected={tab === t.key}
            aria-controls={`venue-tabpanel-${t.key}`}
            className={`px-3 py-1 text-xs font-semibold rounded-md whitespace-nowrap transition-all ${
              tab === t.key
                ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                : "text-muted-foreground hover:text-foreground hover:bg-background/60"
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
