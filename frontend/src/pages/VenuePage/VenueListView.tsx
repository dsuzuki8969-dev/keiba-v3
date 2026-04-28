import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { useVenueProfile } from "@/api/hooks";
import type { VenueProfileItem } from "@/api/client";
import { JRA_VENUE_CODES } from "@/lib/constants";
import { ArrowRight, Mountain, Flag } from "lucide-react";

const JRA_SET = new Set(JRA_VENUE_CODES);

function VenueLogo({ name, isJra }: { name: string; isJra: boolean }) {
  const src = isJra ? "/logos/JRA.jpg" : `/logos/${name}.jpg`;
  const srcFallback = `/logos/${name}.svg`;
  return (
    <img
      src={src}
      alt={name}
      className="flex-shrink-0 w-12 h-12 rounded-lg object-contain"
      onError={(e) => {
        const el = e.currentTarget;
        if (el.src.endsWith(".jpg") && !isJra) {
          el.src = srcFallback;
          return;
        }
        el.style.display = "none";
        const div = document.createElement("div");
        div.className = `flex-shrink-0 w-12 h-12 rounded-lg ${isJra ? "bg-primary" : "bg-orange-500"} flex items-center justify-center`;
        div.innerHTML = `<span class="text-white font-bold text-lg">${name.slice(0, 1)}</span>`;
        el.parentNode?.insertBefore(div, el);
      }}
    />
  );
}

const SLOPE_LABEL: Record<string, string> = {
  "急坂": "急坂",
  "軽坂": "軽坂",
  "坂なし": "平坦",
};

export function VenueListView() {
  const { data, isLoading } = useVenueProfile();
  const [region, setRegion] = useState<"jra" | "nar">("jra");
  const navigate = useNavigate();

  if (isLoading) {
    return <div className="flex items-center justify-center py-20 text-sm text-muted-foreground">読み込み中...</div>;
  }

  const venues: VenueProfileItem[] = (data && "venues" in data ? data.venues : []) as VenueProfileItem[];
  const jra = venues.filter((v) => JRA_SET.has(v.venue_code));
  const nar = venues.filter((v) => !JRA_SET.has(v.venue_code));
  const list = region === "jra" ? jra : nar;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div>
          <div className="section-eyebrow mb-1">Venue Research</div>
          <h1 className="text-xl font-bold border-b border-brand-gold/30 pb-1">競馬場研究</h1>
        </div>
        <div className="flex gap-1">
          <Button
            variant={region === "jra" ? "default" : "outline"}
            size="sm"
            onClick={() => setRegion("jra")}
          >
            JRA ({jra.length})
          </Button>
          <Button
            variant={region === "nar" ? "default" : "outline"}
            size="sm"
            onClick={() => setRegion("nar")}
          >
            地方 ({nar.length})
          </Button>
        </div>
      </div>

      <div
        role="list"
        aria-label={`${region === "jra" ? "JRA" : "NAR"} 競馬場一覧`}
        className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3"
      >
        {list.map((v) => (
          <PremiumCard
            key={v.venue_code}
            variant={v.is_jra ? "default" : "soft"}
            padding="md"
            interactive
            as="button"
            role="listitem"
            aria-label={`${v.venue}競馬場の詳細を開く`}
            onClick={() => navigate(`/venue/${v.venue_code}`)}
            className="group text-left space-y-2 stylish-card-hover border border-border/60"
          >
            <div className="flex items-center gap-3">
              <VenueLogo name={v.venue} isJra={v.is_jra} />
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="heading-section text-lg">{v.venue}</span>
                  <Badge
                    className={
                      v.is_jra
                        ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white border border-brand-navy-light/60 text-[10px]"
                        : "bg-gradient-to-br from-amber-500 to-amber-700 text-white border border-amber-400/60 text-[10px]"
                    }
                  >
                    {v.is_jra ? "JRA" : "NAR"}
                  </Badge>
                </div>
                <div className="flex items-center gap-1.5 text-sm text-muted-foreground">
                  {v.has_turf && <span className="text-turf font-semibold">芝</span>}
                  {v.has_turf && v.has_dirt && <span className="opacity-40">/</span>}
                  {v.has_dirt && <span className="text-dirt font-semibold">ダート</span>}
                  <span className="opacity-40 mx-1">|</span>
                  <span>{v.direction}回り</span>
                </div>
              </div>
              <ArrowRight
                size={14}
                className="text-muted-foreground opacity-0 group-hover:opacity-100 group-hover:translate-x-0.5 transition-all duration-200"
              />
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground pt-1 border-t border-border/50">
              <span className="inline-flex items-center gap-1">
                <Flag size={10} className="text-brand-gold" />
                直線<span className="stat-mono">{v.max_straight_m}</span>m
              </span>
              <span className="inline-flex items-center gap-0.5">
                <Mountain size={10} className={v.slope_type === "急坂" ? "text-destructive" : "text-muted-foreground/70"} />
                {SLOPE_LABEL[v.slope_type] || v.slope_type}
              </span>
              <span>{v.corner_type_dominant}</span>
              <span className="ml-auto text-foreground"><span className="stat-mono">{v.n_courses}</span>コース</span>
            </div>
          </PremiumCard>
        ))}
      </div>
    </div>
  );
}
