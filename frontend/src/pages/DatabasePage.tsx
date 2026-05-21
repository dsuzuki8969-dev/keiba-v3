import { useState } from "react";
import { Input } from "@/components/ui/input";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Database, Search } from "lucide-react";
import { PersonnelTable } from "./DatabasePage/PersonnelTable";
import { CourseExplorer } from "./DatabasePage/CourseExplorer";

const TABS = [
  { key: "jockey", label: "騎手" },
  { key: "trainer", label: "調教師" },
  { key: "sire", label: "種牡馬" },
  { key: "bms", label: "母父（BMS）" },
  { key: "course", label: "コース" },
] as const;

type TabKey = (typeof TABS)[number]["key"];

export default function DatabasePage() {
  const [tab, setTab] = useState<TabKey>("jockey");
  const [search, setSearch] = useState("");

  return (
    <div className="space-y-6 max-w-5xl mx-auto">

      {/* ================================================================ */}
      {/* ヒーローセクション */}
      {/* ================================================================ */}
      <PremiumCard variant="gold" padding="lg">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Database size={10} className="inline mr-1" />
              <span className="section-eyebrow">Personnel Database</span>
            </PremiumCardAccent>
            <PremiumCardTitle>データベース</PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <p className="text-sm text-muted-foreground leading-relaxed">
          騎手・調教師・種牡馬・母父・コースの統計データ
        </p>
      </PremiumCard>

      {/* ================================================================ */}
      {/* タブ — セグメントコントロール風 */}
      {/* ================================================================ */}
      <div
        role="tablist"
        aria-label="データカテゴリ"
        className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg shadow-[var(--shadow-xs)] overflow-x-auto"
      >
        {TABS.map((t) => {
          const active = tab === t.key;
          return (
            <button
              key={t.key}
              role="tab"
              aria-selected={active}
              onClick={() => { setTab(t.key); setSearch(""); }}
              className={[
                "px-3 py-1 text-xs font-semibold rounded-md whitespace-nowrap",
                "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
                active
                  ? "bg-gradient-to-br from-brand-navy to-brand-navy-light text-white shadow-[0_1px_3px_rgba(0,0,0,0.2),0_0_0_1px_var(--brand-gold)]"
                  : "text-muted-foreground hover:text-foreground hover:bg-background/60",
              ].join(" ")}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      {/* ================================================================ */}
      {/* コンテンツ — PremiumCard で包装 */}
      {/* ================================================================ */}
      <PremiumCard variant="default" padding="lg">
        {/* 検索バー（コース以外で表示） */}
        {tab !== "course" && (
          <div className="mb-4">
            <div className="relative max-w-xs">
              <Search size={14} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
              <Input
                type="text"
                placeholder="名前で検索..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-8"
              />
            </div>
          </div>
        )}

        {/* テーブル / エクスプローラ */}
        {tab === "course" ? (
          <CourseExplorer />
        ) : (
          <PersonnelTable type={tab} search={search} />
        )}
      </PremiumCard>
    </div>
  );
}
