import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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
    <div className="space-y-3 max-w-5xl mx-auto">
      {/* ページ eyebrow */}
      <div className="section-eyebrow">Personnel Database</div>
      {/* タブ */}
      <div className="flex gap-1 overflow-x-auto">
        {TABS.map((t) => (
          <Button
            key={t.key}
            size="sm"
            variant={tab === t.key ? "default" : "outline"}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </Button>
        ))}
      </div>

      {/* 検索バー */}
      {tab !== "course" && (
        <Input
          type="text"
          placeholder="名前で検索..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="max-w-xs"
        />
      )}

      {/* コンテンツ */}
      {tab === "course" ? (
        <CourseExplorer />
      ) : (
        <PersonnelTable type={tab} search={search} />
      )}
    </div>
  );
}
