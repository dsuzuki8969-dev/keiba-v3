import { useState, useCallback, useMemo, useRef, useEffect } from "react";

// マスター指示 2026-04-23: レース切り替え時もタブを固定する
// （買い目指南にいたら他Rを押しても買い目指南のままにする）
// sessionStorage はタブ単位で分離されるため、タブ内の永続化として機能。
// typescript-reviewer 指摘: SSR 環境（window 未定義）でも落ちないよう typeof チェック。
const TAB_STORAGE_KEY = "TabGroup3Horse.activeTab";
function _loadPersistedTab(): string {
  if (typeof window === "undefined" || typeof sessionStorage === "undefined") {
    return "shutsuba";
  }
  try {
    const v = sessionStorage.getItem(TAB_STORAGE_KEY);
    return v || "shutsuba";
  } catch {
    return "shutsuba";
  }
}
function _savePersistedTab(tab: string): void {
  if (typeof window === "undefined" || typeof sessionStorage === "undefined") {
    return;
  }
  try {
    sessionStorage.setItem(TAB_STORAGE_KEY, tab);
  } catch {
    /* ignore */
  }
}
import { HorseTable } from "./HorseTable";
import { MarkSummary } from "./MarkSummary";
import { AbilityTable } from "./AbilityTable";
import { OddsPanel } from "./OddsPanel";
import { TrainingPanel } from "./TrainingPanel";
import { StableCommentPanel } from "./StableCommentPanel";
import { DataAnalysisPanel } from "./DataAnalysisPanel";
import { RaceResultPanel } from "./RaceResultPanel";
import PaceFormation from "./PaceFormation";
import { PastRunsPanel } from "./PastRunsPanel";
import { TicketSection } from "./TicketSection";
import { MovieEmbed } from "./MovieEmbed";
import type { HorseData, RaceDetail } from "./RaceDetailView";

// NAR映像トラックマップ
const NAR_TRACK_MAP: Record<string, string> = {
  "帯広": "obihiro", "門別": "monbetsu", "盛岡": "morioka", "水沢": "mizusawa",
  "浦和": "urawa", "船橋": "funabashi", "大井": "ooi", "川崎": "kawasaki",
  "金沢": "kanazawa", "笠松": "kasamatsu", "名古屋": "nagoya", "園田": "sonoda",
  "姫路": "himeji", "高知": "kouchi", "佐賀": "saga",
};
// JRA映像ターゲット: race_id (YYYY JJ KK NN RR) → (YYYY KK JJ NN RR)
function jraVideoTarget(raceId: string): string {
  if (!raceId || raceId.length < 12) return "";
  return raceId.slice(0, 4) + raceId.slice(6, 8) + raceId.slice(4, 6) + raceId.slice(8, 12);
}

// タブ項目の種類
type TabItem =
  | { type: "content"; key: string; label: string }
  | { type: "link"; key: string; label: string; href: string };

interface Props {
  horses: HorseData[];
  race: RaceDetail;
  isBanei: boolean;
  raceId?: string;
  date: string;
  raceNo: number;
}

export function TabGroup3Horse({
  horses, race, isBanei, raceId,
  date, raceNo,
}: Props) {
  const [activeTab, _setActiveTab] = useState<string>(() => _loadPersistedTab());
  // activeTab を変えたら sessionStorage に保存して他レース切り替えでも保持
  const setActiveTab = useCallback((tab: string) => {
    _setActiveTab(tab);
    _savePersistedTab(tab);
  }, []);
  // race.race_id が変わった時に永続化された値を再読込（別タブで変更があった場合など）
  useEffect(() => {
    const persisted = _loadPersistedTab();
    if (persisted !== activeTab) {
      _setActiveTab(persisted);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [race?.race_id]);
  const dateStr = date.replace(/-/g, "");

  // 外部リンクURL生成（レース映像のみ。レースライブはヘッダー側へ移動）
  // NAR は HTTPS で配信（HTTP のままだと Mixed Content で埋め込み拒否される）
  // v6.1.4: useCallback + 即時呼び出し `()` は useMemo と等価なので useMemo に正規化
  const movieUrl = useMemo(() => {
    if (race.is_jra === false) {
      const track = NAR_TRACK_MAP[race.venue || ""];
      if (!track) return "";
      return `https://keiba-lv-st.jp/movie/player?date=${dateStr}&race=${raceNo}&track=${track}`;
    } else {
      const target = jraVideoTarget(race.race_id || "");
      if (!target) return "";
      return `/static/video_jra.html?target=${target}`;
    }
  }, [race, dateStr, raceNo]);

  // コンテンツタブ（1-2行目: 4列×2行）
  const contentTabs: TabItem[] = [
    // 1行目: 出馬表・前三走成績・能力表・展開
    { type: "content", key: "shutsuba", label: "出馬表" },
    { type: "content", key: "pastruns", label: "前三走成績" },
    { type: "content", key: "ability", label: "能力表" },
    { type: "content", key: "pace", label: "展開" },
    // 2行目: 調教・厩舎コメント・データ分析・オッズ
    { type: "content", key: "training", label: "調教" },
    { type: "content", key: "stable", label: "厩舎コメント" },
    { type: "content", key: "analysis", label: "データ分析" },
    { type: "content", key: "odds", label: "オッズ" },
  ];

  // 3行目: 印断層分析・買い目指南・レース結果・レース映像 で合計4列
  // レース映像はページ内に iframe 埋め込み。外部リンクは MovieEmbed 内の「別タブで開く↗」を使用
  const marksTab: TabItem = { type: "content", key: "marks", label: "印断層分析" };
  const baimeTab: TabItem = { type: "content", key: "baime", label: "買い目指南" };
  const resultTab: TabItem = { type: "content", key: "result", label: "レース結果" };
  const movieTab: TabItem | null = movieUrl
    ? { type: "content", key: "movie", label: "レース映像" }
    : null;
  // 3行目に表示するコンテンツタブ（最大4つ）
  const row3Tabs: TabItem[] = [marksTab, baimeTab, resultTab, ...(movieTab ? [movieTab] : [])];

  // トップ・ボトム両方のタブグリッドの参照（ボトムクリック時にトップへスクロール）
  const topTabsRef = useRef<HTMLDivElement | null>(null);

  const handleCellClick = (tab: TabItem, fromBottom = false) => {
    if (tab.type === "content") {
      setActiveTab(tab.key);
      // ボトムのタブをクリックしたらコンテンツ先頭（トップのタブ位置）までスクロール
      if (fromBottom && topTabsRef.current) {
        topTabsRef.current.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    } else if (tab.type === "link") {
      window.open(tab.href, "_blank", "noopener,noreferrer");
    }
  };

  // タブグリッド（上下両方で再利用） — fromBottom=true のときクリックで上へスクロール
  const renderTabGrid = (fromBottom: boolean) => (
    <div className="space-y-0">
      {/* 1-2行目: コンテンツタブ（4列×2行） */}
      <div className="grid grid-cols-4 border-t border-l border-border">
        {contentTabs.map((tab) => {
          const isActive = tab.key === activeTab;
          return (
            <button
              key={tab.key}
              onClick={() => handleCellClick(tab, fromBottom)}
              className={[
                "border-r border-b border-border",
                "px-1 py-2 sm:px-2 sm:py-2.5 text-[11px] sm:text-base font-medium text-center",
                "transition-colors truncate",
                isActive
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted/50 text-foreground hover:bg-muted",
              ].join(" ")}
            >
              {tab.label}
            </button>
          );
        })}
      </div>

      {/* 3行目: 印断層分析・買い目指南・レース結果・レース映像 の4列コンテンツタブ */}
      <div className="grid grid-cols-4 border-l border-border">
        {row3Tabs.map((tab) => {
          const isActive = tab.key === activeTab;
          return (
            <button
              key={tab.key}
              onClick={() => handleCellClick(tab, fromBottom)}
              className={[
                "border-r border-b border-border",
                "px-1 py-2 sm:px-2 sm:py-2.5 text-[11px] sm:text-base font-medium text-center",
                "transition-colors truncate",
                isActive
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted/50 text-foreground hover:bg-muted",
              ].join(" ")}
            >
              {tab.label}
            </button>
          );
        })}
        {/* タブが4未満なら空セルで4列レイアウトを維持 */}
        {Array.from({ length: Math.max(0, 4 - row3Tabs.length) }).map((_, i) => (
          <div
            key={`empty-${fromBottom ? "b" : "t"}-${i}`}
            className="border-r border-b border-border bg-muted/20"
          />
        ))}
      </div>
    </div>
  );

  return (
    <div className="space-y-0">
      {/* 上部タブ — v6.1: sticky 化（AppShell の --header-h に追従） */}
      <div
        ref={topTabsRef}
        className="sticky z-30 glass -mx-4 px-4 pt-1 pb-1 shadow-[var(--shadow-sm)]"
        style={{ top: "var(--header-h, 56px)" }}
      >
        {renderTabGrid(false)}
      </div>

      {/* コンテンツ領域 */}
      <div className="pt-2">
        {activeTab === "shutsuba" && <HorseTable horses={horses} isBanei={isBanei} raceId={raceId} />}
        {activeTab === "ability" && <AbilityTable horses={horses} isBanei={isBanei} />}
        {activeTab === "marks" && <MarkSummary horses={horses} race={race} />}
        {activeTab === "odds" && <OddsPanel horses={horses} race={race} />}
        {activeTab === "training" && <TrainingPanel horses={horses} />}
        {activeTab === "stable" && <StableCommentPanel horses={horses} />}
        {activeTab === "analysis" && <DataAnalysisPanel horses={horses} race={race} />}
        {activeTab === "pace" && (
          <PaceFormation horses={horses} race={race as never} />
        )}
        {activeTab === "result" && (
          <RaceResultPanel date={date} raceId={raceId || ""} />
        )}
        {activeTab === "pastruns" && <PastRunsPanel horses={horses} />}
        {activeTab === "baime" && (() => {
          // Phase 1-c: 3モード / 旧tickets / 買わない判定のいずれかがあれば表示
          const tbm = race.tickets_by_mode;
          const hasModes =
            tbm &&
            (((tbm as { fixed?: unknown[] })?.fixed?.length || 0) +
              (tbm.accuracy?.length || 0) +
              (tbm.balanced?.length || 0) +
              (tbm.recovery?.length || 0) > 0);
          const hasTickets =
            (race.tickets && race.tickets.length > 0) ||
            (race.formation_tickets && race.formation_tickets.length > 0);
          const skip = race.bet_decision?.skip;

          if (!hasModes && !hasTickets && !skip) {
            return (
              <p className="text-sm text-muted-foreground py-6 text-center">
                買い目の推奨がありません（データ未算出）。
              </p>
            );
          }
          return <TicketSection race={race} />;
        })()}
        {activeTab === "movie" && movieUrl && (
          <MovieEmbed
            url={movieUrl}
            externalUrl={movieUrl}
            label={race.is_jra === false ? "NAR レース映像" : "JRA レース映像"}
            external={race.is_jra === false}
          />
        )}
      </div>

      {/* v6.1.4: 下部タブ削除 — 上部タブが sticky 化されたため重複 */}
    </div>
  );
}
