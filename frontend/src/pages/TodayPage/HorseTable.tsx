import { useState, useEffect, lazy, Suspense } from "react";
import { useViewMode } from "@/hooks/useViewMode";
import type { HorseData } from "./RaceDetailView";

const HorseCardMobile = lazy(() => import("./HorseCardMobile").then(m => ({ default: m.HorseCardMobile })));
const HorseCardPCLazy = lazy(() => import("./HorseCardPC").then(m => ({ default: m.HorseCardPC })));

interface Props {
  horses: HorseData[];
  isBanei?: boolean;
  raceId?: string;
}

// localStorage キー
function dMarkStorageKey(raceId: string): string {
  return `dmark_${raceId}`;
}

// D印の読み込み
function loadDMarks(raceId: string | undefined): Record<number, string> {
  if (!raceId) return {};
  try {
    const raw = localStorage.getItem(dMarkStorageKey(raceId));
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
}

// D印の保存
function saveDMarks(raceId: string, marks: Record<number, string>) {
  localStorage.setItem(dMarkStorageKey(raceId), JSON.stringify(marks));
}

export function HorseTable({ horses, isBanei, raceId }: Props) {
  const { viewMode, isMobile } = useViewMode();
  const [dMarks, setDMarks] = useState<Record<number, string>>(() => loadDMarks(raceId));

  // raceId が変わったら D印をリロード
  useEffect(() => {
    setDMarks(loadDMarks(raceId));
  }, [raceId]);

  // viewMode=auto の場合はCSSブレークポイントで自動切替
  // viewMode=mobile/desktop の場合はcontextで強制切替
  const showMobileCards = viewMode === "auto" ? undefined : isMobile;
  const showDesktopTable = viewMode === "auto" ? undefined : !isMobile;

  return (
    <>
    {/* モバイル: カード表示 */}
    <div className={
      showMobileCards === true ? "" :
      showMobileCards === false ? "hidden" :
      "md:hidden"
    }>
      <Suspense fallback={<p className="text-sm text-muted-foreground py-4 text-center">読み込み中...</p>}>
        <HorseCardMobile
          horses={horses}
          isBanei={isBanei}
          dMarks={dMarks}
          onDMarkSelect={(horseNo, mark) => {
            if (!raceId) return;
            setDMarks((prev) => {
              const updated = { ...prev };
              if (mark === "－") delete updated[horseNo];
              else updated[horseNo] = mark;
              saveDMarks(raceId, updated);
              return updated;
            });
          }}
        />
      </Suspense>
    </div>
    {/* PC: カード表示 */}
    <div className={
      showDesktopTable === true ? "block" :
      showDesktopTable === false ? "hidden" :
      "hidden md:block"
    }>
      <Suspense fallback={<p className="text-sm text-muted-foreground py-4 text-center">読み込み中...</p>}>
        <HorseCardPCLazy
          horses={horses}
          raceId={raceId}
          isBanei={isBanei}
          dMarks={dMarks}
          setDMarks={setDMarks}
        />
      </Suspense>
    </div>
    </>
  );
}
