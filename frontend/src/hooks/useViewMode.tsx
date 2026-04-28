import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from "react";
import { BREAKPOINTS } from "@/lib/breakpoints";

// auto=ブラウザ幅に従う / mobile=強制モバイル / desktop=強制PC
type ViewMode = "auto" | "mobile" | "desktop";

interface ViewModeContextValue {
  viewMode: ViewMode;
  /** 解決済みの「モバイルかどうか」フラグ（auto時はブラウザ幅判定） */
  isMobile: boolean;
  setViewMode: (m: ViewMode) => void;
  /** auto → mobile → desktop → auto の順に切替 */
  cycle: () => void;
}

const STORAGE_KEY = "d-ai-keiba-viewmode";

const ViewModeContext = createContext<ViewModeContextValue>({
  viewMode: "auto",
  isMobile: false,
  setViewMode: () => {},
  cycle: () => {},
});

function getInitialMode(): ViewMode {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === "mobile" || stored === "desktop") return stored;
  return "auto";
}

export function ViewModeProvider({ children }: { children: ReactNode }) {
  const [viewMode, setViewModeState] = useState<ViewMode>(getInitialMode);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  // ウィンドウサイズ監視
  useEffect(() => {
    const onResize = () => setWindowWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const setViewMode = useCallback((m: ViewMode) => {
    setViewModeState(m);
    if (m === "auto") {
      localStorage.removeItem(STORAGE_KEY);
    } else {
      localStorage.setItem(STORAGE_KEY, m);
    }
  }, []);

  const cycle = useCallback(() => {
    setViewMode(
      viewMode === "auto" ? "mobile" : viewMode === "mobile" ? "desktop" : "auto"
    );
  }, [viewMode, setViewMode]);

  const isMobile =
    viewMode === "mobile"
      ? true
      : viewMode === "desktop"
        ? false
        : windowWidth < BREAKPOINTS.MD;

  return (
    <ViewModeContext.Provider value={{ viewMode, isMobile, setViewMode, cycle }}>
      {children}
    </ViewModeContext.Provider>
  );
}

export function useViewMode() {
  return useContext(ViewModeContext);
}
