import { type ReactNode, useRef, useEffect } from "react";
import { Link } from "react-router-dom";
import { TopNav } from "./TopNav";
import { Clock } from "./Clock";
import { useTheme } from "@/hooks/useTheme";
import { useViewMode } from "@/hooks/useViewMode";

interface AppShellProps {
  children: ReactNode;
}

// ビューモードアイコン
function ViewModeIcon({ mode, size = 16 }: { mode: "auto" | "mobile" | "desktop"; size?: number }) {
  if (mode === "mobile") {
    // スマートフォンアイコン
    return (
      <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <rect width="14" height="20" x="5" y="2" rx="2" ry="2"/>
        <path d="M12 18h.01"/>
      </svg>
    );
  }
  if (mode === "desktop") {
    // モニターアイコン
    return (
      <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <rect width="20" height="14" x="2" y="3" rx="2"/>
        <line x1="8" x2="16" y1="21" y2="21"/>
        <line x1="12" x2="12" y1="17" y2="21"/>
      </svg>
    );
  }
  // auto: レスポンシブアイコン（モニター+スマホ）
  return (
    <svg xmlns="http://www.w3.org/2000/svg" width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect width="14" height="10" x="1" y="3" rx="1.5"/>
      <line x1="5" x2="10" y1="16" y2="16"/>
      <line x1="7.5" x2="7.5" y1="13" y2="16"/>
      <rect width="7" height="12" x="16" y="4" rx="1"/>
      <path d="M19.5 13h.01"/>
    </svg>
  );
}

const VIEW_MODE_LABELS = {
  auto: "自動",
  mobile: "モバイル",
  desktop: "PC",
} as const;

export function AppShell({ children }: AppShellProps) {
  const { theme, toggle } = useTheme();
  const { viewMode, cycle } = useViewMode();
  const headerRef = useRef<HTMLElement>(null);

  // ヘッダー高さをCSS変数に設定（子コンポーネントのsticky位置計算用）
  useEffect(() => {
    const el = headerRef.current;
    if (!el) return;
    const update = () => {
      document.documentElement.style.setProperty("--header-h", `${el.offsetHeight}px`);
    };
    update();
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return (
    <div className="min-h-screen bg-background">
      {/* ヘッダー — ダークネイビー + backdrop-blur */}
      <header ref={headerRef} className="sticky top-0 z-50 bg-header-bg/95 backdrop-blur-md shadow-lg">
        {/* PC: 1行レイアウト */}
        <div className="hidden sm:flex max-w-7xl mx-auto px-4 py-2 items-center justify-between gap-4">
          <div className="flex items-center gap-6">
            <Link to="/">
              <img
                src="/logos/d-ai-keiba.svg"
                alt="D-AI Keiba"
                className="h-9 object-contain"
              />
            </Link>
            <TopNav />
          </div>
          <div className="flex items-center gap-3">
            {/* ビューモード切替 */}
            <button
              onClick={cycle}
              className="flex items-center gap-1 px-2 py-1 rounded-md text-header-text/60 hover:text-brand-gold transition-colors text-[11px]"
              aria-label="表示モード切替"
              title={`表示: ${VIEW_MODE_LABELS[viewMode]}`}
            >
              <ViewModeIcon mode={viewMode} size={14} />
              <span className="hidden lg:inline">{VIEW_MODE_LABELS[viewMode]}</span>
            </button>
            {/* テーマ切替 */}
            <button
              onClick={toggle}
              className="p-1.5 rounded-md text-header-text/60 hover:text-brand-gold transition-colors"
              aria-label="テーマ切替"
            >
              {theme === "light" ? (
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/></svg>
              ) : (
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/></svg>
              )}
            </button>
            <Clock />
          </div>
        </div>
        {/* スマホ: 2行レイアウト */}
        <div className="sm:hidden">
          <div className="flex items-center justify-between px-3 py-1.5">
            <Link to="/">
              <img
                src="/logos/d-ai-keiba.svg"
                alt="D-AI Keiba"
                className="h-7 object-contain"
              />
            </Link>
            <div className="flex items-center gap-2">
              {/* ビューモード切替（スマホ） */}
              <button
                onClick={cycle}
                className="p-1 rounded-md text-header-text/60 hover:text-brand-gold transition-colors"
                aria-label="表示モード切替"
                title={`表示: ${VIEW_MODE_LABELS[viewMode]}`}
              >
                <ViewModeIcon mode={viewMode} size={14} />
              </button>
              <button
                onClick={toggle}
                className="p-1 rounded-md text-header-text/60 hover:text-brand-gold transition-colors"
                aria-label="テーマ切替"
              >
                {theme === "light" ? (
                  <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/></svg>
                ) : (
                  <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="m4.93 4.93 1.41 1.41"/><path d="m17.66 17.66 1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="m6.34 17.66-1.41 1.41"/><path d="m19.07 4.93-1.41 1.41"/></svg>
                )}
              </button>
              <Clock />
            </div>
          </div>
          <div className="px-2 pb-1.5">
            <TopNav />
          </div>
        </div>
      </header>

      {/* メインコンテンツ */}
      <main
        className={`mx-auto px-4 py-6 transition-all duration-300 ${
          viewMode === "mobile" ? "max-w-[430px]" : "max-w-7xl"
        }`}
      >
        {children}
      </main>
    </div>
  );
}
