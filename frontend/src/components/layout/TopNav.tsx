import { useCallback } from "react";
import { NavLink } from "react-router-dom";
import { useQueryClient } from "@tanstack/react-query";
import { TABS, localDate } from "@/lib/constants";
import { api } from "@/api/client";

export function TopNav() {
  const qc = useQueryClient();

  // タブホバー/タッチ時に遷移先データを先読み
  const prefetch = useCallback(
    (path: string) => {
      const date = localDate();
      const year = String(new Date().getFullYear());
      const opts = { staleTime: 5 * 60 * 1000 };
      if (path === "/home" || path === "/today") {
        qc.prefetchQuery({ queryKey: ["predictions", date], queryFn: () => api.todayPredictions(date), ...opts });
      }
      if (path === "/home") {
        qc.prefetchQuery({ queryKey: ["homeInfo", date], queryFn: () => api.homeInfo(date), ...opts });
      }
      if (path === "/results") {
        qc.prefetchQuery({ queryKey: ["resultsSummary", year], queryFn: () => api.resultsSummary(year), ...opts });
      }
      if (path === "/venue") {
        qc.prefetchQuery({ queryKey: ["venueProfile", ""], queryFn: () => api.venueProfile(), ...opts });
      }
    },
    [qc],
  );

  return (
    <nav className="flex gap-0.5 overflow-x-auto">
      {TABS.map((tab) => (
        <NavLink
          key={tab.key}
          to={tab.path}
          onMouseEnter={() => prefetch(tab.path)}
          onTouchStart={() => prefetch(tab.path)}
          className={({ isActive }) =>
            `px-3 sm:px-4 py-1.5 sm:py-2 text-xs sm:text-sm font-medium whitespace-nowrap transition-colors border-b-2 ${
              isActive
                ? "border-brand-gold text-brand-gold"
                : "border-transparent text-header-text/60 hover:text-brand-gold hover:border-brand-gold/40"
            }`
          }
        >
          <span className="sm:hidden">{tab.shortLabel}</span>
          <span className="hidden sm:inline">{tab.label}</span>
        </NavLink>
      ))}
    </nav>
  );
}
