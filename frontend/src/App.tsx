import { lazy, Suspense } from "react";
import { HashRouter, Routes, Route, Navigate } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AppShell } from "@/components/layout/AppShell";
import { ViewModeProvider } from "@/hooks/useViewMode";

// ページ単位のコード分割
const HomePage = lazy(() => import("@/pages/HomePage"));
const TodayPage = lazy(() => import("@/pages/TodayPage"));
const ResultsPage = lazy(() => import("@/pages/ResultsPage"));
// T-038: 開催カレンダーページ
const CalendarPage = lazy(() => import("@/pages/CalendarPage"));
const VenuePage = lazy(() => import("@/pages/VenuePage"));
const DatabasePage = lazy(() => import("@/pages/DatabasePage"));
const AboutPage = lazy(() => import("@/pages/AboutPage"));

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

function PageFallback() {
  return (
    <div className="flex items-center justify-center py-20">
      <div className="text-sm text-muted-foreground">読み込み中...</div>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ViewModeProvider>
      <HashRouter>
        <AppShell>
          <Suspense fallback={<PageFallback />}>
            <Routes>
              <Route path="/" element={<Navigate to="/home" replace />} />
              <Route path="/home" element={<HomePage />} />
              <Route path="/today" element={<TodayPage />} />
              <Route path="/results" element={<ResultsPage />} />
              {/* T-038: 開催カレンダーページ */}
              <Route path="/calendar" element={<CalendarPage />} />
              <Route path="/venue" element={<VenuePage />} />
              <Route path="/venue/:code" element={<VenuePage />} />
              <Route path="/db" element={<DatabasePage />} />
              <Route path="/about" element={<AboutPage />} />
            </Routes>
          </Suspense>
        </AppShell>
      </HashRouter>
      </ViewModeProvider>
    </QueryClientProvider>
  );
}
