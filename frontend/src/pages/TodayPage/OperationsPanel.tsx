import { useState, useCallback, useRef } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ProgressTracker } from "@/components/keiba/ProgressTracker";
import { useAuth } from "@/hooks/useAuth";
import { api } from "@/api/client";

interface Props {
  date: string;
  venues: string[];
  onAnalyzeComplete?: () => void;
}

type OpMode = "today" | "range" | "unfetched";

// 各オペレーションカード共通の状態
interface OpState {
  running: boolean;
  status: string;
  error: string;
  pct: number;
  countLabel?: string;
  elapsedSec?: number;
  remainSec?: number;
  phaseLabel?: string;
  currentLog?: string;
}

const INIT_STATE: OpState = {
  running: false,
  status: "",
  error: "",
  pct: 0,
};

export function OperationsPanel({ date, venues, onAnalyzeComplete }: Props) {
  const { isAdmin } = useAuth();
  const queryClient = useQueryClient();

  // ── オッズ更新 ──
  const [odds, setOdds] = useState<OpState>(INIT_STATE);
  const [oddsRange, setOddsRange] = useState(false);
  const [oddsStart, setOddsStart] = useState("");
  const [oddsEnd, setOddsEnd] = useState("");
  const oddsTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  // ── 予想作成 ──
  const [pred, setPred] = useState<OpState>(INIT_STATE);
  const [predDetail, setPredDetail] = useState(false);
  const [predRange, setPredRange] = useState(false);
  const [predStart, setPredStart] = useState("");
  const [predEnd, setPredEnd] = useState("");
  const [selectedVenues, setSelectedVenues] = useState<Set<string>>(new Set());
  const [predVenues, setPredVenues] = useState<string[]>([]); // 期間指定時のvenues
  const predTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  // ── 結果取得 ──
  const [results, setResults] = useState<OpState>(INIT_STATE);
  const [resultsRange, setResultsRange] = useState(false);
  const [resultsStart, setResultsStart] = useState("");
  const [resultsEnd, setResultsEnd] = useState("");
  const resultsTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  // ── DB更新 ──
  const [db, setDb] = useState<OpState>(INIT_STATE);
  const [dbRange, setDbRange] = useState(false);
  const [dbStart, setDbStart] = useState("");
  const [dbEnd, setDbEnd] = useState("");
  const dbTimer = useRef<ReturnType<typeof setInterval> | undefined>(undefined);

  // ── 共通: ポーリング停止 ──
  const stopTimer = (ref: React.MutableRefObject<ReturnType<typeof setInterval> | undefined>) => {
    if (ref.current) {
      clearInterval(ref.current);
      ref.current = undefined;
    }
  };

  // ════════════════════════════════════
  // オッズ更新
  // ════════════════════════════════════
  const prepareOdds = useCallback(async (mode: OpMode) => {
    if (mode === "range") {
      setOddsRange((v) => !v);
      return;
    }
    let dates: string[] = [];
    if (mode === "today") {
      dates = [date];
    } else if (mode === "unfetched") {
      setOdds((s) => ({ ...s, status: "未取得日付を確認中..." }));
      try {
        const r = await api.oddsUnfetchedDates();
        dates = r.dates || [];
        if (!dates.length) {
          setOdds((s) => ({ ...s, status: "未取得の日付はありません" }));
          return;
        }
      } catch (e) {
        setOdds((s) => ({ ...s, error: (e as Error).message }));
        return;
      }
    }
    startOdds(dates);
  }, [date]);

  const startOdds = useCallback(async (dates: string[]) => {
    setOdds({ running: true, status: "オッズ取得中...", error: "", pct: 5 });
    try {
      await api.oddsUpdate({ dates });
    } catch (e) {
      setOdds((s) => ({ ...s, running: false, error: (e as Error).message }));
      return;
    }
    oddsTimer.current = setInterval(async () => {
      try {
        const s = await api.oddsUpdateStatus();
        const done = s.count || 0;
        const total = s.total || 0;
        const st = s.started_at || 0;
        const elapsed = st > 0 ? Math.round(Date.now() / 1000 - st) : 0;
        const pct = total > 0 ? Math.round((done / total) * 100) : 5;
        const remain = done > 0 && total > 0 ? Math.round((elapsed / done) * (total - done)) : undefined;
        if (s.done || !s.running) {
          stopTimer(oddsTimer);
          setOdds({
            running: false,
            status: s.error ? `エラー: ${s.error}` : `✓ オッズ更新完了`,
            error: "",
            pct: 100,
          });
          onAnalyzeComplete?.();
        } else {
          setOdds({
            running: true,
            status: "",
            error: "",
            pct,
            countLabel: total > 0 ? `${done}/${total}` : undefined,
            elapsedSec: elapsed,
            remainSec: remain,
            phaseLabel: s.current_race || "オッズ取得中...",
          });
        }
      } catch { /* ignore */ }
    }, 2000);
  }, [onAnalyzeComplete]);

  const cancelOdds = useCallback(async () => {
    try { await api.oddsUpdateCancel(); } catch { /* */ }
    stopTimer(oddsTimer);
    setOdds({ ...INIT_STATE, status: "中断しました" });
  }, []);

  // ════════════════════════════════════
  // 予想作成
  // ════════════════════════════════════
  const preparePred = useCallback(async (mode: OpMode) => {
    if (mode === "range") {
      setPredRange((v) => !v);
      return;
    }
    let targetDates: string[] = [];
    if (mode === "today") {
      targetDates = [date];
    } else if (mode === "unfetched") {
      setPred((s) => ({ ...s, status: "未予想日付を確認中..." }));
      try {
        const r = await api.predictionsUnfetchedDates();
        targetDates = r.dates || [];
        if (!targetDates.length) {
          setPred((s) => ({ ...s, status: "未予想の日付はありません" }));
          return;
        }
      } catch (e) {
        setPred((s) => ({ ...s, error: (e as Error).message }));
        return;
      }
    }
    setPred((s) => ({ ...s, status: `${targetDates.length}日分`, error: "" }));
    setPredDetail(true);
    setPredVenues(venues);
    setSelectedVenues(new Set(venues));
  }, [date, venues]);

  const runAnalyze = useCallback(async () => {
    const selected = [...selectedVenues];
    if (!selected.length) {
      setPred((s) => ({ ...s, error: "競馬場を1つ以上選択してください" }));
      return;
    }
    setPred({ running: true, status: "", error: "", pct: 3 });
    setPredDetail(false);
    try {
      // 期間指定時は predStart の日付を使う（未設定なら今日）
      await api.analyze({ date: predStart || date, venues: selected });
    } catch (e) {
      setPred((s) => ({ ...s, running: false, error: (e as Error).message }));
      return;
    }
    predTimer.current = setInterval(async () => {
      try {
        const s = await api.analyzeStatus();
        const done = s.done_races || 0;
        const total = s.total_races || 0;
        const pct = total > 0 ? Math.round(10 + (done / total) * 85) : 3;
        const elapsed = s.elapsed_sec || 0;
        const remain = done > 0 && total > 0 ? Math.round((elapsed / done) * (total - done)) : undefined;
        if (s.done) {
          stopTimer(predTimer);
          setPred({
            running: false,
            status: s.error ? `エラー: ${s.error}` : "✓ 予想作成完了",
            error: "",
            pct: 100,
          });
          onAnalyzeComplete?.();
        } else {
          setPred({
            running: true,
            status: "",
            error: "",
            pct,
            countLabel: total > 0 ? `${done}/${total} レース` : undefined,
            elapsedSec: elapsed,
            remainSec: remain,
            phaseLabel: s.progress || "分析中...",
            currentLog: s.current_race,
          });
        }
      } catch { /* ignore */ }
    }, 2000);
  }, [date, predStart, selectedVenues, onAnalyzeComplete]);

  const cancelPred = useCallback(async () => {
    try { await api.analyzeCancel(); } catch { /* */ }
    stopTimer(predTimer);
    setPred({ ...INIT_STATE, status: "中断しました" });
  }, []);

  // ════════════════════════════════════
  // 結果取得
  // ════════════════════════════════════
  const prepareResults = useCallback(async (mode: OpMode) => {
    if (mode === "range") {
      setResultsRange((v) => !v);
      return;
    }
    let dates: string[] = [];
    if (mode === "today") {
      dates = [date];
    } else if (mode === "unfetched") {
      setResults((s) => ({ ...s, status: "未照合日付を確認中..." }));
      try {
        const r = await api.unmatchedDates();
        dates = r.dates || [];
        if (!dates.length) {
          setResults((s) => ({ ...s, status: "未照合の日付はありません" }));
          return;
        }
      } catch (e) {
        setResults((s) => ({ ...s, error: (e as Error).message }));
        return;
      }
    }
    startResults(dates);
  }, [date]);

  const startResults = useCallback(async (dates: string[]) => {
    setResults({ running: true, status: `${dates.length}日分の結果を取得中...`, error: "", pct: 5 });
    try {
      await api.resultsFetchBatch({ dates });
    } catch (e) {
      setResults((s) => ({ ...s, running: false, error: (e as Error).message }));
      return;
    }
    resultsTimer.current = setInterval(async () => {
      try {
        const s = await api.resultsFetchStatus();
        const done = s.completed || 0;
        const total = s.total || 0;
        const elapsed = s.elapsed_sec || 0;
        const pct = total > 0 ? Math.round((done / total) * 100) : 5;
        const remain = done > 0 && total > 0 ? Math.round((elapsed / done) * (total - done)) : undefined;
        if (!s.running) {
          stopTimer(resultsTimer);
          setResults({
            running: false,
            status: s.error ? `エラー: ${s.error}` : "✓ 結果取得完了",
            error: "",
            pct: 100,
          });
          // レース結果キャッシュを全て無効化して即反映させる
          queryClient.invalidateQueries({ queryKey: ["raceResult"] });
          onAnalyzeComplete?.();
        } else {
          setResults({
            running: true,
            status: "",
            error: "",
            pct,
            countLabel: total > 0 ? `${done}/${total}日` : undefined,
            elapsedSec: elapsed,
            remainSec: remain,
            phaseLabel: s.current_date ? `${s.current_date} の結果を取得中...` : "結果取得中...",
          });
        }
      } catch { /* ignore */ }
    }, 2000);
  }, [onAnalyzeComplete]);

  const cancelResults = useCallback(async () => {
    try { await api.resultsFetchCancel(); } catch { /* */ }
    stopTimer(resultsTimer);
    setResults({ ...INIT_STATE, status: "中断しました" });
  }, []);

  // ════════════════════════════════════
  // DB更新
  // ════════════════════════════════════
  const prepareDb = useCallback(async (mode: OpMode) => {
    if (mode === "range") {
      setDbRange((v) => !v);
      return;
    }
    const body: { type: string; date?: string } = { type: "all" };
    if (mode === "today") {
      body.date = date;
    } else if (mode === "unfetched") {
      body.date = date;
    }
    startDb(body);
  }, [date]);

  const startDb = useCallback(async (body: { type: string; date?: string; start_date?: string; end_date?: string }) => {
    setDb({ running: true, status: "DB更新中...", error: "", pct: 5 });
    try {
      await api.dbUpdate(body);
    } catch (e) {
      setDb((s) => ({ ...s, running: false, error: (e as Error).message }));
      return;
    }
    dbTimer.current = setInterval(async () => {
      try {
        const s = await api.dbUpdateStatus();
        if (!s.running) {
          stopTimer(dbTimer);
          setDb({
            running: false,
            status: s.error ? `エラー: ${s.error}` : "✓ DB更新完了",
            error: "",
            pct: 100,
          });
          onAnalyzeComplete?.();
        } else {
          const step = s.step || 0;
          const totalSteps = s.total_steps || 3;
          const elapsed = s.elapsed_sec || 0;
          const remain = step > 0 ? Math.round((elapsed / step) * (totalSteps - step)) : undefined;
          setDb({
            running: true,
            status: "",
            error: "",
            pct: Math.round((step / totalSteps) * 100),
            elapsedSec: elapsed,
            remainSec: remain,
            phaseLabel: s.progress || "DB更新中...",
          });
        }
      } catch { /* ignore */ }
    }, 2000);
  }, [onAnalyzeComplete]);

  const cancelDb = useCallback(async () => {
    try { await api.dbUpdateCancel(); } catch { /* */ }
    stopTimer(dbTimer);
    setDb({ ...INIT_STATE, status: "中断しました" });
  }, []);

  // ── 会場チェック切替 ──
  const toggleVenue = useCallback((v: string) => {
    setSelectedVenues((prev) => {
      const next = new Set(prev);
      if (next.has(v)) next.delete(v);
      else next.add(v);
      return next;
    });
  }, []);

  if (!isAdmin) return null;

  return (
    <div className="hidden sm:block space-y-2">
      <div className="text-sm font-bold">各種取得</div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {/* ─── オッズ更新 ─── */}
        <OpCard
          title="オッズ更新"
          desc="予想済レースのオッズを更新"
          state={odds}
          onPrepare={prepareOdds}
          onCancel={cancelOdds}
          rangeOpen={oddsRange}
          rangeStart={oddsStart}
          rangeEnd={oddsEnd}
          onRangeStartChange={setOddsStart}
          onRangeEndChange={setOddsEnd}
          onRangeConfirm={() => {
            if (oddsStart && oddsEnd) {
              const dates = genDateRange(oddsStart, oddsEnd);
              startOdds(dates);
              setOddsRange(false);
            }
          }}
          mobileSimple
        />

        {/* ─── 予想作成 ─── */}
        <PremiumCard variant="default" padding="sm" className="space-y-2">
          <div className="text-sm font-semibold">予想作成</div>
          <p className="text-xs text-muted-foreground">競馬場を選択して予想作成</p>
          <div className="flex gap-2 flex-wrap">
            <Button size="sm" className="h-7 text-xs bg-emerald-600 hover:bg-emerald-700 text-white" onClick={() => preparePred("today")} disabled={pred.running}>
              本日
            </Button>
            <Button size="sm" className="h-7 text-xs bg-blue-600 hover:bg-blue-700 text-white hidden sm:inline-flex" onClick={() => preparePred("range")} disabled={pred.running}>
              期間指定
            </Button>
            <Button size="sm" className="h-7 text-xs bg-red-600 hover:bg-red-700 text-white hidden sm:inline-flex" onClick={() => preparePred("unfetched")} disabled={pred.running}>
              未取得分
            </Button>
          </div>
          {predRange && (
            <RangeInput
              start={predStart}
              end={predEnd}
              onStartChange={setPredStart}
              onEndChange={setPredEnd}
              onConfirm={async () => {
                setPredRange(false);
                setPredDetail(true);
                // 期間指定時は指定日付のvenuesをAPIから取得
                const targetDate = predStart || date;
                try {
                  const info = await api.homeInfo(targetDate);
                  const fetched = ((info?.venues || []) as { name: string }[]).map((v) => v.name);
                  if (fetched.length > 0) {
                    setPredVenues(fetched);
                    setSelectedVenues(new Set(fetched));
                  } else {
                    setPredVenues(venues);
                    setSelectedVenues(new Set(venues));
                  }
                } catch {
                  setPredVenues(venues);
                  setSelectedVenues(new Set(venues));
                }
              }}
            />
          )}
          {/* 会場選択 */}
          {predDetail && !pred.running && (
            <div className="space-y-2">
              <div className="flex flex-wrap gap-2">
                {(predVenues.length > 0 ? predVenues : venues).map((v) => (
                  <label key={v} className="flex items-center gap-1.5 text-xs cursor-pointer">
                    <input
                      type="checkbox"
                      checked={selectedVenues.has(v)}
                      onChange={() => toggleVenue(v)}
                      className="rounded border-border"
                    />
                    {v}
                  </label>
                ))}
              </div>
              <div className="flex gap-2">
                <Button size="sm" className="h-7 text-xs" onClick={runAnalyze} disabled={!selectedVenues.size}>
                  ▶ 予想開始
                </Button>
              </div>
            </div>
          )}
          {pred.error && <p className="text-xs text-destructive">{pred.error}</p>}
          {pred.status && !pred.running && <p className="text-xs text-muted-foreground">{pred.status}</p>}
          {pred.running && (
            <div className="space-y-1">
              <ProgressTracker
                pct={pred.pct}
                countLabel={pred.countLabel}
                elapsedSec={pred.elapsedSec}
                remainSec={pred.remainSec}
                phaseLabel={pred.phaseLabel}
                currentLog={pred.currentLog}
              />
              <Button size="sm" variant="destructive" className="h-6 text-xs" onClick={cancelPred}>
                ⏹ 中断
              </Button>
            </div>
          )}
        </PremiumCard>

        {/* ─── 結果取得 ─── */}
        <OpCard
          title="結果取得"
          desc="着順結果を取得・照合"
            state={results}
            onPrepare={prepareResults}
            onCancel={cancelResults}
            rangeOpen={resultsRange}
            rangeStart={resultsStart}
            rangeEnd={resultsEnd}
            onRangeStartChange={setResultsStart}
            onRangeEndChange={setResultsEnd}
            onRangeConfirm={() => {
              if (resultsStart && resultsEnd) {
                const dates = genDateRange(resultsStart, resultsEnd);
                startResults(dates);
                setResultsRange(false);
              }
            }}
          />

        {/* ─── DB更新 ─── */}
        <OpCard
          title="DB更新"
          desc="騎手・調教師・コースDB更新"
            state={db}
            onPrepare={prepareDb}
            onCancel={cancelDb}
            rangeOpen={dbRange}
            rangeStart={dbStart}
            rangeEnd={dbEnd}
            onRangeStartChange={setDbStart}
            onRangeEndChange={setDbEnd}
            onRangeConfirm={() => {
              if (dbStart && dbEnd) {
                startDb({ type: "all", start_date: dbStart, end_date: dbEnd });
                setDbRange(false);
              }
            }}
          />
      </div>
    </div>
  );
}

// ── 汎用オペレーションカード（確認ステップ付き） ──
function OpCard({
  title,
  desc,
  state,
  onPrepare,
  onCancel,
  rangeOpen,
  rangeStart,
  rangeEnd,
  onRangeStartChange,
  onRangeEndChange,
  onRangeConfirm,
  mobileSimple,
}: {
  title: string;
  desc: string;
  state: OpState;
  onPrepare: (mode: OpMode) => void;
  onCancel: () => void;
  rangeOpen: boolean;
  rangeStart: string;
  rangeEnd: string;
  onRangeStartChange: (v: string) => void;
  onRangeEndChange: (v: string) => void;
  onRangeConfirm: () => void;
  mobileSimple?: boolean;
}) {
  const [confirm, setConfirm] = useState<OpMode | null>(null);

  const handleClick = (mode: OpMode) => {
    if (mode === "range") {
      onPrepare("range");
      return;
    }
    setConfirm(mode);
  };

  const handleConfirm = () => {
    if (confirm) {
      onPrepare(confirm);
      setConfirm(null);
    }
  };

  return (
    <PremiumCard variant="default" padding="sm" className="space-y-2">
      <div className="text-sm font-semibold">{title}</div>
      <p className="text-xs text-muted-foreground">{desc}</p>
      <div className="flex gap-2 flex-wrap">
        <Button size="sm" className="h-7 text-xs bg-emerald-600 hover:bg-emerald-700 text-white" onClick={() => handleClick("today")} disabled={state.running}>
          本日
        </Button>
        <Button size="sm" className={`h-7 text-xs bg-blue-600 hover:bg-blue-700 text-white${mobileSimple ? " hidden sm:inline-flex" : ""}`} onClick={() => handleClick("range")} disabled={state.running}>
          期間指定
        </Button>
        <Button size="sm" className={`h-7 text-xs bg-red-600 hover:bg-red-700 text-white${mobileSimple ? " hidden sm:inline-flex" : ""}`} onClick={() => handleClick("unfetched")} disabled={state.running}>
          未取得分
        </Button>
      </div>
      {confirm && !state.running && (
        <div className="flex items-center gap-2 text-xs bg-muted/50 rounded p-2">
          <span>{confirm === "today" ? "本日分" : "未取得分"}を実行しますか？</span>
          <Button size="sm" className="h-6 text-xs" onClick={handleConfirm}>実行</Button>
          <Button size="sm" variant="ghost" className="h-6 text-xs" onClick={() => setConfirm(null)}>キャンセル</Button>
        </div>
      )}
      {rangeOpen && (
        <RangeInput
          start={rangeStart}
          end={rangeEnd}
          onStartChange={onRangeStartChange}
          onEndChange={onRangeEndChange}
          onConfirm={onRangeConfirm}
        />
      )}
      {state.error && <p className="text-xs text-destructive">エラー: {state.error}</p>}
      {state.status && !state.running && <p className="text-xs text-muted-foreground">{state.status}</p>}
      {state.running && (
        <div className="space-y-1">
          <ProgressTracker
            pct={state.pct}
            countLabel={state.countLabel}
            elapsedSec={state.elapsedSec}
            remainSec={state.remainSec}
            phaseLabel={state.phaseLabel}
            currentLog={state.currentLog}
          />
          <Button size="sm" variant="destructive" className="h-6 text-xs" onClick={onCancel}>
            ⏹ 中断
          </Button>
        </div>
      )}
    </PremiumCard>
  );
}

// ── 期間入力 ──
function RangeInput({
  start,
  end,
  onStartChange,
  onEndChange,
  onConfirm,
}: {
  start: string;
  end: string;
  onStartChange: (v: string) => void;
  onEndChange: (v: string) => void;
  onConfirm: () => void;
}) {
  return (
    <div className="flex gap-2 items-center flex-wrap">
      <Input type="date" value={start} onChange={(e) => onStartChange(e.target.value)} className="h-7 text-xs w-32" />
      <span className="text-xs text-muted-foreground">〜</span>
      <Input type="date" value={end} onChange={(e) => onEndChange(e.target.value)} className="h-7 text-xs w-32" />
      <Button size="sm" className="h-7 text-xs" onClick={onConfirm} disabled={!start || !end}>
        確認
      </Button>
    </div>
  );
}

// ── 日付範囲生成 ──
function genDateRange(start: string, end: string): string[] {
  const dates: string[] = [];
  const s = new Date(start);
  const e = new Date(end);
  for (let d = new Date(s); d <= e; d.setDate(d.getDate() + 1)) {
    dates.push(d.toISOString().slice(0, 10));
  }
  return dates;
}
