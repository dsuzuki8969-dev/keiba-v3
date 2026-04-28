import { useState, useMemo, useCallback } from "react";
import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Users } from "lucide-react";
import { usePersonnelAgg } from "@/api/hooks";
import { VENUE_MAP } from "@/lib/constants";

interface Props {
  type: "jockey" | "trainer" | "sire" | "bms";
  search: string;
}

const TYPE_LABELS: Record<string, string> = {
  jockey: "騎手",
  trainer: "調教師",
  sire: "種牡馬",
  bms: "母父（BMS）",
};

const SORT_OPTIONS = [
  { value: "dev", label: "指数" },
  { value: "win", label: "勝利数" },
  { value: "total", label: "出走数" },
  { value: "win_rate", label: "勝率" },
  { value: "place2_rate", label: "連対率" },
  { value: "place3_rate", label: "複勝率" },
  { value: "roi", label: "単勝回収率" },
  { value: "fukusho_roi", label: "複勝回収率" },
  { value: "nige_rate", label: "逃げ率" },
  { value: "maeiki_rate", label: "前行き率" },
  { value: "makuri_rate", label: "マクリ率" },
];


const SMILE_OPTIONS = [
  { value: "", label: "全" },
  { value: "SS", label: "SS" },
  { value: "S", label: "S" },
  { value: "M", label: "M" },
  { value: "I", label: "I" },
  { value: "L", label: "L" },
  { value: "E", label: "E" },
];

const JRA_VENUES = [
  { code: "03", name: "札幌" }, { code: "04", name: "函館" },
  { code: "01", name: "福島" }, { code: "02", name: "新潟" },
  { code: "06", name: "中山" }, { code: "05", name: "東京" },
  { code: "07", name: "中京" }, { code: "08", name: "京都" },
  { code: "09", name: "阪神" }, { code: "10", name: "小倉" },
];
const NAR_VENUES = [
  { code: "30", name: "門別" }, { code: "35", name: "盛岡" },
  { code: "36", name: "水沢" }, { code: "42", name: "浦和" },
  { code: "43", name: "船橋" }, { code: "44", name: "大井" },
  { code: "45", name: "川崎" }, { code: "48", name: "名古屋" },
  { code: "47", name: "笠松" }, { code: "46", name: "金沢" },
  { code: "49", name: "園田" }, { code: "51", name: "姫路" },
  { code: "54", name: "高知" }, { code: "55", name: "佐賀" },
];

// 所属バッジの色
const LOC_COLORS: Record<string, string> = {
  美浦: "#3b82f6",
  栗東: "#d97706",
  大井: "#0d9488", 船橋: "#0d9488", 川崎: "#0d9488", 浦和: "#0d9488",
};

function LocationBadge({ loc }: { loc?: string }) {
  if (!loc) return null;
  const bg = LOC_COLORS[loc] || "#8b5cf6";
  return (
    <span
      className="inline-block ml-1 px-1.5 py-0 rounded-full text-white align-middle whitespace-nowrap"
      style={{ background: bg, fontSize: "10px", lineHeight: "16px" }}
    >
      {loc}
    </span>
  );
}

// 偏差値計算
function calcStats(arr: PersonRow[], fn: (p: PersonRow) => number) {
  if (!arr.length) return { mean: 0, std: 1 };
  const vals = arr.map(fn);
  const mean = vals.reduce((s, v) => s + v, 0) / vals.length;
  const variance = vals.reduce((s, v) => s + (v - mean) ** 2, 0) / vals.length;
  return { mean, std: Math.sqrt(variance) || 1 };
}

function calcDev(p: PersonRow, wrS: { mean: number; std: number }, p3S: { mean: number; std: number }, roiS: { mean: number; std: number }, p2S: { mean: number; std: number }) {
  if ((p.total || 0) < 10) return null;
  const wrZ = ((+(p.win_rate || 0)) - wrS.mean) / wrS.std;
  const p2Z = ((+(p.place2_rate || 0)) - p2S.mean) / p2S.std;
  const p3Z = ((+(p.place3_rate || 0)) - p3S.mean) / p3S.std;
  const roiZ = p.roi != null ? ((+(p.roi || 0)) - roiS.mean) / roiS.std : 0;
  // 勝率35% 連対率25% 複勝率35% 単勝回収率5%
  const roiW = p.roi != null ? 0.05 : 0;
  const w1 = 0.35, w2 = 0.25, w3 = 0.35, w4 = roiW;
  const wSum = w1 + w2 + w3 + w4;
  const composite = (w1 * wrZ + w2 * p2Z + w3 * p3Z + w4 * roiZ) / wSum;
  const reliability = 1 - Math.exp(-(p.total || 0) / 40);
  // 指数: 20〜100 スケール（中央50 = B帯）
  const raw = 50 + 8.0 * composite * reliability;
  return Math.round(Math.min(100, Math.max(20, raw)));
}

function devGrade(dev: number | null): string {
  if (dev == null) return "—";
  if (dev >= 65) return "SS";
  if (dev >= 60) return "S";
  if (dev >= 55) return "A";
  if (dev >= 50) return "B";
  if (dev >= 45) return "C";
  return "D";
}

function devColor(dev: number | null): string {
  if (dev == null) return "";
  if (dev >= 65) return "#d97706";  // SS: ゴールド
  if (dev >= 60) return "var(--positive)";  // S: 緑
  if (dev >= 55) return "var(--info)";  // A: 青
  if (dev <= 45) return "var(--negative)";  // D: 赤
  return "";  // B/C: デフォルト
}

export function PersonnelTable({ type, search }: Props) {
  const [sort, setSort] = useState("dev");
  const [jraNar, setJraNar] = useState("");
  const [venue, setVenue] = useState("");
  const [surface, setSurface] = useState("");
  const [smile, setSmile] = useState("");
  const curYear = String(new Date().getFullYear());
  const [year, setYear] = useState(curYear);
  const [detail, setDetail] = useState<{ id: string; name: string; dev: number | null } | null>(null);

  const isBloodline = type === "sire" || type === "bms";

  // JRA/NAR切替時にvenueをリセット
  const handleJraNar = useCallback((val: string) => {
    setJraNar(val);
    setVenue("");
  }, []);

  // クエリ文字列構築
  const qs = useMemo(() => {
    const params = new URLSearchParams();
    params.set("type", type);
    if (search) params.set("q", search);
    // 偏差値はクライアント側計算のため全件取得、他はAPI側ソート
    const clientSort = sort === "dev" || sort === "fukusho_roi"
      || sort === "nige_rate" || sort === "maeiki_rate" || sort === "makuri_rate";
    params.set("sort", clientSort ? "win_rate" : sort);
    params.set("limit", clientSort ? "500" : "200");
    if (jraNar) params.set("jra_nar", jraNar);
    if (venue) params.set("venue", venue);
    if (surface) params.set("surface", surface);
    if (smile) params.set("smile", smile);
    if (year) params.set("year", year);
    return params.toString();
  }, [type, search, sort, jraNar, venue, surface, smile, year]);

  const { data, isLoading, error } = usePersonnelAgg(qs);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const rawPersons = ((data as any)?.persons || []) as PersonRow[];
  const period = data?.period;

  // 偏差値計算 & ソート
  const persons = useMemo(() => {
    const valid = rawPersons.filter((p) => (p.total || 0) >= 10);
    const wrS = calcStats(valid, (p) => +(p.win_rate || 0));
    const p2S = calcStats(valid, (p) => +(p.place2_rate || 0));
    const p3S = calcStats(valid, (p) => +(p.place3_rate || 0));
    const roiS = calcStats(valid.filter((p) => p.roi != null), (p) => +(p.roi || 0));
    const withDev = rawPersons.map((p) => ({
      ...p,
      _dev: calcDev(p, wrS, p3S, roiS, p2S),
    }));
    if (sort === "dev") {
      withDev.sort((a, b) => {
        if (a._dev != null && b._dev != null) return b._dev - a._dev;
        if (a._dev != null) return -1;
        if (b._dev != null) return 1;
        return (b.total || 0) - (a.total || 0);
      });
      return withDev.slice(0, 200);
    }
    if (sort === "fukusho_roi") {
      withDev.sort((a, b) => ((b as Record<string, unknown>).fukusho_roi as number || 0) - ((a as Record<string, unknown>).fukusho_roi as number || 0));
      return withDev.slice(0, 200);
    }
    if (sort === "nige_rate" || sort === "maeiki_rate" || sort === "makuri_rate") {
      withDev.sort((a, b) => ((b as Record<string, unknown>)[sort] as number || 0) - ((a as Record<string, unknown>)[sort] as number || 0));
      return withDev.slice(0, 200);
    }
    return withDev;
  }, [rawPersons, sort]);

  const onRowClick = useCallback((p: PersonRow & { _dev: number | null }) => {
    setDetail({ id: p.id, name: p.name, dev: p._dev });
  }, []);

  return (
    <>
      <PremiumCard variant="default" padding="md">
        <PremiumCardHeader>
          <div className="flex flex-col gap-0.5">
            <PremiumCardAccent>
              <Users size={10} className="inline mr-1" />
              <span className="section-eyebrow">Personnel</span>
            </PremiumCardAccent>
            <PremiumCardTitle className="text-sm flex items-center gap-2">
              {TYPE_LABELS[type]}
              {period && (
                <span className="text-xs font-normal text-muted-foreground tnum">
                  {period}
                </span>
              )}
            </PremiumCardTitle>
          </div>
        </PremiumCardHeader>
        <div className="space-y-2">
          {/* フィルター */}
          <div className="flex flex-wrap gap-2 text-xs">
            {/* JRA/NAR */}
            <FilterGroup
              options={[
                { value: "", label: "全体" },
                { value: "JRA", label: "JRA" },
                { value: "NAR", label: "NAR" },
              ]}
              current={jraNar}
              onChange={handleJraNar}
            />
            {/* 場別サブボタン */}
            {jraNar && (
              <div className="flex gap-0.5 flex-wrap">
                <button
                  onClick={() => setVenue("")}
                  className={`px-1.5 py-0.5 rounded text-[11px] ${
                    !venue
                      ? "bg-primary text-primary-foreground"
                      : "bg-muted hover:bg-muted/80"
                  }`}
                >
                  全場
                </button>
                {(jraNar === "JRA" ? JRA_VENUES : NAR_VENUES).map((v) => (
                  <button
                    key={v.code}
                    onClick={() => setVenue(v.code)}
                    className={`px-1.5 py-0.5 rounded text-[11px] ${
                      venue === v.code
                        ? "bg-primary text-primary-foreground"
                        : "bg-muted hover:bg-muted/80"
                    }`}
                  >
                    {v.name}
                  </button>
                ))}
              </div>
            )}
            {/* 馬場 */}
            <FilterGroup
              options={[
                { value: "", label: "全" },
                { value: "芝", label: "芝" },
                { value: "ダート", label: "ダ" },
              ]}
              current={surface}
              onChange={setSurface}
            />
            {/* SMILE */}
            <FilterGroup options={SMILE_OPTIONS} current={smile} onChange={setSmile} />
            {/* 年 */}
            <FilterGroup
              options={[
                { value: "", label: "全年" },
                { value: curYear, label: curYear },
                { value: String(Number(curYear) - 1), label: String(Number(curYear) - 1) },
              ]}
              current={year}
              onChange={setYear}
            />
            {/* ソート */}
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value)}
              className="border border-border rounded px-2 py-1 bg-background"
            >
              {SORT_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </div>

          {/* 読み込み中 */}
          {isLoading && (
            <p className="text-sm text-muted-foreground py-4 text-center animate-pulse">
              読み込み中...
            </p>
          )}

          {/* エラー */}
          {error && (
            <p className="text-sm text-destructive">
              エラー: {(error as Error).message}
            </p>
          )}

          {/* テーブル */}
          {!isLoading && persons.length > 0 && (
            <div className="overflow-x-auto">
              <table className="min-w-[700px] w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-xs">
                    <th className="text-left py-1.5 px-1 section-eyebrow">{TYPE_LABELS[type]}名</th>
                    {!isBloodline && <th className="text-right py-1.5 px-1 section-eyebrow hidden sm:table-cell">ID</th>}
                    <th className="text-right py-1.5 px-1 section-eyebrow">出走</th>
                    <th className="text-right py-1.5 px-1 section-eyebrow hidden sm:table-cell">成績</th>
                    <th className="text-right py-1.5 px-1 section-eyebrow">勝率</th>
                    <th className="text-right py-1.5 px-1 section-eyebrow hidden sm:table-cell">連対率</th>
                    <th className="text-right py-1.5 px-1 section-eyebrow">複勝率</th>
                    <th className="text-right py-1.5 px-1 section-eyebrow">
                      {sort === "nige_rate" ? "逃げ率" : sort === "maeiki_rate" ? "前行き率" : sort === "makuri_rate" ? "マクリ率" : "単回収率"}
                    </th>
                    <th className="text-right py-1.5 px-1 section-eyebrow">指数</th>
                  </tr>
                </thead>
                <tbody>
                  {persons.map((p, i) => {
                    const wr = p.win_rate != null ? (+p.win_rate).toFixed(1) + "%" : "—";
                    const p2r = p.place2_rate != null ? (+p.place2_rate).toFixed(1) + "%" : "—";
                    const p3r = p.place3_rate != null ? (+p.place3_rate).toFixed(1) + "%" : "—";
                    const roi = p.roi != null ? (+p.roi).toFixed(1) + "%" : "—";
                    const lose = (p.total || 0) - (p.place3 || 0);
                    const record = `${p.win || 0}-${(p.place2 || 0) - (p.win || 0)}-${(p.place3 || 0) - (p.place2 || 0)}-${Math.max(0, lose)}`;
                    return (
                      <tr
                        key={p.id || i}
                        className="border-b border-border/50 hover:bg-brand-gold/5 cursor-pointer transition-colors"
                        onClick={() => onRowClick(p)}
                      >
                        <td className="py-1.5 px-1 font-semibold text-primary">
                          {p.name}
                          {!isBloodline && <LocationBadge loc={p.location} />}
                        </td>
                        {!isBloodline && (
                          <td className="text-right py-1.5 px-1 text-muted-foreground hidden sm:table-cell">
                            {p.id}
                          </td>
                        )}
                        <td className="text-right py-1.5 px-1 stat-mono">
                          {p.total}
                        </td>
                        <td className="text-right py-1.5 px-1 text-muted-foreground tabular-nums hidden sm:table-cell">
                          {record}
                        </td>
                        <td
                          className="text-right py-1.5 px-1 stat-mono"
                          style={{ color: rateColor(p.win_rate) }}
                        >
                          {wr}
                        </td>
                        <td
                          className="text-right py-1.5 px-1 stat-mono hidden sm:table-cell"
                          style={{ color: rateColor(p.place2_rate) }}
                        >
                          {p2r}
                        </td>
                        <td
                          className="text-right py-1.5 px-1 stat-mono"
                          style={{ color: rateColor(p.place3_rate) }}
                        >
                          {p3r}
                        </td>
                        <td
                          className="text-right py-1.5 px-1 stat-mono"
                          style={{ color: sort === "nige_rate" || sort === "maeiki_rate" || sort === "makuri_rate" ? rateColor(
                            sort === "nige_rate" ? (p as Record<string, unknown>).nige_rate as number :
                            sort === "maeiki_rate" ? (p as Record<string, unknown>).maeiki_rate as number :
                            (p as Record<string, unknown>).makuri_rate as number
                          ) : roiColor(p.roi) }}
                        >
                          {sort === "nige_rate" ? ((p as Record<string, unknown>).nige_rate as number ?? 0).toFixed(1) + "%"
                            : sort === "maeiki_rate" ? ((p as Record<string, unknown>).maeiki_rate as number ?? 0).toFixed(1) + "%"
                            : sort === "makuri_rate" ? ((p as Record<string, unknown>).makuri_rate as number ?? 0).toFixed(1) + "%"
                            : roi}
                        </td>
                        <td
                          className="text-right py-1.5 px-1 stat-mono"
                          style={{ color: devColor(p._dev) }}
                        >
                          {p._dev != null ? <>{devGrade(p._dev)} <span className="text-muted-foreground text-[10px]">{p._dev}</span></> : "—"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
              <p className="text-xs text-muted-foreground mt-1">
                {persons.length}件 — 行クリックで詳細
              </p>
            </div>
          )}

          {/* データなし */}
          {!isLoading && persons.length === 0 && !error && (
            <p className="text-sm text-muted-foreground py-4 text-center">
              データがありません
            </p>
          )}
        </div>
      </PremiumCard>

      {/* 詳細モーダル */}
      {detail && (
        <PersonnelDetailModal
          type={type}
          id={detail.id}
          name={detail.name}
          devVal={detail.dev}
          isBloodline={isBloodline}
          year={year}
          onClose={() => setDetail(null)}
        />
      )}
    </>
  );
}

// フィルターグループ
function FilterGroup({
  options,
  current,
  onChange,
}: {
  options: { value: string; label: string }[];
  current: string;
  onChange: (v: string) => void;
}) {
  return (
    <div className="flex gap-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          onClick={() => onChange(o.value)}
          className={`px-2 py-1 rounded text-xs ${
            current === o.value
              ? "bg-primary text-primary-foreground"
              : "bg-muted hover:bg-muted/80"
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

// 詳細モーダル
function PersonnelDetailModal({
  type,
  id,
  name,
  devVal,
  isBloodline,
  year,
  onClose,
}: {
  type: string;
  id: string;
  name: string;
  devVal: number | null;
  isBloodline: boolean;
  year?: string;
  onClose: () => void;
}) {
  const qs = `type=${type}&id=${encodeURIComponent(id)}${year ? `&year=${year}` : ""}`;
  const { data, isLoading } = usePersonnelAgg(qs);
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const agg = data as any;

  const typeLabel = TYPE_LABELS[type] || type;

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center pt-8 px-4" onClick={onClose}>
      <div className="fixed inset-0 bg-black/40" />
      <div
        className="relative bg-background border border-border rounded-lg shadow-xl w-full max-w-3xl max-h-[85vh] overflow-y-auto p-4 space-y-4"
        onClick={(e) => e.stopPropagation()}
      >
        {/* ヘッダー */}
        <div className="flex items-center justify-between">
          <h3 className="text-base font-bold">
            {name}（{typeLabel}）詳細
          </h3>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground text-lg px-2">
            ✕
          </button>
        </div>

        {isLoading && (
          <p className="text-sm text-muted-foreground animate-pulse py-4 text-center">
            読み込み中...
          </p>
        )}

        {agg && !isLoading && (
          <>
            {/* サマリー */}
            <div className="space-y-1 text-sm">
              <SummaryLine label="全体" st={agg} showExtra loc={!isBloodline ? agg.location : undefined} devVal={devVal} roi={agg.roi} />
              <SummaryLine label="JRA" st={agg.jra} />
              <SummaryLine label="NAR" st={agg.nar} />
            </div>

            {/* 位置取り傾向（騎手・調教師のみ） */}
            {!isBloodline && agg.position_stats && (
              <PositionStatsSection stats={agg.position_stats} typeLabel={typeLabel} />
            )}

            {/* 競馬場別 */}
            {agg.by_venue && Object.keys(agg.by_venue).length > 0 && (
              <div>
                <h4 className="text-sm font-bold mb-1">競馬場別</h4>
                <BreakdownTable
                  data={Object.fromEntries(
                    Object.entries(agg.by_venue as Record<string, Record<string, number>>).map(
                      ([vc, st]) => [VENUE_MAP[vc] || vc, st]
                    )
                  )}
                  keyLabel="競馬場"
                />
              </div>
            )}

            {/* 脚質別 */}
            {agg.by_running_style && Object.keys(agg.by_running_style).length > 0 && (
              <div>
                <h4 className="text-sm font-bold mb-1">脚質別</h4>
                <BreakdownTable
                  data={agg.by_running_style}
                  keyLabel="脚質"
                  sortKeys={["逃げ", "先行", "差し", "追込"]}
                />
              </div>
            )}

            {/* 馬場×距離区分別 */}
            {agg.by_smile && Object.keys(agg.by_smile).length > 0 && (
              <div>
                <h4 className="text-sm font-bold mb-1">馬場×距離区分別</h4>
                <BreakdownTable
                  data={agg.by_smile}
                  keyLabel="距離区分"
                  sortKeys={["芝SS", "芝S", "芝M", "芝I", "芝L", "芝E", "ダートSS", "ダートS", "ダートM", "ダートI", "ダートL", "ダートE"]}
                />
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

// サマリー行
function SummaryLine({
  label,
  st,
  showExtra,
  loc,
  devVal,
  roi,
}: {
  label: string;
  st?: Record<string, unknown>;
  showExtra?: boolean;
  loc?: string;
  devVal?: number | null;
  roi?: number;
}) {
  if (!st) return null;
  const t = Number(st.total || 0);
  if (t === 0 && !showExtra) return null;
  const lose = t - Number(st.place3 || 0);
  const record = `${st.win || 0}-${Number(st.place2 || 0) - Number(st.win || 0)}-${Number(st.place3 || 0) - Number(st.place2 || 0)}-${Math.max(0, lose)}`;
  const wr = t ? (+Number(st.win_rate || 0)).toFixed(1) + "%" : "—";
  const p3r = t ? (+Number(st.place3_rate || 0)).toFixed(1) + "%" : "—";

  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-0.5 text-sm">
      <span className="font-bold text-primary">{label}</span>
      {loc && <LocationBadge loc={loc} />}
      <span>出走 <strong>{t}</strong></span>
      <span>成績 <strong>{record}</strong></span>
      <span>勝率 <strong style={{ color: rateColor(Number(st.win_rate || 0)) }}>{wr}</strong></span>
      <span>複勝率 <strong style={{ color: rateColor(Number(st.place3_rate || 0)) }}>{p3r}</strong></span>
      {showExtra && roi != null && (
        <span>単回収率 <strong style={{ color: roiColor(roi) }}>{(+roi).toFixed(1)}%</strong></span>
      )}
      {showExtra && devVal != null && (
        <span>指数 <strong style={{ color: devColor(devVal) }}>{devGrade(devVal)} {devVal}</strong></span>
      )}
    </div>
  );
}

// ブレイクダウンテーブル
function BreakdownTable({
  data,
  keyLabel,
  sortKeys,
}: {
  data: Record<string, Record<string, number>>;
  keyLabel: string;
  sortKeys?: string[];
}) {
  const entries = useMemo(() => {
    if (sortKeys) {
      const seen = new Set<string>();
      const result: [string, Record<string, number>][] = [];
      for (const k of sortKeys) {
        if (data[k]) {
          result.push([k, data[k]]);
          seen.add(k);
        }
      }
      Object.entries(data).forEach(([k, v]) => {
        if (!seen.has(k)) result.push([k, v]);
      });
      return result;
    }
    return Object.entries(data).sort(
      (a, b) => (b[1].total || b[1].runs || 0) - (a[1].total || a[1].runs || 0)
    );
  }, [data, sortKeys]);

  if (!entries.length)
    return <p className="text-xs text-muted-foreground">データなし</p>;

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border text-xs text-muted-foreground">
            <th className="text-left py-1 px-1">{keyLabel}</th>
            <th className="text-right py-1 px-1">出走</th>
            <th className="text-right py-1 px-1">成績</th>
            <th className="text-right py-1 px-1">勝率</th>
            <th className="text-right py-1 px-1">連対率</th>
            <th className="text-right py-1 px-1">複勝率</th>
          </tr>
        </thead>
        <tbody>
          {entries.map(([key, st]) => {
            const t = st.total || st.runs || 0;
            const wr = t
              ? (st.win_rate != null ? (+st.win_rate).toFixed(1) : (((st.win || 0) / t) * 100).toFixed(1)) + "%"
              : "—";
            const p2r = t
              ? (st.place2_rate != null ? (+st.place2_rate).toFixed(1) : (((st.place2 || 0) / t) * 100).toFixed(1)) + "%"
              : "—";
            const p3r = t
              ? (st.place3_rate != null ? (+st.place3_rate).toFixed(1) : (((st.place3 || 0) / t) * 100).toFixed(1)) + "%"
              : "—";
            const lose = t - (st.place3 || 0);
            const record = `${st.win || 0}-${(st.place2 || 0) - (st.win || 0)}-${(st.place3 || 0) - (st.place2 || 0)}-${Math.max(0, lose)}`;
            return (
              <tr key={key} className="border-b border-border/50 hover:bg-brand-gold/5 transition-colors">
                <td className="py-1 px-1 font-semibold">{key}</td>
                <td className="text-right py-1 px-1 stat-mono">{t}</td>
                <td className="text-right py-1 px-1 text-muted-foreground tabular-nums text-xs">{record}</td>
                <td className="text-right py-1 px-1 stat-mono" style={{ color: rateColor(parseFloat(wr)) }}>{wr}</td>
                <td className="text-right py-1 px-1 stat-mono" style={{ color: rateColor(parseFloat(p2r)) }}>{p2r}</td>
                <td className="text-right py-1 px-1 stat-mono" style={{ color: rateColor(parseFloat(p3r)) }}>{p3r}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

// ヘルパー
function rateColor(rate: number | null | undefined): string {
  if (rate == null) return "";
  if (rate >= 20) return "var(--positive)";
  if (rate >= 10) return "var(--info)";
  return "";
}

function roiColor(roi: number | null | undefined): string {
  if (roi == null) return "";
  if (roi >= 100) return "var(--positive)";
  if (roi >= 80) return "var(--warning)";
  return "var(--negative)";
}

// 位置取り率の色分け（threshold以上で緑、threshold*0.5以上で青）
function posColor(rate: number | null | undefined, threshold: number): string {
  if (rate == null) return "";
  if (rate >= threshold) return "var(--positive)";
  if (rate >= threshold * 0.5) return "var(--info)";
  return "";
}

// 位置取り傾向セクション（詳細モーダル内）
function PositionStatsSection({ stats, typeLabel }: { stats: Record<string, number | null>; typeLabel: string }) {
  const fmt = (v: number | null | undefined) => v != null ? (v * 100).toFixed(1) + "%" : "—";
  const deltaFmt = (v: number | null | undefined) => {
    if (v == null) return "—";
    const pct = v * 100;
    const sign = pct >= 0 ? "+" : "";
    return sign + pct.toFixed(1) + "%";
  };
  const deltaColor = (v: number | null | undefined) => {
    if (v == null) return "";
    if (v <= -0.02) return "var(--positive)";  // 前に行く = 良い
    if (v >= 0.02) return "var(--negative)";   // 下がる = 悪い
    return "";
  };

  const rows: { label: string; items: { key: string; label: string; threshold: number }[] }[] = [
    {
      label: "1角（スタート直後）",
      items: [
        { key: "nige_rate", label: "逃げ率", threshold: 0.15 },
        { key: "mae_iki_rate", label: "前行き率", threshold: 0.35 },
        { key: "ds_mae_iki_rate", label: "差追前行き率", threshold: 0.15 },
      ],
    },
    {
      label: "最終コーナー（勝負所）",
      items: [
        { key: "pos_4c_nige_rate", label: "逃げ率", threshold: 0.15 },
        { key: "pos_4c_mae_iki_rate", label: "前行き率", threshold: 0.35 },
        { key: "pos_4c_ds_mae_iki_rate", label: "マクリ率", threshold: 0.05 },
      ],
    },
  ];

  const hasAny = Object.values(stats).some((v) => v != null);
  if (!hasAny) return null;

  return (
    <div>
      <h4 className="text-sm font-bold mb-1">位置取り傾向（{typeLabel}管理馬）</h4>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 text-sm">
        {rows.map((group) => (
          <div key={group.label} className="border border-border rounded p-2">
            <div className="text-xs font-semibold text-muted-foreground mb-1">{group.label}</div>
            {group.items.map((item) => {
              const v = stats[item.key];
              return (
                <div key={item.key} className="flex justify-between py-0.5">
                  <span className="text-muted-foreground">{item.label}</span>
                  <span className="font-semibold tabular-nums" style={{ color: posColor(v, item.threshold) }}>
                    {fmt(v)}
                  </span>
                </div>
              );
            })}
          </div>
        ))}
        {/* 軌跡 */}
        <div className="border border-border rounded p-2 sm:col-span-2">
          <div className="text-xs font-semibold text-muted-foreground mb-1">1角→最終角 軌跡</div>
          <div className="flex flex-wrap gap-x-6 gap-y-0.5">
            <div className="flex justify-between gap-2">
              <span className="text-muted-foreground">位置変化</span>
              <span className="font-semibold tabular-nums" style={{ color: deltaColor(stats.pos_delta) }}>
                {deltaFmt(stats.pos_delta)}
              </span>
            </div>
            <div className="flex justify-between gap-2">
              <span className="text-muted-foreground">位置維持率</span>
              <span className="font-semibold tabular-nums" style={{ color: posColor(stats.hold_rate, 0.65) }}>
                {fmt(stats.hold_rate)}
              </span>
            </div>
            <div className="flex justify-between gap-2">
              <span className="text-muted-foreground">差追位置変化</span>
              <span className="font-semibold tabular-nums" style={{ color: deltaColor(stats.ds_pos_delta) }}>
                {deltaFmt(stats.ds_pos_delta)}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

// 型
interface PersonRow {
  id: string;
  name: string;
  total: number;
  win?: number;
  place2?: number;
  place3?: number;
  win_rate?: number;
  place2_rate?: number;
  place3_rate?: number;
  roi?: number;
  location?: string;
  _dev: number | null;
  [key: string]: unknown;
}
