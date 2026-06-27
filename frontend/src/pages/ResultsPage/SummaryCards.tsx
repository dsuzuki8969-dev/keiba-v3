import { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Trophy, ListChecks, CheckCircle2, Target } from "lucide-react";
import type { HybridSummaryResponse, MPrimeSanrenpukuSummary, MPrimeByConfidence, MPrimeTopPayout } from "@/api/client";
import { UmarenCards } from "./UmarenCards";
import { SanrenpukuExtendedCards } from "./SanrenpukuExtendedCards";

interface Props {
  data: Record<string, unknown>;
  hybrid?: HybridSummaryResponse | null;
  mpByYear?: Record<string, MPrimeSanrenpukuSummary | null | undefined>;
}

function fmtPct(v: number | null | undefined): string {
  return (v ?? 0).toFixed(1) + "%";
}

function fmtNum(v: number | null | undefined): string {
  return (v ?? 0).toLocaleString();
}

export function SummaryCards({ data, hybrid, mpByYear }: Props) {
  if (!data.total_races) return null;

  // 結果 X-X-X-X（1着-2着-3着-着外）
  const win = Number(data.honmei_win ?? 0);
  const p2 = Number(data.honmei_place2 ?? 0);
  const p3 = Number(data.honmei_placed ?? 0);
  const total = Number(data.honmei_total ?? 0);
  const second = p2 - win;
  const third = p3 - p2;
  const out = total - p3;

  // 的中率指標
  const winRate = total > 0 ? (win / total) * 100 : 0;
  const rentaiRate = total > 0 ? (p2 / total) * 100 : 0;
  const fukushoRate = total > 0 ? (p3 / total) * 100 : 0;

  // 期間情報
  const periodParts: string[] = [];
  if (data.fetched_oldest && data.fetched_newest) {
    periodParts.push(`${data.fetched_oldest} 〜 ${data.fetched_newest}`);
  }
  if (data.period_days) {
    periodParts.push(`${data.period_days}日分`);
  }

  return (
    <div className="space-y-3">
      {periodParts.length > 0 && (
        <div className="text-xs text-muted-foreground">
          {periodParts.join("　")}
        </div>
      )}

      {/* ── 的中実績ヒーロー (主役・上段) ── */}
      <div className="space-y-2">
        {/* セクションラベル */}
        <div className="flex items-center gap-1.5">
          <Target size={13} className="text-brand-gold" />
          <span className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
            ◎本命 的中実績
          </span>
        </div>

        {/* 3指標カード: 複勝率(主役)・連対率・勝率 */}
        <div className="grid grid-cols-3 gap-3">
          {/* 複勝率 — 最重要指標 */}
          <PremiumCard
            variant={fukushoRate >= 70 ? "gold" : "default"}
            padding="md"
            className="text-center stylish-card-hover border border-border/60"
          >
            <div className="inline-flex items-center gap-1 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
              <CheckCircle2 size={11} className="text-emerald-500" />
              複勝率
            </div>
            <div className={`stat-mono text-[1.9rem] sm:text-[2.3rem] tnum font-bold ${
              fukushoRate >= 70 ? "text-emerald-500" : fukushoRate >= 50 ? "text-foreground" : "text-foreground/70"
            }`}>
              {fukushoRate.toFixed(1)}
              <span className="text-base ml-0.5 font-semibold text-muted-foreground">%</span>
            </div>
            <div className="mt-1 text-[11px] text-muted-foreground tnum">
              {fmtNum(p3)} / {fmtNum(total)} R
            </div>
          </PremiumCard>

          {/* 連対率 */}
          <PremiumCard variant="default" padding="md" className="text-center stylish-card-hover border border-border/60">
            <div className="inline-flex items-center gap-1 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
              <ListChecks size={11} className="text-brand-gold" />
              連対率
            </div>
            <div className={`stat-mono text-[1.9rem] sm:text-[2.3rem] tnum font-bold ${
              rentaiRate >= 50 ? "text-emerald-500" : "text-foreground/70"
            }`}>
              {rentaiRate.toFixed(1)}
              <span className="text-base ml-0.5 font-semibold text-muted-foreground">%</span>
            </div>
            <div className="mt-1 text-[11px] text-muted-foreground tnum">
              {fmtNum(p2)} / {fmtNum(total)} R
            </div>
          </PremiumCard>

          {/* 勝率 */}
          <PremiumCard
            variant={win > 0 ? "gold" : "default"}
            padding="md"
            className="text-center stylish-card-hover border border-border/60"
          >
            <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
              <Trophy size={11} className="text-brand-gold-dark" />
              勝率
            </div>
            <div className={`stat-mono text-[1.9rem] sm:text-[2.3rem] tnum font-bold ${
              winRate >= 30 ? "stat-mono-gold" : "text-foreground/70"
            }`}>
              {winRate.toFixed(1)}
              <span className="text-base ml-0.5 font-semibold text-muted-foreground">%</span>
            </div>
            <div className="mt-1 text-[11px] text-muted-foreground tnum">
              {fmtNum(win)} / {fmtNum(total)} R
            </div>
          </PremiumCard>
        </div>

        {/* 着順詳細 + 予想R数 (副次情報・小さく) */}
        <PremiumCard variant="default" padding="sm" className="border border-border/60">
          <div className="flex flex-wrap items-center gap-x-5 gap-y-1 text-xs text-muted-foreground">
            <span className="flex items-center gap-1">
              <ListChecks size={11} />
              予想 <span className="text-foreground font-semibold tnum">{fmtNum(total)}R</span>
            </span>
            <span className="tnum">
              着順 <span className="text-positive font-semibold">{fmtNum(win)}</span>
              <span className="mx-0.5 text-muted-foreground/50">-</span>
              {fmtNum(second)}
              <span className="mx-0.5 text-muted-foreground/50">-</span>
              {fmtNum(third)}
              <span className="mx-0.5 text-muted-foreground/50">-</span>
              <span className="text-muted-foreground/50">{fmtNum(out)}</span>
            </span>
          </div>
        </PremiumCard>
      </div>

      {/* ─── M' 戦略 採用成績 (全期間+年度別タブ) ─── */}
      {hybrid?.m_prime_sanrenpuku && (
        <MPrimeSummarySection mp={hybrid.m_prime_sanrenpuku} mpByYear={mpByYear} />
      )}

      {/* ─── 買い目系成績 (副次・降格) ─── */}
      {/* 馬連・三連複の買い目回収率は参考情報として下部に残置 */}
      {hybrid?.umaren_5tickets && (
        <div>
          <div className="relative h-px bg-gradient-to-r from-transparent via-border to-transparent mb-4" aria-hidden />
          <div className="text-[10px] text-muted-foreground/60 uppercase tracking-widest mb-2 font-semibold">
            参考 — 買い目回収実績
          </div>
          <UmarenCards
            umaren={hybrid.umaren_5tickets}
            umarenByYear={hybrid.umaren_5tickets_by_year}
          />
        </div>
      )}

      {hybrid?.sanrenpuku_7tickets && (
        <SanrenpukuExtendedCards
          sanrenpuku={hybrid.sanrenpuku_7tickets}
          sanrenpukuByYear={hybrid.sanrenpuku_7tickets_by_year}
        />
      )}
    </div>
  );
}

// ──────────────────────────────────────────────────────────
// M' 戦略 採用成績セクション (emerald 系カラー)
// ──────────────────────────────────────────────────────────

// M' 各自信度に対応する点数定義
const M_PRIME_POINTS: Record<string, number> = {
  SS: 4,
  S:  7,
  A:  7,
  B: 10,
  C: 10,
  D: 10,
};

// M' セクション用セルコンポーネント (レンダー外定義で ESLint static-components 準拠)
const StatCell = ({
  label, value, sub, color, hero,
}: { label: string; value: string; sub?: string; color: string; hero?: boolean }) => (
  <PremiumCard
    variant="default"
    padding={hero ? "md" : "sm"}
    className="text-center stylish-card-hover border border-border/60"
  >
    <div className={`font-semibold tracking-wider uppercase text-muted-foreground mb-1 ${hero ? "text-[11px]" : "text-[10px]"}`}>
      {label}
    </div>
    <div
      className={`stat-mono font-bold ${hero ? "text-[1.5rem] sm:text-[1.8rem]" : "text-base"}`}
      style={{ color }}
    >
      {value}
    </div>
    {sub && (
      <div className={`text-muted-foreground mt-0.5 tnum ${hero ? "text-[10px]" : "text-[9px]"}`}>
        {sub}
      </div>
    )}
  </PremiumCard>
);

function MPrimeSummarySection({ mp, mpByYear }: {
  mp: MPrimeSanrenpukuSummary;
  mpByYear?: Record<string, MPrimeSanrenpukuSummary | null | undefined>;
}) {
  const EMERALD  = "#10b981";

  // M' セクション独自の年度タブ
  const MP_YEARS = (() => {
    const cur = new Date().getFullYear();
    const ys: string[] = ["all"];
    for (let y = 2024; y <= cur; y++) ys.push(String(y));
    return ys;
  })();
  const [mpYear, setMpYear] = useState<string>("all");

  // 選択中の年度データ（タブ切替で参照先を変更）
  const activeMp: MPrimeSanrenpukuSummary | null =
    mpYear === "all"
      ? (mpByYear?.all ?? mp)
      : (mpByYear?.[mpYear] ?? null);

  if (!activeMp) {
    // 選択年度にデータなし
    return (
      <div className="space-y-3 pt-5 mt-2">
        <div className="relative h-px bg-gradient-to-r from-transparent via-emerald-500/40 to-transparent" aria-hidden />
        <MPrimeHeader mpYear={mpYear} setMpYear={setMpYear} mpYears={MP_YEARS} />
        <p className="text-sm text-muted-foreground py-4 text-center">
          {mpYear}年の M' 戦略データはありません
        </p>
      </div>
    );
  }

  const PROFIT_COLOR = activeMp.balance >= 0 ? EMERALD : "#ef4444";

  // 自信度別カード (SS/S/A/B/C/D)
  const confidenceOrder = ["SS", "S", "A", "B", "C", "D"] as const;

  return (
    <div className="space-y-3 pt-5 mt-2">
      {/* セクション区切り */}
      <div className="relative h-px bg-gradient-to-r from-transparent via-emerald-500/40 to-transparent" aria-hidden />
      <MPrimeHeader
        mpYear={mpYear}
        setMpYear={setMpYear}
        mpYears={MP_YEARS}
        dateFrom={activeMp.date_from}
        dateTo={activeMp.date_to}
      />

      {/* 上段ヒーロー (的中率 / 回収率 / 純利) */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <StatCell
          label="的中率"
          value={fmtPct(activeMp.hit_rate_pct)}
          sub={`${fmtNum(activeMp.races_hit)} / ${fmtNum(activeMp.races_played)} R`}
          color={EMERALD}
          hero
        />
        <StatCell
          label="回収率 ROI"
          value={fmtPct(activeMp.roi_pct)}
          sub={`購入 ${fmtNum(activeMp.total_stake)} 円`}
          color={activeMp.roi_pct >= 100 ? EMERALD : "#ef4444"}
          hero
        />
        <StatCell
          label="純利"
          value={(activeMp.balance >= 0 ? "+" : "") + fmtNum(activeMp.balance) + "円"}
          sub={`払戻 ${fmtNum(activeMp.total_payback)} 円`}
          color={PROFIT_COLOR}
          hero
        />
      </div>

      {/* 自信度別内訳 6カード */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-2">
        {confidenceOrder.map((rank) => {
          const _raw: MPrimeByConfidence | undefined = activeMp.by_confidence[rank];
          const pts = M_PRIME_POINTS[rank] ?? "?";
          // バックエンドは "races" を返すが型は "played" — 両方を安全に参照
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const c = _raw ? { ..._raw, played: _raw.played ?? (_raw as any).races ?? 0 } : undefined;
          if (!c) {
            // データなし → グレーアウト表示
            return (
              <PremiumCard key={rank} variant="default" padding="sm" className="text-center opacity-40">
                <div className="text-[10px] font-semibold text-muted-foreground mb-1">{rank}</div>
                <div className="text-[9px] text-muted-foreground">{pts}点</div>
                <div className="text-xs text-muted-foreground mt-1">—</div>
              </PremiumCard>
            );
          }
          const roiColor = c.roi_pct >= 100 ? EMERALD : "#ef4444";
          // backend は balance フィールドを返さないため payback - stake で計算
          const balance = (c.payback ?? 0) - (c.stake ?? 0);
          const balColor = balance >= 0 ? EMERALD : "#ef4444";
          return (
            <PremiumCard key={rank} variant="default" padding="sm" className="text-center stylish-card-hover border border-border/60">
              {/* ランク名 + 点数 */}
              <div className="flex items-center justify-center gap-1 mb-1">
                <span className="text-[11px] font-extrabold" style={{ color: EMERALD }}>{rank}</span>
                <span className="text-[9px] text-muted-foreground">{pts}点</span>
              </div>
              {/* 購入R数 */}
              <div className="text-[9px] text-muted-foreground">{fmtNum(c.played)}R購入</div>
              {/* 的中率 */}
              <div className="text-[10px] text-muted-foreground mt-1">的中率</div>
              <div className="stat-mono text-sm font-bold" style={{ color: EMERALD }}>
                {fmtPct(c.hit_rate_pct)}
              </div>
              {/* ROI */}
              <div className="text-[10px] text-muted-foreground mt-0.5">ROI</div>
              <div className="stat-mono text-sm font-bold" style={{ color: roiColor }}>
                {fmtPct(c.roi_pct)}
              </div>
              {/* 純利 */}
              <div className="text-[10px] text-muted-foreground mt-0.5">純利</div>
              <div className="stat-mono text-xs font-bold" style={{ color: balColor }}>
                {(balance >= 0 ? "+" : "") + fmtNum(balance)}
              </div>
            </PremiumCard>
          );
        })}
      </div>

      {/* 三連複高配当 TOP10 */}
      {activeMp.top_payouts && activeMp.top_payouts.length > 0 && (
        <div className="mt-4">
          <div className="text-xs font-semibold text-muted-foreground mb-2">三連複高配当 TOP10</div>
          <PremiumCard variant="default" padding="sm">
            <div className="overflow-x-auto">
              <table className="w-full text-xs tabular-nums">
                <thead>
                  <tr className="text-muted-foreground border-b border-border/40">
                    <th className="text-left px-2 py-1">#</th>
                    <th className="text-right px-2 py-1">配当</th>
                    <th className="text-left px-2 py-1">日付</th>
                    <th className="text-left px-2 py-1">場</th>
                    <th className="text-right px-2 py-1">R</th>
                    <th className="text-left px-2 py-1">レース</th>
                    <th className="text-left px-2 py-1">買い目</th>
                    <th className="text-left px-2 py-1">自信度</th>
                  </tr>
                </thead>
                <tbody>
                  {activeMp.top_payouts!.map((tp: MPrimeTopPayout, i: number) => (
                    <tr key={i} className="border-b border-border/20 hover:bg-brand-gold/5 transition-colors">
                      <td className="px-2 py-1">{i + 1}</td>
                      <td className="px-2 py-1 text-right font-bold" style={{ color: EMERALD }}>
                        {fmtNum(tp.payback)}円
                      </td>
                      <td className="px-2 py-1">{tp.date.slice(5)}</td>
                      <td className="px-2 py-1">{tp.venue}</td>
                      <td className="px-2 py-1 text-right">{tp.race_no}</td>
                      <td className="px-2 py-1">{tp.race_name || "—"}</td>
                      <td className="px-2 py-1 stat-mono">{tp.combo}</td>
                      <td className="px-2 py-1">{tp.confidence || "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </PremiumCard>
        </div>
      )}
    </div>
  );
}

// M' セクションヘッダー (タイトル + 年度タブ)
const EMERALD_STATIC = "#10b981";
function MPrimeHeader({
  mpYear, setMpYear, mpYears, dateFrom, dateTo,
}: {
  mpYear: string;
  setMpYear: (y: string) => void;
  mpYears: readonly string[];
  dateFrom?: string;
  dateTo?: string;
}) {
  return (
    <div className="flex items-center justify-between flex-wrap gap-2">
      <div className="flex items-baseline gap-2">
        <span className="font-extrabold tracking-wider uppercase text-xs" style={{ color: EMERALD_STATIC }}>
          M'
        </span>
        <span className="heading-section text-sm">
          M' 戦略 採用成績 (三連複)
        </span>
        {dateFrom && dateTo && (
          <span className="text-xs text-muted-foreground">
            {dateFrom} 〜 {dateTo}
          </span>
        )}
      </div>
      {/* M' 独自年度タブ */}
      <div
        role="tablist"
        aria-label="M' 期間フィルタ"
        className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg shadow-[var(--shadow-xs)]"
      >
        {mpYears.map((y) => {
          const active = mpYear === y;
          return (
            <button
              key={y}
              role="tab"
              aria-selected={active}
              onClick={() => setMpYear(y)}
              className={[
                "px-2.5 py-0.5 text-[10px] font-semibold rounded-md whitespace-nowrap tnum",
                "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
                active
                  ? "text-white shadow-sm"
                  : "text-muted-foreground hover:text-foreground hover:bg-background/60",
              ].join(" ")}
              style={active ? { background: EMERALD_STATIC } : undefined}
            >
              {y === "all" ? "全期間" : y + "年"}
            </button>
          );
        })}
      </div>
    </div>
  );
}
