import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Trophy, ListChecks, CheckCircle2 } from "lucide-react";
import type { HybridSummaryResponse, MPrimeSanrenpukuSummary, MPrimeByConfidence, MPrimeTopPayout } from "@/api/client";

interface Props {
  data: Record<string, unknown>;
  hybrid?: HybridSummaryResponse | null;
}

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}

function fmtNum(v: number): string {
  return v.toLocaleString();
}

export function SummaryCards({ data, hybrid }: Props) {
  if (!data.total_races) return null;

  // 数値取得
  const tanshoStake = Number(data.honmei_tansho_stake ?? 0);
  const tanshoRet = Number(data.honmei_tansho_ret ?? 0);
  const tanshoRoi = Number(data.honmei_tansho_roi ?? 0);

  // 結果 X-X-X-X（1着-2着-3着-着外）
  const win = Number(data.honmei_win ?? 0);
  const p2 = Number(data.honmei_place2 ?? 0);
  const p3 = Number(data.honmei_placed ?? 0);
  const total = Number(data.honmei_total ?? 0);
  const second = p2 - win;
  const third = p3 - p2;
  const out = total - p3;

  // 期間情報
  const periodParts: string[] = [];
  if (data.fetched_oldest && data.fetched_newest) {
    periodParts.push(`${data.fetched_oldest} 〜 ${data.fetched_newest}`);
  }
  if (data.period_days) {
    periodParts.push(`${data.period_days}日分`);
  }

  // 下段サブカード (2-1〜2-6: 勝率 / 連対率 / 複勝率 / 回収率 / 購入額 / 払戻額)
  const subCards: { label: string; value: string; isRoi?: boolean }[] = [
    { label: "◉◎勝率", value: fmtPct(Number(data.honmei_win_rate ?? 0)) },
    { label: "◉◎連対率", value: fmtPct(Number(data.honmei_place2_rate ?? 0)) },
    { label: "◉◎複勝率", value: fmtPct(Number(data.honmei_rate ?? 0)) },
    { label: "◉◎回収率", value: fmtPct(tanshoRoi), isRoi: true },
    { label: "購入額", value: fmtNum(tanshoStake) + "円" },
    { label: "払戻額", value: fmtNum(tanshoRet) + "円" },
  ];

  return (
    <div className="space-y-3">
      {periodParts.length > 0 && (
        <div className="text-xs text-muted-foreground">
          {periodParts.join("　")}
        </div>
      )}

      {/* 上段ヒーロー (1-1: 結果 / 1-2: 予想R数 / 1-3: 的中R数)
          結果カードに広めのスペースを割り当て (1.6fr) で大きいフォント維持 */}
      <div
        className="grid grid-cols-1 gap-3 sm:gap-3"
        style={{ gridTemplateColumns: "minmax(0, 1.6fr) minmax(0, 1fr) minmax(0, 1fr)" }}
      >
        {/* 1-1: ◉◎結果 X-X-X-X 1 行・大きく表示 */}
        <PremiumCard variant="default" padding="md" className="text-center stylish-card-hover border border-border/60">
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            <Trophy size={12} className="text-brand-gold" />
            ◉◎結果
          </div>
          <div
            className="stat-mono tnum leading-tight whitespace-nowrap overflow-hidden"
            style={{
              // 結果カード幅 (約 ~480px / sm 以上) なら 18 桁でも 1.6rem 維持可能
              fontSize: (() => {
                const totalDigits = String(win).length + String(second).length + String(third).length + String(out).length;
                if (totalDigits >= 18) return "1.5rem";  // 例: 13126-7632-5182-13461
                if (totalDigits >= 14) return "1.7rem";
                if (totalDigits >= 10) return "1.9rem";
                return "2.1rem";
              })(),
              letterSpacing: "-0.01em",
            }}
          >
            <span className="text-positive">{fmtNum(win)}</span>
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            {fmtNum(second)}
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            {fmtNum(third)}
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            <span className="text-muted-foreground">{fmtNum(out)}</span>
          </div>
          <div className="mt-1 text-[11px] text-muted-foreground tnum">
            複勝 {total > 0 ? ((p3 / total) * 100).toFixed(1) : "—"}%
          </div>
        </PremiumCard>

        {/* 1-2: 予想R数 */}
        <PremiumCard variant="default" padding="md" className="text-center stylish-card-hover border border-border/60">
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            <ListChecks size={12} className="text-brand-gold" />
            予想R数
          </div>
          <div className="stat-mono text-[1.9rem] sm:text-[2.3rem] tnum">
            {fmtNum(total)}
            <span className="text-base ml-0.5 font-semibold text-muted-foreground">R</span>
          </div>
        </PremiumCard>

        {/* 1-3: 的中R数 */}
        <PremiumCard
          variant={win > 0 ? "gold" : "default"}
          padding="md"
          className="text-center stylish-card-hover border border-border/60"
        >
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            <CheckCircle2 size={12} className="text-brand-gold-dark" />
            的中R数
          </div>
          <div className={`text-[1.9rem] sm:text-[2.3rem] tnum ${win > 0 ? "stat-mono-gold" : "stat-mono"}`}>
            {fmtNum(win)}
            <span className="text-base ml-0.5 font-semibold text-muted-foreground">R</span>
          </div>
          <div className="mt-1 text-[11px] text-muted-foreground tnum">
            的中率 {total > 0 ? ((win / total) * 100).toFixed(1) : "—"}%
          </div>
        </PremiumCard>
      </div>

      {/* 下段サブ (2-1〜2-6: 勝率/連対率/複勝率/回収率/購入額/払戻額) */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-2">
        {subCards.map((c) => (
          <PremiumCard
            key={c.label}
            variant="default"
            padding="sm"
            className="text-center"
          >
            <div className="text-[11px] text-muted-foreground mb-0.5">
              {c.label}
            </div>
            <div className={`stat-mono text-base ${c.isRoi ? (tanshoRoi >= 100 ? "text-positive font-bold" : "text-negative font-bold") : ""}`}>
              {c.value}
            </div>
          </PremiumCard>
        ))}
      </div>

      {/* ─── M' 戦略 採用成績 ─── */}
      {hybrid?.m_prime_sanrenpuku && <MPrimeSummarySection mp={hybrid.m_prime_sanrenpuku} />}

      {/* ─── T-050 採用戦略成績 (三連複動的 + 単勝) — 3×3 グリッド (fallback) ─── */}
      {hybrid && (() => {
        const spuku = hybrid.sanrenpuku_dynamic;
        const tansho = hybrid.tansho_t4;
        if (!spuku || !tansho) return null;

        // マスター指示 2026-05-01: 「絞り」廃止 → by_variant の絞り分を減算して再計算
        // (バックエンドが旧コード稼働中の場合に備えてフロント側で除外)
        const bv = (spuku.by_variant ?? {}) as Record<string, { races: number; hit: number; stake: number; payback: number }>;
        const narrow = bv["絞り"] ?? { races: 0, hit: 0, stake: 0, payback: 0 };

        const spukuStake   = spuku.total_stake   - narrow.stake;
        const spukuPayback = spuku.total_payback - narrow.payback;
        const spukuRaces   = spuku.races_played  - narrow.races;
        const spukuHits    = spuku.races_hit     - narrow.hit;
        const spukuRoi     = spukuStake > 0 ? spukuPayback / spukuStake * 100 : 0;
        const spukuHitRate = spukuRaces > 0 ? spukuHits / spukuRaces * 100 : 0;
        const spukuProfit  = spukuPayback - spukuStake;

        const tanshoProfit = tansho.total_payback - tansho.total_stake;

        // 合算 (絞り除外後の三連複 + 単勝)
        const totalStake   = spukuStake + tansho.total_stake;
        const totalPayback = spukuPayback + tansho.total_payback;
        const totalProfit  = totalPayback - totalStake;
        const totalRoi     = totalStake > 0 ? totalPayback / totalStake * 100 : 0;
        const totalRaces   = spukuRaces + tansho.races_played;
        const totalHits    = spukuHits + tansho.races_hit;
        const totalHitRate = totalRaces > 0 ? totalHits / totalRaces * 100 : 0;

        // セルレンダラー (hero=上段大カード / 通常=下段サブカード)
        const StatCell = ({
          label, value, sub, color, isProfit, hero,
        }: { label: string; value: string; sub?: string; color: string; isProfit?: boolean; hero?: boolean }) => (
          <PremiumCard
            variant="default"
            padding={hero ? "md" : "sm"}
            className="text-center stylish-card-hover border border-border/60"
          >
            <div className={`font-semibold tracking-wider uppercase text-muted-foreground mb-1 ${hero ? "text-[11px]" : "text-[10px]"}`}>
              {label}
            </div>
            <div
              className={`stat-mono font-bold ${
                hero ? "text-[1.5rem] sm:text-[1.8rem]" : "text-base"
              } ${isProfit && value.startsWith("-") ? "text-negative" : ""}`}
              style={!(isProfit && value.startsWith("-")) ? { color } : undefined}
            >
              {value}
            </div>
            {sub && <div className={`text-muted-foreground mt-0.5 tnum ${hero ? "text-[10px]" : "text-[9px]"}`}>{sub}</div>}
          </PremiumCard>
        );

        const COLORS = {
          combined:  totalProfit  >= 0 ? "#3b82f6" : "#ef4444", // 青 / 赤
          sanrenpuku: spukuProfit >= 0 ? "#3b82f6" : "#ef4444",
          tansho:     tanshoProfit >= 0 ? "#22c55e" : "#ef4444", // 緑 / 赤
        };

        return (
          <div className="space-y-3 pt-5 mt-2">
            {/* セクション区切り */}
            <div className="relative h-px bg-gradient-to-r from-transparent via-brand-gold/40 to-transparent" aria-hidden />
            <div className="flex items-baseline gap-2">
              <span className="font-extrabold tracking-wider uppercase text-xs" style={{ color: "#3b82f6" }}>
                Hybrid
              </span>
              <span className="heading-section text-sm">
                T-050 採用戦略成績 (三連複動的 + 単勝)
              </span>
              <span className="text-xs text-muted-foreground">（A-NONE 本番採用 / 絞り廃止）</span>
            </div>

            {/* 上段ヒーロー (1-1: 合算収支 / 1-2: 合算回収率 / 1-3: 合算的中率) */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <StatCell
                label="合算 収支"
                value={(totalProfit >= 0 ? "+" : "") + fmtNum(totalProfit) + "円"}
                sub={`購入 ${fmtNum(totalStake)} / 払戻 ${fmtNum(totalPayback)}`}
                color={COLORS.combined}
                isProfit
                hero
              />
              <StatCell
                label="合算 回収率"
                value={fmtPct(totalRoi)}
                sub={`${totalRaces}R 適用`}
                color={COLORS.combined}
                hero
              />
              <StatCell
                label="合算 的中率"
                value={fmtPct(totalHitRate)}
                sub={`${totalHits}R / ${totalRaces}R 加重`}
                color={COLORS.combined}
                hero
              />
            </div>

            {/* 下段サブ (2-1〜2-3: 三連複 / 2-4〜2-6: 単勝) */}
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-2">
              {/* 2-1: 三連複 収支 */}
              <StatCell
                label="三連複 収支"
                value={(spukuProfit >= 0 ? "+" : "") + fmtNum(spukuProfit) + "円"}
                sub={`購入 ${fmtNum(spukuStake)}`}
                color={COLORS.sanrenpuku}
                isProfit
              />
              {/* 2-2: 三連複 回収率 */}
              <StatCell
                label="三連複 回収率"
                value={fmtPct(spukuRoi)}
                sub={`${spukuRaces}R`}
                color={COLORS.sanrenpuku}
              />
              {/* 2-3: 三連複 的中率 */}
              <StatCell
                label="三連複 的中率"
                value={fmtPct(spukuHitRate)}
                sub={`${spukuHits}/${spukuRaces}R`}
                color={COLORS.sanrenpuku}
              />
              {/* 2-4: 単勝 収支 */}
              <StatCell
                label="単勝 収支"
                value={(tanshoProfit >= 0 ? "+" : "") + fmtNum(tanshoProfit) + "円"}
                sub={`購入 ${fmtNum(tansho.total_stake)}`}
                color={COLORS.tansho}
                isProfit
              />
              {/* 2-5: 単勝 回収率 */}
              <StatCell
                label="単勝 回収率"
                value={fmtPct(tansho.roi_pct)}
                sub={`${tansho.races_played}R`}
                color={COLORS.tansho}
              />
              {/* 2-6: 単勝 的中率 */}
              <StatCell
                label="単勝 的中率"
                value={fmtPct(tansho.hit_rate_pct)}
                sub={`${tansho.races_hit}/${tansho.races_played}R`}
                color={COLORS.tansho}
              />
            </div>
          </div>
        );
      })()}
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

function MPrimeSummarySection({ mp }: { mp: MPrimeSanrenpukuSummary }) {
  const EMERALD  = "#10b981";
  const PROFIT_COLOR = mp.balance >= 0 ? EMERALD : "#ef4444";

  // セルレンダラー
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

  // 自信度別カード (SS/S/A/B/C/D)
  const confidenceOrder = ["SS", "S", "A", "B", "C", "D"] as const;

  return (
    <div className="space-y-3 pt-5 mt-2">
      {/* セクション区切り */}
      <div className="relative h-px bg-gradient-to-r from-transparent via-emerald-500/40 to-transparent" aria-hidden />
      <div className="flex items-baseline gap-2">
        <span className="font-extrabold tracking-wider uppercase text-xs" style={{ color: EMERALD }}>
          M'
        </span>
        <span className="heading-section text-sm">
          M' 戦略 採用成績 (三連複)
        </span>
        <span className="text-xs text-muted-foreground">
          {mp.date_from} 〜 {mp.date_to}
        </span>
      </div>

      {/* 上段ヒーロー (的中率 / 回収率 / 純利) */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <StatCell
          label="的中率"
          value={fmtPct(mp.hit_rate_pct)}
          sub={`${fmtNum(mp.races_hit)} / ${fmtNum(mp.races_played)} R`}
          color={EMERALD}
          hero
        />
        <StatCell
          label="回収率 ROI"
          value={fmtPct(mp.roi_pct)}
          sub={`購入 ${fmtNum(mp.total_stake)} 円`}
          color={mp.roi_pct >= 100 ? EMERALD : "#ef4444"}
          hero
        />
        <StatCell
          label="純利"
          value={(mp.balance >= 0 ? "+" : "") + fmtNum(mp.balance) + "円"}
          sub={`払戻 ${fmtNum(mp.total_payback)} 円`}
          color={PROFIT_COLOR}
          hero
        />
      </div>

      {/* 自信度別内訳 6カード */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-2">
        {confidenceOrder.map((rank) => {
          const c: MPrimeByConfidence | undefined = mp.by_confidence[rank];
          const pts = M_PRIME_POINTS[rank] ?? "?";
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
              {/* 的中率 */}
              <div className="text-[10px] text-muted-foreground">的中率</div>
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
      {mp.top_payouts && mp.top_payouts.length > 0 && (
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
                  {mp.top_payouts.map((tp: MPrimeTopPayout, i: number) => (
                    <tr key={i} className="border-b border-border/20 hover:bg-muted/30">
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
