import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { TrendingUp, TrendingDown, Target, Trophy } from "lucide-react";
import type { SanrentanSummaryResponse, HybridSummaryResponse } from "@/api/client";

interface Props {
  data: Record<string, unknown>;
  sanrentan?: SanrentanSummaryResponse | null;
  hybrid?: HybridSummaryResponse | null;
}

function fmtPct(v: number): string {
  return v.toFixed(1) + "%";
}

function fmtNum(v: number): string {
  return v.toLocaleString();
}

export function SummaryCards({ data, sanrentan, hybrid }: Props) {
  if (!data.total_races) return null;

  // ヒーロー数値（◉◎単勝ベースの収支・回収率・結果）
  const tanshoStake = Number(data.honmei_tansho_stake ?? 0);
  const tanshoRet = Number(data.honmei_tansho_ret ?? 0);
  const profit = tanshoRet - tanshoStake;
  const tanshoRoi = Number(data.honmei_tansho_roi ?? 0);

  // 結果 X-X-X-X（1着-2着-3着-着外）
  const win = Number(data.honmei_win ?? 0);
  const p2 = Number(data.honmei_place2 ?? 0);
  const p3 = Number(data.honmei_placed ?? 0);
  const total = Number(data.honmei_total ?? 0);
  const second = p2 - win;
  const third = p3 - p2;
  const out = total - p3;

  const cards: { label: string; value: string; color?: string }[] = [
    { label: "予想R数", value: (data.honmei_total || 0) + " R" },
    { label: "的中R数", value: win + " R" },
    { label: "◉◎勝率", value: fmtPct(Number(data.honmei_win_rate ?? 0)) },
    { label: "◉◎連対率", value: fmtPct(Number(data.honmei_place2_rate ?? 0)) },
    { label: "◉◎複勝率", value: fmtPct(Number(data.honmei_rate ?? 0)) },
    { label: "購入額", value: fmtNum(tanshoStake) + "円" },
    { label: "払戻額", value: fmtNum(tanshoRet) + "円" },
  ];

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

      {/* ヒーロー: 収支 + 回収率を大きく表示（v6.1.4 PremiumCard化） */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {/* 収支 — プラスなら金、マイナスなら default */}
        <PremiumCard
          variant={profit >= 0 ? "gold" : "default"}
          padding="md"
          className="text-center stylish-card-hover border border-border/60"
        >
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            {profit >= 0 ? <TrendingUp size={12} className="text-brand-gold-dark" /> : <TrendingDown size={12} className="text-negative" />}
            収支
          </div>
          <div className={`stat-mono text-[1.9rem] sm:text-[2.3rem] ${profit >= 0 ? "stat-mono-gold" : "text-negative heading-display"}`}>
            {profit >= 0 ? "+" : ""}{fmtNum(profit)}
            <span className="text-base ml-0.5 font-semibold">円</span>
          </div>
        </PremiumCard>

        {/* ◉◎単勝回収率 — プラスなら金 */}
        <PremiumCard
          variant={tanshoRoi >= 100 ? "gold" : "default"}
          padding="md"
          className="text-center stylish-card-hover border border-border/60"
        >
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            <Target size={12} className={tanshoRoi >= 100 ? "text-brand-gold-dark" : "text-negative"} />
            ◉◎単勝回収率
          </div>
          <div className={`text-[1.9rem] sm:text-[2.3rem] ${tanshoRoi >= 100 ? "stat-mono-gold" : "stat-mono text-negative"}`}>
            {fmtPct(tanshoRoi)}
          </div>
        </PremiumCard>

        {/* 結果 X-X-X-X — 桁数によって自動リサイズ */}
        <PremiumCard variant="default" padding="md" className="text-center stylish-card-hover border border-border/60">
          <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
            <Trophy size={12} className="text-brand-gold" />
            ◉◎結果
          </div>
          <div
            className="stat-mono leading-tight whitespace-nowrap"
            style={{
              // 5桁を超えると 1.9rem が枠に収まらないため文字数で fontSize を可変に
              fontSize: total >= 10000 ? "1.35rem" : total >= 1000 ? "1.7rem" : "2.1rem",
            }}
          >
            <span className="text-positive">{win}</span>
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            {second}
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            {third}
            <span className="text-muted-foreground/50 mx-0.5">-</span>
            <span className="text-muted-foreground">{out}</span>
          </div>
          {/* 補助: 勝率サマリ */}
          <div className="mt-1 text-[11px] text-muted-foreground tnum">
            的中{total > 0 ? ((p3 / total) * 100).toFixed(1) : "—"}%
          </div>
        </PremiumCard>
      </div>

      {/* サブ指標カード */}
      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 xl:grid-cols-7 gap-2">
        {cards.map((c) => (
          <PremiumCard
            key={c.label}
            variant="default"
            padding="sm"
            className="text-center"
          >
            <div className="text-[11px] text-muted-foreground mb-0.5">
              {c.label}
            </div>
            <div
              className={`stat-mono text-base ${c.color || ""}`}
            >
              {c.value}
            </div>
          </PremiumCard>
        ))}
      </div>

      {/* ─── 三連単フォーメーション成績（Phase 3） ─── */}
      {sanrentan && sanrentan.races_played > 0 && (() => {
        const sProfit = sanrentan.balance;
        const sRoi = sanrentan.roi_pct;
        const sHitRate = sanrentan.race_hit_rate_pct;
        const sCards: { label: string; value: string }[] = [
          { label: "予想R数", value: sanrentan.races_played + " R" },
          { label: "的中R数", value: sanrentan.races_hit + " R" },
          { label: "購入額", value: fmtNum(sanrentan.stake) + "円" },
          { label: "払戻額", value: fmtNum(sanrentan.payback) + "円" },
        ];
        return (
          <div className="space-y-3 pt-5 mt-2">
            {/* セクション区切り — ゴールドグラデのヘアライン */}
            <div className="relative h-px bg-gradient-to-r from-transparent via-brand-gold/40 to-transparent" aria-hidden />
            <div className="flex items-baseline gap-2">
              <span className="gold-gradient font-extrabold tracking-wider uppercase text-xs">
                Trifecta
              </span>
              <span className="heading-section text-sm">
                三連単フォーメーション成績
              </span>
              <span className="text-xs text-muted-foreground">（旧戦略・参考）</span>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {/* 収支 */}
              <PremiumCard
                variant={sProfit >= 0 ? "navy-glow" : "default"}
                padding="md"
                className="text-center"
              >
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  {sProfit >= 0 ? <TrendingUp size={12} /> : <TrendingDown size={12} className="text-negative" />}
                  収支
                </div>
                <div className={`text-[1.9rem] sm:text-[2.3rem] ${sProfit >= 0 ? "stat-mono-gold" : "stat-mono text-negative"}`}>
                  {sProfit >= 0 ? "+" : ""}{fmtNum(sProfit)}
                  <span className="text-base ml-0.5 font-semibold">円</span>
                </div>
              </PremiumCard>
              {/* 三連単F回収率 */}
              <PremiumCard
                variant={sRoi >= 100 ? "navy-glow" : "default"}
                padding="md"
                className="text-center"
              >
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  <Target size={12} />
                  三連単F回収率
                </div>
                <div className={`text-[1.9rem] sm:text-[2.3rem] ${sRoi >= 100 ? "stat-mono-gold" : "stat-mono text-negative"}`}>
                  {fmtPct(sRoi)}
                </div>
              </PremiumCard>
              {/* 三連単F的中率 */}
              <PremiumCard variant="default" padding="md" className="text-center">
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  <Trophy size={12} />
                  三連単F的中率
                </div>
                <div className="stat-mono text-[1.9rem] sm:text-[2.3rem]">
                  {fmtPct(sHitRate)}
                </div>
              </PremiumCard>
            </div>
            {/* サブ指標: 予想R / 的中R / 購入 / 払戻 */}
            <div className="grid grid-cols-2 sm:grid-cols-2 md:grid-cols-4 gap-2">
              {sCards.map((c) => (
                <PremiumCard
                  key={c.label}
                  variant="default"
                  padding="sm"
                  className="text-center"
                >
                  <div className="text-[11px] text-muted-foreground mb-0.5">{c.label}</div>
                  <div className="stat-mono text-base">{c.value}</div>
                </PremiumCard>
              ))}
            </div>
          </div>
        );
      })()}
      {/* ─── 新戦略成績 (三連複動的 + 単勝 T-4) ─── */}
      {hybrid && (() => {
        const spuku = hybrid.sanrenpuku_dynamic;
        const tansho = hybrid.tansho_t4;
        if (!spuku || !tansho) return null;

        // 合算収支・ROI
        const totalStake   = spuku.total_stake + tansho.total_stake;
        const totalPayback = spuku.total_payback + tansho.total_payback;
        const totalProfit  = totalPayback - totalStake;
        const totalRoi     = totalStake > 0 ? totalPayback / totalStake * 100 : 0;

        // 三連複内訳
        const bv = spuku.by_variant ?? {};
        const variantCards = (["絞り", "中", "広"] as const).map((v) => {
          const vd = bv[v];
          if (!vd || vd.races === 0) return null;
          return (
            <PremiumCard key={v} variant="default" padding="sm" className="text-center">
              <div className="text-[11px] text-muted-foreground mb-0.5">三連複{v} ({vd.races}R)</div>
              <div className={`stat-mono text-base ${vd.roi_pct >= 100 ? "text-positive" : ""}`}>
                ROI {vd.roi_pct.toFixed(1)}%
              </div>
              <div className="text-[10px] text-muted-foreground mt-0.5">的中率 {vd.hit_rate_pct.toFixed(1)}%</div>
            </PremiumCard>
          );
        }).filter(Boolean);

        return (
          <div className="space-y-3 pt-5 mt-2">
            {/* セクション区切り */}
            <div className="relative h-px bg-gradient-to-r from-transparent via-brand-gold/40 to-transparent" aria-hidden />
            <div className="flex items-baseline gap-2">
              <span className="font-extrabold tracking-wider uppercase text-xs" style={{ color: "#3b82f6" }}>
                Hybrid
              </span>
              <span className="heading-section text-sm">
                新戦略成績 (三連複動的 + 単勝 T-4)
              </span>
              <span className="text-xs text-muted-foreground">（A-NONE / 本番採用）</span>
            </div>

            {/* ヒーロー 3 枚: 合算収支 / 三連複動的 ROI / 単勝 T-4 ROI */}
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              {/* 合算収支 */}
              <PremiumCard
                variant={totalProfit >= 0 ? "navy-glow" : "default"}
                padding="md"
                className="text-center"
              >
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  {totalProfit >= 0
                    ? <TrendingUp size={12} style={{ color: "#3b82f6" }} />
                    : <TrendingDown size={12} className="text-negative" />}
                  合算収支
                </div>
                <div className={`text-[1.9rem] sm:text-[2.3rem] ${totalProfit >= 0 ? "stat-mono" : "stat-mono text-negative"}`}
                  style={totalProfit >= 0 ? { color: "#3b82f6" } : undefined}
                >
                  {totalProfit >= 0 ? "+" : ""}{fmtNum(totalProfit)}
                  <span className="text-base ml-0.5 font-semibold">円</span>
                </div>
                <div className="text-[11px] text-muted-foreground mt-1 tnum">
                  合算 ROI {totalRoi.toFixed(1)}%
                </div>
              </PremiumCard>

              {/* 三連複動的 ROI */}
              <PremiumCard
                variant={spuku.roi_pct >= 100 ? "navy-glow" : "default"}
                padding="md"
                className="text-center"
              >
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  <Target size={12} style={{ color: "#3b82f6" }} />
                  三連複動的 ROI
                </div>
                <div
                  className={`text-[1.9rem] sm:text-[2.3rem] ${spuku.roi_pct >= 100 ? "stat-mono" : "stat-mono text-negative"}`}
                  style={spuku.roi_pct >= 100 ? { color: "#3b82f6" } : undefined}
                >
                  {fmtPct(spuku.roi_pct)}
                </div>
                <div className="text-[11px] text-muted-foreground mt-1 tnum">
                  {spuku.races_played}R / 的中 {spuku.races_hit}R ({spuku.hit_rate_pct.toFixed(1)}%)
                </div>
              </PremiumCard>

              {/* 単勝 T-4 ROI */}
              <PremiumCard
                variant={tansho.roi_pct >= 100 ? "navy-glow" : "default"}
                padding="md"
                className="text-center"
              >
                <div className="inline-flex items-center gap-1.5 text-[11px] font-semibold tracking-wider uppercase text-muted-foreground mb-1">
                  <Trophy size={12} style={{ color: "#22c55e" }} />
                  単勝 T-4 ROI
                </div>
                <div
                  className={`text-[1.9rem] sm:text-[2.3rem] ${tansho.roi_pct >= 100 ? "stat-mono" : "stat-mono text-negative"}`}
                  style={tansho.roi_pct >= 100 ? { color: "#22c55e" } : undefined}
                >
                  {fmtPct(tansho.roi_pct)}
                </div>
                <div className="text-[11px] text-muted-foreground mt-1 tnum">
                  {tansho.races_played}R / 的中 {tansho.races_hit}R ({tansho.hit_rate_pct.toFixed(1)}%)
                </div>
              </PremiumCard>
            </div>

            {/* サブカード: 三連複内訳 (絞り/中/広) */}
            {variantCards.length > 0 && (
              <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-2">
                {variantCards}
                {/* 単勝 T-4 サブカード */}
                <PremiumCard variant="default" padding="sm" className="text-center">
                  <div className="text-[11px] text-muted-foreground mb-0.5">単勝 T-4</div>
                  <div className="stat-mono text-base">
                    {tansho.races_played}R
                  </div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">
                    購入 {fmtNum(tansho.total_stake)}円
                  </div>
                </PremiumCard>
                <PremiumCard variant="default" padding="sm" className="text-center">
                  <div className="text-[11px] text-muted-foreground mb-0.5">単勝 T-4 払戻</div>
                  <div className="stat-mono text-base">
                    {fmtNum(tansho.total_payback)}円
                  </div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">
                    収支 {tansho.balance >= 0 ? "+" : ""}{fmtNum(tansho.balance)}円
                  </div>
                </PremiumCard>
                <PremiumCard variant="default" padding="sm" className="text-center">
                  <div className="text-[11px] text-muted-foreground mb-0.5">三連複 購入</div>
                  <div className="stat-mono text-base">
                    {fmtNum(spuku.total_stake)}円
                  </div>
                  <div className="text-[10px] text-muted-foreground mt-0.5">
                    払戻 {fmtNum(spuku.total_payback)}円
                  </div>
                </PremiumCard>
              </div>
            )}
          </div>
        );
      })()}
    </div>
  );
}
