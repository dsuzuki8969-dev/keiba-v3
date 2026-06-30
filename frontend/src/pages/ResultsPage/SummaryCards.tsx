import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import { Trophy, ListChecks, CheckCircle2, Target } from "lucide-react";

interface Props {
  data: Record<string, unknown>;
}

function fmtNum(v: number | null | undefined): string {
  return (v ?? 0).toLocaleString();
}

export function SummaryCards({ data }: Props) {
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

        {/* 3指標カード: 勝率・連対率・複勝率（他テーブルと並び統一） */}
        <div className="grid grid-cols-3 gap-3">
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
    </div>
  );
}
