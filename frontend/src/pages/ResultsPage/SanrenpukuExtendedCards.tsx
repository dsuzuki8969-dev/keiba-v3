import { useState } from "react";
import { PremiumCard } from "@/components/ui/premium/PremiumCard";
import type { MultiTicketSummary, MPrimeByConfidence } from "@/api/client";

// 三連複拡張カラーテーマ: 紫系 (#8b5cf6)
const SANRENPUKU_PURPLE = "#8b5cf6";

// 三連複 7 馬券の ticket_id 順
const SANRENPUKU_TICKET_IDS = [
  "trio_honmei_taikou_renka",
  "trio_honmei_taikou_wide1",
  "trio_honmei_taikou_wide2",
  "trio_honmei_taikou_wide3",
  "trio_honmei_renka_wide1",
  "trio_honmei_renka_wide2",
  "trio_honmei_renka_wide3",
] as const;

// 自信度 7 段階
const CONFIDENCE_ORDER = ["SS", "S", "A", "B", "C", "D", "E"] as const;
type ConfidenceRank = typeof CONFIDENCE_ORDER[number];

function fmtPct(v: number | null | undefined): string {
  return (v ?? 0).toFixed(1) + "%";
}

function fmtNum(v: number | null | undefined): string {
  return (v ?? 0).toLocaleString();
}

// 年度リスト生成
function buildYears(): string[] {
  const cur = new Date().getFullYear();
  const ys: string[] = ["all"];
  for (let y = 2024; y <= cur; y++) ys.push(String(y));
  return ys;
}

interface Props {
  sanrenpuku: MultiTicketSummary;
  sanrenpukuByYear?: Record<string, MultiTicketSummary | null | undefined>;
}

// 自信度カード 1 枚 (データなしはグレーアウト)
const ConfCell = ({
  rank,
  cell,
}: {
  rank: ConfidenceRank;
  cell: MPrimeByConfidence | undefined;
}) => {
  if (!cell) {
    return (
      <PremiumCard
        key={rank}
        variant="default"
        padding="sm"
        className="text-center opacity-40"
      >
        <div className="text-[10px] font-semibold text-muted-foreground mb-1">
          {rank}
        </div>
        <div className="text-xs text-muted-foreground mt-1">—</div>
      </PremiumCard>
    );
  }

  const roiColor = cell.roi_pct >= 100 ? SANRENPUKU_PURPLE : "#ef4444";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const played = cell.played ?? (cell as any).races ?? 0;
  const balance = (cell.payback ?? 0) - (cell.stake ?? 0);
  const balColor = balance >= 0 ? SANRENPUKU_PURPLE : "#ef4444";

  return (
    <PremiumCard
      variant="default"
      padding="sm"
      className="text-center stylish-card-hover border border-border/60"
    >
      <div className="flex items-center justify-center gap-1 mb-1">
        <span
          className="text-[11px] font-extrabold"
          style={{ color: SANRENPUKU_PURPLE }}
        >
          {rank}
        </span>
      </div>
      <div className="text-[9px] text-muted-foreground">{fmtNum(played)}R購入</div>
      <div className="text-[10px] text-muted-foreground mt-1">的中率</div>
      <div
        className="stat-mono text-sm font-bold"
        style={{ color: SANRENPUKU_PURPLE }}
      >
        {fmtPct(cell.hit_rate_pct)}
      </div>
      <div className="text-[10px] text-muted-foreground mt-0.5">ROI</div>
      <div className="stat-mono text-sm font-bold" style={{ color: roiColor }}>
        {fmtPct(cell.roi_pct)}
      </div>
      <div className="text-[10px] text-muted-foreground mt-0.5">純利</div>
      <div className="stat-mono text-xs font-bold" style={{ color: balColor }}>
        {(balance >= 0 ? "+" : "") + fmtNum(balance)}
      </div>
    </PremiumCard>
  );
};

// 三連複拡張セクションヘッダー + 年度タブ
const SanrenpukuHeader = ({
  sanrenpukuYear,
  setSanrenpukuYear,
  years,
  dateFrom,
  dateTo,
}: {
  sanrenpukuYear: string;
  setSanrenpukuYear: (y: string) => void;
  years: string[];
  dateFrom?: string;
  dateTo?: string;
}) => (
  <div className="flex items-center justify-between flex-wrap gap-2">
    <div className="flex items-baseline gap-2">
      <span
        className="font-extrabold tracking-wider uppercase text-xs"
        style={{ color: SANRENPUKU_PURPLE }}
      >
        三連複
      </span>
      <span className="heading-section text-sm">
        三連複 採用成績 (7 馬券)
      </span>
      {dateFrom && dateTo && (
        <span className="text-xs text-muted-foreground">
          {dateFrom} 〜 {dateTo}
        </span>
      )}
    </div>
    {/* 年度タブ */}
    <div
      role="tablist"
      aria-label="三連複 期間フィルタ"
      className="inline-flex items-center gap-0.5 p-0.5 bg-muted/60 border border-border rounded-lg shadow-[var(--shadow-xs)]"
    >
      {years.map((y) => {
        const active = sanrenpukuYear === y;
        return (
          <button
            key={y}
            role="tab"
            aria-selected={active}
            onClick={() => setSanrenpukuYear(y)}
            className={[
              "px-2.5 py-0.5 text-[10px] font-semibold rounded-md whitespace-nowrap tnum",
              "transition-all duration-[var(--dur-base)] ease-[var(--ease-out)]",
              active
                ? "text-white shadow-sm"
                : "text-muted-foreground hover:text-foreground hover:bg-background/60",
            ].join(" ")}
            style={active ? { background: SANRENPUKU_PURPLE } : undefined}
          >
            {y === "all" ? "全期間" : y + "年"}
          </button>
        );
      })}
    </div>
  </div>
);

// メインコンポーネント
export function SanrenpukuExtendedCards({ sanrenpuku, sanrenpukuByYear }: Props) {
  const YEARS = buildYears();
  const [sanrenpukuYear, setSanrenpukuYear] = useState<string>("all");

  // 選択中の年度データ
  const activeData: MultiTicketSummary | null =
    sanrenpukuYear === "all"
      ? (sanrenpukuByYear?.all ?? sanrenpuku)
      : (sanrenpukuByYear?.[sanrenpukuYear] ?? null);

  return (
    <div className="space-y-3 pt-5 mt-2">
      {/* セクション区切り */}
      <div
        className="relative h-px bg-gradient-to-r from-transparent via-violet-500/40 to-transparent"
        aria-hidden
      />

      <SanrenpukuHeader
        sanrenpukuYear={sanrenpukuYear}
        setSanrenpukuYear={setSanrenpukuYear}
        years={YEARS}
        dateFrom={activeData?.date_from}
        dateTo={activeData?.date_to}
      />

      {!activeData ? (
        <p className="text-sm text-muted-foreground py-4 text-center">
          {sanrenpukuYear}年の三連複データはありません
        </p>
      ) : (
        <div className="space-y-4">
          {/* 7 馬券 × 7 confidence = 49 カード */}
          {SANRENPUKU_TICKET_IDS.map((ticketId) => {
            const ticket = activeData.tickets.find(
              (t) => t.ticket_id === ticketId
            );

            // チケットが存在しない場合は行ごとグレーアウト
            if (!ticket) {
              return (
                <div key={ticketId} className="space-y-1">
                  <div className="text-[11px] font-semibold text-muted-foreground opacity-40">
                    {ticketId}
                  </div>
                  <div className="grid grid-cols-4 sm:grid-cols-7 gap-1.5 opacity-40">
                    {CONFIDENCE_ORDER.map((rank) => (
                      <PremiumCard
                        key={rank}
                        variant="default"
                        padding="sm"
                        className="text-center"
                      >
                        <div className="text-[10px] font-semibold text-muted-foreground mb-1">
                          {rank}
                        </div>
                        <div className="text-xs text-muted-foreground">—</div>
                      </PremiumCard>
                    ))}
                  </div>
                </div>
              );
            }

            return (
              <div key={ticketId} className="space-y-1">
                {/* 馬券ラベル + 全体集計 */}
                <div className="flex items-center gap-2 flex-wrap">
                  <span
                    className="text-[12px] font-extrabold"
                    style={{ color: SANRENPUKU_PURPLE }}
                  >
                    {ticket.ticket_label}
                  </span>
                  <span className="text-[10px] text-muted-foreground">
                    {fmtNum(ticket.total.played)}R /
                    的中率 {fmtPct(ticket.total.hit_rate_pct)} /
                    ROI{" "}
                    <span
                      className="font-bold"
                      style={{
                        color:
                          ticket.total.roi_pct >= 100
                            ? SANRENPUKU_PURPLE
                            : "#ef4444",
                      }}
                    >
                      {fmtPct(ticket.total.roi_pct)}
                    </span>
                  </span>
                </div>
                {/* 7 confidence カード */}
                <div className="grid grid-cols-4 sm:grid-cols-7 gap-1.5">
                  {CONFIDENCE_ORDER.map((rank) => (
                    <ConfCell
                      key={rank}
                      rank={rank}
                      cell={ticket.by_confidence[rank]}
                    />
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
