import { PremiumCard, PremiumCardHeader, PremiumCardTitle, PremiumCardAccent } from "@/components/ui/premium/PremiumCard";
import { Ticket } from "lucide-react";
import { circledNum, confColorClass, markCls } from "@/lib/constants";
import type {
  BetDecision,
  HorseData,
  TicketData,
  TicketsByMode,
} from "./RaceDetailView";

// 英字キー→印記号の変換
const MARK_SYM: Record<string, string> = {
  tekipan: "◉", honmei: "◎", taikou: "○", tannuke: "▲",
  rendashi: "△", rendashi2: "★", oana: "☆", kiken: "×",
  "◉": "◉", "◎": "◎", "○": "○", "▲": "▲",
  "△": "△", "★": "★", "☆": "☆", "×": "×",
};

interface Props {
  race: {
    confidence?: string;
    tickets?: TicketData[];
    formation_tickets?: TicketData[];
    formation_columns?: { col1?: number[]; col2?: number[]; col3?: number[] };
    bet_decision?: BetDecision;
    tickets_by_mode?: TicketsByMode;
    horses?: HorseData[];
    [key: string]: unknown;
  };
}

// ───────────── 補助コンポーネント ─────────────

function FmtStats({
  prob,
  odds,
  ev,
  isPredicted,
  oddsSource,
}: {
  prob: number;
  odds: number;
  ev: number;
  isPredicted?: boolean;
  oddsSource?: "real" | "estimated";
}) {
  // odds_source が "estimated" なら明示マーク、"real" なら目立たせる
  const oddsMark =
    oddsSource === "estimated" || isPredicted ? "*" : "";
  // モバイル幅で「EV:」とパーセント値が改行で分離するのを防ぐため、
  // 各メトリクスは whitespace-nowrap + inline-block で 1 セットを保つ
  return (
    <span className="text-xs text-muted-foreground inline-flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
      {prob > 0 && <span className="whitespace-nowrap">Rate: {(prob * 100).toFixed(1)}%</span>}
      {odds > 0 && (
        <span className="whitespace-nowrap">
          Odds: {odds.toFixed(1)}倍{oddsMark}
        </span>
      )}
      {ev > 0 && (
        <span className={`whitespace-nowrap ${ev >= 100 ? "text-positive font-semibold" : ""}`}>
          EV: {ev.toFixed(1)}%{oddsMark}
        </span>
      )}
    </span>
  );
}

/** 馬番→印記号の逆引きマップ */
function buildNoToMark(horses: HorseData[]): Record<number, string> {
  const m: Record<number, string> = {};
  for (const h of horses) {
    const sym = MARK_SYM[h.mark || ""] || "";
    if (sym && h.horse_no) m[h.horse_no] = sym;
  }
  return m;
}

/** マーク付きコンボ表示（◎⑫-○②-▲⑧） */
function ComboDisplay({
  ticket,
  noToMark,
}: {
  ticket: TicketData;
  noToMark: Record<number, string>;
}) {
  // combo 優先 / fallback a,b,c
  const nums =
    ticket.combo && ticket.combo.length > 0
      ? ticket.combo
      : ([ticket.a, ticket.b, ticket.c].filter(
          (x): x is number => typeof x === "number",
        ) as number[]);
  const keys = ["a", "b", "c"] as const;
  return (
    <strong className="whitespace-nowrap">
      {nums.map((n, i) => {
        const mk =
          (ticket[`mark_${keys[i]}` as keyof TicketData] as string) ||
          noToMark[n] ||
          "";
        return (
          <span key={i}>
            {i > 0 && <span className="text-muted-foreground mx-0.5">-</span>}
            <span className={markCls(mk)}>{mk}</span>
            {circledNum(n)}
          </span>
        );
      })}
    </strong>
  );
}

/** Phase 3: 三連単フォーメーションの列表示（軸/2着/3着） */
function SanrentanFormationColumns({
  formation,
}: {
  formation: {
    rank1?: Array<{ horse_no: number; mark: string }>;
    rank2?: Array<{ horse_no: number; mark: string }>;
    rank3?: Array<{ horse_no: number; mark: string }>;
  };
}) {
  const renderRow = (horses: Array<{ horse_no: number; mark: string }> | undefined) => {
    if (!horses || horses.length === 0) {
      return <span className="text-xs text-muted-foreground italic">なし</span>;
    }
    return (
      <span className="flex flex-wrap items-center gap-1">
        {horses.map((h, i) => (
          <span key={i} className="inline-flex items-center gap-0.5">
            <span className={`${markCls(h.mark || "－")} text-base leading-none`}>
              {h.mark || "－"}
            </span>
            <span className="tabular-nums font-semibold">{circledNum(h.horse_no)}</span>
          </span>
        ))}
      </span>
    );
  };

  const r1 = formation.rank1;
  const r2 = formation.rank2;
  const r3 = formation.rank3;

  return (
    <div className="space-y-2">
      {/* パターンA: ◎/◉ 1着 */}
      <div className="rounded-md border border-emerald-600/30 bg-emerald-50/30 dark:bg-emerald-950/10 p-2 text-sm">
        <div className="text-[11px] font-bold text-emerald-700 dark:text-emerald-400 mb-1">
          パターンA：◎/◉ 1着 → 相手 → ヒモ
        </div>
        <div className="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 items-center">
          <span className="text-xs text-muted-foreground">1着（軸）</span>
          {renderRow(r1)}
          <span className="text-xs text-muted-foreground">2着（相手）</span>
          {renderRow(r2)}
          <span className="text-xs text-muted-foreground">3着（ヒモ）</span>
          {renderRow(r3)}
        </div>
      </div>

      {/* パターンB: 相手 1着 → ◎/◉ 2着 */}
      <div className="rounded-md border border-blue-600/30 bg-blue-50/30 dark:bg-blue-950/10 p-2 text-sm">
        <div className="text-[11px] font-bold text-blue-700 dark:text-blue-400 mb-1">
          パターンB：相手 1着 → ◎/◉ 2着 → ヒモ
        </div>
        <div className="grid grid-cols-[72px_1fr] gap-x-3 gap-y-1 items-center">
          <span className="text-xs text-muted-foreground">1着（相手）</span>
          {renderRow(r2)}
          <span className="text-xs text-muted-foreground">2着（軸）</span>
          {renderRow(r1)}
          <span className="text-xs text-muted-foreground">3着（ヒモ）</span>
          {renderRow(r3)}
        </div>
      </div>
    </div>
  );
}


/** Phase 3: 三連単フォーメーション買い目表示 */
function SanrentanFormationView({
  tickets,
  noToMark,
  meta,
}: {
  tickets: TicketData[];
  noToMark: Record<number, string>;
  meta?: { skip_reason?: string; race_ev_ratio?: number; format?: string };
}) {
  const count = tickets.length;
  const totalStake = tickets.reduce((s, t) => s + (t.stake || 0), 0);
  const maxPayback = tickets.reduce((m, t) => Math.max(m, t.payback_if_hit || 0), 0);
  const evRatio = meta?.race_ev_ratio || 0;

  // パターン別グループ化（A: ◎軸1着 / B: 相手1着→◎2着）
  const patternA = tickets.filter((t) => (t as { pattern?: string }).pattern === "A");
  const patternB = tickets.filter((t) => (t as { pattern?: string }).pattern === "B");
  const otherTickets = tickets.filter((t) => {
    const p = (t as { pattern?: string }).pattern;
    return p !== "A" && p !== "B";
  });

  return (
    <div className="space-y-3">
      {/* サマリー */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground border-b border-border/50 pb-2">
        <span>
          <strong className="text-foreground">{count}</strong> 点
        </span>
        <span>
          投入 <strong className="text-foreground">{totalStake.toLocaleString()}</strong> 円
        </span>
        {maxPayback > 0 && (
          <span>
            最大払戻 <strong className="text-foreground">{maxPayback.toLocaleString()}</strong> 円
          </span>
        )}
        {evRatio > 0 && (
          <span>
            レース期待値倍率 <strong className={evRatio >= 1.5 ? "text-positive" : "text-foreground"}>{(evRatio * 100).toFixed(0)}%</strong>
          </span>
        )}
      </div>

      {/* パターンA: ◎1着 */}
      {patternA.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="px-2 py-0.5 bg-emerald-600 text-white text-xs font-bold rounded">
              ◎/◉ 1着 → 2着 → 3着
            </span>
            <span className="text-xs text-muted-foreground">{patternA.length}点</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-0.5">
            {patternA.map((t, i) => (
              <div key={`A-${i}`} className="flex flex-wrap items-center gap-2 text-sm py-0.5">
                <ComboDisplay ticket={t} noToMark={noToMark} />
                <FmtStats prob={t.prob || 0} odds={t.odds || 0} ev={t.ev || 0} oddsSource={t.odds_source} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* パターンB: 相手1着 → ◎2着 */}
      {patternB.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="px-2 py-0.5 bg-blue-600 text-white text-xs font-bold rounded">
              相手 1着 → ◎/◉ 2着 → 3着
            </span>
            <span className="text-xs text-muted-foreground">{patternB.length}点</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-0.5">
            {patternB.map((t, i) => (
              <div key={`B-${i}`} className="flex flex-wrap items-center gap-2 text-sm py-0.5">
                <ComboDisplay ticket={t} noToMark={noToMark} />
                <FmtStats prob={t.prob || 0} odds={t.odds || 0} ev={t.ev || 0} oddsSource={t.odds_source} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* その他（pattern情報なし） */}
      {otherTickets.length > 0 && (
        <div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-0.5">
            {otherTickets.map((t, i) => (
              <div key={`X-${i}`} className="flex flex-wrap items-center gap-2 text-sm py-0.5">
                <ComboDisplay ticket={t} noToMark={noToMark} />
                <FmtStats prob={t.prob || 0} odds={t.odds || 0} ev={t.ev || 0} oddsSource={t.odds_source} />
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}


/** T-050: 単勝1行表示 */
function TanshoRow({
  ticket,
  noToMark,
}: {
  ticket: TicketData;
  noToMark: Record<number, string>;
}) {
  const horseNo = ticket.horse_no;
  const mark = ticket.mark || (horseNo ? noToMark[horseNo] : "") || "";
  return (
    <div className="flex flex-wrap items-center gap-2 text-sm py-0.5">
      <strong className="whitespace-nowrap">
        <span className={markCls(mark)}>{mark}</span>
        {horseNo ? circledNum(horseNo) : "?"}
      </strong>
      <span className="text-xs text-muted-foreground inline-flex flex-wrap items-baseline gap-x-2 gap-y-0.5">
        {(ticket.odds ?? 0) > 0 && (
          <span className="whitespace-nowrap">Odds: {(ticket.odds!).toFixed(1)}倍</span>
        )}
      </span>
    </div>
  );
}

/** T-050: 三連複動的フォーメーション + 単勝T-4 ハイブリッド表示 */
function Phase4HybridFormation({
  tickets,
  noToMark,
  meta,
}: {
  tickets: TicketData[];
  noToMark: Record<number, string>;
  meta?: {
    skip_reason?: string;
    race_ev_ratio?: number;
    format?: string;
    sanrenpuku_count?: number;
    tansho_count?: number;
  };
}) {
  // 券種別グループ分け
  const sanrenpuku = tickets.filter((t) => t.type === "三連複");
  const tansho = tickets.filter((t) => t.type === "単勝");
  const totalPoints = sanrenpuku.length + tansho.length;
  const totalStake = totalPoints * 100;

  return (
    <div className="space-y-3">
      {/* サマリー */}
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground border-b border-border/50 pb-2">
        {sanrenpuku.length > 0 && (
          <span>
            三連複 <strong className="text-foreground">{sanrenpuku.length}</strong> 点
          </span>
        )}
        {tansho.length > 0 && (
          <span>
            単勝 <strong className="text-foreground">{tansho.length}</strong> 点
          </span>
        )}
        <span>
          計 <strong className="text-foreground">{totalPoints}</strong> 点
        </span>
        <span>
          投入 <strong className="text-foreground">{totalStake.toLocaleString()}</strong> 円
        </span>
      </div>

      {/* 三連複セクション */}
      {sanrenpuku.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="px-2 py-0.5 bg-purple-600 text-white text-xs font-bold rounded">
              三連複 動的フォーメーション
            </span>
            <span className="text-xs text-muted-foreground">{sanrenpuku.length}点</span>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-0.5">
            {sanrenpuku.map((t, i) => (
              <div key={`SR-${i}`} className="flex flex-wrap items-center gap-2 text-sm py-0.5">
                <ComboDisplay ticket={t} noToMark={noToMark} />
                <FmtStats
                  prob={t.prob || 0}
                  odds={t.odds || 0}
                  ev={t.ev || 0}
                  oddsSource={t.odds_source}
                />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* 単勝セクション */}
      {tansho.length > 0 && (
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="px-2 py-0.5 bg-amber-500 text-white text-xs font-bold rounded">
              単勝 T-4（◉◎＋○）
            </span>
            <span className="text-xs text-muted-foreground">{tansho.length}点</span>
          </div>
          <div className="flex flex-wrap gap-x-6 gap-y-0.5">
            {tansho.map((t, i) => (
              <TanshoRow key={`TS-${i}`} ticket={t} noToMark={noToMark} />
            ))}
          </div>
        </div>
      )}

      {/* データなし */}
      {tickets.length === 0 && meta?.skip_reason && (
        <div className="text-sm text-muted-foreground italic">
          見送り: {meta.skip_reason}
        </div>
      )}
    </div>
  );
}


/** 「買わない」判定の表示 + 参考ヒモグレー表示 */
function BetSkipPanel({
  decision,
  noToMark,
}: {
  decision: BetDecision;
  noToMark: Record<number, string>;
}) {
  return (
    <div className="flex flex-col gap-3 border-2 border-red-500/50 dark:border-red-400/40 rounded-lg p-5 bg-red-50/60 dark:bg-red-950/30">
      <div className="flex items-center gap-3 flex-wrap">
        <span className="text-4xl" aria-hidden>🚫</span>
        <div className="flex flex-col">
          <span className="text-2xl md:text-3xl font-extrabold text-red-700 dark:text-red-400 leading-tight">
            買わない（見送り）
          </span>
          <span className="text-sm md:text-base font-semibold text-red-800 dark:text-red-300 mt-1">
            {decision.reason && <>理由: {decision.reason}　</>}
            {decision.message || "このレースは対象外です"}
          </span>
        </div>
      </div>
      {decision.reference_tickets &&
        decision.reference_tickets.length > 0 && (
          <div className="text-sm space-y-0.5 border-t border-red-300/50 dark:border-red-700/40 pt-2 mt-1">
            <div className="text-xs text-muted-foreground">
              参考ヒモ（買わないが一応）:
            </div>
            {decision.reference_tickets.map((t, i) => (
              <div
                key={i}
                className="flex flex-wrap items-center gap-2 py-0.5 text-muted-foreground"
              >
                <ComboDisplay ticket={t} noToMark={noToMark} />
              </div>
            ))}
          </div>
        )}
    </div>
  );
}

// ───────────── メインコンポーネント ─────────────

export function TicketSection({ race }: Props) {
  const horses = (race.horses || []) as HorseData[];
  const tickets = (race.tickets || []) as TicketData[];
  const fmTickets = (race.formation_tickets || []) as TicketData[];

  const umarenTickets = tickets.filter((t) => t.type === "馬連");
  const sanrenTickets = tickets.filter((t) => t.type === "三連複");
  const fmSanren = fmTickets.filter(
    (t) => t.type === "三連複" && (t.stake || 0) > 0,
  );

  const tbm = race.tickets_by_mode;
  // fixed キー = Phase 3 三連単フォーメーション / T-050 ハイブリッドの両方に対応
  const fixedTickets: TicketData[] = (tbm?.fixed || []) as TicketData[];
  const hasFixed = fixedTickets.length > 0;
  // 後方互換: 旧 3モードキーがあっても無視。新方式は fixed のみ。
  const hasModes = hasFixed;
  const decision = race.bet_decision;
  // _meta: TicketsByMode 型に追加済みのフィールドを直接参照
  const tbmMeta = tbm?._meta as {
    skip_reason?: string;
    race_ev_ratio?: number;
    format?: string;
    sanrenpuku_count?: number;
    tansho_count?: number;
    formation_sanrentan?: {
      rank1?: Array<{ horse_no: number; mark: string }>;
      rank2?: Array<{ horse_no: number; mark: string }>;
      rank3?: Array<{ horse_no: number; mark: string }>;
    };
  } | undefined;

  // T-050 フォーマット判定（"T-050:" で始まる format 文字列）
  const isT050Format = !!(tbmMeta?.format?.startsWith("T-050:"));

  // 何も出力するものがなければ null
  if (
    !hasModes &&
    !decision?.skip &&
    umarenTickets.length === 0 &&
    sanrenTickets.length === 0 &&
    fmSanren.length === 0
  )
    return null;

  const confCol = confColorClass(race.confidence || "C");
  const noToMark = buildNoToMark(horses);

  return (
    <>
      {/* T-050 / Phase 3: 買い目指南単一表示 */}
      {(hasFixed || decision?.skip) && (
        <PremiumCard variant={decision?.skip ? "default" : "gold"} padding="md">
          <PremiumCardHeader>
            <div className="flex flex-col gap-0.5">
              <PremiumCardAccent>
                <Ticket size={10} className="inline mr-1" />
                Betting Guide
              </PremiumCardAccent>
              <PremiumCardTitle className="text-base flex items-center gap-3 flex-wrap">
                {isT050Format
                  ? "買い目指南（三連複動的 + 単勝T-4）"
                  : "買い目指南（三連単フォーメーション）"}
                <span
                  className="text-sm font-normal text-muted-foreground"
                  title="SS=鉄板級 / S=高信頼 / A=有力 / B=印通り / C=波乱含み / D=見送り"
                >
                  自信度: <strong className={confCol}>{race.confidence || "—"}</strong>
                </span>
                {decision?.skip && (
                  <span className="text-xs px-2 py-0.5 rounded bg-muted text-muted-foreground">
                    買わない
                  </span>
                )}
              </PremiumCardTitle>
              <p className="text-[11px] text-muted-foreground mt-1 leading-snug">
                {isT050Format
                  ? "三連複動的フォーメーション（中7点/広10点）＋単勝T-4（◉◎＋○）— 各点 100円固定。EV は期待払戻倍率の推定値。"
                  : "フォーメーション ◉/◎⇔○/▲/(☆)⇒○/▲/△/★/(☆)/(同断層内無印1-2頭) — 各点 100円固定。SS / C / D 信頼度は過去成績マイナスのため見送り。EV は期待払戻倍率の推定値。"}
              </p>
            </div>
          </PremiumCardHeader>
          <div className="space-y-3">
            {decision?.skip ? (
              <BetSkipPanel decision={decision} noToMark={noToMark} />
            ) : hasFixed ? (
              isT050Format ? (
                /* T-050: 三連複動的 + 単勝T-4 ハイブリッド表示 */
                <Phase4HybridFormation
                  tickets={fixedTickets}
                  noToMark={noToMark}
                  meta={tbmMeta}
                />
              ) : (
                /* Phase 3 旧: 三連単フォーメーション表示（fallback） */
                <>
                  {tbmMeta?.formation_sanrentan && (
                    <SanrentanFormationColumns formation={tbmMeta.formation_sanrentan} />
                  )}
                  <SanrentanFormationView
                    tickets={fixedTickets}
                    noToMark={noToMark}
                    meta={tbmMeta}
                  />
                </>
              )
            ) : null}
          </div>
        </PremiumCard>
      )}

      {/* 固定買い目（旧）— 互換のため継続表示。3モードが出ている場合は非表示 */}
      {!hasModes && !decision?.skip &&
        (umarenTickets.length > 0 || sanrenTickets.length > 0) && (
        <PremiumCard variant="default" padding="md">
          <PremiumCardHeader>
            <PremiumCardTitle className="text-base flex items-center gap-3">
              買い目（旧形式）
              <span className="text-sm font-normal text-muted-foreground">
                {umarenTickets.length + sanrenTickets.length}点　自信度:{" "}
                <strong className={confCol}>{race.confidence || "—"}</strong>
              </span>
            </PremiumCardTitle>
          </PremiumCardHeader>
          <div className="space-y-3">
            {umarenTickets.length > 0 && (
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <span className="px-2 py-0.5 bg-blue-600 text-white text-xs font-bold rounded">
                    馬連
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {umarenTickets.length}点
                  </span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-0.5">
                  {umarenTickets.map((t, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-2 text-sm py-0.5"
                    >
                      <ComboDisplay ticket={t} noToMark={noToMark} />
                      <FmtStats
                        prob={t.prob || 0}
                        odds={t.odds || 0}
                        ev={t.ev || 0}
                        oddsSource={t.odds_source}
                      />
                    </div>
                  ))}
                </div>
              </div>
            )}
            {sanrenTickets.length > 0 && (
              <div>
                <div className="flex items-center gap-2 mb-2">
                  <span className="px-2 py-0.5 bg-red-600 text-white text-xs font-bold rounded">
                    三連複
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {sanrenTickets.length}点
                  </span>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-0.5">
                  {sanrenTickets.map((t, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-2 text-sm py-0.5"
                    >
                      <ComboDisplay ticket={t} noToMark={noToMark} />
                      <FmtStats
                        prob={t.prob || 0}
                        odds={t.odds || 0}
                        ev={t.ev || 0}
                        oddsSource={t.odds_source}
                      />
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </PremiumCard>
      )}

      {/* 旧 formation 表示 — 3モードが出ている場合は非表示 */}
      {!hasModes && !decision?.skip && fmSanren.length > 0 && (
        <PremiumCard variant="default" padding="md">
          <PremiumCardHeader>
            <PremiumCardTitle className="text-base flex items-center gap-3">
              フォーメーション買い目（旧）
              <span className="text-sm font-normal text-muted-foreground">
                {fmSanren.length}点 /{" "}
                {fmSanren
                  .reduce((s, t) => s + (t.stake || 0), 0)
                  .toLocaleString()}
                円
              </span>
            </PremiumCardTitle>
          </PremiumCardHeader>
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="px-2 py-0.5 bg-purple-600 text-white text-xs font-bold rounded">
                三連複
              </span>
              <span className="text-xs text-muted-foreground">
                {fmSanren.length}点
              </span>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-0.5">
              {fmSanren.map((t, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 text-sm py-0.5"
                >
                  <ComboDisplay ticket={t} noToMark={noToMark} />
                  <FmtStats
                    prob={t.prob || 0}
                    odds={t.odds || 0}
                    ev={t.ev || 0}
                    oddsSource={t.odds_source}
                  />
                </div>
              ))}
            </div>
          </div>
        </PremiumCard>
      )}
    </>
  );
}
