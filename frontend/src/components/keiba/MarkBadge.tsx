import { memo } from "react";
import { MARKS, type MarkType } from "@/lib/constants";

/**
 * 印バッジ（v6.1 プレミアム刷新）
 * --------------------------------------------------------------
 * ◉鉄板・◎本命は金箔グラデ + 微発光。○/☆/▲/△/★ は
 * セマンティックカラーに統一し、ホバーで微弱な scale/glow。
 *
 * a11y: MARKS[key].label を title に載せる既存仕様を維持。
 */

type MarkKey = MarkType | string;

interface Style {
  color: string;     // Tailwind クラス（text-*）
  gradient?: boolean; // true の場合 gold-gradient テキストに
  glow?: string;      // ホバー時の発光色（CSS 変数/色）
}

const STYLES: Record<string, Style> = {
  // ◉(鉄板): Phase 2+3 で赤系に変更。金箔グラデは廃止し赤発光で最上位強調
  tekipan:   { color: "text-mark-tekipan",   gradient: false, glow: "rgba(220,38,38,0.7)"   },
  honmei:    { color: "text-mark-honmei",    gradient: false, glow: "rgba(22,163,74,0.45)" },
  taikou:    { color: "text-mark-taikou",    glow: "rgba(37,99,235,0.45)" },
  tannuke:   { color: "text-mark-tannuke",   glow: "rgba(220,38,38,0.45)" },
  rendashi:  { color: "text-mark-rendashi",  glow: "rgba(124,58,237,0.45)" },
  // rendashi2 (★): 注目印としてフォアグラウンド色
  rendashi2: { color: "text-foreground",     glow: "rgba(148,163,184,0.45)" },
  // oana (☆＝押さえ・総合6位の序列印): 青系。constants.ts markCls() の色系統と整合 (2026-07-01 分離)
  oana:      { color: "text-blue-600 dark:text-blue-400", glow: "rgba(37,99,235,0.45)" },
  // ana_home (穴＝厳選穴馬 select_dark_horses): amber系で穴馬を強調 (2026-07-01 分離)
  ana_home:  { color: "text-amber-600 dark:text-amber-400", glow: "rgba(217,119,6,0.45)" },
  // oshi (抑): 削除（表示しない）
};

interface Props {
  mark: MarkKey;
  size?: "sm" | "md" | "lg" | "xl";
  /** true で控えめ（ホバー効果なし・一覧用）。false で華やか（詳細・ヒーロー用）。 */
  subtle?: boolean;
  className?: string;
}

export const MarkBadge = memo(function MarkBadge({ mark, size = "md", subtle = false, className = "" }: Props) {
  // 무 (抑え) / × (危険・廃止印) は表示しない
  if (mark === "무" || mark === "抑" || mark === "×") return null;

  // ☆ は MARKS.oana（symbol:"☆" label:"押さえ"）にマップして表示 (2026-07-01 分離)
  // 生の "穴"（厳選穴馬 select_dark_horses が付与するシンボル）は MARKS の直接キーとして
  // 存在しない（MARKS.ana_home に symbol:"穴" で登録）ため ana_home へマップする。
  // ここを通さないと m が undefined になり厳選穴馬バッジが描画されないリスクがあった。
  const normalizedMark =
    mark === "☆" ? "oana" :
    mark === "穴" ? "ana_home" :
    mark;

  const m = MARKS[normalizedMark as MarkType] ?? MARKS[mark as MarkType];
  if (!m) return null;

  const style = STYLES[normalizedMark] ?? STYLES[mark] ?? { color: "text-muted-foreground" };
  const sizeClass =
    size === "sm" ? "text-sm" :
    size === "lg" ? "text-2xl" :
    size === "xl" ? "text-4xl" :
    "text-lg";

  const isGradient = !subtle && style.gradient;

  // 鉄板（◉）は金箔グラデテキストで最上位装飾
  const colorClass = isGradient ? "gold-gradient" : style.color;

  // インライン style でホバー glow を可変色に
  const glowVar = !subtle && style.glow ? { "--mb-glow": style.glow } as React.CSSProperties : undefined;

  // 表示シンボル・ラベルは MARKS 定義をそのまま使用（☆と穴の上書き変換は廃止）
  const displaySymbol = m.symbol;
  const displayLabel = m.label;

  return (
    <span
      className={[
        colorClass,
        sizeClass,
        "font-extrabold leading-none inline-block",
        subtle ? "" : "transition-all duration-200 ease-out hover:scale-110",
        subtle ? "" : "hover:drop-shadow-[0_0_6px_var(--mb-glow)]",
        className,
      ].join(" ")}
      style={glowVar}
      title={displayLabel}
      aria-label={displayLabel}
    >
      {displaySymbol}
    </span>
  );
});
MarkBadge.displayName = "MarkBadge";
