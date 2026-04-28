import { MARKS, type MarkType } from "@/lib/constants";

/**
 * 印バッジ（v6.1 プレミアム刷新）
 * --------------------------------------------------------------
 * ◉鉄板・◎本命は金箔グラデ + 微発光。○/☆/▲/△/★/× は
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
  tekipan:   { color: "text-mark-tekipan",   gradient: true,  glow: "rgba(212,168,83,0.6)"  },
  honmei:    { color: "text-mark-honmei",    gradient: false, glow: "rgba(22,163,74,0.45)" },
  taikou:    { color: "text-mark-taikou",    glow: "rgba(37,99,235,0.45)" },
  tannuke:   { color: "text-mark-tannuke",   glow: "rgba(220,38,38,0.45)" },
  rendashi:  { color: "text-mark-rendashi",  glow: "rgba(124,58,237,0.45)" },
  rendashi2: { color: "text-foreground",     glow: "rgba(148,163,184,0.45)" },
  oana:      { color: "text-mark-oana",      glow: "rgba(37,99,235,0.45)" },
  kiken:     { color: "text-mark-tannuke",   glow: "rgba(220,38,38,0.45)" },
};

interface Props {
  mark: MarkKey;
  size?: "sm" | "md" | "lg" | "xl";
  /** true で控えめ（ホバー効果なし・一覧用）。false で華やか（詳細・ヒーロー用）。 */
  subtle?: boolean;
  className?: string;
}

export function MarkBadge({ mark, size = "md", subtle = false, className = "" }: Props) {
  const m = MARKS[mark as MarkType];
  if (!m) return null;

  const style = STYLES[mark] ?? { color: "text-muted-foreground" };
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
      title={m.label}
      aria-label={m.label}
    >
      {m.symbol}
    </span>
  );
}
