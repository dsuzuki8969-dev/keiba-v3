import { Badge } from "@/components/ui/badge";

/**
 * グレードバッジ（G1/G2/G3/L/OP、v6.1 プレミアム）
 * --------------------------------------------------------------
 * G1 は金箔グラデ + 発光、G2/G3 はグラデ単色、L/OP は単色。
 */

const STYLES: Record<string, string> = {
  G2: "bg-gradient-to-br from-blue-500 to-blue-700 text-white border border-blue-400/50",
  G3: "bg-gradient-to-br from-emerald-500 to-emerald-700 text-white border border-emerald-400/50",
  L:  "bg-gradient-to-br from-purple-500 to-purple-700 text-white border border-purple-400/50",
  OP: "bg-gradient-to-br from-gray-500 to-gray-700 text-white border border-gray-400/50",
  Jpn1: "bg-gradient-to-br from-red-600 to-red-800 text-white border border-red-400/50",
  Jpn2: "bg-gradient-to-br from-blue-500 to-blue-700 text-white border border-blue-400/50",
  Jpn3: "bg-gradient-to-br from-emerald-500 to-emerald-700 text-white border border-emerald-400/50",
};

interface Props {
  grade: string;
  className?: string;
}

export function GradeBadge({ grade, className = "" }: Props) {
  if (!grade) return null;
  const isG1 = grade === "G1";

  // G1 は最上位 — 金箔グラデ + 発光
  const g1Style = isG1
    ? {
        background: "linear-gradient(135deg, #fff1b8 0%, #e0b34a 50%, #c48717 100%)",
        color: "#3a260a",
        boxShadow: "0 0 0 1px rgba(212,168,83,0.35), 0 4px 12px -4px rgba(212,168,83,0.45)",
      }
    : undefined;

  const cls = isG1
    ? "border border-brand-gold/70 font-extrabold tracking-wider"
    : (STYLES[grade] ?? "bg-gray-400 text-white border border-gray-300");

  return (
    <Badge
      className={`${cls} text-[11px] font-extrabold px-2 py-0.5 ${className}`}
      style={g1Style}
    >
      {grade}
    </Badge>
  );
}
