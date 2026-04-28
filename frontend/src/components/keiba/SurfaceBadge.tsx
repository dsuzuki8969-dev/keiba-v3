import { Badge } from "@/components/ui/badge";

/**
 * SurfaceBadge — 馬場タイプ（芝/ダート/障害）バッジ v6.1 プレミアム
 * --------------------------------------------------------------
 * 視覚識別を強化：グラデ背景 + 微細ボーダー
 *   芝   → 緑系グラデ
 *   ダート → 茶砂グラデ
 *   障害  → 紫系グラデ
 */

const STYLES: Record<string, { className: string; label: string }> = {
  芝: {
    className:
      "bg-gradient-to-br from-emerald-500 to-emerald-700 text-white border border-emerald-400/50",
    label: "芝",
  },
  ダート: {
    className:
      "bg-gradient-to-br from-amber-600 to-amber-800 text-white border border-amber-500/50",
    label: "ダート",
  },
  障害: {
    className:
      "bg-gradient-to-br from-purple-500 to-purple-700 text-white border border-purple-400/50",
    label: "障害",
  },
};

interface Props {
  surface: string;
  className?: string;
}

export function SurfaceBadge({ surface, className = "" }: Props) {
  const s = STYLES[surface] ?? {
    className: "bg-gradient-to-br from-gray-400 to-gray-600 text-white border border-gray-300/50",
    label: surface,
  };
  return (
    <Badge className={`${s.className} text-[11px] font-bold px-2 py-0.5 ${className}`}>
      {s.label}
    </Badge>
  );
}
