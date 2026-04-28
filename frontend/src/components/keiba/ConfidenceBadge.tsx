import { Badge } from "@/components/ui/badge";
import type { ConfidenceRank } from "@/lib/constants";

/**
 * 自信度バッジ（v6.1 プレミアム）
 * --------------------------------------------------------------
 * SS=金箔グラデ / S=青グラデ / A=赤グラデ / B=墨 / C-F=灰。
 * 枠線強度で視認性を上げ、SS は微発光。
 */

const STYLES: Record<string, string> = {
  SS: "border border-brand-gold/70 text-[#1a1205] shadow-[0_0_0_1px_rgba(212,168,83,0.35),0_4px_12px_-4px_rgba(212,168,83,0.45)]",
  S:  "bg-gradient-to-br from-blue-500 to-blue-700 text-white border border-blue-400/50",
  A:  "bg-gradient-to-br from-red-500 to-red-700 text-white border border-red-400/50",
  B:  "bg-gradient-to-br from-gray-700 to-gray-900 text-white border border-gray-600/60",
  C:  "bg-gradient-to-br from-gray-400 to-gray-500 text-white border border-gray-300/50",
  D:  "bg-gradient-to-br from-gray-400 to-gray-500 text-white border border-gray-300/50",
  E:  "bg-gray-300 text-gray-600 border border-gray-200",
  F:  "bg-gray-300 text-gray-600 border border-gray-200",
};

interface Props {
  rank: ConfidenceRank | string;
  className?: string;
}

export function ConfidenceBadge({ rank, className = "" }: Props) {
  const style = STYLES[rank] ?? STYLES.C;

  // SS のみ金箔グラデ背景を直接スタイルで（Tailwind クラスでは表現しづらい多段グラデ）
  const isSS = rank === "SS";
  const ssStyle = isSS
    ? {
        background: "linear-gradient(135deg, #fff1b8 0%, #e0b34a 50%, #c48717 100%)",
      }
    : undefined;

  return (
    <Badge
      className={`${style} text-[11px] font-extrabold px-2 py-0.5 tracking-wider uppercase ${className}`}
      style={ssStyle}
    >
      {rank}
    </Badge>
  );
}
