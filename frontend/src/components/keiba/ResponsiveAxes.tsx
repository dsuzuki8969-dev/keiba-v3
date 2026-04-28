import type { ReactNode } from "react";

interface ResponsiveAxesProps {
  children: ReactNode;
  /** 軸数。8 なら xl で 8 列、4 列で折返し。7 なら xl で 7 列、4 列折返し。6 なら xl で 6 列、3 列折返し */
  count: 6 | 7 | 8;
  className?: string;
}

/**
 * 8/7/6 軸を狭幅では半分列で折返し、xl 以上で全列横並びにする共通コンポーネント。
 * - 8軸 → grid-cols-4 xl:grid-cols-8（モバイル/狭PCで4×2、wide PCで8×1）
 * - 7軸 → grid-cols-2 sm:grid-cols-4 xl:grid-cols-7
 * - 6軸 → grid-cols-3 xl:grid-cols-6
 *
 * 親に `min-w-0` を付与すること（flex 子の overflow 防止）。
 */
export function ResponsiveAxes({
  children,
  count,
  className = "",
}: ResponsiveAxesProps) {
  const colCls =
    count === 8
      ? "grid grid-cols-4 xl:grid-cols-8"
      : count === 7
        ? "grid grid-cols-2 sm:grid-cols-4 xl:grid-cols-7"
        : "grid grid-cols-3 xl:grid-cols-6";
  return (
    <div className={`${colCls} gap-1 items-start ${className}`}>
      {children}
    </div>
  );
}
