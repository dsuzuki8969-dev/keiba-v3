import { cn } from "@/lib/utils";
import { PremiumCard } from "./PremiumCard";

/**
 * Skeleton — 読み込み中のプレースホルダ
 * --------------------------------------------------------------
 * shimmer アニメーションで「今読み込んでいる」感を出す。
 * text: 高さ 1em / card: PremiumCard 風の箱 / circle: 丸
 */

interface SkeletonProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: "default" | "shimmer";
}

export function Skeleton({ variant = "shimmer", className, ...props }: SkeletonProps) {
  return (
    <div
      className={cn(
        "rounded-md bg-muted/60",
        variant === "shimmer" && "relative overflow-hidden before:absolute before:inset-0 before:-translate-x-full before:animate-[shimmer_1.8s_infinite] before:bg-gradient-to-r before:from-transparent before:via-white/30 before:to-transparent dark:before:via-white/10",
        className,
      )}
      aria-hidden="true"
      {...props}
    />
  );
}

/**
 * SummaryCardsSkeleton — ResultsPage のサマリカード用プレースホルダ
 */
export function SummaryCardsSkeleton() {
  return (
    <div className="space-y-3">
      <Skeleton className="h-3 w-64" />
      {/* ヒーロー 3 枚 */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {[0, 1, 2].map((i) => (
          <PremiumCard key={i} variant="default" padding="md" className="text-center space-y-2">
            <Skeleton className="h-3 w-24 mx-auto" />
            <Skeleton className="h-10 w-40 mx-auto" />
          </PremiumCard>
        ))}
      </div>
      {/* サブ 7 枚 */}
      <div className="grid grid-cols-2 sm:grid-cols-4 md:grid-cols-7 gap-2">
        {Array.from({ length: 7 }).map((_, i) => (
          <PremiumCard key={i} variant="default" padding="sm" className="text-center space-y-1.5">
            <Skeleton className="h-2.5 w-12 mx-auto" />
            <Skeleton className="h-5 w-16 mx-auto" />
          </PremiumCard>
        ))}
      </div>
    </div>
  );
}

/**
 * ChartSkeleton — TrendCharts 用のプレースホルダ
 */
export function ChartSkeleton({ count = 2 }: { count?: number }) {
  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {Array.from({ length: count }).map((_, i) => (
        <PremiumCard key={i} variant="default" padding="md" className="space-y-3">
          <div className="flex items-center justify-between">
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-3 w-12" />
          </div>
          <Skeleton className="h-[220px] w-full" />
        </PremiumCard>
      ))}
    </div>
  );
}
