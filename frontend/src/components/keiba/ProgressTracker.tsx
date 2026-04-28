import { fmtTime } from "@/lib/constants";

interface Props {
  /** 0〜100 */
  pct: number;
  /** 例: "3/12 レース" */
  countLabel?: string;
  /** 経過秒数 */
  elapsedSec?: number;
  /** 推定残り秒数 */
  remainSec?: number;
  /** フェーズラベル */
  phaseLabel?: string;
  /** 現在処理中ログ */
  currentLog?: string;
}

export function ProgressTracker({
  pct,
  countLabel,
  elapsedSec,
  remainSec,
  phaseLabel,
  currentLog,
}: Props) {
  return (
    <div className="space-y-2 text-sm">
      {/* フェーズラベル */}
      {phaseLabel && (
        <div className="font-medium">{phaseLabel}</div>
      )}

      {/* プログレスバー */}
      <div className="w-full bg-muted rounded-full h-3 overflow-hidden">
        <div
          className="h-full bg-primary rounded-full transition-all duration-300"
          style={{ width: `${Math.min(pct, 100)}%` }}
        />
      </div>

      {/* 数値情報 */}
      <div className="flex items-center gap-4 text-xs text-muted-foreground flex-wrap">
        <span className="font-semibold text-foreground">{pct}%</span>
        {countLabel && <span>{countLabel}</span>}
        {elapsedSec != null && <span>経過: {fmtTime(elapsedSec)}</span>}
        {remainSec != null && remainSec > 0 && (
          <span>残り: 約{fmtTime(remainSec)}</span>
        )}
      </div>

      {/* ログ */}
      {currentLog && (
        <div className="text-xs text-muted-foreground truncate">
          {currentLog}
        </div>
      )}
    </div>
  );
}
