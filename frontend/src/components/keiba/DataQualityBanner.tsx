/**
 * データ品質警告バナー（再発防止策 #4）
 *
 * /api/data_quality の結果が has_violation=true のとき、
 * 赤色バナーでユーザーに警告を表示する。
 * 平常時（has_violation=false）は何も表示しない。
 */
import { useDataQuality } from "@/api/hooks";
import { AlertTriangle, X } from "lucide-react";
import { useState } from "react";

interface ViolationItem {
  name: string;
  ratio: number;
  threshold: number;
  count: number;
  total: number;
}

export function DataQualityBanner() {
  const { data, isLoading } = useDataQuality();
  const [dismissed, setDismissed] = useState(false);

  if (isLoading || dismissed) return null;

  // status === 'not_run' または has_violation が false の場合は非表示
  const hasViolation = (data as { has_violation?: boolean } | undefined)?.has_violation;
  if (!hasViolation) return null;

  const violations: ViolationItem[] =
    (data as { violations?: ViolationItem[] } | undefined)?.violations ?? [];
  const checkedAt: string =
    (data as { checked_at?: string } | undefined)?.checked_at ?? "";

  // 日時のフォーマット（YYYY-MM-DDTHH:MM:SS → MM/DD HH:MM）
  const formatCheckedAt = (s: string): string => {
    try {
      const d = new Date(s);
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const hh = String(d.getHours()).padStart(2, "0");
      const min = String(d.getMinutes()).padStart(2, "0");
      return `${mm}/${dd} ${hh}:${min}`;
    } catch {
      return s;
    }
  };

  return (
    <div
      role="alert"
      aria-live="polite"
      className="w-full rounded-lg border border-red-500/60 bg-red-950/30 dark:bg-red-950/50 px-4 py-3 text-sm"
    >
      <div className="flex items-start gap-3">
        <AlertTriangle
          size={16}
          className="mt-0.5 shrink-0 text-red-400"
          aria-hidden="true"
        />
        <div className="flex-1 min-w-0">
          {/* ヘッダー行 */}
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-semibold text-red-300">
              データ品質警告
            </span>
            {checkedAt && (
              <span className="text-xs text-red-400/70">
                （チェック: {formatCheckedAt(checkedAt)}）
              </span>
            )}
          </div>
          {/* 違反項目一覧 */}
          {violations.length > 0 && (
            <ul className="mt-1.5 space-y-0.5 text-red-300/90">
              {violations.map((v, i) => (
                <li key={i} className="flex items-center gap-1.5 text-xs">
                  <span className="text-red-400 font-mono">!</span>
                  <span>
                    {v.name}:{" "}
                    <strong className="font-semibold tabular-nums">
                      {(v.ratio * 100).toFixed(1)}%
                    </strong>{" "}
                    <span className="text-red-400/60">
                      （閾値 {(v.threshold * 100).toFixed(1)}% / {v.count.toLocaleString()}/{v.total.toLocaleString()}件）
                    </span>
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
        {/* 閉じるボタン */}
        <button
          type="button"
          onClick={() => setDismissed(true)}
          aria-label="警告バナーを閉じる"
          className="shrink-0 p-0.5 rounded text-red-400/60 hover:text-red-300 transition-colors"
        >
          <X size={14} />
        </button>
      </div>
    </div>
  );
}
