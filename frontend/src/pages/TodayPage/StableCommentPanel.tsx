import { WAKU_BG } from "@/lib/constants";
import { parseStableComment } from "@/lib/parseStableComment";
import type { HorseData } from "./RaceDetailView";

const MARK_COLORS: Record<string, string> = {
  "◉": "text-emerald-600", "◎": "text-emerald-600",
  "○": "text-blue-600", "▲": "text-red-600", "△": "text-purple-600",
  "★": "text-foreground", "☆": "text-blue-600", "×": "text-red-600",
};

interface Props {
  horses: HorseData[];
}

export function StableCommentPanel({ horses }: Props) {
  const sorted = [...horses].sort((a, b) => (a.horse_no || 0) - (b.horse_no || 0));

  // 厩舎コメントがある馬だけ抽出
  const withComment = sorted.filter((h) => {
    const trRecs = (h as Record<string, unknown>).training_records as Array<Record<string, unknown>> | undefined;
    return trRecs?.[0]?.stable_comment;
  });

  if (withComment.length === 0) {
    return <div className="text-sm text-muted-foreground py-4">厩舎コメントなし</div>;
  }

  return (
    <div className="space-y-2">
      {withComment.map((h) => {
        const trRecs = (h as Record<string, unknown>).training_records as Array<Record<string, unknown>>;
        const rec = trRecs[0];
        const bullets = rec.stable_comment_bullets as string[] | undefined;
        const rawComment = rec.stable_comment as string;
        const mark = h.mark || "";

        return (
          <div key={h.horse_no} className="border border-border/50 rounded-md p-3">
            <div className="flex items-center gap-2 mb-1.5">
              <span className={`inline-flex w-5 h-5 items-center justify-center rounded-sm text-[10px] font-bold shrink-0 ${WAKU_BG[h.gate_no as number] || "bg-gray-200"}`}>
                {h.horse_no}
              </span>
              {mark && <span className={`${MARK_COLORS[mark] || ""} font-bold text-base leading-none`}>{mark}</span>}
              <span className="text-sm font-bold">{h.horse_name}</span>
              <span className="text-xs text-muted-foreground">{h.trainer || ""}</span>
            </div>
            {/* 箇条書き表示: T-025 (2026-04-28) bullets / rawComment 両方を parseStableComment で統一処理 */}
            {(() => {
              // 統一: bullets 配列があれば join、無ければ原文 → parseStableComment で prefix/曖昧表現を除去
              const inputText = bullets && bullets.length > 0
                ? bullets.join('\n')
                : rawComment;
              const parsed = parseStableComment(inputText);
              if (parsed.length === 0) return null;
              return (
                <ul className="text-[13px] leading-relaxed text-foreground bg-muted/30 rounded p-2 space-y-1">
                  {parsed.map((b, i) => (
                    <li key={i} className="flex gap-1.5 items-start">
                      {/* T-024 (2026-04-28): 箇条書き「・」マーカー追加 */}
                      <span className="text-muted-foreground shrink-0">・</span>
                      <span>{b.text}</span>
                    </li>
                  ))}
                </ul>
              );
            })()}
          </div>
        );
      })}
    </div>
  );
}
