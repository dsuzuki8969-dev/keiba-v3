/**
 * 予想印の表示変換ヘルパー
 * --------------------------------------------------------------
 * MarkBadge を経由しない生テキスト表示箇所で使用する。
 * MarkBadge 自体は内部で同等の変換を実施済みなので変更不要。
 *
 * 変換ルール:
 *   ☆           → 穴   （穴馬表記に統一）
 *   抑 / 무 / × → －   （非表示印。無印と区別不要）
 *   それ以外      → そのまま返す
 */
export function displayMark(mark: string | null | undefined): string {
  if (mark == null) return "";
  if (mark === "☆") return "穴";
  if (mark === "抑" || mark === "무" || mark === "×") return "－";
  return mark;
}
