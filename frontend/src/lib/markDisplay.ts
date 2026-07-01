/**
 * 予想印の表示変換ヘルパー
 * --------------------------------------------------------------
 * MarkBadge を経由しない生テキスト表示箇所で使用する。
 * MarkBadge 自体は内部で同等の変換を実施済みなので変更不要。
 *
 * 変換ルール (2026-07-01: ☆/穴 分離。詳細は MEMORY.md 参照):
 *   ☆           → ☆   （総合6位の序列印＝押さえ。そのままpass-through）
 *   穴           → 穴   （厳選穴馬。select_dark_horses が付与する専用印。pass-through）
 *   抑 / 무 / × → －   （非表示印。無印と区別不要）
 *   それ以外      → そのまま返す
 */
export function displayMark(mark: string | null | undefined): string {
  if (mark == null) return "";
  if (mark === "抑" || mark === "무" || mark === "×") return "－";
  return mark;
}
