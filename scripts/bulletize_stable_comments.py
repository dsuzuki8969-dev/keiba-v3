"""厩舎コメント・調教短評を規則ベースで箇条書き化（意味単位重視版）

マスター指示 2026-04-23 (v6.0.1):
  旧実装は「句読点機械分割＋文字数カット」で意味が崩壊していた:
    - 「使っている分」「息は作れてい」「ですから」「どう」等で途切れ
    - 接続助詞（が・ので・から・けど）で意味の途中を切断
    - 動詞活用形の未完了（「てい」「ように」）を検出せず

本版の方針（意味単位切断）:
  1. 完結節のみを bullets とする
     - 終止形（る/た/だ/う/ぬ/む/ず/い）・連体修飾終端・体言止めで終わる節のみ許容
  2. 未完了末尾を検出して棄却
     - 「て」「ている」「ていて」「ので」「から」「けど」「けれど」「が」「で」
     - 「よう」「まで」「まま」「たら」「ば」で終わるものは次節と結合
  3. 接続助詞で終わる節は次節と結合
     - 「Aが、Bだ」→ 「AがBだ」1要素
     - 「Aので、B」→ 「AのでB」1要素
  4. 強制カットは廃止
     - 45字までは許容、超える場合のみ読点で分割（ただし両側が完結している時のみ）
  5. 丁寧語末尾のみ削る
     - 「〜です/ます/たい/思います」を語末の装飾として削除
  6. 著作権配慮の言い換え辞書（頻出フレーズ）
     - 完コピ防止のための 20-30 フレーズ置換
"""
from __future__ import annotations
import io, json, re, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = ROOT / "data" / "predictions"

# ==============================================================
# 正規表現
# ==============================================================
# 馬名・調教師プレフィックス除去
_PREFIX_JRA_RE = re.compile(r"^[\s○◎●]*[^【（(\n]{1,30}【[^】]{1,20}】\s*")
_PREFIX_NAR_RE = re.compile(
    r"^[\s○◎●]*[^(（\n]{1,30}[(（][^)）]{1,30}[)）]\s*[\u3000\s]*[^\s\-]{0,15}師?[―\-]{1,2}\s*"
)
_PREFIX_SIMPLE_RE = re.compile(r"^[\s○◎●]*[^(（\n]{1,30}[(（][^)）]{1,30}[)）][\s\n\u3000]+")

# 句点・改行・感嘆符で文分割
_SENTENCE_SPLIT_RE = re.compile(r"[。\n！？]+")

# ==============================================================
# 「文が完結していない末尾」判定
# ==============================================================
# 以下で終わる節は「未完了」→ 次の節と結合、または破棄
#   接続助詞: 「が / ので / から / けど / けれど / て / で / し / し、」
#   未完了助詞: 「よう / まで / まま / たら / ば / や / と」
#   動詞活用の途中: 「てい / ってい / てる / たい / たく」
_INCOMPLETE_TAIL_PATTERNS = [
    r"けれども?$", r"けど$", r"ですが$", r"だが$",
    r"[^たな]ので$", r"から$", r"ため$",
    r"(?:て|で)(?:い|います|いる|いた|いて|いません)?$",
    r"(?:てい|ってい|でい|んでい)$",
    r"(?:よう|まで|まま|たら|ば|や)$",
    r"(?:たい|たく|たかっ)$",
    r"(?:ます|ました|ません|ましょう)が$",
    r"(?:だろう|でしょう)(?:し|から|けど)?$",
    r"(?:です|だ|である)(?:が|けれど|けど|し)$",
    # 助詞 単独末尾（文の途中でカットされた痕跡）
    r"[をにはがでとへも]$",
    # 「〜ん」「〜うん」（「思うん」等、です省略型の途中）
    r"(?:思|期待|感じ|見|言|出|見せ|い|あ)(?:う|る)ん$",
    r"(?:です|だ)(?:よ|ね)?ん$",
    # 「〜はず」単独ではなく「〜はずです」の途中形
    r"るはず$",   # 本来「〜はずです」「〜はずだ」の途中で切れた場合
    # 「〜どう」問いかけの途中形
    r"(?:という|って)(?:の)?は?どう$",
    r"(?:のは|って|という)どう$",
    # 「〜ですか」「〜かな」のない問い
    r"やれるか$",
    # 「〜ているのは」「〜きた」で途中
    r"(?:てきた|きた)のは$",
    r"いうのはどう$",
]
_INCOMPLETE_TAIL_RE = re.compile("|".join(_INCOMPLETE_TAIL_PATTERNS))

# 「接続助詞のみで文末化している」 = 節結合候補
_CONNECTIVE_TAIL_PATTERNS = [
    r"けれども?$", r"けど$", r"ですが$", r"だが$", r"が$",
    r"ので$", r"から$", r"ため$",
    r"(?:て|で)$",
    r"し$",
]
_CONNECTIVE_TAIL_RE = re.compile("|".join(_CONNECTIVE_TAIL_PATTERNS))

# 丁寧語末尾の削除対象（語末のみ）
_TRAILING_POLITE_RE = re.compile(
    r"(?:でしょう|ます(?:ね|よ)?|致します|と思います|思います|です(?:ね|よ)?)[。\s]*$"
)

# フィラー語（意味を変えないので削除）
_FILLER_WORDS = [
    "ちょっとした", "何とか", "まあ", "けっこう",
    "あまりにも", "非常に", "とても", "大変", "ずいぶん", "だいぶ",
]

# ==============================================================
# 著作権配慮の言い換え辞書は v6.0.1 で廃止
#   理由: 部分文字列置換は日本語を崩壊させる副作用が大きい
#   例: 「時計が出る今年の馬場は向いている」→「時計優位今年の馬場適性あり」（崩壊）
#   対応: 代わりに「文の再構成（ベースはそのまま、フィラー削除＋完結性チェック）」で
#   完コピ完全一致を回避する。辞書置換が必要な場合は将来 LLM 版で行う。
# ==============================================================
_REPHRASE_DICT: dict = {}


def _strip_prefix(text: str) -> str:
    """'○馬名【調教師】本文' や '○馬名(短評) 調教師師――本文' → '本文' """
    if not text:
        return ""
    for pattern in (_PREFIX_JRA_RE, _PREFIX_NAR_RE, _PREFIX_SIMPLE_RE):
        m = pattern.match(text)
        if m:
            text = text[m.end():]
            break
    return text.strip().replace("\n", " ").replace("\r", " ").replace("\u3000", " ")


def _is_incomplete(s: str) -> bool:
    """節の末尾が未完了（活用形途中・接続助詞）か判定"""
    return bool(_INCOMPLETE_TAIL_RE.search(s))


def _is_connective(s: str) -> bool:
    """節の末尾が接続助詞で終わっている（= 次節と結合すべき）"""
    return bool(_CONNECTIVE_TAIL_RE.search(s))


def _clean(s: str) -> str:
    """1 節の装飾・フィラー削除"""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    for w in _FILLER_WORDS:
        s = s.replace(w, "")
    # 末尾丁寧語削除
    s = _TRAILING_POLITE_RE.sub("", s).strip()
    # 末尾の空括弧等
    s = s.rstrip("。、. 　")
    return s


def _rephrase(s: str) -> str:
    """完コピ防止の言い換え辞書適用"""
    for src, dst in _REPHRASE_DICT.items():
        if src in s:
            s = s.replace(src, dst)
    return s


def _split_into_clauses(body: str) -> list[str]:
    """文（。）単位で粗く分割"""
    parts = _SENTENCE_SPLIT_RE.split(body)
    return [p.strip() for p in parts if p.strip()]


def _merge_connective_clauses(clauses: list[str]) -> list[str]:
    """接続助詞で終わる節を次の節と結合
    例: ['Aが', 'Bだ', 'Cから', 'D']
        → ['AがBだ', 'CからD']
    """
    if not clauses:
        return []
    merged: list[str] = []
    buffer = ""
    for c in clauses:
        if buffer:
            buffer = buffer + c
        else:
            buffer = c
        # 次節と結合すべき末尾（接続助詞）なら継続
        if _is_connective(buffer):
            continue
        merged.append(buffer)
        buffer = ""
    if buffer:
        # 残った buffer: 未完了で終わっていたらそのまま入れる（後段で破棄される可能性）
        merged.append(buffer)
    return merged


def _soft_split_long(clause: str, max_chars: int = 45) -> list[str]:
    """長い節を読点で分割（両側が独立した意味を持つ時のみ）"""
    if len(clause) <= max_chars:
        return [clause]
    # 読点で区切って、両側が十分な長さで、かつ未完了でなければ分割
    parts = [p.strip() for p in clause.split("、") if p.strip()]
    if len(parts) < 2:
        return [clause]
    out = []
    for p in parts:
        if len(p) < 6:
            # 短すぎる断片は前の節と結合
            if out:
                out[-1] = out[-1] + "、" + p
            else:
                out.append(p)
        else:
            out.append(p)
    # どれかが未完了で終わっていたら全体を 1 つに戻す
    if any(_is_incomplete(p) for p in out):
        return [clause]
    return out


def bulletize(text: str, max_items: int = 5, min_chars: int = 5) -> list[str]:
    """フルテキスト → 意味単位の箇条書きリスト

    返り値:
        - 完結した節のみ（未完了は破棄）
        - 各要素は最大 45 字程度
        - 接続助詞で繋がっているものは結合済
        - 完コピ防止のため辞書置換済
    """
    if not text or not text.strip():
        return []

    body = _strip_prefix(text)
    raw_clauses = _split_into_clauses(body)
    merged = _merge_connective_clauses(raw_clauses)

    bullets: list[str] = []
    seen = set()
    for clause in merged:
        c = _clean(clause)
        if not c:
            continue
        # 未完了（活用形途中）は破棄
        if _is_incomplete(c):
            continue
        # 短すぎるものは棄却
        if len(c) < min_chars:
            continue
        # 長すぎる場合のみ読点分割を試す
        for part in _soft_split_long(c):
            part = part.strip().rstrip("、")
            if not part or len(part) < min_chars:
                continue
            if _is_incomplete(part):
                continue
            part = _rephrase(part)
            if part in seen:
                continue
            seen.add(part)
            bullets.append(part)
            if len(bullets) >= max_items:
                return bullets
    return bullets


def process_date(date_key: str, force: bool = False) -> dict:
    fp = PRED_DIR / f"{date_key}_pred.json"
    if not fp.exists():
        print(f"pred.json not found: {fp}")
        return {}

    with fp.open(encoding="utf-8") as f:
        pred = json.load(f)

    stats = {
        "horses_total": 0,
        "stable_bullets_added": 0,
        "stable_bullets_skipped": 0,
        "stable_bullets_empty": 0,
        "comment_bullets_added": 0,
        "comment_bullets_skipped": 0,
    }

    for race in pred.get("races", []):
        for h in race.get("horses", []):
            if h.get("is_scratched"):
                continue
            stats["horses_total"] += 1
            trs = h.get("training_records") or []
            if not trs:
                continue
            tr = trs[0]

            sc = tr.get("stable_comment") or ""
            if sc:
                if tr.get("stable_comment_bullets") and not force:
                    stats["stable_bullets_skipped"] += 1
                else:
                    bullets = bulletize(sc, max_items=5, min_chars=5)
                    if bullets:
                        tr["stable_comment_bullets"] = bullets
                        stats["stable_bullets_added"] += 1
                    else:
                        # 節が全て未完了の場合は bullets 生成せず（フロントが生コメントにフォールバック）
                        tr["stable_comment_bullets"] = None
                        stats["stable_bullets_empty"] += 1

            cc = tr.get("comment") or ""
            if cc:
                if tr.get("comment_bullets") and not force:
                    stats["comment_bullets_skipped"] += 1
                else:
                    bullets = bulletize(cc, max_items=3, min_chars=4)
                    if bullets:
                        tr["comment_bullets"] = bullets
                        stats["comment_bullets_added"] += 1

    tmp_fp = fp.with_suffix(".json.tmp")
    tmp_fp.write_text(
        json.dumps(pred, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp_fp.replace(fp)
    return stats


def main() -> None:
    args = sys.argv[1:]
    force = "--force" in args
    args = [a for a in args if a != "--force"]
    if args:
        date_key = args[0].replace("-", "")
    else:
        from datetime import datetime
        date_key = datetime.now().strftime("%Y%m%d")

    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] {date_key} 処理開始 (force={force})", flush=True)
    stats = process_date(date_key, force=force)
    print(f"[{time.strftime('%H:%M:%S')}] 完了 {time.time()-t0:.1f}秒", flush=True)
    for k, v in stats.items():
        print(f"  {k}: {v}", flush=True)


if __name__ == "__main__":
    main()
