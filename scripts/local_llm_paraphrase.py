#!/usr/bin/env python
"""
ローカル LLM (Qwen2.5-7B via LM Studio) で厩舎コメントを夜間バッチパラフレーズ。

パラフレーズ結果は stable_comment_paraphrase_cache テーブルにキャッシュし、
pred.json の training_records[0].stable_comment_bullets フィールドを更新する。
フロント側は既存の stable_comment_bullets 読取ロジックでそのまま表示可能。

事前条件:
  LM Studio API サーバが http://localhost:1234 で稼働中
  qwen-7b モデル ロード済 (lms load qwen2.5-7b-instruct --gpu max --identifier qwen-7b)

使い方:
  python scripts/local_llm_paraphrase.py             # 当日 pred.json
  python scripts/local_llm_paraphrase.py --recent 7  # 直近 7 日分
  python scripts/local_llm_paraphrase.py --dry-run   # 計算のみ・書込なし
  python scripts/local_llm_paraphrase.py --limit 50  # キャッシュミス先頭 50 件のみ
"""
import argparse
import hashlib
import json
import re
import shutil
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from openai import OpenAI

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB = PROJECT_ROOT / "data" / "keiba.db"
PRED_DIR = PROJECT_ROOT / "data" / "predictions"

LM_STUDIO_BASE = "http://localhost:1234/v1"
MODEL_ID = "qwen-7b"

# LLM システムプロンプト (T-027 強化版 2026-04-28)
# 出力は箇条書きの 1 項目として表示されるため、簡潔な断定形・体言止めを強制する。
# 強化点: 語順「条件→結果」・体言止め優先・短文化・前向き表現・冗長禁止を明示
SYSTEM_PROMPT = (
    "あなたはプロの競馬ライター。厩舎コメントを以下のルールでリライトしてください。\n\n"
    "【絶対ルール】\n"
    "1. 出力は1行のみ、30 字以内目安。説明・前置き・改行・引用符は禁止。\n"
    "2. 必ず「体言止め」または「断定形（だ/である）」で終える。\n"
    "   体言止め例：「○○良好」「○○問題なし」「○○のケア」「○○維持」「○○期待」\n"
    "3. 語順は「条件 → 結果」の順にする。\n"
    "   例: 「自分のリズムで運べれば距離は問題ない」（条件を先に、結果を後に）\n"
    "4. 冗長表現を削除し、簡潔に圧縮する。\n"
    "   削除対象: 「〜してきた」「〜してくる」「〜することができる」「〜ということだ」\n"
    "5. 以下の語尾は **絶対禁止**：「ね」「よ」「です」「ます」「だろう」「でしょう」"
    "「と思う」「かもしれない」「〜だなあ」「らしい」「思われる」「考えられる」。\n"
    "6. 馬の評価が伝わる前向き・簡潔な表現を優先する。\n"
    "7. 出力は日本語のみ。英単語・記号（？！…）の混入は厳禁。"
    "**ハングル・中国語簡体字・その他外国語は絶対禁止**。\n"
    "8. 競馬専門用語（鞍上、追切、調教、馬体、仕上がり、上がり、ゲート、叩き、ハナ、"
    "差し、追込、距離、馬場、稍重、ダート、芝、内枠、外枠 等）は保持。\n"
    "9. 元コメントと同じ意味を保つこと。誇張・縮減・新情報の追加は禁止。\n\n"
    "【良い例 (体言止め・断定形)】\n"
    "入力: ひと開催あけて体をケアした\n"
    "出力: ひと開催あけて馬体のケア\n\n"
    "入力: 距離は問題ないので、自分のリズムで運べれば\n"
    "出力: 自分のリズムで運べれば距離は問題ない\n\n"
    "入力: 休み明けを２連勝した後も好調だ\n"
    "出力: 休み明け連勝後も好調を維持\n\n"
    "入力: 再始動期待\n"
    "出力: 改めてここから期待\n\n"
    "入力: 中間順調。仕上がりは良好\n"
    "出力: 中間順調、仕上がり良好\n\n"
    "入力: 叩いて良化しているよ\n"
    "出力: 叩き良化\n\n"
    "入力: 強敵の競演でもチャンスはある\n"
    "出力: 強敵相手でも勝機あり\n\n"
    "入力: 馬体に張りあり、鞍上強気の発言\n"
    "出力: 馬体張りあり、鞍上強気\n\n"
    "【悪い例 (禁止パターン)】\n"
    "「〜と思う」→ NG / 「〜だなあ」→ NG / 「〜かもしれない」→ NG\n"
    "「〜してきた」→ NG / 「〜してくれれば」→ NG\n"
    "語順NG: 「距離は問題ない、自分のリズムで運べれば」（条件が後ろ）→ NG\n\n"
    "【話者明示の削除 (必須)】\n"
    "「山崎助手――」「平松師――」「大林助手――」「近藤助手――」のような\n"
    "話者の冒頭明示は全て削除して、本文のみを bullets 化する。\n"
    "出力に『○○師』『○○助手』『○○厩務員』『○○マネジャー』を含めない。\n"
    "例: 「山崎助手――気が高ぶらないように調整」→「気が高ぶらないよう調整」\n"
    "例: 「平松師――追い切りの動きは上々」→「追い切りの動き上々」"
)


# 「○馬名【XX師】」「●馬名【XX師】」等の冒頭 prefix を除去するパターン
# PREFIX_RE_HEADER: 印 (任意) + 馬名 + 【...】 or （...） 形式
PREFIX_RE_HEADER = re.compile(
    r"^[○●◯◎▲△★☆×]?[ぁ-ゟ゠-ヿ一-鿿！-￯A-Za-z0-9]+(【[^】]+】|（[^）]+）)"
)
# PREFIX_RE_INLINE: 改行後/冒頭 の「XX師――」「XX助手――」「XX厩務員――」「XXマネジャー――」
# verify_parse_stable_comment.py L55 と同等のロジックを統合
PREFIX_RE_INLINE = re.compile(
    r"(^|\n)[\s　]*[^\s　\n。．]*?(師|厩務員|助手|マネジャー)[—―－\-]+\s*"
)


def strip_prefix(text: str) -> str:
    """raw コメントの話者 prefix を除去する (二重防御)。

    対象パターン:
      1. 「○馬名【XX師】」「○馬名（前で捌ければ）」等の header prefix
      2. 「山崎助手――」「平松師――」等の inline 話者明示 (改行後・冒頭)
    """
    text = PREFIX_RE_HEADER.sub("", text)
    text = PREFIX_RE_INLINE.sub(r"\1", text)
    return text.strip()

# 中国語簡体字: 日本語では使わない特定コードポイント（则/每/应/优/说/过/时/还/对/没/种 等）
# 関数呼び出しごとの辞書作成を避けるためモジュールレベルで定義
_CHINESE_SIMPLIFIED_CHARS: frozenset[int] = frozenset({
    0x5219,  # 则 (zé)
    0x6BCF,  # 每 (měi)
    0x5E94,  # 应 (yīng)
    0x4F18,  # 优 (yōu)
    0x8BF4,  # 说 (shuō)
    0x8FC7,  # 过 (guò)
    0x65F6,  # 时 (shí)
    0x8FD8,  # 还 (hái)
    0x5BF9,  # 对 (duì)
    0x6CA1,  # 没 (méi)
    0x79CD,  # 种 (zhǒng)
    0x53D1,  # 发 (fā) - 發の簡体
    0x957F,  # 长 (cháng) - 長の簡体
    0x8FDB,  # 进 (jìn) - 進の簡体
    0x4E3A,  # 为 (wèi) - 為の簡体
})


def hash_text(s: str) -> str:
    """SHA-256 ハッシュ（キャッシュキー）"""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def split_bullets(text: str) -> list[str]:
    """
    句点で分割して空白除去。
    parseStableComment.ts と同等のロジック（フロント側と一致させる）。
    """
    if not text:
        return []
    parts: list[str] = []
    for p in text.replace("。", "。\n").split("\n"):
        p = p.strip().rstrip("。").strip()
        if p:
            parts.append(p)
    return parts


def get_cached(conn: sqlite3.Connection, h: str) -> str | None:
    """キャッシュテーブルからパラフレーズ済み文を取得"""
    row = conn.execute(
        "SELECT paraphrased FROM stable_comment_paraphrase_cache WHERE input_hash=?",
        (h,),
    ).fetchone()
    return row[0] if row else None


def save_cached(conn: sqlite3.Connection, h: str, original: str, paraphrased: str) -> None:
    """パラフレーズ結果をキャッシュテーブルに保存"""
    conn.execute(
        "INSERT OR REPLACE INTO stable_comment_paraphrase_cache "
        "(input_hash, original, paraphrased) VALUES (?, ?, ?)",
        (h, original, paraphrased),
    )


def paraphrase_one(client: OpenAI, original: str) -> str:
    """
    1 文をパラフレーズ。
    失敗・Latin 文字混入時は原文をそのまま返す（エラー耐性設計）。
    """
    # 修正 (T-044.fix): LLM 入力前に prefix strip (話者明示を事前除去)
    original = strip_prefix(original)
    if not original:
        return ""
    try:
        res = client.chat.completions.create(
            model=MODEL_ID,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": original},
            ],
            temperature=0.3,
            max_tokens=200,
            timeout=60,
        )
        out = res.choices[0].message.content.strip().rstrip("。")
        # Latin アルファベットが 1 文字でも含まれれば原文を返す
        # （競馬専門用語は全て漢字/ひらがな/カタカナで表現できるため）
        latin_chars = [c for c in out if c.isascii() and c.isalpha()]
        if latin_chars:
            try:
                print(f"  [WARN] latin chars detected ({''.join(latin_chars[:5])}) -> use original")
            except Exception:
                pass
            return original
        # ハングル（韓国語）・キリル文字・タイ文字等が 1 文字でも含まれれば原文採用
        # 範囲: ハングル U+AC00-U+D7A3 / U+3131-U+318E / U+1100-U+11FF
        # キリル U+0400-U+04FF / タイ U+0E00-U+0E7F / アラビア U+0600-U+06FF
        # 中国語簡体字: _CHINESE_SIMPLIFIED_CHARS（モジュールレベルで定義）
        for ch in out:
            cp = ord(ch)
            if (0xAC00 <= cp <= 0xD7A3) or (0x3131 <= cp <= 0x318E) or (0x1100 <= cp <= 0x11FF) \
                    or (0x0400 <= cp <= 0x04FF) or (0x0E00 <= cp <= 0x0E7F) \
                    or (0x0600 <= cp <= 0x06FF) \
                    or cp in _CHINESE_SIMPLIFIED_CHARS:
                try:
                    print(f"  [WARN] non-japanese script detected (U+{cp:04X} {ch}) -> use original")
                except Exception:
                    pass
                return original
        return out
    except Exception as e:
        # cp932 console での日本語混入時の UnicodeEncodeError を防ぐため、
        # エラーメッセージは ASCII にサニタイズしてから print する。
        try:
            msg = str(e).encode("ascii", "replace").decode("ascii")[:200]
            print(f"  [WARN] paraphrase failed ({msg}) -> use original")
        except Exception:
            pass
        return original


def process_pred_file(
    pred_path: Path,
    conn: sqlite3.Connection,
    client: OpenAI,
    dry_run: bool,
    limit: int,
) -> dict:
    """
    1 日分の pred.json を処理し、training_records[0].stable_comment_bullets を更新する。

    Returns:
        統計情報 dict (date, status, cache_hit, cache_miss, miss_processed, written)
    """
    date_str = pred_path.stem.replace("_pred", "")
    if not pred_path.exists():
        return {"date": date_str, "status": "missing"}

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    races = pred.get("races", [])
    cache_hit = 0
    cache_miss = 0
    miss_processed = 0
    written = 0  # 更新した training_records 数

    for race in races:
        for horse in race.get("horses", []):
            tr_recs = horse.get("training_records") or []
            if not tr_recs:
                continue
            rec = tr_recs[0]
            stable = rec.get("stable_comment") or ""
            if not stable:
                continue

            bullets_raw = split_bullets(stable)
            if not bullets_raw:
                continue

            paraphrased_bullets: list[str] = []
            for b in bullets_raw:
                # 冒頭 prefix (○馬名【XX師】/（）+ 話者名――) を除去してからキャッシュキー生成・paraphrase
                b_clean = strip_prefix(b)
                if not b_clean:
                    b_clean = b  # prefix 全消滅は使わない
                h = hash_text(b_clean)
                cached = get_cached(conn, h)
                if cached:
                    paraphrased_bullets.append(cached)
                    cache_hit += 1
                else:
                    cache_miss += 1
                    # 上限超過の場合は原文をそのまま使用
                    if limit > 0 and miss_processed >= limit:
                        paraphrased_bullets.append(b_clean)
                        continue
                    out = paraphrase_one(client, b_clean)
                    paraphrased_bullets.append(out)
                    miss_processed += 1
                    if not dry_run:
                        save_cached(conn, h, b_clean, out)
                        conn.commit()

            # フロント側 stable_comment_bullets として上書き保存
            rec["stable_comment_bullets"] = paraphrased_bullets
            written += 1

    if not dry_run and written > 0:
        # バックアップ作成（上書き前の1世代）
        bak = pred_path.with_suffix(f".json.bak_paraphrase_{date.today():%Y%m%d}")
        if not bak.exists():
            shutil.copy(pred_path, bak)
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, separators=(",", ":"))

    return {
        "date": date_str,
        "status": "ok",
        "cache_hit": cache_hit,
        "cache_miss": cache_miss,
        "miss_processed": miss_processed,
        "written": written,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="ローカル LLM で厩舎コメントをパラフレーズして pred.json に反映"
    )
    parser.add_argument(
        "date",
        nargs="?",
        default=date.today().strftime("%Y%m%d"),
        help="処理日付 YYYYMMDD (デフォルト: 当日)",
    )
    parser.add_argument("--dry-run", action="store_true", help="DB・ファイル書込なし")
    parser.add_argument("--recent", type=int, default=0, help="直近 N 日分を処理")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="キャッシュミス処理上限（0=無制限）",
    )
    args = parser.parse_args()

    # LM Studio API 疎通確認
    try:
        client = OpenAI(base_url=LM_STUDIO_BASE, api_key="lm-studio", timeout=10.0)
        models = client.models.list()
        if not any(m.id == MODEL_ID for m in models.data):
            print(
                f"[ERROR] LM Studio で model '{MODEL_ID}' がロードされていません",
                file=sys.stderr,
            )
            print(
                f"  実行: lms load qwen2.5-7b-instruct --gpu max --identifier {MODEL_ID}",
                file=sys.stderr,
            )
            return 1
    except Exception as e:
        print(f"[ERROR] LM Studio API 接続失敗: {e}", file=sys.stderr)
        print("  起動: lms server start", file=sys.stderr)
        return 1

    # DB 接続（テーブル保証）
    conn = sqlite3.connect(str(DB))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stable_comment_paraphrase_cache (
            input_hash TEXT PRIMARY KEY,
            original TEXT NOT NULL,
            paraphrased TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_paraphrase_created "
        "ON stable_comment_paraphrase_cache(created_at)"
    )
    conn.commit()

    # 対象日付リスト
    targets: list[Path] = []
    if args.recent > 0:
        today = date.today()
        for i in range(args.recent):
            d = today - timedelta(days=i)
            targets.append(PRED_DIR / f"{d.strftime('%Y%m%d')}_pred.json")
    else:
        targets.append(PRED_DIR / f"{args.date}_pred.json")

    print(
        f"[INFO] 対象: {len(targets)} ファイル "
        f"({'DRY-RUN' if args.dry_run else '本実行'})"
    )
    print(f"[INFO] LLM: {MODEL_ID} @ {LM_STUDIO_BASE}")
    if args.limit > 0:
        print(f"[INFO] キャッシュミス上限: {args.limit} 件")

    t0 = time.time()
    total_hit = 0
    total_miss = 0
    total_processed = 0
    total_written = 0

    for path in targets:
        result = process_pred_file(path, conn, client, args.dry_run, args.limit)
        if result["status"] == "missing":
            print(f"  [SKIP] {result['date']} (ファイル不在)")
            continue
        print(
            f"  [{result['date']}] "
            f"hit={result['cache_hit']} "
            f"miss={result['cache_miss']} "
            f"processed={result['miss_processed']} "
            f"written={result['written']}"
        )
        total_hit += result["cache_hit"]
        total_miss += result["cache_miss"]
        total_processed += result["miss_processed"]
        total_written += result["written"]

    dur = time.time() - t0
    print(
        f"\n[完了 {dur:.1f}s] "
        f"cache_hit={total_hit} miss={total_miss} "
        f"processed={total_processed} written={total_written}"
    )

    # キャッシュ累積件数
    n = conn.execute(
        "SELECT COUNT(*) FROM stable_comment_paraphrase_cache"
    ).fetchone()[0]
    print(f"[INFO] キャッシュ累積: {n} 件")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
