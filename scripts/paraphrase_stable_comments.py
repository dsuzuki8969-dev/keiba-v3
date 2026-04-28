"""
厩舎コメント（stable_comment）を LLM で箇条書きパラフレーズする。

目的:
  競馬ブック掲載の厩舎コメント（調教師談話）を完コピ表示するのは
  著作権上問題があるため、Claude Code CLI 経由で LLM に 1〜5 点の
  箇条書きへ要約・書き換えさせて pred.json へ注入する。

呼び出し方式:
  `claude -p "..." --output-format json` を subprocess で叩く。
  MAX プランの quota を使うので追加 API 課金なし。

処理:
  1. data/predictions/YYYYMMDD_pred.json を読み込み
  2. 各馬の training_records[0].stable_comment を抽出
  3. 本文（"○馬名【調教師】"以降）をハッシュ化
  4. キャッシュ（data/cache/stable_comment_paraphrase.json）にヒット
     すればそれを利用、無ければ claude -p を呼び出して生成
  5. 最終マージ時、filelock 下で pred.json を再読込し、body ハッシュで
     突合して training_records[0].stable_comment_bullets へ注入
     （dashboard の odds-scheduler との書き込み衝突を防止する）
  6. pred.json を原子書き込み

使い方:
  python scripts/paraphrase_stable_comments.py                    # 今日
  python scripts/paraphrase_stable_comments.py 2026-04-19         # 特定日付
  python scripts/paraphrase_stable_comments.py --force            # キャッシュ無視
  python scripts/paraphrase_stable_comments.py --dry-run          # 書き込みなし
  python scripts/paraphrase_stable_comments.py --limit 5          # 先頭5件のみ
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# UTF-8 出力
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = ROOT / "data" / "predictions"
CACHE_DIR = ROOT / "data" / "cache"
CACHE_FILE = CACHE_DIR / "stable_comment_paraphrase.json"

# src.utils.atomic_json を import 可能にする（プロジェクトルートを sys.path へ）
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.utils.atomic_json import atomic_read_modify_write_json  # noqa: E402

# claude CLI 呼び出し設定
# Windows で Python subprocess は .cmd ラッパーを解決しないため絶対パス指定
def _resolve_claude_cmd() -> str:
    """claude CLI の実体パスを返す。Windows と Unix 両対応。"""
    import os
    import shutil

    # 環境変数で override 可
    env = os.environ.get("CLAUDE_CLI_PATH")
    if env and Path(env).exists():
        return env

    # PATH 解決（.cmd / .exe 含む）
    for name in ("claude.cmd", "claude.exe", "claude"):
        p = shutil.which(name)
        if p:
            return p

    # フォールバック: npm グローバルインストールの既定パス
    candidates = [
        Path.home() / "AppData/Roaming/npm/claude.cmd",
        Path.home() / "AppData/Roaming/npm/claude",
        Path("/usr/local/bin/claude"),
        Path("/opt/homebrew/bin/claude"),
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return "claude"  # 最終フォールバック（PATH 依存）


CLAUDE_CMD = _resolve_claude_cmd()
CLAUDE_TIMEOUT = 90  # 1件あたりの最大秒数（Sonnet 4.6 で 10s 前後 + 余裕）

# レート制限耐性パラメータ
CLI_BASE_SLEEP = 1.5           # 各呼び出し後の基本 sleep 秒
CLI_RETRY_DELAYS = (3, 10, 30) # 失敗時のリトライ sleep（3回まで）
CONSECUTIVE_FAIL_LIMIT = 5     # この件数の連続失敗で長時間休憩
LONG_REST_SECONDS = 180        # 連続失敗時の休憩秒数（3分）
PROGRESS_ECHO_INTERVAL = 10    # 何件ごとに改行付きで進捗ログを吐くか

# システムプロンプト v3.0（v6.0.1）
# 重要: Claude CLI は project 直下の CLAUDE.md を自動読込むため
# --system-prompt だけでは上書きされる。対策として user prompt 内で指示を明示。
# 例示は few-shot（マスター 4 例）を含む。
SYSTEM_PROMPT = (
    "あなたは競馬情報の編集者。"
    "入力された厩舎コメントを短く圧縮し、JSON のみで返せ。"
)

# user prompt: シンプルに徹する
# マスター指示 2026-04-23: CLAUDE.md の「玄人クロード対話キャラ」を強制リセットするため
#   冒頭に【役割強制リセット】+ minimum instruction + body のみ
#   few-shot は長すぎて対話応答を誘発するので削除（system-prompt 側で軽くスタイル指示）
USER_PROMPT_TEMPLATE = """厩舎コメント原文:
{body}

上記コメントを 1〜4 個の短いbulletsに圧縮し、JSON のみで返してください。

形式: {{"bullets":["...", "..."]}}
各bullet 15〜30字目安。以下の文体ルールを必ず守ること:
- 常体（だ・である調）で書く。敬体（です・ます）禁止
- 終助詞「ね」「よ」禁止
- 体言止めまたは断定形（〜だ/〜である）で終える
- 丁寧語削除（「〜てください」「〜しております」等は圧縮）
- 未完了形(〜て/〜ので/〜から)で文を終えない
- 原文にない情報は足さない。解説や前置きは不要。"""

# JSON Schema（Claude CLI の structured output は top-level object 必須）
OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "bullets": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "maxItems": 5,
        }
    },
    "required": ["bullets"],
}


def progress(i: int, total: int, t0: float, prefix: str = "") -> None:
    """プログレスバー表示。"""
    if total == 0:
        return
    pct = 100.0 * i / total
    dt = time.time() - t0
    eta = dt * (total - i) / max(i, 1) if i > 0 else 0
    bar_len = 30
    filled = int(bar_len * i / total)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(
        f"\r{prefix}[{bar}] {pct:5.1f}% ({i:,}/{total:,}) "
        f"経過{dt:6.1f}s 残{eta:6.1f}s",
        end="",
        flush=True,
    )


def extract_body(raw: str) -> str:
    """
    stable_comment 原文 "○馬名【調教師】本文" から本文のみ取り出す。
    """
    if not raw:
        return ""
    # 先頭の "○...】" を除去
    m = re.match(r"^[◎○▲△★☆×◯]?[^【]{0,30}【[^】]{1,20}】", raw)
    if m:
        return raw[m.end():].strip()
    # 稀に別形式
    if "――" in raw:
        return raw.split("――", 1)[1].strip()
    return raw.strip()


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def load_cache() -> dict[str, list[str]]:
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_cache(cache: dict[str, list[str]]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    tmp.replace(CACHE_FILE)


def _sanitize_bullets(arr: list) -> list[str]:
    """
    サロゲート文字や空要素を除去し、有効な str のみを返す。
    """
    clean: list[str] = []
    for s in arr:
        if not isinstance(s, str):
            continue
        # surrogate を含む文字列は UTF-8 エンコード不可なので破棄
        try:
            s.encode("utf-8")
        except UnicodeEncodeError:
            continue
        s = s.strip()
        if s:
            clean.append(s)
    return clean[:5]


def _call_claude_once(body: str) -> list[str] | None:
    """
    claude -p 1回呼び出し。失敗（ネットワーク・レート制限・空結果）は None。
    """
    user_prompt = USER_PROMPT_TEMPLATE.format(body=body)
    schema_json = json.dumps(OUTPUT_SCHEMA, ensure_ascii=False)
    # マスター指示 2026-04-23 v6.0.1:
    # - prompt を -p 引数でなく stdin 経由で渡す（Windows argv 長制限・改行切断回避）
    # - cwd を一時ディレクトリに（CLAUDE.md auto-discovery 回避）
    cmd = [
        CLAUDE_CMD,
        "-p",  # prompt 引数省略 → stdin から読む
        "--system-prompt", SYSTEM_PROMPT,
        "--json-schema", schema_json,
        "--output-format", "json",
        "--tools", "",
        "--no-session-persistence",
    ]
    try:
        import tempfile
        _neutral_cwd = tempfile.gettempdir()
        proc = subprocess.run(
            cmd,
            input=user_prompt,   # stdin で prompt 渡す
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=CLAUDE_TIMEOUT,
            cwd=_neutral_cwd,
        )
    except subprocess.TimeoutExpired:
        print(f"\n  ⚠ CLI タイムアウト（{CLAUDE_TIMEOUT}s）: {body[:40]}", file=sys.stderr)
        return None
    except FileNotFoundError:
        print(f"\n  ⚠ claude コマンドが見つからない。PATH を確認してください。", file=sys.stderr)
        return None

    # claude CLI は is_error=true でも returncode=1 を返す場合があるため、
    # returncode が 0 でなくとも stdout に JSON が入っている可能性を優先して見る
    try:
        outer = json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        print(
            f"\n  ⚠ CLI 出力が JSON でない code={proc.returncode}: "
            f"{e} / stderr={proc.stderr[:150]} / stdout={proc.stdout[:200]}",
            file=sys.stderr,
        )
        return None

    # API エラーは result に 400 メッセージが入り is_error=true
    if outer.get("is_error"):
        print(f"\n  ⚠ API エラー: {(outer.get('result') or '')[:200]}", file=sys.stderr)
        return None

    # --json-schema 使用時は structured_output フィールドに結果が入る
    structured = outer.get("structured_output")
    if isinstance(structured, dict):
        arr = structured.get("bullets")
        if isinstance(arr, list):
            bullets = _sanitize_bullets(arr)
            if bullets:
                return bullets

    # フォールバック: result テキストから JSON を抽出
    result_text = (outer.get("result") or "").strip()
    if result_text:
        m = re.search(r"\{[\s\S]*?\}", result_text)
        if m:
            try:
                obj = json.loads(m.group(0))
                arr = obj.get("bullets") if isinstance(obj, dict) else None
                if isinstance(arr, list):
                    bullets = _sanitize_bullets(arr)
                    if bullets:
                        return bullets
            except json.JSONDecodeError:
                pass
        m = re.search(r"\[[\s\S]*?\]", result_text)
        if m:
            try:
                arr = json.loads(m.group(0))
                if isinstance(arr, list):
                    bullets = _sanitize_bullets(arr)
                    if bullets:
                        return bullets
            except json.JSONDecodeError:
                pass

    return None


def call_claude_cli(body: str) -> list[str] | None:
    """
    リトライ付き CLI 呼び出し。失敗時は指数バックオフで最大 3 回リトライ。
    全失敗で None を返す。
    """
    bullets = _call_claude_once(body)
    if bullets:
        return bullets
    for delay in CLI_RETRY_DELAYS:
        time.sleep(delay)
        bullets = _call_claude_once(body)
        if bullets:
            return bullets
    return None


def collect_targets(pred: dict) -> list[tuple[dict, str, str]]:
    """
    pred.json から (training_record_dict, horse_name, body) 一覧を返す。
    training_record_dict は直接参照で返す。
    """
    targets: list[tuple[dict, str, str]] = []
    for r in pred.get("races", []):
        for h in r.get("horses", []):
            trs = h.get("training_records") or []
            if not trs:
                continue
            tr = trs[0]
            raw = tr.get("stable_comment") or ""
            body = extract_body(raw)
            if not body:
                continue
            targets.append((tr, h.get("horse_name", ""), body))
    return targets


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", help="YYYY-MM-DD (既定: 今日)")
    parser.add_argument("--force", action="store_true", help="キャッシュ無視で再生成")
    parser.add_argument("--dry-run", action="store_true", help="書き込みなし、CLI 呼び出しも抑止")
    parser.add_argument("--limit", type=int, default=0, help="先頭 N 件のみ処理（動作確認用）")
    args = parser.parse_args()

    if args.date:
        date_str = args.date.replace("-", "")
    else:
        date_str = datetime.now().strftime("%Y%m%d")

    pred_path = PRED_DIR / f"{date_str}_pred.json"
    if not pred_path.exists():
        print(f"pred.json が存在しない: {pred_path}", file=sys.stderr)
        return 1

    print(f"pred: {pred_path}")
    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)

    targets = collect_targets(pred)
    total_found = len(targets)
    if args.limit > 0:
        targets = targets[: args.limit]
    print(f"対象コメント: {len(targets):,} 件 / 全体 {total_found:,} 件")
    if not targets:
        print("対象なし。終了。")
        return 0

    cache = load_cache()
    print(f"キャッシュ: {len(cache):,} 件")

    t0 = time.time()
    cache_hit = 0
    cli_call = 0
    cli_fail = 0
    consecutive_fail = 0  # 連続失敗カウンタ
    # 最終マージ用の「body hash → bullets」マップ
    # （pred.json は最後に filelock 下で再読込→注入するため、
    #   in-memory object 参照ではなく body ハッシュで突合する）
    bullets_by_hash: dict[str, list[str]] = {}
    for i, (tr, name, body) in enumerate(targets, 1):
        key = hash_text(body)
        bullets: list[str] | None = None
        if not args.force and key in cache:
            bullets = cache[key]
            cache_hit += 1
        elif args.dry_run:
            bullets = None
        else:
            bullets = call_claude_cli(body)
            cli_call += 1
            if bullets:
                cache[key] = bullets
                consecutive_fail = 0
                # 5件ごとにキャッシュ保存（途中中断対策）
                if cli_call % 5 == 0:
                    save_cache(cache)
            else:
                cli_fail += 1
                consecutive_fail += 1
                # 連続失敗しきい値で長時間休憩（レート制限回復待ち）
                if consecutive_fail >= CONSECUTIVE_FAIL_LIMIT:
                    print(
                        f"\n  ⚠ 連続失敗 {consecutive_fail} 件 → {LONG_REST_SECONDS}秒休憩（レート制限回復待ち）",
                        flush=True,
                    )
                    save_cache(cache)
                    time.sleep(LONG_REST_SECONDS)
                    consecutive_fail = 0
            # 基本 sleep（連続呼び出し過多によるブロック回避）
            time.sleep(CLI_BASE_SLEEP)

        if bullets:
            bullets_by_hash[key] = bullets
        progress(i, len(targets), t0, "  ")

        # 10件ごとに改行付き進捗ログ（ログファイル tail -f でも見えるように）
        if i % PROGRESS_ECHO_INTERVAL == 0:
            dt = time.time() - t0
            rate = i / dt if dt > 0 else 0
            eta = (len(targets) - i) / rate if rate > 0 else 0
            print(
                f"\n  [{datetime.now().strftime('%H:%M:%S')}] "
                f"{i}/{len(targets)} 件完了  "
                f"キャッシュ命中={cache_hit} CLI成功={cli_call - cli_fail} 失敗={cli_fail} "
                f"残り推定 {eta/60:.1f} 分",
                flush=True,
            )

    print()
    print(f"  キャッシュ命中: {cache_hit:,}")
    print(f"  CLI 呼び出し : {cli_call:,}（失敗 {cli_fail:,}）")
    print(f"  所要時間     : {time.time() - t0:.1f} s")

    save_cache(cache)
    print(f"キャッシュ保存: {CACHE_FILE}")

    if args.dry_run:
        print("dry-run のため pred.json は変更しない")
        return 0

    # pred.json への注入を filelock 下で read-modify-write
    # こうすることで dashboard の odds-scheduler など他プロセスの書き込みと
    # 衝突しなくなる（単純な上書きだと odds-scheduler の古いスナップショットで
    # 箇条書きが消失する事故があった: 2026-04-19 14:07 retry2 クロバー）
    def _inject(pred_dict: dict | None) -> dict:
        if pred_dict is None:
            # ロック取得直後に pred.json が消えていた場合のフォールバック
            raise FileNotFoundError(f"pred.json が消失: {pred_path}")
        injected = 0
        skipped = 0
        for r in pred_dict.get("races", []):
            for h in r.get("horses", []):
                trs = h.get("training_records") or []
                if not trs:
                    continue
                tr = trs[0]
                raw = tr.get("stable_comment") or ""
                body = extract_body(raw)
                if not body:
                    continue
                key = hash_text(body)
                # 今回生成したものを優先、無ければキャッシュからフォールバック
                bullets = bullets_by_hash.get(key) or cache.get(key)
                if bullets:
                    tr["stable_comment_bullets"] = bullets
                    injected += 1
                else:
                    skipped += 1
        print(f"  注入: {injected} 件 / 未注入: {skipped} 件")
        return pred_dict

    atomic_read_modify_write_json(pred_path, _inject, lock_timeout=60.0)
    print(f"pred.json 更新: {pred_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
