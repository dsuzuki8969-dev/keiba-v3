#!/usr/bin/env python3
"""
apply_keiba_terminology_dict.py — Qwen 誤訳補正スクリプト
==========================================================
frontend/src/lib/paraphrase.ts の PARAPHRASE_MAP エントリに対して
data/masters/keiba_terminology_dict.json の辞書エントリで補正を適用する。

使用方法:
    python scripts/apply_keiba_terminology_dict.py --dry-run   # 変更プレビュー
    python scripts/apply_keiba_terminology_dict.py --apply     # 本実行

注意:
    - バックアップ取得失敗時は本実行禁止
    - 辞書の wrong/right は単純な文字列完全一致 (.replace) で適用
    - フォールバック禁止: 辞書未収載の誤訳は触らない
"""

import io
import json
import re
import shutil
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Windows コンソール出力を UTF-8 に強制
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# プロジェクトルートからの相対パス
PROJECT_ROOT = Path(__file__).parent.parent
PARAPHRASE_TS = PROJECT_ROOT / "frontend" / "src" / "lib" / "paraphrase.ts"
DICT_JSON = PROJECT_ROOT / "data" / "masters" / "keiba_terminology_dict.json"
LOG_DIR = PROJECT_ROOT / "log"


def load_dictionary() -> list[dict]:
    """辞書 JSON を読み込む。"""
    if not DICT_JSON.exists():
        print(f"[ERROR] 辞書ファイルが見つかりません: {DICT_JSON}", file=sys.stderr)
        sys.exit(1)

    with open(DICT_JSON, encoding="utf-8") as f:
        data = json.load(f)

    entries = data.get("entries", [])
    print(f"[INFO] 辞書読み込み完了: {len(entries)} エントリ (ver={data.get('version', '?')})")
    return entries


def extract_map_values(content: str) -> list[tuple[int, str, str]]:
    """
    paraphrase.ts の MAP から全エントリを抽出する。

    Returns:
        list of (line_number, key, value) tuples (1-indexed)
    """
    results = []
    lines = content.splitlines()
    # TypeScript の MAP エントリパターン: "key": "value",
    # 複数行対応は不要（各エントリは 1 行）
    pattern = re.compile(r'^\s+"([^"]+)":\s+"([^"]*)"')

    for i, line in enumerate(lines, start=1):
        m = pattern.match(line)
        if m:
            key = m.group(1)
            value = m.group(2)
            results.append((i, key, value))

    return results


def apply_entries(entries: list[tuple[int, str, str]], dict_entries: list[dict]) \
        -> tuple[str, list[dict], int]:
    """
    辞書エントリを MAP value に適用する。

    Returns:
        (modified_content, changes, total_replaced)
    """
    # wrong → right の辞書を構築
    wrong_to_right: dict[str, str] = {}
    for e in dict_entries:
        wrong = e.get("wrong", "").strip()
        right = e.get("right", "").strip()
        if wrong and right:
            wrong_to_right[wrong] = right

    content = PARAPHRASE_TS.read_text(encoding="utf-8")
    changes = []
    total_replaced = 0

    # 行ごとに処理して MAP value のみを置換
    lines = content.splitlines(keepends=True)
    pattern = re.compile(r'^(\s+"[^"]+": )"([^"]*)"(,?\s*)$')

    for i, line in enumerate(lines):
        m = pattern.match(line)
        if m:
            prefix = m.group(1)
            value = m.group(2)
            suffix = m.group(3)

            new_value = value
            applied: list[str] = []
            for wrong, right in wrong_to_right.items():
                if wrong in new_value:
                    new_value = new_value.replace(wrong, right)
                    applied.append(f"{wrong!r} → {right!r}")

            if applied:
                # キーを抽出
                key_m = re.match(r'\s+"([^"]+)":', line)
                key = key_m.group(1) if key_m else "(不明)"
                changes.append({
                    "line": i + 1,
                    "key": key,
                    "old_value": value,
                    "new_value": new_value,
                    "replacements": applied,
                })
                lines[i] = f'{prefix}"{new_value}"{suffix}\n'
                total_replaced += len(applied)

    modified_content = "".join(lines)
    return modified_content, changes, total_replaced


def print_changes(changes: list[dict], total_replaced: int) -> None:
    """変更内容を表示する。"""
    if not changes:
        print("[INFO] 補正対象エントリなし。辞書の誤訳パターンは MAP に存在しませんでした。")
        return

    print(f"\n[結果] 修正対象エントリ数: {len(changes)} / 置換回数合計: {total_replaced}")
    print("-" * 70)
    for c in changes:
        print(f"  行 {c['line']:4d} | KEY: {c['key'][:40]}")
        print(f"          OLD: {c['old_value']}")
        print(f"          NEW: {c['new_value']}")
        for r in c["replacements"]:
            print(f"          補正: {r}")
        print()


def backup_file() -> Path:
    """paraphrase.ts のバックアップを取得する。失敗時は sys.exit。"""
    today = datetime.now().strftime("%Y%m%d_%H%M%S")
    bak_path = PARAPHRASE_TS.with_name(f"paraphrase.ts.bak_t029_{today}")
    try:
        shutil.copy2(PARAPHRASE_TS, bak_path)
        print(f"[INFO] バックアップ取得: {bak_path.name}")
        return bak_path
    except Exception as e:
        print(f"[ERROR] バックアップ取得失敗: {e}", file=sys.stderr)
        print("[ERROR] 安全のため本実行を中止します。", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Qwen 誤訳補正スクリプト")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dry-run", action="store_true", help="変更プレビュー（ファイル変更なし）")
    group.add_argument("--apply", action="store_true", help="本実行（ファイル変更あり）")
    args = parser.parse_args()

    # ファイル存在チェック
    if not PARAPHRASE_TS.exists():
        print(f"[ERROR] paraphrase.ts が見つかりません: {PARAPHRASE_TS}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] 対象ファイル: {PARAPHRASE_TS.relative_to(PROJECT_ROOT)}")
    print(f"[INFO] 辞書ファイル: {DICT_JSON.relative_to(PROJECT_ROOT)}")
    print(f"[INFO] モード: {'dry-run (変更なし)' if args.dry_run else '本実行 (ファイル更新)'}")
    print()

    # 辞書読み込み
    dict_entries = load_dictionary()

    # MAP エントリ数確認
    content = PARAPHRASE_TS.read_text(encoding="utf-8")
    map_entries = extract_map_values(content)
    print(f"[INFO] MAP エントリ数: {len(map_entries)} 件")
    print()

    # 変更適用（dry-run でも計算は実行する）
    modified_content, changes, total_replaced = apply_entries(map_entries, dict_entries)

    # 結果表示
    print_changes(changes, total_replaced)

    if args.dry_run:
        print("[DRY-RUN] ファイルへの書き込みはスキップしました。")
        print(f"[DRY-RUN] --apply を付けて実行すると {len(changes)} エントリが修正されます。")
        # ログ保存
        LOG_DIR.mkdir(exist_ok=True)
        log_path = LOG_DIR / "t029_dryrun.log"
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"=== T-029 dry-run 結果 ({datetime.now().isoformat()}) ===\n")
            f.write(f"辞書エントリ数: {len(dict_entries)}\n")
            f.write(f"MAP エントリ数: {len(map_entries)}\n")
            f.write(f"修正対象エントリ数: {len(changes)}\n")
            f.write(f"置換回数合計: {total_replaced}\n\n")
            for c in changes:
                f.write(f"行 {c['line']:4d} KEY: {c['key']}\n")
                f.write(f"  OLD: {c['old_value']}\n")
                f.write(f"  NEW: {c['new_value']}\n")
                for r in c["replacements"]:
                    f.write(f"  補正: {r}\n")
                f.write("\n")
        print(f"[INFO] ログ保存: {log_path}")
        return

    # 本実行: バックアップ → 書き込み
    if not changes:
        print("[INFO] 修正対象なし。ファイルへの書き込みをスキップします。")
        return

    bak_path = backup_file()

    # .tmp ファイルに書き出してから確認 → リネーム
    tmp_path = PARAPHRASE_TS.with_suffix(".ts.tmp")
    try:
        tmp_path.write_text(modified_content, encoding="utf-8")
        print(f"[INFO] 一時ファイル書き出し完了: {tmp_path.name}")

        # 一時ファイルを本体に上書き
        shutil.move(str(tmp_path), str(PARAPHRASE_TS))
        print(f"[INFO] paraphrase.ts 更新完了")
        print(f"[INFO] バックアップ: {bak_path.name}")
        print(f"[INFO] 修正件数: {len(changes)} エントリ / 置換回数: {total_replaced}")

    except Exception as e:
        print(f"[ERROR] ファイル書き込み失敗: {e}", file=sys.stderr)
        # バックアップからリストア
        try:
            shutil.copy2(bak_path, PARAPHRASE_TS)
            print(f"[INFO] バックアップからリストアしました: {bak_path.name}")
        except Exception as restore_e:
            print(f"[CRITICAL] リストア失敗: {restore_e}", file=sys.stderr)
        if tmp_path.exists():
            tmp_path.unlink()
        sys.exit(1)

    # ログ保存
    LOG_DIR.mkdir(exist_ok=True)
    log_path = LOG_DIR / "t029_apply.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"=== T-029 apply 結果 ({datetime.now().isoformat()}) ===\n")
        f.write(f"辞書エントリ数: {len(dict_entries)}\n")
        f.write(f"MAP エントリ数: {len(map_entries)}\n")
        f.write(f"修正件数: {len(changes)} エントリ / 置換回数: {total_replaced}\n")
        f.write(f"バックアップ: {bak_path}\n\n")
        for c in changes:
            f.write(f"行 {c['line']:4d} KEY: {c['key']}\n")
            f.write(f"  OLD: {c['old_value']}\n")
            f.write(f"  NEW: {c['new_value']}\n")
            for r in c["replacements"]:
                f.write(f"  補正: {r}\n")
            f.write("\n")
    print(f"[INFO] ログ保存: {log_path}")


if __name__ == "__main__":
    main()
