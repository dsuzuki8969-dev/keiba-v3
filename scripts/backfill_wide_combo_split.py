#!/usr/bin/env python
"""ワイド combo 連結バグ DB バックフィルスクリプト

旧スクレイパーが複数ワイド組 (例: 3-11, 3-7, 7-11) を
1エントリに文字列連結した combo="3-11-3-7-7-11" を
正しい配列 [{combo:"3-11",...},{combo:"3-7",...},{combo:"7-11",...}] に修復する。

使用方法:
    python scripts/backfill_wide_combo_split.py           # ドライラン (変更なし)
    python scripts/backfill_wide_combo_split.py --execute  # 実際に修正を DB に書き込む
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
# ワイド combo 連結バグ対象キー
WIDE_KEYS = {"ワイド", "wide", "quinella_place"}


def split_wide_combo(combo: str) -> list:
    """連結 combo を分割する。偶数部品を2個ずつペアに分割。
    正常な1組(dash=1)はそのままリスト1要素を返す。
    """
    parts = combo.split("-")
    if len(parts) % 2 == 0 and len(parts) >= 4:
        return [f"{parts[i]}-{parts[i+1]}" for i in range(0, len(parts), 2)]
    return [combo]


def fix_payouts_json(payouts_json_str: str) -> tuple[str | None, int]:
    """payouts_json 文字列のワイド combo 連結バグを修復する。

    Returns:
        (修復後JSON文字列 or None, 修復したバグ件数)
    """
    try:
        data = json.loads(payouts_json_str)
    except Exception:
        return None, 0

    fixed_count = 0
    changed = False

    for key in list(data.keys()):
        if key not in WIDE_KEYS:
            continue
        entries = data.get(key)
        if not isinstance(entries, list):
            continue

        new_entries = []
        for entry in entries:
            if not isinstance(entry, dict):
                new_entries.append(entry)
                continue
            combo = entry.get("combo", "") or ""
            if combo.count("-") >= 3:
                split_combos = split_wide_combo(combo)
                if len(split_combos) > 1:
                    payout_val = entry.get("payout")
                    for sc in split_combos:
                        new_entries.append({"combo": sc, "payout": payout_val})
                    fixed_count += 1
                    changed = True
                    continue
            new_entries.append(entry)

        if changed:
            data[key] = new_entries

    if not changed:
        return None, 0

    return json.dumps(data, ensure_ascii=False), fixed_count


def main():
    parser = argparse.ArgumentParser(description="ワイド combo 連結バグ DB バックフィル")
    parser.add_argument("--execute", action="store_true",
                        help="実際に DB を更新する（省略時はドライラン）")
    parser.add_argument("--limit", type=int, default=0,
                        help="処理件数上限（デバッグ用、0=無制限）")
    args = parser.parse_args()

    dry_run = not args.execute
    mode_label = "ドライラン" if dry_run else "実行モード"
    print(f"=== ワイド combo 連結バグ バックフィル [{mode_label}] ===")
    print(f"DB: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.cursor()

    # 対象レース全件取得
    cur.execute("SELECT race_id, payouts_json FROM race_results WHERE payouts_json IS NOT NULL")
    rows = cur.fetchall()
    print(f"対象レコード数: {len(rows)}件")

    total_fixed_races = 0
    total_fixed_combos = 0
    fix_targets = []  # (race_id, new_payouts_json)

    for race_id, pj in rows:
        if args.limit and len(fix_targets) >= args.limit:
            break
        new_pj, cnt = fix_payouts_json(pj)
        if new_pj is not None:
            fix_targets.append((race_id, new_pj))
            total_fixed_combos += cnt
            total_fixed_races += 1

    print(f"修復対象レース数: {total_fixed_races}件")
    print(f"修復対象 combo 数: {total_fixed_combos}件 (1件あたり複数組に展開)")

    if dry_run:
        print()
        print("=== ドライランのため DB 未更新 ===")
        print("修復サンプル (最大5件):")
        count = 0
        for race_id, pj in rows:
            if count >= 5:
                break
            new_pj, cnt = fix_payouts_json(pj)
            if new_pj is not None:
                orig = json.loads(pj)
                fixed = json.loads(new_pj)
                for key in WIDE_KEYS:
                    orig_w = orig.get(key, [])
                    fixed_w = fixed.get(key, [])
                    if orig_w != fixed_w:
                        print(f"  race={race_id} key={key}")
                        print(f"    修正前: {orig_w}")
                        print(f"    修正後: {fixed_w}")
                count += 1
        print()
        print("実際に修正するには --execute オプションを付けて実行してください。")
        conn.close()
        return

    # 実行モード: DB更新
    print()
    print("DB 更新中...")
    batch_size = 500
    for i in range(0, len(fix_targets), batch_size):
        batch = fix_targets[i:i + batch_size]
        cur.executemany(
            "UPDATE race_results SET payouts_json = ? WHERE race_id = ?",
            [(pj, race_id) for race_id, pj in batch]
        )
        conn.commit()
        print(f"  進捗: {min(i + batch_size, len(fix_targets))}/{len(fix_targets)}件更新済")

    conn.close()
    print()
    print(f"=== 完了 ===")
    print(f"修復したレース: {total_fixed_races}件")
    print(f"修復した combo エントリ: {total_fixed_combos}件 (展開前)")


if __name__ == "__main__":
    main()
