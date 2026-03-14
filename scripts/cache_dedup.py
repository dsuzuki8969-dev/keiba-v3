"""
キャッシュ重複削除スクリプト

.html と .html.lz4 の両方が存在するファイルについて、
.html を削除して .html.lz4 に一本化する。

低負荷で動作するよう、バッチ処理にスリープを挟む。
"""

import os
import sys
import time

CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "cache",
)

# 負荷制御: N件ごとにスリープ
BATCH_SIZE = 500
SLEEP_SEC = 0.1


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"Cache dir not found: {CACHE_DIR}")
        return

    # .html.lz4 ファイルのセットを構築
    print("Scanning cache directory...")
    all_files = os.listdir(CACHE_DIR)
    lz4_set = set()
    html_files = []

    for f in all_files:
        if f.endswith(".html.lz4"):
            # .html.lz4 -> 対応する .html 名を記録
            base = f[:-4]  # strip ".lz4" -> "xxx.html"
            lz4_set.add(base)
        elif f.endswith(".html"):
            html_files.append(f)

    print(f"  LZ4 files: {len(lz4_set)}")
    print(f"  Plain HTML files: {len(html_files)}")

    # 重複を検出: .html があり、同名 .html.lz4 も存在するもの
    duplicates = [f for f in html_files if f in lz4_set]
    print(f"  Duplicates (html with lz4 counterpart): {len(duplicates)}")

    if not duplicates:
        print("No duplicates found. Nothing to do.")
        return

    # ドライラン表示
    total_freed = 0
    removed = 0
    errors = 0

    print(f"\nRemoving {len(duplicates)} duplicate .html files...")
    for i, fname in enumerate(duplicates):
        fpath = os.path.join(CACHE_DIR, fname)
        try:
            size = os.path.getsize(fpath)
            os.remove(fpath)
            total_freed += size
            removed += 1
        except OSError as e:
            errors += 1
            if errors <= 5:
                print(f"  Error: {fname}: {e}")

        # 進捗表示
        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i + 1}/{len(duplicates)} "
                  f"(freed {total_freed / 1024 / 1024:.0f} MB)")

        # 負荷制御
        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_SEC)

    print(f"\nDone.")
    print(f"  Removed: {removed} files")
    print(f"  Errors: {errors}")
    print(f"  Freed: {total_freed / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
