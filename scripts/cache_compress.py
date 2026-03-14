"""
キャッシュ一括lz4圧縮スクリプト

.html.lz4 が存在しない .html ファイルを lz4 圧縮し、元の .html を削除する。
低負荷で動作するよう、バッチ処理にスリープを挟む。
"""

import os
import sys
import time

try:
    import lz4.frame as lz4f
except ImportError:
    print("ERROR: python-lz4 is required. Install with: pip install lz4")
    sys.exit(1)

CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "cache",
)

# 負荷制御: N件ごとにスリープ
BATCH_SIZE = 200
SLEEP_SEC = 0.2


def main():
    if not os.path.isdir(CACHE_DIR):
        print(f"Cache dir not found: {CACHE_DIR}")
        return

    print("Scanning cache directory...")
    all_files = os.listdir(CACHE_DIR)
    lz4_bases = set()
    html_only = []

    for f in all_files:
        if f.endswith(".html.lz4"):
            lz4_bases.add(f[:-4])  # "xxx.html"

    for f in all_files:
        if f.endswith(".html") and f not in lz4_bases:
            # 先頭が _ のファイルはテストサンプルの可能性があるのでスキップ
            if f.startswith("_"):
                continue
            html_only.append(f)

    print(f"  Plain HTML without lz4 counterpart: {len(html_only)}")

    if not html_only:
        print("Nothing to compress.")
        return

    compressed = 0
    errors = 0
    freed = 0

    print(f"\nCompressing {len(html_only)} files to lz4...")
    for i, fname in enumerate(html_only):
        src = os.path.join(CACHE_DIR, fname)
        dst = src + ".lz4"
        try:
            with open(src, "r", encoding="utf-8") as f:
                content = f.read()

            # 空ファイルや非HTMLはスキップ
            stripped = content.strip()
            if not stripped or ("<" not in stripped):
                continue

            compressed_data = lz4f.compress(content.encode("utf-8"))
            with open(dst, "wb") as f:
                f.write(compressed_data)

            original_size = os.path.getsize(src)
            os.remove(src)
            freed += original_size - len(compressed_data)
            compressed += 1
        except UnicodeDecodeError:
            # cp932等の可能性、スキップ
            errors += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error: {fname}: {e}")
            # 失敗時はdstが壊れている可能性があるので削除
            if os.path.exists(dst):
                try:
                    os.remove(dst)
                except OSError:
                    pass

        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i + 1}/{len(html_only)} "
                  f"(compressed {compressed}, freed {freed / 1024 / 1024:.0f} MB)")

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(SLEEP_SEC)

    print(f"\nDone.")
    print(f"  Compressed: {compressed} files")
    print(f"  Errors/Skipped: {errors}")
    print(f"  Space freed: {freed / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    main()
