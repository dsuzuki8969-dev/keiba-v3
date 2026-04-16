"""
data/results/YYYYMMDD_results.json の order を HTMLキャッシュから再パースして修正する。

背景:
  旧 _parse_finish_order() は cells[8:] から最初の数値を odds として拾っており、
  実際は人気順位（cells[9]）を odds に保存していた。
  さらに通過順・着差・走破タイム・後3F・人気が未保存だった。

このスクリプトは:
  1. data/results/*.json を順に読み込み
  2. 各レースについて HTMLキャッシュ (data/cache/*race_result*) を探し
  3. 修正後の _parse_finish_order() で再パース
  4. 既存 payouts は保持（払戻データは正しい）
  5. order だけ上書きして保存

使い方:
  python scripts/backfill_results_full.py            # 全期間
  python scripts/backfill_results_full.py --since 2026-04-01
  python scripts/backfill_results_full.py --dry-run  # 修正せず確認のみ
"""

import argparse
import glob
import json
import os
import sys

# プロジェクトルートをimport pathに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lz4.frame
from bs4 import BeautifulSoup

from src.results_tracker import _parse_finish_order


def _find_cache(race_id: str) -> str | None:
    """JRA / NAR の結果ページキャッシュを探す"""
    candidates = [
        f"data/cache/race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
        f"data/cache/nar.netkeiba.com_race_result.html_race_id={race_id}.html.lz4",
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None


def _load_html_from_cache(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            data = lz4.frame.decompress(f.read())
        return data.decode("utf-8", errors="replace")
    except Exception as e:
        print(f"  ! キャッシュ読込失敗: {path}: {e}")
        return None


def process_file(path: str, dry_run: bool = False) -> dict:
    """1ファイル分の修正処理"""
    stats = {"races": 0, "updated": 0, "no_cache": 0, "errors": 0}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"! ファイル読込失敗: {path}: {e}")
        return stats

    changed = False
    for race_id, race in data.items():
        stats["races"] += 1
        cache_path = _find_cache(race_id)
        if not cache_path:
            stats["no_cache"] += 1
            continue
        html = _load_html_from_cache(cache_path)
        if not html:
            stats["errors"] += 1
            continue
        soup = BeautifulSoup(html, "html.parser")
        new_order = _parse_finish_order(soup)
        if not new_order:
            stats["errors"] += 1
            continue
        # 既存 order と比較
        old_order = race.get("order", [])
        # 既存 order の len を保持しつつ、各 horse_no 単位で更新
        old_by_no = {o.get("horse_no"): o for o in old_order}
        merged = []
        for new in new_order:
            entry = dict(old_by_no.get(new["horse_no"], {}))
            entry.update({k: v for k, v in new.items() if v is not None or k in ("corners", "horse_no", "finish")})
            merged.append(entry)
        race["order"] = merged
        stats["updated"] += 1
        changed = True

    if changed and not dry_run:
        # アトミック書き込み
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="", help="YYYY-MM-DD 以降のみ処理")
    ap.add_argument("--dry-run", action="store_true", help="ファイル書き込みしない")
    args = ap.parse_args()

    since_key = args.since.replace("-", "") if args.since else ""

    files = sorted(glob.glob("data/results/2*_results.json"))
    if since_key:
        files = [f for f in files if os.path.basename(f).split("_")[0] >= since_key]

    print(f"対象ファイル: {len(files)}件")
    if args.dry_run:
        print("[DRY-RUN モード] ファイル書き込みは行いません")
    print()

    total_races = total_updated = total_no_cache = total_errors = 0
    for i, fp in enumerate(files, 1):
        stats = process_file(fp, dry_run=args.dry_run)
        total_races += stats["races"]
        total_updated += stats["updated"]
        total_no_cache += stats["no_cache"]
        total_errors += stats["errors"]
        # プログレス表示（10ファイルごと）
        if i % 10 == 0 or i == len(files):
            pct = i * 100 / len(files)
            print(f"  [{pct:5.1f}%] {i}/{len(files)} files | "
                  f"races={total_races} updated={total_updated} "
                  f"no_cache={total_no_cache} errors={total_errors}")

    print()
    print(f"=== 完了 ===")
    print(f"  処理ファイル: {len(files)}")
    print(f"  処理レース: {total_races}")
    print(f"  更新レース: {total_updated}")
    print(f"  キャッシュなし: {total_no_cache}")
    print(f"  エラー: {total_errors}")


if __name__ == "__main__":
    main()
