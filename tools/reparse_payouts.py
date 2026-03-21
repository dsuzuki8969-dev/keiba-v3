"""results.jsonの払戻データをキャッシュHTMLから再パースして修正するスクリプト。

_parse_payouts() の修正後に既存データを更新するために使用。
"""
import json
import os
import sys
import time
import glob
import lz4.frame
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.results_tracker import _parse_payouts

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "results")
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")


def _load_cached_html(race_id: str):
    """キャッシュHTMLを読み込んでBeautifulSoupを返す"""
    vc = race_id[4:6]
    jra_codes = {"01","02","03","04","05","06","07","08","09","10"}
    if vc in jra_codes:
        fname = f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"
    else:
        fname = f"nar.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"

    fpath = os.path.join(CACHE_DIR, fname)
    if not os.path.exists(fpath):
        # NARの場合、JRA URLでキャッシュされている可能性
        if vc not in jra_codes:
            fname2 = f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"
            fpath2 = os.path.join(CACHE_DIR, fname2)
            if os.path.exists(fpath2):
                fpath = fpath2
            else:
                return None
        else:
            return None

    try:
        with open(fpath, "rb") as f:
            html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
        return BeautifulSoup(html, "html.parser")
    except Exception:
        return None


def reparse_one_file(fpath: str) -> tuple:
    """1つのresults.jsonファイルを再パースする。
    Returns: (filename, modified_count, total_count)
    """
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    modified = 0
    total = 0
    for race_id, rdata in data.items():
        if not isinstance(rdata, dict):
            continue
        total += 1

        # キャッシュHTMLから再パース
        soup = _load_cached_html(race_id)
        if not soup:
            continue

        new_payouts = _parse_payouts(soup)
        if not new_payouts:
            continue

        old_payouts = rdata.get("payouts", {})
        # 変更があった場合のみ更新
        if json.dumps(old_payouts, sort_keys=True) != json.dumps(new_payouts, sort_keys=True):
            rdata["payouts"] = new_payouts
            modified += 1

    if modified > 0:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return (os.path.basename(fpath), modified, total)


def main():
    files = sorted(glob.glob(os.path.join(RESULTS_DIR, "*_results.json")))
    print(f"results.jsonファイル数: {len(files)}")

    start = time.time()
    total_modified = 0
    total_files_modified = 0

    for i, fpath in enumerate(files):
        fname, modified, total = reparse_one_file(fpath)
        if modified > 0:
            total_files_modified += 1
            total_modified += modified

        if (i + 1) % 50 == 0 or i == len(files) - 1:
            elapsed = time.time() - start
            print(f"  ({i+1}/{len(files)}) 修正ファイル: {total_files_modified}, 修正レース: {total_modified}  経過: {elapsed:.0f}s")

    elapsed = time.time() - start
    print(f"\n完了: {total_files_modified}/{len(files)}ファイル修正, {total_modified}レース修正  経過: {elapsed:.0f}s")


if __name__ == "__main__":
    main()
