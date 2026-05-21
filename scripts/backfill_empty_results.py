# -*- coding: utf-8 -*-
"""三連複欠損 NAR レースの結果を楽天競馬から取得 + ゴースト重複削除

results.json 内で三連複 payouts が欠損している NAR レースを修正する。
帯広(ばんえい, venue=65)は三連複制度なしのため除外。

2 段階で修正:
  Phase 1: ゴースト重複エントリ削除
    - 0頭 + payouts空 + 同日同会場に別sessionの結果あり → 重複なので削除
  Phase 2: 楽天競馬から結果取得
    - 楽天スケジュールページから venue base を発見
    - race_id を構築して結果取得

使用方法:
  python scripts/backfill_empty_results.py --dry-run    # 確認のみ
  python scripts/backfill_empty_results.py               # 実行
  python scripts/backfill_empty_results.py --phase1-only  # ゴースト削除のみ
"""
import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import RESULTS_DIR as _DEFAULT_RESULTS_DIR

BANEI_VENUE = "65"
# グローバル (main() で上書き可能)
_RESULTS_DIR = _DEFAULT_RESULTS_DIR

# 楽天 venue prefix (race_id の 9-12桁目) → netkeiba venue code
RAKUTEN_PREFIX_TO_NK = {
    "2726": "49",  # 園田
    "1106": "36",  # 水沢
    "3230": "55",  # 佐賀
    "2015": "44",  # 大井
    "3601": "30",  # 門別
    "2433": "47",  # 笠松 (名古屋競馬場)
    "3129": "50",  # 高知
    "0304": "65",  # 帯広ば (skip)
}

# netkeiba venue code → 楽天 venue prefix
NK_TO_RAKUTEN_PREFIX = {v: k for k, v in RAKUTEN_PREFIX_TO_NK.items()}


def find_missing_races():
    """三連複が欠損している非ばんえいレースを抽出"""
    missing = []
    res_dir = Path(_RESULTS_DIR)
    for fp in sorted(res_dir.glob("*_results.json")):
        date_str = fp.stem.split("_")[0]
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        for race_id, race in data.items():
            if race_id[4:6] == BANEI_VENUE:
                continue
            payouts = race.get("payouts", {})
            has_trio = bool(payouts.get("三連複") or payouts.get("sanrenpuku"))
            if not has_trio:
                missing.append({
                    "file": str(fp),
                    "date": date_str,
                    "race_id": race_id,
                    "venue_code": race_id[4:6],
                    "session": race_id[6:10],
                    "race_no": race_id[-2:],
                    "horses": len(race.get("order", [])),
                    "payouts_empty": len(payouts) == 0,
                })
    return missing


def find_ghost_duplicates(missing):
    """ゴースト重複エントリを特定

    条件A: 0頭 + payouts空 + 同日同会場に別sessionの結果あり
    条件B: 0頭 + payouts空 + 同日の姉妹会場に結果あり (盛岡↔水沢)
    """
    SISTER_VENUES = {"35": "36", "36": "35"}

    ghosts = []
    non_ghosts = []

    by_file = defaultdict(list)
    for m in missing:
        by_file[m["file"]].append(m)

    for fpath, entries in by_file.items():
        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)

        for entry in entries:
            if entry["horses"] > 0:
                non_ghosts.append(entry)
                continue

            vc = entry["venue_code"]
            is_ghost = False

            check_codes = {vc}
            if vc in SISTER_VENUES:
                check_codes.add(SISTER_VENUES[vc])

            for rid, race in data.items():
                if rid[4:6] not in check_codes:
                    continue
                if rid == entry["race_id"]:
                    continue
                if len(race.get("order", [])) > 0:
                    is_ghost = True
                    break

            if is_ghost:
                ghosts.append(entry)
            else:
                non_ghosts.append(entry)

    return ghosts, non_ghosts


def phase1_remove_ghosts(ghosts, dry_run=False):
    """Phase 1: ゴースト重複エントリを削除"""
    if not ghosts:
        print("Phase 1: ゴースト重複なし")
        return 0

    print(f"Phase 1: ゴースト重複 {len(ghosts)} 件を削除")

    by_file = defaultdict(list)
    for g in ghosts:
        by_file[g["file"]].append(g["race_id"])

    removed = 0
    for fpath, race_ids in sorted(by_file.items()):
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        before = len(data)
        for rid in race_ids:
            if rid in data:
                del data[rid]
                removed += 1

        after = len(data)
        fname = Path(fpath).name
        print(f"  {fname}: {before} → {after} (-{before - after})")

        if not dry_run:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    return removed


def discover_rakuten_bases(date_str):
    """楽天スケジュールページから日付の venue base マップを取得

    Returns: {netkeiba_venue_code: rakuten_base_16digits}
    """
    import requests
    from bs4 import BeautifulSoup

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
    }

    url = f"https://keiba.rakuten.co.jp/race_card/list/RACEID/{date_str}0000000000"
    time.sleep(2.0)

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print(f"  楽天スケジュール取得失敗 ({date_str}): {e}")
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")

    bases = set()
    for link in soup.select("a[href*='/RACEID/']"):
        m = re.search(r"/RACEID/(\d{18})", link.get("href", ""))
        if m:
            rid = m.group(1)
            if rid.startswith(date_str):
                bases.add(rid[:16])

    result = {}
    for base in bases:
        venue_part = base[8:]  # 8桁の venue+session コード
        prefix = venue_part[:4]
        nk_code = RAKUTEN_PREFIX_TO_NK.get(prefix)
        if nk_code and nk_code != BANEI_VENUE:
            result[nk_code] = base

    return result


def phase2_fetch_from_rakuten(non_ghosts, dry_run=False):
    """Phase 2: 楽天競馬から結果を取得"""
    if not non_ghosts:
        print("Phase 2: 取得対象なし")
        return 0, 0

    from src.scraper.rakuten_keiba import RakutenKeibaScraper

    scraper = RakutenKeibaScraper()

    # 日付ごとにグループ化
    by_date = defaultdict(list)
    for entry in non_ghosts:
        by_date[entry["date"]].append(entry)

    total = len(non_ghosts)
    print(f"Phase 2: 楽天競馬から {total} 件取得 ({len(by_date)} 日分)")

    done = 0
    fixed = 0
    failed = 0
    skipped = 0

    # 楽天 base のキャッシュ
    base_cache = {}

    for date_str in sorted(by_date.keys()):
        entries = by_date[date_str]
        needed_venues = set(e["venue_code"] for e in entries)

        # 楽天 base を発見
        if date_str not in base_cache:
            print(f"  {date_str}: 楽天スケジュール取得中...")
            base_cache[date_str] = discover_rakuten_bases(date_str)

        bases = base_cache[date_str]
        found_venues = set(bases.keys())
        missing_venues = needed_venues - found_venues

        if missing_venues:
            print(f"  {date_str}: 楽天に未発見の venue: {missing_venues}")

        # ファイルごとに処理
        by_file = defaultdict(list)
        for entry in entries:
            by_file[entry["file"]].append(entry)

        for fpath, file_entries in sorted(by_file.items()):
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)

            changed = False

            for entry in file_entries:
                vc = entry["venue_code"]
                race_no = int(entry["race_no"])

                if vc not in bases:
                    skipped += 1
                    done += 1
                    continue

                base = bases[vc]
                rakuten_rid = f"{base}{race_no:02d}"

                try:
                    result = scraper.get_result(rakuten_rid, date=date_str)
                    if result and result.get("payouts"):
                        data[entry["race_id"]] = {
                            "order": result.get("order", []),
                            "payouts": result["payouts"],
                            "source": "rakuten",
                        }
                        changed = True
                        fixed += 1
                    elif result and result.get("order"):
                        data[entry["race_id"]] = {
                            "order": result["order"],
                            "payouts": result.get("payouts", {}),
                            "source": "rakuten",
                        }
                        changed = True
                        fixed += 1
                        print(f"    {entry['race_id']}: order あり payouts なし")
                    else:
                        failed += 1
                        print(f"    {entry['race_id']}: 結果取得失敗")
                except Exception as e:
                    failed += 1
                    print(f"    {entry['race_id']}: {e}")

                done += 1
                if done % 10 == 0 or done == total:
                    pct = done / total * 100
                    filled = int(30 * done / total)
                    bar = "█" * filled + "░" * (30 - filled)
                    print(
                        f"  [{bar}] {pct:5.1f}% "
                        f"({done}/{total}) fixed={fixed} failed={failed} skip={skipped}"
                    )

            if changed and not dry_run:
                with open(fpath, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

    return fixed, failed


def verify_coverage(year="2026"):
    """修正後の三連複カバー率を検証"""
    res_dir = Path(_RESULTS_DIR)
    total = 0
    missing = 0
    for fp in sorted(res_dir.glob(f"{year}*_results.json")):
        with open(fp, encoding="utf-8") as f:
            data = json.load(f)
        for race_id, race in data.items():
            if race_id[4:6] == BANEI_VENUE:
                continue
            total += 1
            payouts = race.get("payouts", {})
            has_trio = bool(payouts.get("三連複") or payouts.get("sanrenpuku"))
            if not has_trio:
                missing += 1

    pct = (total - missing) / total * 100 if total > 0 else 0
    print(f"\n=== {year}年 三連複カバー率 ===")
    print(f"  対象: {total} レース")
    print(f"  欠損: {missing} レース")
    print(f"  カバー率: {pct:.1f}%")
    return missing


def main():
    parser = argparse.ArgumentParser(description="三連複欠損 backfill (楽天競馬)")
    parser.add_argument("--dry-run", action="store_true", help="変更を保存しない")
    parser.add_argument("--phase1-only", action="store_true", help="ゴースト削除のみ")
    parser.add_argument("--year", default="2026", help="対象年 (default: 2026)")
    parser.add_argument("--data-dir", type=str, help="data/results ディレクトリ (worktree 用)")
    args = parser.parse_args()

    global _RESULTS_DIR
    if args.data_dir:
        _RESULTS_DIR = args.data_dir

    print("=== 三連複欠損 backfill ===")
    if args.dry_run:
        print("[DRY-RUN モード]")
    print()

    # 欠損レース収集
    all_missing = find_missing_races()
    # 年フィルタ
    all_missing = [m for m in all_missing if m["date"].startswith(args.year)]
    print(f"三連複欠損 ({args.year}年): {len(all_missing)} レース")

    if not all_missing:
        print("欠損なし!")
        return

    # Phase 1: ゴースト重複の特定と削除
    ghosts, non_ghosts = find_ghost_duplicates(all_missing)
    print(f"  ゴースト重複: {len(ghosts)} 件")
    print(f"  要取得: {len(non_ghosts)} 件")
    print()

    removed = phase1_remove_ghosts(ghosts, dry_run=args.dry_run)
    if removed:
        print(f"  → {removed} 件削除{'(dry-run)' if args.dry_run else ''}")
    print()

    if args.phase1_only:
        if not args.dry_run:
            verify_coverage(args.year)
        return

    # Phase 2: 楽天から取得
    fixed, failed = phase2_fetch_from_rakuten(non_ghosts, dry_run=args.dry_run)
    print()
    print(f"Phase 2 完了: 取得={fixed}, 失敗={failed}")

    # 検証
    if not args.dry_run:
        verify_coverage(args.year)


if __name__ == "__main__":
    main()
