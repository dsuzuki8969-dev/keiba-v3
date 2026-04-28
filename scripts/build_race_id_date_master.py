#!/usr/bin/env python
"""
HTMLキャッシュから race_id ↔ 正しい日付の信頼マスタを構築する。

data/cache/ 内の race_result HTML（.html / .html.lz4）を走査し、
og:description / og:title / title タグから「YYYY年M月D日」パターンを抽出して
race_id ↔ YYYY-MM-DD のマッピングを生成する。

抽出失敗時はフォールバック禁止（feedback_no_easy_escape）。

Usage:
  python scripts/build_race_id_date_master.py
  python scripts/build_race_id_date_master.py --output data/masters/race_id_date_master.json
  python scripts/build_race_id_date_master.py --verbose
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lz4.frame
from bs4 import BeautifulSoup
from tqdm import tqdm

# ===== 定数 =====
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")
DEFAULT_OUTPUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "masters", "race_id_date_master.json")
TEMP_RID_TO_DATE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "temp_rid_to_date.json")

# 日付パターン（例: 2026年1月3日）
DATE_PATTERN = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def _read_html(fpath: str) -> str:
    """LZ4圧縮またはそのままのHTMLファイルを読む"""
    if fpath.endswith(".lz4"):
        with lz4.frame.open(fpath, "rb") as f:
            return f.read().decode("utf-8", errors="replace")
    with open(fpath, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _extract_race_id(fpath: str) -> str:
    """ファイルパスから race_id を抽出する"""
    m = re.search(r"race_id=(\d+)", fpath)
    return m.group(1) if m else ""


def _extract_date(html: str) -> tuple[str | None, str]:
    """
    HTMLから日付を抽出する。

    Returns:
        (日付文字列 or None, 抽出元の説明)
        フォールバック禁止: 抽出失敗時は None を返す。
    """
    soup = BeautifulSoup(html, "html.parser")

    # 1. og:description → og:title → title の順で試みる
    selectors = [
        ("meta[property='og:description']", "content"),
        ("meta[property='og:title']", "content"),
        ("title", None),
    ]
    for selector, attr in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        text = node.get(attr) if attr else node.get_text()
        m = DATE_PATTERN.search(text or "")
        if m:
            y, mo, d = m.groups()
            date_str = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            return date_str, selector
    return None, "not_found"


def _collect_cache_files() -> list[str]:
    """JRA + NAR の race_result キャッシュファイル一覧を返す"""
    patterns = [
        os.path.join(CACHE_DIR, "race.netkeiba.com_race_result.html_race_id=*.html"),
        os.path.join(CACHE_DIR, "race.netkeiba.com_race_result.html_race_id=*.html.lz4"),
        os.path.join(CACHE_DIR, "nar.netkeiba.com_race_result.html_race_id=*.html"),
        os.path.join(CACHE_DIR, "nar.netkeiba.com_race_result.html_race_id=*.html.lz4"),
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat))
    # .html と .html.lz4 が両方ある場合は .lz4 を優先（重複排除）
    seen_ids: dict[str, str] = {}
    for fpath in sorted(files):
        rid = _extract_race_id(fpath)
        if not rid:
            continue
        if rid not in seen_ids:
            seen_ids[rid] = fpath
        elif fpath.endswith(".lz4"):
            # lz4 優先
            seen_ids[rid] = fpath
    return list(seen_ids.values())


def build_master(verbose: bool = False) -> dict:
    """
    全キャッシュファイルを走査して race_id ↔ 日付マッピングを構築する。

    Returns:
        結果辞書（JSON 出力形式）
    """
    files = _collect_cache_files()
    total_files = len(files)
    print(f"[INFO] キャッシュファイル総数: {total_files} 件")

    mapping: dict[str, str] = {}
    failures: list[dict] = []
    extracted_ok = 0
    extracted_fail = 0

    for fpath in tqdm(files, desc="HTMLキャッシュ走査", unit="ファイル", ncols=80):
        race_id = _extract_race_id(fpath)
        if not race_id:
            failures.append({
                "race_id": "",
                "fpath": os.path.basename(fpath),
                "reason": "race_id抽出失敗",
            })
            extracted_fail += 1
            continue

        try:
            html = _read_html(fpath)
        except Exception as e:
            failures.append({
                "race_id": race_id,
                "fpath": os.path.basename(fpath),
                "reason": f"ファイル読込エラー: {e}",
            })
            extracted_fail += 1
            if verbose:
                print(f"  [FAIL] {race_id}: ファイル読込エラー: {e}")
            continue

        date_str, source = _extract_date(html)

        if date_str is not None:
            mapping[race_id] = date_str
            extracted_ok += 1
        else:
            # フォールバック禁止: スキップ（feedback_no_easy_escape）
            reason = f"og:description / og:title / title に日付パターン未検出"
            failures.append({
                "race_id": race_id,
                "fpath": os.path.basename(fpath),
                "reason": reason,
            })
            extracted_fail += 1
            if verbose:
                print(f"  [FAIL] {race_id}: {reason}")

    return {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_entries": len(mapping),
        "extraction_stats": {
            "total_files": total_files,
            "extracted_ok": extracted_ok,
            "extracted_fail": extracted_fail,
        },
        "mapping": dict(sorted(mapping.items())),
        "extraction_failures": failures,
    }


def _print_validation_report(result: dict) -> None:
    """検証レポートを表示する"""
    mapping = result["mapping"]
    stats = result["extraction_stats"]

    print("\n" + "=" * 60)
    print("■ 抽出統計")
    print(f"  総ファイル数   : {stats['total_files']:,}")
    print(f"  抽出成功       : {stats['extracted_ok']:,}")
    print(f"  抽出失敗       : {stats['extracted_fail']:,}")
    success_rate = stats['extracted_ok'] / stats['total_files'] * 100 if stats['total_files'] > 0 else 0
    print(f"  成功率         : {success_rate:.1f}%")

    # 元旦判定（2026-01-01 が JRA race_id に含まれていないか）
    print("\n■ 元旦（2026-01-01）汚染チェック")
    jan1_ids = {k: v for k, v in mapping.items() if v == "2026-01-01"}
    if not jan1_ids:
        print("  OK: 元旦（2026-01-01）の race_id はゼロ件（汚染なし）")
    else:
        print(f"  WARNING: {len(jan1_ids)} 件の race_id が 2026-01-01 にマッピングされています")
        # 会場コード別内訳（race_id[4:6]）
        from collections import Counter
        venue_counts = Counter(k[4:6] for k in jan1_ids)
        for venue, cnt in sorted(venue_counts.items(), key=lambda x: -x[1]):
            print(f"    会場コード {venue}: {cnt} 件")
        for k, v in list(jan1_ids.items())[:10]:
            print(f"    {k} -> {v}")

    # 既存 temp_rid_to_date.json との差分
    print("\n■ temp_rid_to_date.json との差分（汚染率）")
    if os.path.exists(TEMP_RID_TO_DATE):
        with open(TEMP_RID_TO_DATE, encoding="utf-8") as f:
            old_map = json.load(f)
        common_ids = set(mapping) & set(old_map)
        mismatches = {k: (old_map[k], mapping[k]) for k in common_ids if old_map[k] != mapping[k]}
        print(f"  旧マップ件数       : {len(old_map):,}")
        print(f"  新マップ件数       : {len(mapping):,}")
        print(f"  共通 race_id 数    : {len(common_ids):,}")
        print(f"  日付不一致件数     : {len(mismatches):,}")
        if mismatches:
            contam_rate = len(mismatches) / len(common_ids) * 100 if common_ids else 0
            print(f"  不一致率（汚染率） : {contam_rate:.1f}%")
            print("  不一致サンプル（最大10件）:")
            for k, (old_v, new_v) in list(mismatches.items())[:10]:
                print(f"    {k}: 旧={old_v} → 新（正）={new_v}")
    else:
        print(f"  {TEMP_RID_TO_DATE} が存在しない → 差分比較スキップ")

    # 主要 race_id の sanity check
    print("\n■ 主要 race_id Sanity Check")
    sanity_cases = [
        ("202605010101", "東京（JRA）", ["2026-01-03", "2026-01-04"], "2026-01-01"),
        ("202606010101", "中山（JRA）", ["2026-01-03", "2026-01-04"], "2026-01-01"),
        ("202608010101", "京都（JRA）", ["2026-01-03", "2026-01-04"], "2026-01-01"),
    ]
    for rid, label, expected_ok, expected_ng in sanity_cases:
        actual = mapping.get(rid, "(未収録)")
        if actual in expected_ok:
            status = "OK"
        elif actual == expected_ng:
            status = "NG（元旦汚染）"
        else:
            status = f"UNKNOWN（{actual}）"
        print(f"  {rid} ({label}): {actual} [{status}]")

    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="HTMLキャッシュから race_id ↔ 日付マスタを構築する")
    parser.add_argument(
        "--output", "-o",
        default=DEFAULT_OUTPUT,
        help=f"出力先JSONパス（デフォルト: {DEFAULT_OUTPUT}）",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="抽出失敗詳細を表示する",
    )
    args = parser.parse_args()

    print("[START] race_id <-> 日付マスタ構築開始")
    print(f"  キャッシュディレクトリ: {CACHE_DIR}")
    print(f"  出力先: {args.output}")

    # マスタ構築
    result = build_master(verbose=args.verbose)

    # 出力ディレクトリ作成（存在しない場合）
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # JSON 保存（ASCII 安全 / indent=2 / utf-8）
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=True, indent=2)

    print(f"\n[DONE] 保存完了: {args.output}")
    print(f"  総エントリ数: {result['total_entries']:,}")

    # 検証レポート
    _print_validation_report(result)


if __name__ == "__main__":
    main()
