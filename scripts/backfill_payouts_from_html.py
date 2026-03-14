#!/usr/bin/env python
"""
HTMLキャッシュから確定払戻金を全券種抽出してrace_results.payouts_jsonに保存する。

対象ファイル:
  data/cache/race.netkeiba.com_race_result.html_race_id=*.html.lz4  (JRA)
  data/cache/nar.netkeiba.com_race_result.html_race_id=*.html.lz4   (NAR)

出力フォーマット例:
  {
    "単勝": {"combo": "4",      "payout": 160},
    "複勝": [{"combo": "4", "payout": 120}, {"combo": "10", "payout": 260}, ...],
    "枠連": {"combo": "3-6",    "payout": 1380},
    "馬連": {"combo": "4-10",   "payout": 1550},
    "ワイド": [{"combo": "4-10", "payout": 670}, ...],
    "馬単": {"combo": "4-10",   "payout": 1930},
    "三連複": {"combo": "4-10-11", "payout": 4790},
    "三連単": {"combo": "4-10-11", "payout": 14980}
  }
"""

import argparse
import glob
import json
import re
import sqlite3
import sys
import os

# プロジェクトルートをパスに追加
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lz4.frame
from bs4 import BeautifulSoup

CACHE_DIR = "data/cache"
DB_PATH = "data/keiba.db"

# tr class → 券種名マッピング
CLASS_TO_TYPE = {
    "Tansho":  "単勝",
    "Fukusho": "複勝",
    "Wakuren": "枠連",
    "Umaren":  "馬連",
    "Umatan":  "馬単",
    "Wide":    "ワイド",
    "Fuku3":   "三連複",
    "Tan3":    "三連単",
}

# 複数払戻を持つ券種（リスト形式で保存）
LIST_TYPES = {"複勝", "ワイド"}


def _extract_nums(td_result):
    """Result セルからすべての馬番（数字のみspanテキスト）を抽出する。"""
    return [
        s.get_text(strip=True)
        for s in td_result.find_all("span")
        if s.get_text(strip=True).isdigit()
    ]


def _extract_payouts(td_payout):
    """Payout セルから払戻金額リストを抽出する（例: '4,790円|1,350円' → [4790, 1350]）。"""
    raw = td_payout.get_text(separator="|", strip=True)
    parts = raw.split("|")
    result = []
    for p in parts:
        m = re.search(r"[\d,]+", p)
        if m:
            try:
                result.append(int(m.group().replace(",", "")))
            except ValueError:
                pass
    return result


def parse_payouts(html: str) -> dict:
    """HTML文字列から全券種の払戻データを抽出する。"""
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="Payout_Detail_Table")

    payouts = {}
    for tbl in tables:
        for tr in tbl.find_all("tr"):
            tr_cls = (tr.get("class") or [""])[0]
            if tr_cls not in CLASS_TO_TYPE:
                continue
            ticket_type = CLASS_TO_TYPE[tr_cls]

            td_result = tr.find("td", class_="Result")
            td_payout = tr.find("td", class_="Payout")
            if not td_result or not td_payout:
                continue

            nums = _extract_nums(td_result)
            pay_vals = _extract_payouts(td_payout)

            if not nums or not pay_vals:
                continue

            if ticket_type == "複勝":
                # 3頭 → リスト形式
                entries = []
                for hn, pv in zip(nums, pay_vals):
                    entries.append({"combo": hn, "payout": pv})
                payouts["複勝"] = entries

            elif ticket_type == "ワイド":
                # ペア × N組 → リスト形式
                entries = []
                pair_count = len(pay_vals)
                for i in range(pair_count):
                    if i * 2 + 1 < len(nums):
                        h1, h2 = sorted([int(nums[i * 2]), int(nums[i * 2 + 1])])
                        entries.append({"combo": f"{h1}-{h2}", "payout": pay_vals[i]})
                if "ワイド" not in payouts:
                    payouts["ワイド"] = entries
                else:
                    payouts["ワイド"].extend(entries)

            elif ticket_type in ("三連複", "三連単"):
                # 3頭 → ハイフン区切り
                combo_str = "-".join(nums[:3])
                payouts[ticket_type] = {"combo": combo_str, "payout": pay_vals[0]}

            elif ticket_type in ("枠連", "馬連", "馬単"):
                # 2頭 → ハイフン区切り
                combo_str = "-".join(nums[:2])
                payouts[ticket_type] = {"combo": combo_str, "payout": pay_vals[0]}

            else:
                # 単勝
                payouts[ticket_type] = {"combo": nums[0], "payout": pay_vals[0]}

    return payouts


def extract_race_id(filepath: str) -> str:
    """ファイルパスから race_id を抽出する。"""
    m = re.search(r"race_id=(\d+)", filepath)
    return m.group(1) if m else ""


def main():
    parser = argparse.ArgumentParser(description="HTMLキャッシュから確定払戻金をDBに保存")
    parser.add_argument("--year", type=str, default=None, help="対象年 (例: 2025). 省略時は全年")
    parser.add_argument("--force", action="store_true", help="既存のpayouts_jsonを上書きする")
    parser.add_argument("--dry-run", action="store_true", help="DBに書き込まず結果だけ表示")
    parser.add_argument("--limit", type=int, default=0, help="処理件数の上限 (デバッグ用)")
    args = parser.parse_args()

    # ファイル一覧を取得（.html.lz4 と .html 両方対応）
    year_glob = f"*{args.year}*" if args.year else "*"
    jra_files = (
        glob.glob(os.path.join(CACHE_DIR, f"race.netkeiba.com_race_result.html_race_id={year_glob}.html.lz4"))
        + glob.glob(os.path.join(CACHE_DIR, f"race.netkeiba.com_race_result.html_race_id={year_glob}.html"))
    )
    nar_files = (
        glob.glob(os.path.join(CACHE_DIR, f"nar.netkeiba.com_race_result.html_race_id={year_glob}.html.lz4"))
        + glob.glob(os.path.join(CACHE_DIR, f"nar.netkeiba.com_race_result.html_race_id={year_glob}.html"))
    )
    # .html.lz4 と .html の重複除去（.html.lz4 が優先）
    def _dedup(files):
        seen = {}
        for f in sorted(files):
            key = f.replace(".lz4", "")
            if key not in seen or f.endswith(".lz4"):
                seen[key] = f
        return list(seen.values())
    jra_files = _dedup(jra_files)
    nar_files = _dedup(nar_files)
    all_files = jra_files + nar_files
    all_files.sort()

    if args.limit:
        all_files = all_files[: args.limit]

    total = len(all_files)
    print(f"対象ファイル: {total}件 (JRA:{len(jra_files)}, NAR:{len(nar_files)})")
    if args.year:
        print(f"  絞り込み年: {args.year}")

    if not total:
        print("対象ファイルがありません。")
        return

    # DB接続
    conn = None
    if not args.dry_run:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        # race_results テーブルが存在しない場合は作成
        conn.execute("""
            CREATE TABLE IF NOT EXISTS race_results (
                race_id TEXT PRIMARY KEY,
                date TEXT,
                venue TEXT,
                race_no INTEGER,
                payouts_json TEXT
            )
        """)
        conn.commit()

    # 既存レコードのrace_idセットを取得（--forceなしの場合のスキップ判定用）
    existing_with_payouts = set()
    if conn and not args.force:
        rows = conn.execute(
            "SELECT race_id FROM race_results WHERE payouts_json IS NOT NULL AND payouts_json != '{}' AND payouts_json != ''"
        ).fetchall()
        existing_with_payouts = {r[0] for r in rows}
        print(f"既存payout済みレコード: {len(existing_with_payouts)}件 (--forceなしはスキップ)")

    updated = 0
    skipped = 0
    no_table = 0
    errors = 0

    for i, fpath in enumerate(all_files):
        if (i + 1) % 1000 == 0 or i == 0:
            print(f"  [{i+1}/{total}] 処理中... updated={updated}, skipped={skipped}, errors={errors}")

        race_id = extract_race_id(fpath)
        if not race_id:
            errors += 1
            continue

        # --forceなしで既存payout済みはスキップ
        if not args.force and race_id in existing_with_payouts:
            skipped += 1
            continue

        try:
            if fpath.endswith(".lz4"):
                with lz4.frame.open(fpath, "rb") as f:
                    raw = f.read()
                html = raw.decode("utf-8")
            else:
                with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                    html = f.read()
        except Exception as e:
            errors += 1
            continue

        try:
            payouts = parse_payouts(html)
        except Exception as e:
            errors += 1
            continue

        if not payouts:
            no_table += 1
            continue

        pj = json.dumps(payouts, ensure_ascii=False)

        if args.dry_run:
            if i < 5:
                print(f"  DRY-RUN {race_id}: {pj[:120]}")
            updated += 1
            continue

        # 既存レコードのpayouts_jsonをUPDATE（INSERT不要: race_resultsはバックフィル済み）
        conn.execute(
            "UPDATE race_results SET payouts_json = ? WHERE race_id = ?",
            (pj, race_id),
        )
        updated += 1

        # 1000件ごとにコミット
        if updated % 1000 == 0:
            conn.commit()

    if conn:
        conn.commit()
        conn.close()

    print()
    print("=" * 60)
    print(f"完了: 更新={updated}, スキップ={skipped}, 払戻テーブルなし={no_table}, エラー={errors}")


if __name__ == "__main__":
    main()
