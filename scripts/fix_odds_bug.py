"""レース結果の odds バグ修復

既存の race.netkeiba.com_race_result キャッシュを UTF-8 でデコードし、
`_parse_finish_order` で正しく再パースして race_results / race_log /
results.json を修復する。

マスター指示 2026-04-22: 根本修正
"""
from __future__ import annotations
import io
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Optional

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

import lz4.frame
from bs4 import BeautifulSoup

from config.settings import DATABASE_PATH
from src.results_tracker import _parse_finish_order, _parse_payouts

CACHE_DIR = Path("data/cache")
RESULTS_DIR = Path("data/results")


def read_html_cache(race_id: str) -> Optional[str]:
    """キャッシュ HTML を UTF-8 として取得。無ければ None。"""
    fp = CACHE_DIR / f"race.netkeiba.com_race_result.html_race_id={race_id}.html.lz4"
    if not fp.exists():
        return None
    try:
        raw = lz4.frame.decompress(fp.read_bytes())
        # UTF-8 優先（netkeiba は UTF-8 で配信）、fallback で EUC-JP
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("EUC-JP", errors="replace")
    except Exception:
        return None


def reparse_order(race_id: str) -> Optional[list]:
    """キャッシュから `_parse_finish_order` で正しく parse した rows を返す。"""
    html = read_html_cache(race_id)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    rows = _parse_finish_order(soup)
    if not rows:
        return None
    return rows


def reparse_both(race_id: str) -> Optional[tuple]:
    """キャッシュから order と payouts を両方再パース。"""
    html = read_html_cache(race_id)
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    rows = _parse_finish_order(soup)
    payouts = _parse_payouts(soup)
    return (rows or [], payouts or {})


def merge_into_order(existing_order: list, new_rows: list) -> list:
    """既存の order（horse_no ベース）に new_rows を merge する。
    新値で上書き対象: odds / popularity / time / time_sec / margin / last_3f / corners / gate_no
    """
    by_hno = {r["horse_no"]: r for r in new_rows if r.get("horse_no") is not None}
    for entry in existing_order:
        hno = entry.get("horse_no")
        src = by_hno.get(hno)
        if not src:
            continue
        # 上書き（修復）
        for key in ("time", "time_sec", "popularity", "odds",
                    "last_3f", "margin", "gate_no", "corners"):
            new_val = src.get(key)
            if new_val in (None, "", []):
                continue
            entry[key] = new_val
    return existing_order


def main():
    t0 = time.time()
    stats = {"total": 0, "cache_miss": 0, "parse_fail": 0, "updated": 0, "unchanged": 0}

    # race_results 全件を取得
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    all_rows = conn.execute(
        "SELECT id, race_id, date, order_json, payouts_json FROM race_results"
    ).fetchall()
    stats["total"] = len(all_rows)
    print(f"[{time.strftime('%H:%M:%S')}] race_results 総件数: {stats['total']}")

    # 1. race_results テーブル修復（order + payouts 両方）
    for idx, r in enumerate(all_rows):
        rid = r["race_id"]
        parsed = reparse_both(rid)
        if parsed is None:
            stats["cache_miss"] += 1
            continue
        new_rows, new_payouts = parsed
        if not new_rows and not new_payouts:
            stats["parse_fail"] += 1
            continue

        # order 修復
        try:
            existing = json.loads(r["order_json"]) if r["order_json"] else []
        except Exception:
            existing = []
        if new_rows:
            if not existing:
                existing = new_rows
            else:
                existing = merge_into_order(existing, new_rows)
            new_order_json = json.dumps(existing, ensure_ascii=False)
        else:
            new_order_json = r["order_json"]

        # payouts 修復（_parse_payouts の結果は常に正しいので上書き）
        if new_payouts:
            new_payouts_json = json.dumps(new_payouts, ensure_ascii=False)
        else:
            new_payouts_json = r["payouts_json"] if "payouts_json" in r.keys() else "{}"

        changed = (new_order_json != r["order_json"]) or (new_payouts_json != r["payouts_json"])
        if changed:
            conn.execute(
                "UPDATE race_results SET order_json=?, payouts_json=? WHERE id=?",
                (new_order_json, new_payouts_json, r["id"]),
            )
            stats["updated"] += 1
        else:
            stats["unchanged"] += 1

        if (idx + 1) % 500 == 0:
            conn.commit()
            print(f"  [{idx+1}/{stats['total']}] updated={stats['updated']} miss={stats['cache_miss']} elapsed={time.time()-t0:.1f}s", flush=True)
    conn.commit()
    print(f"[{time.strftime('%H:%M:%S')}] race_results 修復完了: {stats}")

    # 2. race_log.win_odds を race_results.order_json から再バックフィル
    t1 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] race_log.win_odds 再バックフィル開始...")
    conn.execute("UPDATE race_log SET win_odds=NULL")
    conn.commit()
    n_backfill = 0
    batch = 200
    all_rids = [r["race_id"] for r in conn.execute(
        "SELECT DISTINCT race_id FROM race_log"
    ).fetchall()]
    for bi in range(0, len(all_rids), batch):
        chunk = all_rids[bi:bi+batch]
        placeholders = ",".join(["?"] * len(chunk))
        rr_rows = conn.execute(
            f"SELECT race_id, order_json FROM race_results WHERE race_id IN ({placeholders})",
            chunk,
        ).fetchall()
        for rr in rr_rows:
            try:
                orders = json.loads(rr["order_json"])
                for oe in orders:
                    hno = oe.get("horse_no")
                    odds = oe.get("odds")
                    if hno is None or odds is None:
                        continue
                    try:
                        odds_f = float(odds)
                    except (ValueError, TypeError):
                        continue
                    rc = conn.execute(
                        "UPDATE race_log SET win_odds=? WHERE race_id=? AND horse_no=?",
                        (odds_f, rr["race_id"], int(hno)),
                    )
                    n_backfill += rc.rowcount
            except Exception:
                continue
        conn.commit()
    print(f"[{time.strftime('%H:%M:%S')}] race_log 修復: {n_backfill}行更新 / 所要 {time.time()-t1:.1f}s")

    # 3. results.json 再生成
    t2 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] results.json 再生成開始...")
    n_files = 0
    n_races = 0
    # date ごとにグルーピング
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT date FROM race_results ORDER BY date"
    ).fetchall()]
    for date_str in dates:
        # date は "YYYY-MM-DD" 形式？確認要
        file_date = date_str.replace("-", "") if "-" in date_str else date_str
        fp = RESULTS_DIR / f"{file_date}_results.json"
        # 既存ファイル読み（payouts など他フィールドを保持）
        try:
            existing_obj = json.loads(fp.read_text(encoding="utf-8")) if fp.exists() else {}
        except Exception:
            existing_obj = {}
        # 当日の全レースを DB から取得してマージ
        race_rows = conn.execute(
            "SELECT race_id, order_json, payouts_json FROM race_results WHERE date=?",
            (date_str,)
        ).fetchall()
        for rr in race_rows:
            rid = rr["race_id"]
            try:
                order = json.loads(rr["order_json"]) if rr["order_json"] else []
            except Exception:
                order = []
            try:
                payouts = json.loads(rr["payouts_json"]) if rr["payouts_json"] else {}
            except Exception:
                payouts = {}
            existing_race = existing_obj.get(rid, {})
            existing_race["order"] = order
            # payouts は DB のものを優先（無ければ既存を残す）
            if payouts:
                existing_race["payouts"] = payouts
            existing_obj[rid] = existing_race
            n_races += 1
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_text(json.dumps(existing_obj, ensure_ascii=False, indent=1), encoding="utf-8")
        n_files += 1

    print(f"[{time.strftime('%H:%M:%S')}] results.json 再生成完了: {n_files}ファイル / {n_races}レース / 所要 {time.time()-t2:.1f}s")
    print(f"\n✅ 全工程完了 総所要 {time.time()-t0:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
