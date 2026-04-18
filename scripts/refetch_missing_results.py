"""欠損 race_results を再fetch するスクリプト

pred.json の past_3_runs[].date を使って正しい日付で再fetchし、
time_sec / margin / popularity / odds などの欠損フィールドを埋める。
"""
import sys
import os
import json
import time
import sqlite3

# プロジェクトルートを sys.path に追加（scripts/配下から実行するため）
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

sys.stdout.reconfigure(encoding="utf-8")

from data.masters.course_master import get_all_courses
from src.results_tracker import fetch_single_race_result
from src.scraper.netkeiba import NetkeibaScraper
from src.scraper.official_odds import OfficialOddsScraper
from src.database import save_results


def main():
    conn = sqlite3.connect("data/keiba.db")
    c = conn.cursor()

    # pred.json から race_id → date マッピング＆欠損 race_id 抽出
    with open("data/predictions/20260418_pred.json", "r", encoding="utf-8") as f:
        pred = json.load(f)

    rid_to_date = {}
    missing_rids = set()
    for race in pred.get("races", []):
        for h in race.get("horses", []):
            for r in h.get("past_3_runs") or []:
                rid = r.get("race_id")
                dt = r.get("date")
                if rid and dt:
                    rid_to_date[rid] = dt
                cm = r.get("margin")
                fp = r.get("finish_pos")
                if (cm is None or cm == 0) and fp and fp < 90 and fp != 1 and rid:
                    missing_rids.add(rid)

    rids = sorted(missing_rids)
    print(f"[start] refetch target: {len(rids)} races", flush=True)

    all_courses = get_all_courses()
    client = NetkeibaScraper(all_courses)
    official = OfficialOddsScraper()

    ok = 0
    fail = 0
    no_date = 0
    start = time.time()
    for i, rid in enumerate(rids, 1):
        dt = rid_to_date.get(rid)
        if not dt:
            no_date += 1
            fail += 1
            continue
        try:
            result = fetch_single_race_result(
                dt, rid, client, official_scraper=official
            )
            if result and result.get("order"):
                w = next(
                    (e for e in result["order"] if e.get("finish") == 1), None
                )
                if w and w.get("time_sec"):
                    save_results(dt, {rid: result})
                    ok += 1
                else:
                    fail += 1
            else:
                fail += 1
        except Exception as e:
            print(f"  [EXC] {rid} ({dt}): {type(e).__name__}: {e}", flush=True)
            fail += 1

        # プログレス10件おき
        if i % 10 == 0 or i == len(rids):
            elapsed = time.time() - start
            rate = i / elapsed if elapsed > 0 else 0
            eta = (len(rids) - i) / rate if rate > 0 else 0
            pct = i * 100 / len(rids)
            print(
                f"[{i}/{len(rids)}] {pct:.1f}% ok={ok} fail={fail} "
                f"elapsed={elapsed:.0f}s eta={eta:.0f}s",
                flush=True,
            )

    print(
        f"[done] ok={ok} fail={fail} no_date={no_date} "
        f"elapsed={time.time()-start:.0f}s",
        flush=True,
    )


if __name__ == "__main__":
    main()
