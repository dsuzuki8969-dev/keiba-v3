"""2024-03~10 の三連複ペイアウト再取得スクリプト

race_results テーブルの payouts_json が不完全な行を再取得して更新する。
- JRA: keibabook 経由 (2,321 レース)
- NAR: NAR 公式 keiba.go.jp 経由 (9,448 レース)
netkeiba 直接アクセスなし。
"""
import argparse
import json
import os
import sqlite3
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "keiba.db")
RESULTS_DIR = os.path.join(PROJECT_ROOT, "data", "results")


def _is_jra(race_id):
    return int(race_id[4:6]) <= 10


def get_missing_races(conn, start, end, jra_only=False, nar_only=False):
    """三連複ペイアウトがない race_id 一覧"""
    c = conn.cursor()
    sql = """
        SELECT date, race_id, payouts_json
        FROM race_results
        WHERE date BETWEEN ? AND ?
          AND payouts_json NOT LIKE '%三連複%'
          AND payouts_json NOT LIKE '%sanrenpuku%'
          AND cancelled = 0
    """
    if jra_only:
        sql += "  AND CAST(substr(race_id, 5, 2) AS INTEGER) <= 10\n"
    elif nar_only:
        sql += "  AND CAST(substr(race_id, 5, 2) AS INTEGER) > 10\n"
    sql += "  ORDER BY date, race_id"
    c.execute(sql, (start, end))
    return c.fetchall()


def main():
    parser = argparse.ArgumentParser(description="2024 三連複ペイアウト再取得")
    parser.add_argument("--start", default="2024-03-01")
    parser.add_argument("--end", default="2024-10-31")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-fetch", type=int, default=0, help="最大取得数 (0=無制限)")
    parser.add_argument("--rate", type=float, default=2.5, help="リクエスト間隔 (秒)")
    parser.add_argument("--jra-only", action="store_true", help="JRA レースのみ (NAR 除外)")
    parser.add_argument("--nar-only", action="store_true", help="NAR レースのみ (JRA 除外)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    missing = get_missing_races(conn, args.start, args.end,
                                jra_only=args.jra_only, nar_only=args.nar_only)
    print(f"三連複ペイアウトなし: {len(missing):,} レース ({args.start} ~ {args.end})")

    if args.dry_run:
        jra = sum(1 for _, rid, _ in missing if _is_jra(rid))
        print(f"  JRA: {jra:,}, NAR: {len(missing)-jra:,}")
        est_time = len(missing) * args.rate / 3600
        print(f"  推定所要時間: {est_time:.1f} 時間 ({args.rate}秒/件)")
        conn.close()
        return

    kb_scraper = None
    nar_scraper = None

    # JRA 用: keibabook
    if not args.nar_only:
        from src.scraper.keibabook_training import KeibabookClient
        from src.scraper.keibabook_result import KeibabookResultScraper
        kb_client = KeibabookClient()
        kb_scraper = KeibabookResultScraper(kb_client)

    # NAR 用: NAR 公式 (keiba.go.jp)
    if not args.jra_only:
        from src.scraper.official_nar import OfficialNARScraper
        nar_scraper = OfficialNARScraper()

    limit = args.max_fetch if args.max_fetch > 0 else len(missing)
    targets = missing[:limit]

    updated = 0
    failed = 0
    no_sanren = 0
    dates_to_rebuild = set()

    for i, (dt, race_id, old_payouts_json) in enumerate(targets):
        try:
            if _is_jra(race_id):
                result = kb_scraper.fetch_result(race_id, dt) if kb_scraper else None
            else:
                result = nar_scraper.get_result(race_id, dt) if nar_scraper else None
        except Exception as e:
            if i < 5:
                print(f"  ERROR {race_id}: {type(e).__name__}: {e}")
            failed += 1
            time.sleep(args.rate)
            continue

        if i < 5:
            print(f"  DEBUG {race_id}: result={'yes' if result else 'None'}, "
                  f"payouts={list(result.get('payouts',{}).keys()) if result else 'N/A'}")

        if result and result.get("payouts"):
            new_payouts = result["payouts"]
            # 三連複があるか確認
            has_sanren = any(k in ("三連複", "sanrenpuku") for k in new_payouts)

            if has_sanren:
                # 既存ペイアウトとマージ (新しいもので上書き)
                try:
                    old_payouts = json.loads(old_payouts_json) if old_payouts_json else {}
                except (json.JSONDecodeError, TypeError):
                    old_payouts = {}
                old_payouts.update(new_payouts)
                new_json = json.dumps(old_payouts, ensure_ascii=False)

                conn.execute(
                    "UPDATE race_results SET payouts_json = ? WHERE race_id = ? AND date = ?",
                    (new_json, race_id, dt),
                )
                conn.commit()
                updated += 1
                dates_to_rebuild.add(dt)
            else:
                no_sanren += 1
        else:
            failed += 1

        if (i + 1) % 50 == 0 or i == len(targets) - 1:
            pct = (i + 1) / len(targets) * 100
            print(f"  [{i+1}/{len(targets)}] {pct:.0f}% — "
                  f"更新={updated:,}, 三連複なし={no_sanren:,}, 失敗={failed:,}")

        time.sleep(args.rate)

    conn.close()

    # 更新した日付の results JSON を再生成
    if dates_to_rebuild:
        print(f"\nresults JSON 再生成: {len(dates_to_rebuild)} 日")
        conn2 = sqlite3.connect(DB_PATH, timeout=30)
        for dt in sorted(dates_to_rebuild):
            c = conn2.cursor()
            c.execute("SELECT race_id, order_json, payouts_json FROM race_results WHERE date = ? AND cancelled = 0", (dt,))
            rows = c.fetchall()
            data = {}
            for rid, oj, pj in rows:
                try:
                    data[rid] = {
                        "order": json.loads(oj) if oj else [],
                        "payouts": json.loads(pj) if pj else {},
                        "source": "keibabook_backfill",
                    }
                except (json.JSONDecodeError, TypeError):
                    pass
            fname = dt.replace("-", "") + "_results.json"
            with open(os.path.join(RESULTS_DIR, fname), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        conn2.close()

    print(f"\n完了: 更新={updated:,}, 三連複なし={no_sanren:,}, 失敗={failed:,}")
    print(f"日付再生成: {len(dates_to_rebuild)}")


if __name__ == "__main__":
    main()
