"""
pred.json past_3_runs[].margin を race_log.margin_ahead から再注入。

netkeiba から margin を再取得した後 (fetch_missing_margins.py) に実行する。

使い方:
    python scripts/refresh_pred_margin.py             # 当日 pred.json
    python scripts/refresh_pred_margin.py --recent 7  # 直近 7 日
    python scripts/refresh_pred_margin.py --dry-run
"""
from __future__ import annotations
import argparse, json, shutil, sqlite3, sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "data" / "keiba.db"
PRED_DIR = ROOT / "data" / "predictions"


def refresh_one(path: Path, conn: sqlite3.Connection, dry_run: bool) -> dict:
    if not path.exists():
        return {"date": path.stem, "status": "missing"}
    with open(path, encoding="utf-8") as f:
        pred = json.load(f)

    updated = 0
    unchanged = 0
    no_data = 0
    skip_winner = 0

    for r in pred.get("races", []):
        for h in r.get("horses", []):
            for past in (h.get("past_3_runs") or h.get("past_runs") or []):
                fp = past.get("finish_pos") or 0
                rid = past.get("race_id")
                hno = past.get("horse_no")
                if not rid or hno is None:
                    no_data += 1
                    continue
                if fp <= 1:
                    skip_winner += 1
                    continue
                row = conn.execute(
                    "SELECT margin_ahead FROM race_log WHERE race_id=? AND horse_no=?",
                    (rid, hno),
                ).fetchone()
                if not row:
                    no_data += 1
                    continue
                ma = row[0]
                if ma is None or ma == 0:
                    # race_log にもまだ margin なし → null のまま (取得不能)
                    if past.get("margin") is not None:
                        past["margin"] = None
                        updated += 1
                    continue
                if past.get("margin") != ma:
                    past["margin"] = ma
                    updated += 1
                else:
                    unchanged += 1

    if not dry_run and updated > 0:
        ts = date.today().strftime("%Y%m%d")
        bak = path.with_suffix(f".json.bak_margin_{ts}")
        if not bak.exists():
            shutil.copy(path, bak)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, separators=(",", ":"))

    return {
        "date": path.stem.replace("_pred", ""),
        "status": "ok",
        "updated": updated,
        "unchanged": unchanged,
        "no_data": no_data,
        "skip_winner": skip_winner,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("date", nargs="?", default=date.today().strftime("%Y%m%d"))
    parser.add_argument("--recent", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = []
    if args.recent > 0:
        today = date.today()
        for i in range(args.recent):
            d = today - timedelta(days=i)
            targets.append(PRED_DIR / f"{d.strftime('%Y%m%d')}_pred.json")
    else:
        targets.append(PRED_DIR / f"{args.date}_pred.json")

    print(f"[INFO] 対象: {len(targets)} ファイル ({'DRY-RUN' if args.dry_run else '本実行'})")
    conn = sqlite3.connect(str(DB))
    total_updated = 0
    for p in targets:
        result = refresh_one(p, conn, args.dry_run)
        if result["status"] == "missing":
            print(f"  [SKIP] {result['date']}")
            continue
        print(f"  [{result['date']}] updated={result['updated']} unchanged={result['unchanged']} "
              f"no_data={result['no_data']}")
        total_updated += result["updated"]
    conn.close()
    print(f"\n[完了] 合計 updated={total_updated}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
