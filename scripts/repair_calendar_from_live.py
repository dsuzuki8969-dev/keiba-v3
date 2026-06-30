#!/usr/bin/env python
"""カレンダー ライブ修復ツール — scripts/repair_calendar_from_live.py

kaisai_calendar.json は netkeiba 由来で、未来の NAR 本州場 (大井/名古屋/園田 等) を
落とすことがある。本スクリプトは NAR/JRA 公式レース一覧 (=実在の ground truth) を
ライブ取得し、欠落している開催会場を kaisai_calendar.json に補完する。

run_analysis_date.py の T-038 自己補完 (add_venue_to_calendar) と同じ仕組みを、
分析を待たずに近日分へ proactive に適用する。netkeiba 全再生成より確実 (落とさない)。

使い方:
  python scripts/repair_calendar_from_live.py                 # 今日から7日分
  python scripts/repair_calendar_from_live.py --days 14       # 14日分
  python scripts/repair_calendar_from_live.py --start 2026-07-02 --days 3
"""
import argparse
import datetime
import os
import sys

# Windows console cp932 対策
if sys.stdout.encoding and sys.stdout.encoding.lower() in ("cp932", "mbcs"):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.masters.course_master import get_all_courses  # noqa: E402
from data.masters.venue_master import (  # noqa: E402
    VENUE_CODE_TO_NAME,
    JRA_VENUE_CODES,
    is_banei,
)
from src.scraper.auth import PremiumNetkeibaScraper  # noqa: E402
from src.scraper.kaisai_calendar_util import (  # noqa: E402
    add_venue_to_calendar,
    get_open_venues,
    reload_calendar,
)
from src.log import get_logger  # noqa: E402

logger = get_logger(__name__)


def repair(start: datetime.date, days: int) -> int:
    all_courses = get_all_courses()
    scraper = PremiumNetkeibaScraper(all_courses, ignore_ttl=True)
    try:
        scraper.login()
    except Exception as e:  # noqa: BLE001
        # ログイン失敗でも NAR 公式レース一覧 (fetch_date) は取得できる場合があるため続行。
        logger.warning("[repair] ログイン失敗 (続行・キャッシュ/公式のみ): %s", e)
        print(f"  [警告] ログイン失敗 (続行): {e}")

    total_added = 0
    for i in range(days):
        d = start + datetime.timedelta(days=i)
        ds = d.isoformat()
        try:
            race_ids = scraper.fetch_date(ds)
        except Exception as e:  # noqa: BLE001
            logger.warning("[repair] fetch_date 失敗 %s: %s", ds, e)
            print(f"  {ds}: fetch_date 失敗 ({e})")
            continue

        # ライブ検出の (会場名, kind) 集合 (帯広=ばんえいは除外)
        seen: set = set()
        for rid in race_ids:
            vc = rid[4:6] if len(rid) >= 6 else ""
            if not vc or is_banei(vc):
                continue
            vname = VENUE_CODE_TO_NAME.get(vc, "")
            if not vname:
                continue
            kind = "jra" if vc in JRA_VENUE_CODES else "nar"
            seen.add((vname, kind))

        added_today = 0
        for vname, kind in sorted(seen):
            if add_venue_to_calendar(ds, vname, kind):
                added_today += 1
        total_added += added_today
        reload_calendar()
        cur = get_open_venues(ds)
        status = "補完" if added_today else "変更なし"
        print(
            f"  {ds}: ライブ {len(seen)}場 / {status}{added_today}場 "
            f"→ jra={cur['jra']} nar={cur['nar']}"
        )

    return total_added


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="開始日 YYYY-MM-DD (既定=今日)")
    ap.add_argument("--days", type=int, default=7, help="対象日数 (既定=7)")
    args = ap.parse_args()

    start = (
        datetime.date.fromisoformat(args.start)
        if args.start
        else datetime.date.today()
    )
    print(f"=== カレンダー ライブ修復: {start} から {args.days} 日分 ===")
    total = repair(start, args.days)
    print(f"=== 完了: 合計 {total} 場補完 ===")


if __name__ == "__main__":
    main()
