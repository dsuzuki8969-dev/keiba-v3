"""
未収集の調教データを一括スクレイピングするスクリプト

処理内容:
  1. 129日分の未収集日をスクレイピング（新規JSON作成）
  2. 168日分の門別バックフィル（既存JSONに門別レースを追加）

使い方:
  python scripts/scrape_missing_training.py                    # 全実行
  python scripts/scrape_missing_training.py --missing-only     # 未収集日のみ
  python scripts/scrape_missing_training.py --backfill-only    # 門別バックフィルのみ
  python scripts/scrape_missing_training.py --dry-run          # 実際にはスクレイピングしない
"""

import argparse
import json
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from data.masters.venue_master import (
    JRA_CODES,
    get_venue_code_from_race_id,
    get_venue_name,
)
from src.log import get_logger
from src.scraper.keibabook_training import (
    KeibabookClient,
    KeibabookTrainingScraper,
    is_kb_supported_venue,
)
from src.scraper.netkeiba import NetkeibaClient, RaceListScraper
from src.scraper.training_collector import (
    TRAINING_ML_DIR,
    _date_output_path,
    _training_record_to_dict,
)

logger = get_logger(__name__)


def get_missing_dates(start: str, end: str) -> list:
    """未収集の日付リストを返す（YYYYMMDD形式）"""
    existing = set(
        f.replace(".json", "")
        for f in os.listdir(TRAINING_ML_DIR)
        if f.endswith(".json") and not f.startswith("_")
    )
    all_dates = []
    d = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while d <= end_dt:
        ds = d.strftime("%Y%m%d")
        if ds not in existing:
            all_dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return all_dates


def get_monbetsu_backfill_dates() -> list:
    """門別レースがあるのに既存JSONに門別データがない日付を返す"""
    import sqlite3

    db_path = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        "data",
        "keiba.db",
    )
    conn = sqlite3.connect(db_path)
    # 門別レースがある日付とレースIDを取得
    rows = conn.execute(
        """
        SELECT race_date, race_id
        FROM race_log
        WHERE venue_code='30' AND race_date >= '2024-01-01'
        GROUP BY race_id
        ORDER BY race_date
        """
    ).fetchall()
    conn.close()

    # 日付ごとにレースIDをグループ化
    date_races = {}
    for race_date, race_id in rows:
        date_key = race_date.replace("-", "")
        if date_key not in date_races:
            date_races[date_key] = []
        if race_id not in date_races[date_key]:
            date_races[date_key].append(race_id)

    # 既存JSONに門別データがない日付を抽出
    backfill = []
    for date_key, race_ids in sorted(date_races.items()):
        json_path = _date_output_path(
            f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}"
        )
        if not os.path.exists(json_path):
            continue  # JSONファイルなし = 未収集日（別途処理）

        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 既存JSONに門別データがあるかチェック
        existing_venues = set(r.get("venue_code") for r in data.get("races", []))
        if "30" not in existing_venues:
            backfill.append(
                {
                    "date": f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:8]}",
                    "race_ids": race_ids,
                    "json_path": json_path,
                }
            )

    return backfill


def scrape_missing_days(
    kb_client: KeibabookClient,
    race_list_scraper: RaceListScraper,
    missing_dates: list,
    dry_run: bool = False,
):
    """未収集日をスクレイピング"""
    if not missing_dates:
        logger.info("未収集日なし")
        return

    scraper = KeibabookTrainingScraper(kb_client)
    total = len(missing_dates)
    total_races = 0
    total_horses = 0
    skipped = 0

    logger.info(f"=== 未収集日スクレイピング: {total}日 ===")
    start_time = time.time()

    for i, date_str in enumerate(missing_dates):
        elapsed = time.time() - start_time
        pct = 100.0 * (i + 1) / total
        if i > 0:
            eta = elapsed / i * (total - i)
            eta_str = f"残り{eta / 60:.1f}分"
        else:
            eta_str = "計算中"

        if dry_run:
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... DRY RUN"
            )
            continue

        # レースID取得
        race_ids = race_list_scraper.get_race_ids(date_str)
        if not race_ids:
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... レースなし"
            )
            continue

        day_races = []
        day_horses = 0

        for rid in race_ids:
            vc = get_venue_code_from_race_id(rid)
            is_jra = vc in JRA_CODES
            if not is_kb_supported_venue(vc, is_jra):
                continue

            try:
                training_map = scraper.fetch(rid, race_date=date_str)
            except Exception as e:
                logger.warning("取得失敗 %s: %s", rid, e)
                continue

            if not training_map:
                continue

            training_dict = {}
            horse_count = 0
            for hname, records in training_map.items():
                training_dict[hname] = [_training_record_to_dict(r) for r in records]
                horse_count += 1

            day_races.append(
                {
                    "race_id": rid,
                    "venue": get_venue_name(vc),
                    "venue_code": vc,
                    "is_jra": is_jra,
                    "horse_count": horse_count,
                    "training": training_dict,
                }
            )
            day_horses += horse_count

        if day_races:
            # JSON保存
            os.makedirs(TRAINING_ML_DIR, exist_ok=True)
            path = _date_output_path(date_str)
            data = {"date": date_str, "race_count": len(day_races), "races": day_races}
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            total_races += len(day_races)
            total_horses += day_horses
            venues = sorted(set(r["venue"] or "不明" for r in day_races))
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... "
                f"{len(day_races)}R/{day_horses}頭  [{', '.join(venues)}]"
            )
        else:
            skipped += 1
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... 調教データなし"
            )

    logger.info(
        f"未収集日完了: {total_races}R / {total_horses}頭 / スキップ{skipped}日"
    )


def backfill_monbetsu(
    kb_client: KeibabookClient,
    backfill_items: list,
    dry_run: bool = False,
):
    """既存JSONに門別データを追加"""
    if not backfill_items:
        logger.info("門別バックフィル対象なし")
        return

    scraper = KeibabookTrainingScraper(kb_client)
    total = len(backfill_items)
    added_races = 0
    added_horses = 0

    logger.info(f"=== 門別バックフィル: {total}日 ===")
    start_time = time.time()

    for i, item in enumerate(backfill_items):
        date_str = item["date"]
        race_ids = item["race_ids"]
        json_path = item["json_path"]

        elapsed = time.time() - start_time
        pct = 100.0 * (i + 1) / total
        if i > 0:
            eta = elapsed / i * (total - i)
            eta_str = f"残り{eta / 60:.1f}分"
        else:
            eta_str = "計算中"

        if dry_run:
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... "
                f"門別{len(race_ids)}R DRY RUN"
            )
            continue

        # 既存JSONを読み込み
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        new_races = []
        day_horses = 0

        for rid in race_ids:
            vc = get_venue_code_from_race_id(rid)
            is_jra = vc in JRA_CODES

            if not is_kb_supported_venue(vc, is_jra):
                continue

            try:
                training_map = scraper.fetch(rid, race_date=date_str)
            except Exception as e:
                logger.warning("門別取得失敗 %s: %s", rid, e)
                continue

            if not training_map:
                continue

            training_dict = {}
            horse_count = 0
            for hname, records in training_map.items():
                training_dict[hname] = [_training_record_to_dict(r) for r in records]
                horse_count += 1

            new_races.append(
                {
                    "race_id": rid,
                    "venue": get_venue_name(vc),
                    "venue_code": vc,
                    "is_jra": is_jra,
                    "horse_count": horse_count,
                    "training": training_dict,
                }
            )
            day_horses += horse_count

        if new_races:
            # 既存JSONに追加して保存
            data["races"].extend(new_races)
            data["race_count"] = len(data["races"])
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            added_races += len(new_races)
            added_horses += day_horses
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... "
                f"+{len(new_races)}R/{day_horses}頭"
            )
        else:
            logger.info(
                f"  [{i + 1}/{total}] {date_str}  ({pct:.1f}%)  {eta_str} ... 門別データなし"
            )

    logger.info(f"門別バックフィル完了: +{added_races}R / +{added_horses}頭")


def main():
    parser = argparse.ArgumentParser(description="未収集の調教データを一括スクレイピング")
    parser.add_argument("--missing-only", action="store_true", help="未収集日のみ")
    parser.add_argument("--backfill-only", action="store_true", help="門別バックフィルのみ")
    parser.add_argument("--dry-run", action="store_true", help="実際にはスクレイピングしない")
    parser.add_argument(
        "--start", default="2024-01-01", help="開始日 (デフォルト: 2024-01-01)"
    )
    parser.add_argument(
        "--end", default="2026-04-01", help="終了日 (デフォルト: 2026-04-01)"
    )
    args = parser.parse_args()

    do_missing = not args.backfill_only
    do_backfill = not args.missing_only

    # 対象日付の確認
    missing_dates = get_missing_dates(args.start, args.end) if do_missing else []
    backfill_items = get_monbetsu_backfill_dates() if do_backfill else []

    logger.info(f"未収集日: {len(missing_dates)}日")
    logger.info(f"門別バックフィル: {len(backfill_items)}日")

    if not missing_dates and not backfill_items:
        logger.info("処理対象なし")
        return

    if args.dry_run:
        logger.info("=== DRY RUN モード ===")
        if missing_dates:
            scrape_missing_days(None, None, missing_dates, dry_run=True)
        if backfill_items:
            backfill_monbetsu(None, backfill_items, dry_run=True)
        return

    # クライアント初期化
    ne_client = NetkeibaClient(request_interval=1.0)
    race_list_scraper = RaceListScraper(ne_client)
    kb_client = KeibabookClient()

    if not kb_client.ensure_login():
        logger.error("競馬ブックへのログインに失敗。中止します。")
        return

    logger.info("競馬ブックログイン成功")

    try:
        if do_missing and missing_dates:
            scrape_missing_days(kb_client, race_list_scraper, missing_dates)

        if do_backfill and backfill_items:
            backfill_monbetsu(kb_client, backfill_items)

    except KeyboardInterrupt:
        logger.warning("Ctrl+C で中断しました。再実行すれば未処理分から再開します。")

    logger.info("全処理完了")


if __name__ == "__main__":
    main()
