"""
race_logのmargin_ahead / margin_behind / finish_time_sec を
HTMLキャッシュ（result.html）から全件再構築する。

各馬のfinish_time_secを抽出し、
  margin_ahead  = その馬のタイム - 1着のタイム（秒差）
  margin_behind = 次の着順のタイム - その馬のタイム（秒差。最下位は0.0）
を計算してrace_logに一括投入する。

Usage:
    python scripts/rebuild_race_log_margins.py           # 全件
    python scripts/rebuild_race_log_margins.py --dry-run  # 確認のみ
    python scripts/rebuild_race_log_margins.py --limit 100 # 上限指定
"""

import sqlite3
import os
import sys
import io
import re
import argparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH
from src.log import get_logger

logger = get_logger(__name__)


def _parse_finish_time(s: str) -> float:
    """'1:08.8' → 68.8秒"""
    try:
        if ":" in s:
            p = s.split(":")
            return int(p[0]) * 60 + float(p[1])
        return float(s)
    except Exception:
        return 0.0


def parse_finish_times(html: str) -> dict:
    """
    result.htmlから各馬番のfinish_time_sec（走破タイム秒）を抽出する。
    Returns: {horse_no: finish_time_sec}
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # レース結果テーブルを取得
    table = soup.select_one("table.RaceTable01")
    if not table:
        return {}

    results = {}
    rows = table.select("tr")
    for row in rows:
        cells = row.select("td")
        if len(cells) < 4:
            continue

        # 着順（1列目）
        pos_text = cells[0].get_text(strip=True)
        if not pos_text.isdigit():
            continue
        finish_pos = int(pos_text)

        # 馬番（3列目）
        hno_text = cells[2].get_text(strip=True)
        if not hno_text.isdigit():
            continue
        horse_no = int(hno_text)

        # 走破タイム: MM:SS.S 形式のセルを探す
        finish_time_sec = 0.0
        for c in cells:
            t = c.get_text(strip=True)
            if re.match(r"\d+:\d{2}\.\d", t):
                finish_time_sec = _parse_finish_time(t)
                break

        if finish_time_sec > 0:
            results[horse_no] = {
                "finish_pos": finish_pos,
                "finish_time_sec": finish_time_sec,
            }

    return results


def calc_margins(time_data: dict) -> dict:
    """
    各馬のmargin_ahead / margin_behind を計算する。
    time_data: {horse_no: {"finish_pos": int, "finish_time_sec": float}}
    Returns: {horse_no: {"margin_ahead": float, "margin_behind": float, "finish_time_sec": float}}
    """
    if not time_data:
        return {}

    # finish_pos順にソート
    sorted_horses = sorted(time_data.items(), key=lambda x: (x[1]["finish_pos"], x[1]["finish_time_sec"]))

    # 1着タイム
    winner_time = min(
        (v["finish_time_sec"] for v in time_data.values() if v["finish_pos"] == 1),
        default=sorted_horses[0][1]["finish_time_sec"],
    )

    results = {}
    for idx, (horse_no, data) in enumerate(sorted_horses):
        margin_ahead = data["finish_time_sec"] - winner_time
        margin_behind = 0.0
        if idx + 1 < len(sorted_horses):
            next_t = sorted_horses[idx + 1][1]["finish_time_sec"]
            if next_t > data["finish_time_sec"]:
                margin_behind = next_t - data["finish_time_sec"]

        results[horse_no] = {
            "margin_ahead": round(margin_ahead, 1),
            "margin_behind": round(margin_behind, 1),
            "finish_time_sec": round(data["finish_time_sec"], 1),
        }

    return results


def main():
    parser = argparse.ArgumentParser(description="race_logのmargin_ahead/margin_behind/finish_time_secを再構築")
    parser.add_argument("--dry-run", action="store_true", help="確認のみ、DB更新なし")
    parser.add_argument("--limit", type=int, default=0, help="処理レース数上限（0=無制限）")
    args = parser.parse_args()

    import lz4.frame
    from tqdm import tqdm

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")

    # 全race_idを取得
    race_ids = conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE race_id IS NOT NULL AND race_id != '' ORDER BY race_id"
    ).fetchall()
    total_races = len(race_ids)
    logger.info(f"対象レース数: {total_races}")

    # 既にmargin_ahead > 0 のレースを取得（スキップ対象）
    already_done = set()
    rows = conn.execute(
        "SELECT DISTINCT race_id FROM race_log WHERE margin_ahead > 0"
    ).fetchall()
    for r in rows:
        already_done.add(r["race_id"])
    logger.info(f"処理済みレース数（スキップ）: {len(already_done)}")

    updated = 0
    skipped_done = 0
    no_cache = 0
    no_time = 0
    errors = 0
    horses_updated = 0
    db_time_used = 0
    batch_count = 0

    race_list = race_ids[:args.limit] if args.limit else race_ids

    for row in tqdm(race_list, desc="margin再構築", unit="race"):
        race_id = row["race_id"]

        # 既にmargin計算済みならスキップ
        if race_id in already_done:
            skipped_done += 1
            continue

        # まずDB上のfinish_time_secを確認
        horses = conn.execute(
            "SELECT horse_no, finish_pos, finish_time_sec, margin_ahead FROM race_log WHERE race_id = ?",
            (race_id,)
        ).fetchall()

        # DB上にfinish_time_secがあるか確認
        db_times = {}
        has_db_times = False
        for h in horses:
            if h["finish_time_sec"] and h["finish_time_sec"] > 0 and h["finish_pos"] and h["finish_pos"] > 0:
                db_times[h["horse_no"]] = {
                    "finish_pos": h["finish_pos"],
                    "finish_time_sec": h["finish_time_sec"],
                }
                has_db_times = True

        time_data = None

        # DB上にfinish_time_secがあればそれを使う
        if has_db_times and len(db_times) >= 2:
            time_data = db_times
            db_time_used += 1
        else:
            # HTMLキャッシュから取得
            html = None
            for prefix in ["nar.netkeiba.com_race_result.html_race_id=",
                           "race.netkeiba.com_race_result.html_race_id="]:
                key = f"{prefix}{race_id}"
                for ext in [".html.lz4", ".html"]:
                    path = os.path.join(cache_dir, f"{key}{ext}")
                    if os.path.exists(path):
                        try:
                            if ext == ".html.lz4":
                                with open(path, "rb") as f:
                                    html = lz4.frame.decompress(f.read()).decode("utf-8", errors="replace")
                            else:
                                with open(path, "r", encoding="utf-8", errors="replace") as f:
                                    html = f.read()
                            break
                        except Exception as e:
                            logger.debug(f"キャッシュ読み込みエラー: {race_id} - {e}")
                if html:
                    break

            if not html:
                no_cache += 1
                continue

            # HTMLからfinish_timeをパース
            try:
                time_data = parse_finish_times(html)
            except Exception as e:
                logger.debug(f"パースエラー: {race_id} - {e}")
                errors += 1
                continue

        if not time_data or len(time_data) < 2:
            no_time += 1
            continue

        # margin計算
        try:
            margins = calc_margins(time_data)
        except Exception as e:
            logger.debug(f"margin計算エラー: {race_id} - {e}")
            errors += 1
            continue

        if not margins:
            no_time += 1
            continue

        # DB更新
        if not args.dry_run:
            for horse_no, m in margins.items():
                conn.execute(
                    "UPDATE race_log SET margin_ahead = ?, margin_behind = ?, finish_time_sec = ? "
                    "WHERE race_id = ? AND horse_no = ?",
                    (m["margin_ahead"], m["margin_behind"], m["finish_time_sec"], race_id, horse_no)
                )
                horses_updated += 1

            batch_count += 1
            # 1000レースごとにコミット
            if batch_count % 1000 == 0:
                conn.commit()
                logger.info(f"バッチコミット: {batch_count}レース処理済み")

        updated += 1

    # 最終コミット
    if not args.dry_run:
        conn.commit()

    conn.close()

    # 統計出力
    print(f"\n===== 完了 =====")
    print(f"  処理レース: {updated}")
    print(f"  更新した馬数: {horses_updated}")
    print(f"  DB値利用: {db_time_used}")
    print(f"  処理済みスキップ: {skipped_done}")
    print(f"  キャッシュなし: {no_cache}")
    print(f"  タイムなし: {no_time}")
    print(f"  エラー: {errors}")
    if args.dry_run:
        print("  ※ dry-runモード: DB更新なし")


if __name__ == "__main__":
    main()
