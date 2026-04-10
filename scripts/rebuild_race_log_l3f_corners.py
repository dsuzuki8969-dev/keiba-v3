"""
race_logの last_3f_sec と positions_corners を
HTMLキャッシュ（result.html）から一括再構築する。

問題1: last_3f_sec=0 が 57,593件（2026年19.3%）→ 上がり3F順位が取得不能
問題2: positions_cornersが1要素のみ 66,158件 → 通過順が正確でない

解決: result.htmlの結果テーブルから last_3f_sec、
      Corner_Numテーブルから positions_corners を再取得

Usage:
    python scripts/rebuild_race_log_l3f_corners.py           # 全件
    python scripts/rebuild_race_log_l3f_corners.py --dry-run  # 確認のみ
    python scripts/rebuild_race_log_l3f_corners.py --l3f-only  # last_3fのみ
    python scripts/rebuild_race_log_l3f_corners.py --corners-only  # cornersのみ
"""

import sqlite3
import os
import sys
import io
import re
import json
import time
import argparse
from collections import defaultdict

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH


def parse_corner_table(html: str) -> dict:
    """
    result.htmlのCorner_Numテーブルをパースする。
    Returns: {corner_index: {horse_no: position}}
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("table.Corner_Num")
    if not table:
        return {}

    corner_orders = {}
    for tr in table.select("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        m = re.search(r"(\d)", cells[0].get_text(strip=True))
        if not m:
            continue
        ci = int(m.group(1))

        raw = cells[1].get_text()
        raw = raw.split("=")[0]
        raw = raw.replace("（", "(").replace("）", ")")

        horse_pos = {}
        pos = 1
        i = 0
        while i < len(raw):
            ch = raw[i]
            if ch == '(':
                end = raw.find(')', i)
                if end < 0:
                    end = len(raw)
                group_text = raw[i+1:end]
                group_nos = [int(x.strip()) for x in re.split(r'[,\-]', group_text) if x.strip().isdigit()]
                for hno in group_nos:
                    horse_pos[hno] = pos
                pos += len(group_nos)
                i = end + 1
            elif ch.isdigit():
                j = i
                while j < len(raw) and raw[j].isdigit():
                    j += 1
                hno = int(raw[i:j])
                horse_pos[hno] = pos
                pos += 1
                i = j
            else:
                i += 1

        corner_orders[ci] = horse_pos

    return corner_orders


def get_position_for_horse(corner_orders: dict, horse_no: int) -> tuple:
    """
    corner_ordersから指定馬番の各コーナー通過順位と4角位置を返す。
    Returns: (positions_list, position_4c)
    """
    positions = []
    for ci in sorted(corner_orders.keys()):
        pos_map = corner_orders[ci]
        if horse_no in pos_map:
            positions.append(pos_map[horse_no])
    position_4c = positions[-1] if positions else 0
    return positions, position_4c


def parse_result_table_l3f(html: str) -> dict:
    """
    result.htmlの結果テーブルから各馬のlast_3f_secを抽出する。
    Returns: {horse_no: last_3f_sec}
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 結果テーブル取得（JRA: race_table_01, NAR: RaceTable01）
    table = soup.select_one("table.race_table_01")
    if not table:
        table = soup.select_one("table.RaceTable01")
    if not table:
        return {}

    result = {}
    rows = table.select("tbody tr")

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        # 着順
        finish_text = cells[0].get_text(strip=True)
        if not finish_text.isdigit():
            continue

        # 馬番（JRA: cells[2], NAR: cells[2]）
        horse_no_text = cells[2].get_text(strip=True)
        if not horse_no_text.isdigit():
            continue
        horse_no = int(horse_no_text)

        # 上がり3F: XX.X 形式をセルから探す
        # JRA: 通常 cells[10]前後、NAR: cells[11]前後
        # 安全にスキャン
        l3f = 0.0
        for ci in range(7, min(len(cells), 14)):
            t = cells[ci].get_text(strip=True)
            if re.match(r"^\d{2}\.\d$", t):
                val = float(t)
                if 28.0 <= val <= 50.0:
                    l3f = val
                    break

        if l3f > 0:
            result[horse_no] = l3f

    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="確認のみ、DB更新なし")
    parser.add_argument("--l3f-only", action="store_true", help="last_3f_secのみ更新")
    parser.add_argument("--corners-only", action="store_true", help="positions_cornersのみ更新")
    parser.add_argument("--limit", type=int, default=0, help="処理レース数上限（0=無制限）")
    parser.add_argument("--year", type=str, default="", help="対象年度（例: 2026）")
    args = parser.parse_args()

    do_l3f = not args.corners_only
    do_corners = not args.l3f_only

    import lz4.frame

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    cache_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "cache")

    # 対象レース取得
    where_clause = ""
    if args.year:
        where_clause = f" AND race_id LIKE '{args.year}%'"

    race_ids = conn.execute(
        f"SELECT DISTINCT race_id FROM race_log WHERE race_id IS NOT NULL AND race_id != ''{where_clause} ORDER BY race_id"
    ).fetchall()
    total_races = len(race_ids)
    print(f"対象レース数: {total_races:,}")
    print(f"モード: last_3f={'ON' if do_l3f else 'OFF'}, corners={'ON' if do_corners else 'OFF'}")

    l3f_updated = 0
    corners_updated = 0
    no_cache = 0
    processed = 0
    errors = 0
    t0 = time.time()

    for idx, row in enumerate(race_ids):
        race_id = row["race_id"]

        if args.limit and idx >= args.limit:
            break

        # 進捗表示
        if (idx + 1) % 2000 == 0 or idx == 0:
            elapsed = time.time() - t0
            rate = (idx + 1) / elapsed if elapsed > 0 else 0
            remaining = (total_races - idx - 1) / rate if rate > 0 else 0
            print(f"  ({idx+1:,}/{total_races:,}) {idx/total_races*100:.1f}% "
                  f"l3f更新={l3f_updated:,} corners更新={corners_updated:,} "
                  f"nocache={no_cache:,} "
                  f"経過{elapsed:.0f}秒 残り{remaining:.0f}秒")

        # HTMLキャッシュ読み込み
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
                    except Exception:
                        pass
            if html:
                break

        if not html:
            no_cache += 1
            continue

        processed += 1

        try:
            # last_3f_sec バックフィル
            l3f_map = {}
            if do_l3f:
                l3f_map = parse_result_table_l3f(html)

            # corners バックフィル
            corner_orders = {}
            if do_corners:
                corner_orders = parse_corner_table(html)

            if not l3f_map and not corner_orders:
                continue

            # このレースの全馬を取得
            horses = conn.execute(
                "SELECT horse_no, last_3f_sec, positions_corners, position_4c "
                "FROM race_log WHERE race_id = ?",
                (race_id,)
            ).fetchall()

            for h in horses:
                horse_no = h["horse_no"]
                if not horse_no:
                    continue

                updates = {}

                # last_3f_sec 更新（0の場合のみ）
                if do_l3f and horse_no in l3f_map:
                    old_l3f = h["last_3f_sec"] or 0.0
                    if old_l3f <= 0:
                        updates["last_3f_sec"] = l3f_map[horse_no]
                        l3f_updated += 1

                # corners 更新（1要素以下、または空の場合）
                if do_corners and corner_orders:
                    positions, pos_4c = get_position_for_horse(corner_orders, horse_no)
                    if positions and any(p > 0 for p in positions):
                        old_corners_raw = h["positions_corners"] or ""
                        try:
                            old_corners = json.loads(old_corners_raw) if old_corners_raw.startswith("[") else []
                        except Exception:
                            old_corners = []

                        # 新しいデータの方が多いか、既存が1要素以下なら更新
                        if len(positions) > len(old_corners) or len(old_corners) <= 1:
                            updates["positions_corners"] = json.dumps(positions)
                            updates["position_4c"] = pos_4c
                            corners_updated += 1

                if updates and not args.dry_run:
                    set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
                    conn.execute(
                        f"UPDATE race_log SET {set_clause} WHERE race_id = ? AND horse_no = ?",
                        (*updates.values(), race_id, horse_no)
                    )

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  エラー: race_id={race_id}: {e}")

        # 定期コミット
        if not args.dry_run and processed % 5000 == 0:
            conn.commit()

    if not args.dry_run:
        conn.commit()

    conn.close()

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.0f}秒")
    print(f"  処理レース: {processed:,}")
    print(f"  last_3f_sec 更新: {l3f_updated:,}")
    print(f"  corners 更新: {corners_updated:,}")
    print(f"  キャッシュなし: {no_cache:,}")
    print(f"  エラー: {errors}")
    if args.dry_run:
        print("  ※ dry-runモード: DB更新なし")


if __name__ == "__main__":
    main()
