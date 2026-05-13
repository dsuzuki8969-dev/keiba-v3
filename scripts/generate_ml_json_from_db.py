"""race_log DB → data/ml/*.json 変換スクリプト

Walk-Forward バックテスト用に、2022-2023年のML学習用JSONを
既存 race_log テーブルから生成する。
netkeiba スクレイピング不要。
"""

import argparse
import json
import os
import sqlite3
import sys
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

DB_PATH = os.path.join(PROJECT_ROOT, "data", "keiba.db")
ML_DIR = os.path.join(PROJECT_ROOT, "data", "ml")

VENUE_CODE_TO_NAME = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "49": "園田", "50": "園田", "51": "姫路",
    "52": "帯広", "65": "帯広",
    "54": "高知", "55": "佐賀",
}

JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}


def parse_positions_corners(text: str) -> list:
    """positions_corners テキスト → リスト変換"""
    if not text:
        return []
    try:
        parts = text.replace("-", ",").split(",")
        return [int(p.strip()) for p in parts if p.strip().isdigit()]
    except Exception:
        return []


def build_ml_json_for_date(conn: sqlite3.Connection, race_date: str) -> dict:
    """指定日の race_log → ML JSON 形式に変換"""
    c = conn.cursor()
    c.execute(
        """
        SELECT race_id, race_name, venue_code, surface, distance, direction,
               condition, weather, field_count, grade, is_jra,
               race_first_3f, race_pace,
               finish_pos, status, gate_no, horse_no, horse_id, horse_name,
               sex, age, weight_kg, jockey_name, jockey_id, trainer_name, trainer_id,
               finish_time_sec, last_3f_sec, first_3f_sec,
               positions_corners, tansho_odds, popularity, horse_weight, weight_change
        FROM race_log
        WHERE race_date = ?
        ORDER BY race_id, horse_no
        """,
        (race_date,),
    )
    rows = c.fetchall()
    if not rows:
        return None

    cols = [desc[0] for desc in c.description]
    races_dict = defaultdict(list)
    for row in rows:
        d = dict(zip(cols, row))
        races_dict[d["race_id"]].append(d)

    races = []
    for race_id, entries in races_dict.items():
        first = entries[0]
        vc = str(first["venue_code"] or "").zfill(2)
        venue_name = VENUE_CODE_TO_NAME.get(vc, vc)

        horses = []
        for e in entries:
            pc_text = e["positions_corners"] or ""
            horses.append({
                "finish_pos": e["finish_pos"] or 0,
                "status": e["status"] or "",
                "gate_no": e["gate_no"] or 0,
                "horse_no": e["horse_no"] or 0,
                "horse_id": e["horse_id"] or "",
                "horse_name": e["horse_name"] or "",
                "sex": e["sex"] or "",
                "age": e["age"] or 0,
                "weight_kg": e["weight_kg"] or 0.0,
                "jockey": e["jockey_name"] or "",
                "jockey_id": e["jockey_id"] or "",
                "trainer": e["trainer_name"] or "",
                "trainer_id": e["trainer_id"] or "",
                "finish_time_sec": e["finish_time_sec"] or 0.0,
                "margin": "",
                "last_3f_sec": e["last_3f_sec"] or 0.0,
                "first_3f_sec": e["first_3f_sec"] or 0.0,
                "positions_corners": parse_positions_corners(pc_text),
                "odds": e["tansho_odds"] or 0.0,
                "popularity": e["popularity"] or 0,
                "horse_weight": e["horse_weight"] or 0,
                "weight_change": e["weight_change"] or 0,
            })

        race = {
            "race_id": race_id,
            "race_name": first["race_name"] or "",
            "date": race_date,
            "venue": venue_name,
            "venue_code": vc,
            "surface": first["surface"] or "",
            "distance": first["distance"] or 0,
            "direction": first["direction"] or "",
            "condition": first["condition"] or "",
            "weather": first["weather"] or "",
            "field_count": first["field_count"] or len(entries),
            "grade": first["grade"] or "",
            "is_jra": bool(first["is_jra"]),
            "first_3f": first["race_first_3f"] or 0.0,
            "pace": first["race_pace"] or "",
            "horses": horses,
            "payouts": {},
        }
        races.append(race)

    return {
        "date": race_date,
        "race_count": len(races),
        "races": races,
    }


def main():
    parser = argparse.ArgumentParser(description="race_log DB → ML JSON 変換")
    parser.add_argument("--start", default="2022-01-01", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2023-12-31", help="終了日 (YYYY-MM-DD)")
    parser.add_argument("--overwrite", action="store_true", help="既存ファイルを上書き")
    parser.add_argument("--dry-run", action="store_true", help="書き込みせず件数のみ表示")
    args = parser.parse_args()

    os.makedirs(ML_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    c = conn.cursor()
    c.execute(
        "SELECT DISTINCT race_date FROM race_log WHERE race_date BETWEEN ? AND ? ORDER BY race_date",
        (args.start, args.end),
    )
    dates = [r[0] for r in c.fetchall()]
    print(f"対象日数: {len(dates)} ({args.start} ～ {args.end})")

    if args.dry_run:
        print("(dry-run: 書き込みなし)")
        conn.close()
        return

    created = 0
    skipped = 0
    for i, dt in enumerate(dates):
        fname = dt.replace("-", "") + ".json"
        fpath = os.path.join(ML_DIR, fname)

        if os.path.exists(fpath) and not args.overwrite:
            skipped += 1
            continue

        data = build_ml_json_for_date(conn, dt)
        if data is None:
            continue

        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

        created += 1
        if (i + 1) % 50 == 0 or i == len(dates) - 1:
            pct = (i + 1) / len(dates) * 100
            print(f"  [{i+1}/{len(dates)}] {pct:.0f}% - {dt} ({data['race_count']} races)")

    conn.close()
    print(f"\n完了: {created} ファイル生成, {skipped} スキップ")


if __name__ == "__main__":
    main()
