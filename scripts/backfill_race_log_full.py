#!/usr/bin/env python3
"""
race_logテーブル完全バックフィル: data/ml/*.json → race_log全49カラム

MLデータ（1,265ファイル / 701,689頭分）からrace_logの新カラムを埋める。
既存行はUPDATE、未存在行はINSERT。
"""

import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db, init_schema

ML_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "ml")


def backfill_race_log():
    """MLデータからrace_logの全カラムをバックフィル"""
    init_schema()
    conn = get_db()

    ml_files = sorted([
        f for f in os.listdir(ML_DATA_DIR)
        if f.endswith(".json") and f[:8].isdigit()
    ])
    print(f"MLデータファイル数: {len(ml_files)}")

    t0 = time.time()
    total_updated = 0
    total_inserted = 0

    for fi, fname in enumerate(ml_files):
        fpath = os.path.join(ML_DATA_DIR, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        races = data.get("races", [])
        batch_updates = []
        batch_inserts = []

        for race in races:
            race_id = race.get("race_id", "")
            race_date = race.get("date", "")
            venue_code = race.get("venue_code", "")
            surface = race.get("surface", "")
            distance = race.get("distance", 0)
            condition = race.get("condition", "")
            weather = race.get("weather", "")
            field_count = race.get("field_count", 0)
            grade = race.get("grade", "")
            is_jra = 1 if race.get("is_jra") else 0
            direction = race.get("direction", "")
            race_name = race.get("race_name", "")
            race_first_3f = race.get("first_3f")
            race_pace = race.get("pace", "")
            course_id = f"{venue_code}_{surface}_{distance}"

            # レース全体のfinish_time_secからmargin_ahead/behindを計算
            horses_list = race.get("horses", [])
            _time_entries = []
            for _h in horses_list:
                _fp = _h.get("finish_pos")
                _ft = _h.get("finish_time_sec", 0)
                _hno = _h.get("horse_no")
                if _hno is not None and _fp is not None and _fp < 90 and _ft and _ft > 0:
                    _time_entries.append((_hno, _fp, _ft))
            _time_entries.sort(key=lambda x: (x[1], x[2]))
            _winner_time = min((e[2] for e in _time_entries if e[1] == 1), default=(_time_entries[0][2] if _time_entries else 0))
            _margin_map = {}  # horse_no -> (margin_ahead, margin_behind)
            for _idx, (_hno, _fp, _ft) in enumerate(_time_entries):
                _ma = round(_ft - _winner_time, 1)
                _mb = 0.0
                if _idx + 1 < len(_time_entries):
                    _next_t = _time_entries[_idx + 1][2]
                    if _next_t > _ft:
                        _mb = round(_next_t - _ft, 1)
                _margin_map[_hno] = (_ma, _mb)

            for h in horses_list:
                horse_no = h.get("horse_no")
                if horse_no is None:
                    continue

                finish_pos = h.get("finish_pos")
                if finish_pos is None:
                    finish_pos = 99

                corners = h.get("positions_corners", [])
                corners_json = json.dumps(corners) if corners else ""
                position_4c = corners[-1] if corners else 0

                # margin_ahead/behind計算済みの値を取得
                _margins = _margin_map.get(horse_no, (0, 0))

                # UPDATE用パラメータ
                update_params = (
                    h.get("horse_id", ""),
                    h.get("horse_name", ""),
                    h.get("gate_no", 0),
                    h.get("sex", ""),
                    h.get("age", 0),
                    h.get("weight_kg", 0),
                    h.get("odds"),
                    h.get("odds"),  # tansho_odds
                    h.get("popularity"),
                    h.get("horse_weight"),
                    h.get("weight_change"),
                    position_4c,
                    corners_json,
                    h.get("finish_time_sec", 0),
                    h.get("last_3f_sec", 0),
                    None,  # first_3f_sec
                    _margins[0], _margins[1],  # margin_ahead, margin_behind
                    None if finish_pos < 90 else "取消",
                    course_id, grade, race_name, weather, direction,
                    race_first_3f, race_pace,
                    None, 0, None, "ml_backfill",
                    # WHERE
                    race_id, horse_no,
                )
                batch_updates.append(update_params)

                # INSERT用パラメータ
                insert_params = (
                    race_date, race_id, venue_code, surface, distance,
                    horse_no, finish_pos,
                    h.get("jockey_id", ""), h.get("jockey", ""),
                    h.get("trainer_id", ""), h.get("trainer", ""),
                    field_count, is_jra, h.get("odds"),
                    "", "", condition,
                    h.get("horse_id", ""), h.get("horse_name", ""),
                    h.get("gate_no", 0), h.get("sex", ""), h.get("age", 0),
                    h.get("weight_kg", 0),
                    h.get("odds"), h.get("odds"),
                    h.get("popularity"), h.get("horse_weight"), h.get("weight_change"),
                    position_4c, corners_json,
                    h.get("finish_time_sec", 0), h.get("last_3f_sec", 0), None,
                    0, 0, None if finish_pos < 90 else "取消",
                    course_id, grade, race_name, weather, direction,
                    race_first_3f, race_pace,
                    None, 0, None, "ml_backfill",
                )
                batch_inserts.append(insert_params)

        # バッチ実行
        cursor = conn.cursor()
        cursor.executemany(
            """UPDATE race_log SET
                horse_id=?, horse_name=?, gate_no=?, sex=?, age=?,
                weight_kg=?, odds=?, tansho_odds=?,
                popularity=?, horse_weight=?, weight_change=?,
                position_4c=?, positions_corners=?,
                finish_time_sec=?, last_3f_sec=?, first_3f_sec=?,
                margin_ahead=?, margin_behind=?, status=?,
                course_id=?, grade=?, race_name=?, weather=?,
                direction=?, race_first_3f=?, race_pace=?,
                pace=?, is_generation=?, race_level_dev=?, source=?
            WHERE race_id=? AND horse_no=?""",
            batch_updates,
        )
        total_updated += cursor.rowcount

        cursor.executemany(
            """INSERT OR IGNORE INTO race_log
                (race_date, race_id, venue_code, surface, distance,
                 horse_no, finish_pos,
                 jockey_id, jockey_name, trainer_id, trainer_name,
                 field_count, is_jra, win_odds, sire_name, bms_name, condition,
                 horse_id, horse_name, gate_no, sex, age, weight_kg,
                 odds, tansho_odds, popularity, horse_weight, weight_change,
                 position_4c, positions_corners,
                 finish_time_sec, last_3f_sec, first_3f_sec,
                 margin_ahead, margin_behind, status,
                 course_id, grade, race_name, weather, direction,
                 race_first_3f, race_pace,
                 pace, is_generation, race_level_dev, source)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            batch_inserts,
        )
        total_inserted += cursor.rowcount
        conn.commit()

        if (fi + 1) % 50 == 0 or fi == len(ml_files) - 1:
            elapsed = time.time() - t0
            pct = (fi + 1) / len(ml_files) * 100
            remaining = elapsed / (fi + 1) * (len(ml_files) - fi - 1) if fi > 0 else 0
            print(f"  [{fi+1}/{len(ml_files)}] {pct:.1f}% updated={total_updated:,} inserted={total_inserted:,}  経過{elapsed:.0f}秒 残り{remaining:.0f}秒")

    elapsed = time.time() - t0
    print(f"\n完了: updated={total_updated:,}, inserted={total_inserted:,}, {elapsed:.1f}秒")

    # 確認
    total = conn.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
    filled = conn.execute("SELECT COUNT(*) FROM race_log WHERE horse_id != ''").fetchone()[0]
    corners = conn.execute("SELECT COUNT(*) FROM race_log WHERE positions_corners != ''").fetchone()[0]
    print(f"race_log: total={total:,}, horse_id有り={filled:,}, corners有り={corners:,}")


def backfill_training():
    """調教JSON → training_recordsテーブルにバックフィル"""
    init_schema()
    conn = get_db()
    training_dir = os.path.join(os.path.dirname(__file__), "..", "data", "training_ml")

    if not os.path.exists(training_dir):
        print("data/training_ml/ が見つかりません")
        return

    tr_files = sorted([f for f in os.listdir(training_dir) if f.endswith(".json") and f[:8].isdigit()])
    print(f"\n調教データファイル数: {len(tr_files)}")

    t0 = time.time()
    total_inserted = 0

    for fi, fname in enumerate(tr_files):
        fpath = os.path.join(training_dir, fname)
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        batch = []
        for race in data.get("races", []):
            race_id = race.get("race_id", "")
            training = race.get("training", {})
            for horse_name, records in training.items():
                for rec in records:
                    splits = rec.get("splits", {})
                    batch.append((
                        race_id, horse_name, "",
                        rec.get("date", ""),
                        rec.get("course", ""),
                        json.dumps(splits) if splits else "{}",
                        rec.get("rider", ""),
                        rec.get("track_condition", ""),
                        rec.get("lap_count", ""),
                        rec.get("intensity_label", ""),
                        rec.get("sigma_from_mean", 0),
                        rec.get("comment", ""),
                        rec.get("stable_comment", ""),
                        "keibabook",
                    ))

        if batch:
            conn.executemany(
                """INSERT OR IGNORE INTO training_records
                    (race_id, horse_name, horse_id, date, course,
                     splits_json, rider, track_condition, lap_count,
                     intensity_label, sigma_from_mean, comment, stable_comment, source)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                batch,
            )
            total_inserted += conn.execute("SELECT changes()").fetchone()[0]
            conn.commit()

        if (fi + 1) % 100 == 0 or fi == len(tr_files) - 1:
            elapsed = time.time() - t0
            print(f"  [{fi+1}/{len(tr_files)}] training_records={total_inserted:,} ({elapsed:.1f}秒)")

    elapsed = time.time() - t0
    total = conn.execute("SELECT COUNT(*) FROM training_records").fetchone()[0]
    print(f"\n調教バックフィル完了: {total:,}件, {elapsed:.1f}秒")


if __name__ == "__main__":
    backfill_race_log()
    backfill_training()
