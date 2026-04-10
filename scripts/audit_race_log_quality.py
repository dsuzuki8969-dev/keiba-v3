#!/usr/bin/env python3
"""race_logテーブルのデータ品質監査スクリプト"""

import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "keiba.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print("=" * 70)
    print("■ race_log データ品質監査")
    print("=" * 70)

    # 総レコード数
    total = cur.execute("SELECT COUNT(*) FROM race_log WHERE finish_pos < 90").fetchone()[0]
    total_all = cur.execute("SELECT COUNT(*) FROM race_log").fetchone()[0]
    print(f"\n  総レコード数: {total_all:,} (有効着順: {total:,})")

    # 通過順
    print(f"\n{'─' * 50}")
    print("■ positions_corners（通過順）品質")
    print(f"{'─' * 50}")
    empty_corners = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND (positions_corners IS NULL OR positions_corners = '')"
    ).fetchone()[0]
    has_zero = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND positions_corners LIKE '%,0%' OR positions_corners LIKE '%0,%' OR positions_corners = '[0]'"
    ).fetchone()[0]
    valid_json = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND positions_corners LIKE '[%'"
    ).fetchone()[0]
    single_val = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND positions_corners != '' AND positions_corners NOT LIKE '%,%'"
    ).fetchone()[0]

    print(f"  空/NULL:       {empty_corners:>8,} ({empty_corners/total*100:.1f}%)")
    print(f"  有効JSON:      {valid_json:>8,} ({valid_json/total*100:.1f}%)")
    print(f"  0含み:         {has_zero:>8,} ({has_zero/total*100:.1f}%)")
    print(f"  単一値:        {single_val:>8,} ({single_val/total*100:.1f}%)")
    print(f"  正常:          {total - empty_corners - has_zero:>8,} ({(total-empty_corners-has_zero)/total*100:.1f}%)")

    # position_4c
    print(f"\n{'─' * 50}")
    print("■ position_4c 品質")
    print(f"{'─' * 50}")
    p4c_zero = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND (position_4c = 0 OR position_4c IS NULL)"
    ).fetchone()[0]
    p4c_valid = total - p4c_zero
    print(f"  有効:          {p4c_valid:>8,} ({p4c_valid/total*100:.1f}%)")
    print(f"  0/NULL:        {p4c_zero:>8,} ({p4c_zero/total*100:.1f}%)")

    # 着差
    print(f"\n{'─' * 50}")
    print("■ margin_ahead / margin_behind（着差）品質")
    print(f"{'─' * 50}")
    # 2着以降でmargin_ahead=0はデータ不良（1着は正常に0）
    bad_margin_ahead = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos > 1 AND finish_pos < 90 AND (margin_ahead = 0 OR margin_ahead IS NULL)"
    ).fetchone()[0]
    total_non_first = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos > 1 AND finish_pos < 90"
    ).fetchone()[0]
    good_margin_ahead = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos > 1 AND finish_pos < 90 AND margin_ahead > 0"
    ).fetchone()[0]

    bad_margin_behind = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND (margin_behind = 0 OR margin_behind IS NULL)"
    ).fetchone()[0]
    good_margin_behind = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND margin_behind > 0"
    ).fetchone()[0]

    print(f"  margin_ahead (2着以降):")
    print(f"    有効(>0):    {good_margin_ahead:>8,} ({good_margin_ahead/max(1,total_non_first)*100:.1f}%)")
    print(f"    0/NULL:      {bad_margin_ahead:>8,} ({bad_margin_ahead/max(1,total_non_first)*100:.1f}%)")
    print(f"  margin_behind:")
    print(f"    有効(>0):    {good_margin_behind:>8,} ({good_margin_behind/total*100:.1f}%)")
    print(f"    0/NULL:      {bad_margin_behind:>8,} ({bad_margin_behind/total*100:.1f}%)")

    # finish_time_sec
    print(f"\n{'─' * 50}")
    print("■ finish_time_sec（走破タイム）品質")
    print(f"{'─' * 50}")
    ft_valid = cur.execute(
        "SELECT COUNT(*) FROM race_log WHERE finish_pos < 90 AND finish_time_sec > 0"
    ).fetchone()[0]
    ft_zero = total - ft_valid
    print(f"  有効(>0):      {ft_valid:>8,} ({ft_valid/total*100:.1f}%)")
    print(f"  0/NULL:        {ft_zero:>8,} ({ft_zero/total*100:.1f}%)")

    # レースごとの統計
    print(f"\n{'─' * 50}")
    print("■ レース単位の統計")
    print(f"{'─' * 50}")
    total_races = cur.execute("SELECT COUNT(DISTINCT race_id) FROM race_log").fetchone()[0]
    races_with_ft = cur.execute(
        "SELECT COUNT(DISTINCT race_id) FROM race_log WHERE finish_time_sec > 0"
    ).fetchone()[0]
    races_with_corners = cur.execute(
        "SELECT COUNT(DISTINCT race_id) FROM race_log WHERE positions_corners LIKE '[%' AND LENGTH(positions_corners) > 3"
    ).fetchone()[0]
    races_with_margin = cur.execute(
        "SELECT COUNT(DISTINCT race_id) FROM race_log WHERE margin_ahead > 0"
    ).fetchone()[0]
    print(f"  総レース数:    {total_races:>8,}")
    print(f"  タイムあり:    {races_with_ft:>8,} ({races_with_ft/total_races*100:.1f}%)")
    print(f"  通過順あり:    {races_with_corners:>8,} ({races_with_corners/total_races*100:.1f}%)")
    print(f"  着差あり:      {races_with_margin:>8,} ({races_with_margin/total_races*100:.1f}%)")

    conn.close()
    print(f"\n{'=' * 70}")
    print("監査完了")


if __name__ == "__main__":
    main()
