"""
data/ml/*.jsonの通過順(positions_corners)をrace_log（修正済み）から再投入する。

race_logはHTMLキャッシュのCorner_Numテーブルから正しく再構築済み。
MLデータのpositions_cornersは不正確（スクレイピングタイミング問題）なため、
race_logの値で上書きする。
"""

import sqlite3
import os
import sys
import io
import json
import time
import glob

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import DATABASE_PATH

ML_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ml")


def main():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    # race_logの通過順をメモリに読み込み（高速化）
    print("race_logの通過順をメモリに読み込み中...")
    t0 = time.time()
    cursor = conn.execute(
        "SELECT race_id, horse_no, positions_corners, position_4c FROM race_log "
        "WHERE positions_corners IS NOT NULL AND positions_corners != ''"
    )
    # キー: (race_id, horse_no) → (positions_corners_list, position_4c)
    rl_map = {}
    for row in cursor:
        rid = row["race_id"]
        hno = row["horse_no"]
        corners_raw = row["positions_corners"]
        try:
            corners = json.loads(corners_raw) if isinstance(corners_raw, str) else corners_raw
        except Exception:
            continue
        if isinstance(corners, list) and corners:
            rl_map[(rid, hno)] = (corners, row["position_4c"])
    conn.close()
    print(f"  {len(rl_map):,}件読み込み完了 ({time.time()-t0:.1f}秒)")

    # data/ml/*.jsonを処理
    json_files = sorted(glob.glob(os.path.join(ML_DIR, "20*.json")))
    print(f"対象MLファイル: {len(json_files)}件")

    total_fixed = 0
    total_horses = 0
    files_modified = 0
    t0 = time.time()

    for fi, fpath in enumerate(json_files):
        if (fi + 1) % 100 == 0:
            elapsed = time.time() - t0
            remaining = elapsed / (fi + 1) * (len(json_files) - fi - 1)
            print(f"  ({fi+1}/{len(json_files)}) {fi/len(json_files)*100:.1f}% "
                  f"fixed={total_fixed:,} 経過{elapsed:.0f}秒 残り{remaining:.0f}秒")

        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        modified = False
        for race in data.get("races", []):
            rid = race.get("race_id", "")
            for h in race.get("horses", []):
                total_horses += 1
                hno = h.get("horse_no")
                if not hno or not rid:
                    continue
                key = (rid, hno)
                if key not in rl_map:
                    continue

                new_corners, new_4c = rl_map[key]
                old_corners = h.get("positions_corners", [])

                if new_corners != old_corners:
                    h["positions_corners"] = new_corners
                    h["position_4c"] = new_4c
                    total_fixed += 1
                    modified = True

        if modified:
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            files_modified += 1

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.0f}秒")
    print(f"  処理馬数: {total_horses:,}")
    print(f"  修正した通過順: {total_fixed:,}")
    print(f"  変更ファイル数: {files_modified}")


if __name__ == "__main__":
    main()
