"""
race_log SQLite から調教師・騎手の偏差値と馬場状態別成績を再構築するスクリプト。

問題:
  - 調教師 deviation 59.3% がデフォルト50.0（course_dbの出走数不足）
  - condition_records が「良」のみ（PastRunのconditionが空文字→良に丸められていた）

解決:
  - race_log（400K行）から直接集計 → course_dbの制限を回避
  - condition abbreviation ('稍'→'稍重', '重'→'重', '不'→'不良') を正規化

使い方:
  python scripts/rebuild_personnel_from_race_log.py
  python scripts/rebuild_personnel_from_race_log.py --dry-run   # 確認のみ
"""

import sys
import os
import json
import sqlite3
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import PERSONNEL_DB_PATH, DATABASE_PATH as DB_PATH
from src.log import get_logger

logger = get_logger(__name__)

# 馬場状態の正規化マップ
COND_MAP = {
    "良": "良",
    "稍": "稍重",
    "稍重": "稍重",
    "重": "重",
    "不": "不良",
    "不良": "不良",
}

VALID_CONDS = {"良", "稍重", "重", "不良"}


def _normalize_cond(c: str) -> str:
    return COND_MAP.get(c.strip(), "")


def collect_race_log_stats(conn: sqlite3.Connection):
    """
    race_log から調教師・騎手の集計データを取得。
    Returns:
        trainer_stats: {tid: {"wins":int,"runs":int,"places":int,"cond":{cond:{wins,runs}}}}
        jockey_stats:  {jid: {"wins":int,"runs":int,"places":int,"cond":{cond:{wins,runs}}}}
    """
    trainer_stats = defaultdict(lambda: {"wins": 0, "runs": 0, "places": 0, "cond": defaultdict(lambda: {"wins": 0, "runs": 0})})
    jockey_stats  = defaultdict(lambda: {"wins": 0, "runs": 0, "places": 0, "cond": defaultdict(lambda: {"wins": 0, "runs": 0})})

    rows = conn.execute(
        "SELECT trainer_id, jockey_id, finish_pos, condition FROM race_log "
        "WHERE trainer_id IS NOT NULL OR jockey_id IS NOT NULL"
    ).fetchall()

    logger.info("race_log 集計対象: %d行", len(rows))

    for trainer_id, jockey_id, finish_pos, condition in rows:
        if finish_pos is None:
            continue
        cond_raw = (condition or "").strip()
        cond = _normalize_cond(cond_raw) if cond_raw else ""

        if trainer_id:
            ts = trainer_stats[trainer_id]
            ts["runs"] += 1
            if finish_pos == 1:
                ts["wins"] += 1
            if finish_pos <= 3:
                ts["places"] += 1
            if cond:
                ts["cond"][cond]["runs"] += 1
                if finish_pos == 1:
                    ts["cond"][cond]["wins"] += 1

        if jockey_id:
            js = jockey_stats[jockey_id]
            js["runs"] += 1
            if finish_pos == 1:
                js["wins"] += 1
            if finish_pos <= 3:
                js["places"] += 1
            if cond:
                js["cond"][cond]["runs"] += 1
                if finish_pos == 1:
                    js["cond"][cond]["wins"] += 1

    return dict(trainer_stats), dict(jockey_stats)


def compute_deviation(stat: dict, all_wrs: list) -> float:
    """
    全体の勝率リストに対するパーセンタイル偏差値（40〜75）を計算。
    出走なし → 50.0
    """
    runs = stat["runs"]
    if runs < 1:
        return 50.0
    wr = stat["wins"] / runs
    n = len(all_wrs)
    if n == 0:
        return 50.0
    rank_below = sum(1 for w in all_wrs if w < wr)
    pct = rank_below / n
    return round(40.0 + pct * 35.0, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="更新せず確認のみ")
    args = parser.parse_args()

    # race_log から集計
    conn = sqlite3.connect(DB_PATH)
    trainer_stats, jockey_stats = collect_race_log_stats(conn)
    conn.close()

    logger.info("集計完了: 調教師=%d, 騎手=%d", len(trainer_stats), len(jockey_stats))

    # 全勝率リストを計算（偏差値の相対評価用）
    trainer_wrs = sorted(v["wins"] / v["runs"] for v in trainer_stats.values() if v["runs"] >= 1)
    jockey_wrs  = sorted(v["wins"] / v["runs"] for v in jockey_stats.values()  if v["runs"] >= 1)

    # personnel_db.json を更新
    with open(PERSONNEL_DB_PATH, encoding="utf-8") as f:
        db = json.load(f)

    # ── 調教師 deviation 更新 ──
    trainer_updated = 0
    trainer_cond_updated = 0
    for tid, tdata in db.get("trainers", {}).items():
        ts = trainer_stats.get(tid)
        if not ts:
            continue

        old_dev = tdata.get("deviation", 50.0)
        new_dev = compute_deviation(ts, trainer_wrs)
        if old_dev == 50.0 and new_dev != 50.0:
            tdata["deviation"] = new_dev
            trainer_updated += 1
        elif old_dev != 50.0:
            # 既存値を更新
            tdata["deviation"] = new_dev

        # condition_records を race_log ベースに更新（2走以上のみ）
        cond_rec = {c: v for c, v in ts["cond"].items() if v["runs"] >= 2}
        if cond_rec:
            old_cond = tdata.get("condition_records", {})
            if old_cond != cond_rec:
                tdata["condition_records"] = cond_rec
                trainer_cond_updated += 1

    # ── 騎手 condition_records 更新 ──
    jockey_cond_updated = 0
    for jid, jdata in db.get("jockeys", {}).items():
        js = jockey_stats.get(jid)
        if not js:
            continue
        cond_rec = {c: v for c, v in js["cond"].items() if v["runs"] >= 2}
        if cond_rec:
            old_cond = jdata.get("condition_records", {})
            if old_cond != cond_rec:
                jdata["condition_records"] = cond_rec
                jockey_cond_updated += 1

    # 統計レポート
    devs = [v.get("deviation", 50.0) for v in db["trainers"].values()]
    d50 = sum(1 for d in devs if d == 50.0)
    print(f"[調教師] deviation=50.0: {d50}/{len(devs)} ({d50/len(devs)*100:.1f}%)")
    print(f"[調教師] deviation更新: {trainer_updated}件 / condition更新: {trainer_cond_updated}件")
    print(f"[騎手] condition更新: {jockey_cond_updated}件")

    # condition の分布確認
    all_conds = set()
    for v in db["jockeys"].values():
        all_conds.update(v.get("condition_records", {}).keys())
    print(f"[騎手] 馬場状態: {sorted(all_conds)}")

    if args.dry_run:
        print("[DRY-RUN] 保存スキップ")
        return

    with open(PERSONNEL_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)
    print(f"✅ {PERSONNEL_DB_PATH} を更新しました")


if __name__ == "__main__":
    main()
