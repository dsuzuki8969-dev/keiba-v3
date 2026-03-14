#!/usr/bin/env python
"""
D-AI Keiba 詳細集計CSV生成スクリプト

生成ファイル (data/export/stats/):
  monthly_stats.csv   - 月別集計
  venue_stats.csv     - 会場別集計
  distance_stats.csv  - 距離帯別集計
  condition_stats.csv - 馬場状態別集計（surface+grade）
  mark_stats.csv      - 印別成績（◎○▲△☆の勝率・連対率・複勝率）
  confidence_stats.csv- 自信度別集計
  honmei_stats.csv    - 本命◎の軸别成績
  summary.csv         - 全体サマリー

※ is_backfill=1（バックフィル）のデータは学習データと重複のため
   参考値として表示するが、真の成績はis_backfill=0のみ

Usage:
  python scripts/export_stats_csv.py
  python scripts/export_stats_csv.py --start 2025-01-01 --end 2026-03-01
  python scripts/export_stats_csv.py --live-only   # ライブ予測のみ（現在は0件）
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

DB_PATH    = "data/keiba.db"
STATS_DIR  = "data/export/stats"
JRA_CODES  = {"01","02","03","04","05","06","07","08","09","10"}

DISTANCE_BANDS = [
    (0,    1000, "超短距離(<1000m)"),
    (1000, 1400, "短距離(1000-1400m)"),
    (1400, 1800, "マイル(1400-1800m)"),
    (1800, 2200, "中距離(1800-2200m)"),
    (2200, 2800, "中長距離(2200-2800m)"),
    (2800, 9999, "長距離(2800m+)"),
]

def _dist_band(dist):
    for lo, hi, label in DISTANCE_BANDS:
        if lo <= dist < hi:
            return label
    return "不明"


def _is_backfill(tickets):
    return any(t.get("signal") == "簡易" for t in tickets)


def _new_stat():
    return {
        "races": 0, "stake": 0, "ret": 0,
        "um_hit": 0, "san_hit": 0, "any_hit": 0,
        "honmei_win": 0, "honmei_place2": 0, "honmei_place3": 0, "honmei_total": 0,
        "dead_heats": 0,
    }


def _merge(a, b):
    for k in a:
        a[k] += b[k]


def _roi(s):
    return round(s["ret"] / s["stake"] * 100, 1) if s["stake"] > 0 else 0.0


def _hit_rate(hits, races):
    return round(hits / races * 100, 1) if races > 0 else 0.0


def _stat_row(key, s):
    roi = _roi(s)
    um_r  = _hit_rate(s["um_hit"],  s["races"])
    san_r = _hit_rate(s["san_hit"], s["races"])
    any_r = _hit_rate(s["any_hit"], s["races"])
    hm_wr = _hit_rate(s["honmei_win"],    s["honmei_total"])
    hm_p2 = _hit_rate(s["honmei_place2"], s["honmei_total"])
    hm_p3 = _hit_rate(s["honmei_place3"], s["honmei_total"])
    profit = s["ret"] - s["stake"]
    return [
        key,
        s["races"], s["stake"], s["ret"], profit, roi,
        s["um_hit"], um_r, s["san_hit"], san_r, s["any_hit"], any_r,
        s["honmei_total"], s["honmei_win"], hm_wr, s["honmei_place2"], hm_p2, s["honmei_place3"], hm_p3,
        s["dead_heats"],
    ]


STAT_HEADERS = [
    "key",
    "races", "stake", "ret", "profit", "roi(%)",
    "um_hit", "um_hit_rate(%)", "san_hit", "san_hit_rate(%)", "any_hit", "any_hit_rate(%)",
    "honmei_total", "honmei_win", "honmei_win_rate(%)",
    "honmei_place2", "honmei_place2_rate(%)", "honmei_place3", "honmei_place3_rate(%)",
    "dead_heats",
]

MARK_HEADERS = [
    "mark", "total",
    "win", "win_rate(%)", "place2", "place2_rate(%)", "place3", "place3_rate(%)",
]


def collect_data(start, end, live_only=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT p.date, p.race_id, p.venue, p.race_no,"
        "       p.surface, p.distance, p.grade, p.confidence,"
        "       p.field_count, p.horses_json, p.tickets_json,"
        "       r.order_json, r.payouts_json, r.cancelled"
        " FROM predictions p"
        " LEFT JOIN race_results r ON p.race_id = r.race_id"
        " WHERE p.date >= ? AND p.date <= ? AND r.order_json IS NOT NULL"
        " ORDER BY p.date, p.race_id",
        (start, end),
    ).fetchall()
    conn.close()

    by_month  = defaultdict(_new_stat)
    by_venue  = defaultdict(_new_stat)
    by_dist   = defaultdict(_new_stat)
    by_cond   = defaultdict(_new_stat)   # surface+grade
    by_conf   = defaultdict(_new_stat)
    mark_data = defaultdict(lambda: {"total":0,"win":0,"place2":0,"place3":0})
    total     = _new_stat()

    for row in rows:
        tickets = json.loads(row["tickets_json"]) if row["tickets_json"] else []
        if live_only and _is_backfill(tickets):
            continue
        if not tickets:
            continue

        order   = json.loads(row["order_json"])
        payouts = json.loads(row["payouts_json"]) if row["payouts_json"] else {}
        horses  = json.loads(row["horses_json"])  if row["horses_json"]  else []

        finish_map = {r["horse_no"]: r["finish"] for r in order}
        if len(finish_map) < 3:
            continue

        top2 = {h for h, f in finish_map.items() if f <= 2}
        top3 = {h for h, f in finish_map.items() if f <= 3}

        # 同着
        finishes   = list(finish_map.values())
        dead_heat  = 1 if len(finishes) != len(set(finishes)) else 0

        um_ts  = [t for t in tickets if t.get("type") == "馬連"]
        san_ts = [t for t in tickets if t.get("type") == "三連複"]

        um_hit  = any(len(top2) >= 2 and set(int(x) for x in t.get("combo",[])) <= top2 for t in um_ts)
        san_hit = any(len(top3) >= 3 and set(int(x) for x in t.get("combo",[])) == top3 for t in san_ts)
        any_hit = um_hit or san_hit

        um_pay  = payouts.get("馬連",  {}).get("payout", 0) if isinstance(payouts.get("馬連"),  dict) else 0
        san_pay = payouts.get("三連複",{}).get("payout", 0) if isinstance(payouts.get("三連複"), dict) else 0

        stake = (sum(t.get("stake",100) or 100 for t in um_ts)
               + sum(t.get("stake",100) or 100 for t in san_ts))
        ret   = (um_pay if um_hit else 0) + (san_pay if san_hit else 0)

        # 本命着順
        honmei_no = next((h["horse_no"] for h in horses if h.get("mark") in ("◎","◉")), None)
        honmei_fin = finish_map.get(honmei_no, 99) if honmei_no else 99
        hm_win  = 1 if honmei_fin == 1 else 0
        hm_p2   = 1 if honmei_fin <= 2 else 0
        hm_p3   = 1 if honmei_fin <= 3 else 0

        def _upd(d_stat):
            d_stat["races"]   += 1
            d_stat["stake"]   += stake
            d_stat["ret"]     += ret
            d_stat["um_hit"]  += int(um_hit)
            d_stat["san_hit"] += int(san_hit)
            d_stat["any_hit"] += int(any_hit)
            d_stat["dead_heats"] += dead_heat
            if honmei_no:
                d_stat["honmei_total"]  += 1
                d_stat["honmei_win"]    += hm_win
                d_stat["honmei_place2"] += hm_p2
                d_stat["honmei_place3"] += hm_p3

        month  = row["date"][:7]
        venue  = row["venue"] or "不明"
        dist_b = _dist_band(row["distance"] or 0)
        cond   = f"{row['surface'] or '-'}_{row['grade'] or '-'}"
        conf   = row["confidence"] or "B"

        _upd(by_month[month])
        _upd(by_venue[venue])
        _upd(by_dist[dist_b])
        _upd(by_cond[cond])
        _upd(by_conf[conf])
        _upd(total)

        # 印別成績
        for h in horses:
            mk  = h.get("mark", "-")
            hno = h["horse_no"]
            if mk in ("◎","◉","○","▲","△","☆"):
                fin = finish_map.get(hno, 99)
                mark_data[mk]["total"]  += 1
                if fin == 1: mark_data[mk]["win"]    += 1
                if fin <= 2: mark_data[mk]["place2"] += 1
                if fin <= 3: mark_data[mk]["place3"] += 1

    return {
        "by_month":  by_month,
        "by_venue":  by_venue,
        "by_dist":   by_dist,
        "by_cond":   by_cond,
        "by_conf":   by_conf,
        "mark_data": mark_data,
        "total":     total,
    }


def write_stat_csv(path, data_dict, sort_key=None):
    rows = []
    for k, s in data_dict.items():
        if s["races"] == 0:
            continue
        rows.append(_stat_row(k, s))
    if sort_key:
        rows.sort(key=sort_key)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(STAT_HEADERS)
        w.writerows(rows)
    return len(rows)


def write_mark_csv(path, mark_data):
    mark_order = ["◎","◉","○","▲","△","☆"]
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(MARK_HEADERS)
        for mk in mark_order:
            d = mark_data.get(mk)
            if not d or d["total"] == 0:
                continue
            t = d["total"]
            w.writerow([
                mk, t,
                d["win"],    round(d["win"]    / t * 100, 1),
                d["place2"], round(d["place2"] / t * 100, 1),
                d["place3"], round(d["place3"] / t * 100, 1),
            ])


def main():
    parser = argparse.ArgumentParser(description="詳細集計CSV生成")
    parser.add_argument("--start",     default="2025-01-01")
    parser.add_argument("--end",       default="2026-03-01")
    parser.add_argument("--live-only", action="store_true", help="ライブ予測のみ集計")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  D-AI Keiba 詳細集計CSV生成")
    print(f"  期間: {args.start} ～ {args.end}  live_only={args.live_only}")
    print(f"{'='*60}\n")

    os.makedirs(STATS_DIR, exist_ok=True)
    data = collect_data(args.start, args.end, args.live_only)

    s = args.start.replace("-",""); e = args.end.replace("-","")
    suf = f"_{s}_{e}"

    # 月別
    n = write_stat_csv(
        os.path.join(STATS_DIR, f"monthly_stats{suf}.csv"),
        data["by_month"],
        sort_key=lambda r: r[0],
    )
    print(f"[OK] monthly_stats:    {n}行")

    # 会場別
    n = write_stat_csv(
        os.path.join(STATS_DIR, f"venue_stats{suf}.csv"),
        data["by_venue"],
        sort_key=lambda r: -r[5],  # ROI降順
    )
    print(f"[OK] venue_stats:      {n}行")

    # 距離帯別
    n = write_stat_csv(
        os.path.join(STATS_DIR, f"distance_stats{suf}.csv"),
        data["by_dist"],
    )
    print(f"[OK] distance_stats:   {n}行")

    # 馬場状態別
    n = write_stat_csv(
        os.path.join(STATS_DIR, f"condition_stats{suf}.csv"),
        data["by_cond"],
    )
    print(f"[OK] condition_stats:  {n}行")

    # 自信度別
    n = write_stat_csv(
        os.path.join(STATS_DIR, f"confidence_stats{suf}.csv"),
        data["by_conf"],
        sort_key=lambda r: ["SS","S","A","B","C","D"].index(r[0]) if r[0] in ["SS","S","A","B","C","D"] else 99,
    )
    print(f"[OK] confidence_stats: {n}行")

    # 印別
    write_mark_csv(
        os.path.join(STATS_DIR, f"mark_stats{suf}.csv"),
        data["mark_data"],
    )
    print(f"[OK] mark_stats:       各印成績")

    # 全体サマリー
    tot = data["total"]
    summary_path = os.path.join(STATS_DIR, f"summary{suf}.csv")
    with open(summary_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["項目", "値"])
        w.writerows([
            ["期間", f"{args.start} 〜 {args.end}"],
            ["対象レース数",       tot["races"]],
            ["総投資額",           tot["stake"]],
            ["総払戻額",           tot["ret"]],
            ["収支",               tot["ret"] - tot["stake"]],
            ["回収率(%)",          _roi(tot)],
            ["馬連的中数",         tot["um_hit"]],
            ["馬連的中率(%)",      _hit_rate(tot["um_hit"],  tot["races"])],
            ["三連複的中数",       tot["san_hit"]],
            ["三連複的中率(%)",    _hit_rate(tot["san_hit"], tot["races"])],
            ["いずれか的中数",     tot["any_hit"]],
            ["いずれか的中率(%)",  _hit_rate(tot["any_hit"], tot["races"])],
            ["本命総数",           tot["honmei_total"]],
            ["本命勝率(%)",        _hit_rate(tot["honmei_win"],    tot["honmei_total"])],
            ["本命連対率(%)",      _hit_rate(tot["honmei_place2"], tot["honmei_total"])],
            ["本命複勝率(%)",      _hit_rate(tot["honmei_place3"], tot["honmei_total"])],
            ["同着レース数",       tot["dead_heats"]],
            ["※注意", "全データはバックフィル(is_backfill=1)のため真の成績ではありません"],
        ])
    print(f"[OK] summary:          {summary_path}")

    print(f"\n  出力先: {STATS_DIR}/")
    print(f"\n  全体サマリー:")
    print(f"    レース数: {tot['races']:,}")
    print(f"    投資: {tot['stake']:,}円  払戻: {tot['ret']:,}円")
    print(f"    収支: {tot['ret']-tot['stake']:+,}円  回収率: {_roi(tot):.1f}%")
    print(f"    馬連: {_hit_rate(tot['um_hit'],tot['races']):.1f}%  三連複: {_hit_rate(tot['san_hit'],tot['races']):.1f}%")
    print(f"    本命複勝率: {_hit_rate(tot['honmei_place3'],tot['honmei_total']):.1f}%")
    print(f"\n完了！\n")


if __name__ == "__main__":
    main()
