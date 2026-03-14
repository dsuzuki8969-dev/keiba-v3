#!/usr/bin/env python
"""
D-AI keiba の予想印・買い目・実際の結果・払戻金を CSV に一括出力する。

出力ファイル:
  data/export/predictions_YYYYMMDD_YYYYMMDD.csv  ← レース×券種単位（メイン）
  data/export/horses_YYYYMMDD_YYYYMMDD.csv       ← レース×馬単位（印・実着順）

変更履歴:
  v2 (2026-03-02): is_backfill / is_jra / is_dead_heat フラグ追加
                   race_profit（1レース収支）列追加
                   同着レースの的中判定を正確に修正
                   --split-by オプション追加（month / venue / none）

Usage:
  python scripts/export_results_csv.py
  python scripts/export_results_csv.py --start 2025-01-01 --end 2026-03-01
  python scripts/export_results_csv.py --split-by month   # 月別CSV分割
  python scripts/export_results_csv.py --split-by venue   # 会場別CSV分割
"""

import argparse
import csv
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.stdout.reconfigure(encoding="utf-8")

DB_PATH    = "data/keiba.db"
OUTPUT_DIR = "data/export"

# JRA場コード
JRA_CODES = {
    "01","02","03","04","05","06","07","08","09","10"
}


def _sorted_combo(lst):
    try:
        return "-".join(str(x) for x in sorted(int(v) for v in lst))
    except Exception:
        return "-".join(str(x) for x in lst)


def _is_dead_heat(finish_map: dict) -> bool:
    """着順に重複がある（同着）かどうかを返す"""
    finishes = list(finish_map.values())
    return len(finishes) != len(set(finishes))


def _check_umaren_hit(combo: list, top2: set) -> bool:
    """馬連的中判定（同着考慮: top2が2頭以上になる場合も対応）"""
    return bool(top2) and set(int(x) for x in combo) <= top2


def _check_sanren_hit(combo: list, top3: set) -> bool:
    """三連複的中判定"""
    return bool(top3) and set(int(x) for x in combo) == top3


def _build_pred_row(row, pred_headers):
    """1レース分のpredictions CSV行を構築して返す"""
    date      = row["date"]
    race_id   = row["race_id"]
    venue     = row["venue"] or ""
    race_no   = row["race_no"] or (int(race_id[-2:]) if len(race_id) >= 2 else 0)
    race_name = row["race_name"] or ""
    surface   = row["surface"] or ""
    distance  = row["distance"] or 0
    grade     = row["grade"] or ""
    conf      = row["confidence"] or "B"
    field_cnt = row["field_count"] or 0
    cancelled = row["cancelled"] or 0

    # is_jra: race_id[4:6] が JRA_CODES に含まれるか
    vc = race_id[4:6] if len(race_id) >= 6 else ""
    is_jra = 1 if vc in JRA_CODES else 0

    horses  = json.loads(row["horses_json"])  if row["horses_json"]  else []
    tickets = json.loads(row["tickets_json"]) if row["tickets_json"] else []
    order   = json.loads(row["order_json"])   if row["order_json"]   else []
    payouts = json.loads(row["payouts_json"]) if row["payouts_json"] else {}

    # is_backfill: ticketのsignal="簡易" かどうか
    is_backfill = 1 if any(t.get("signal") == "簡易" for t in tickets) else 0

    # 印マップ
    mark_map = {h["horse_no"]: h.get("mark", "-") for h in horses}

    # 本命・相手印
    honmei_no = taikou_no = tannuke_no = rendashi_no = daiyou_no = ""
    honmei_mk = ""
    for h in horses:
        mk = h.get("mark", "")
        no = h["horse_no"]
        if mk in ("◎", "◉"):
            honmei_no = no; honmei_mk = mk
        elif mk == "○":
            taikou_no = no
        elif mk == "▲":
            tannuke_no = no
        elif mk == "△":
            rendashi_no = no
        elif mk == "☆":
            daiyou_no = no

    # 実際の着順
    finish_map = {r["horse_no"]: r["finish"] for r in order}
    odds_map   = {r["horse_no"]: r.get("odds", 0) for r in order}
    actual_1st = actual_2nd = actual_3rd = ""
    actual_odds_1st = ""
    for r in order:
        if r["finish"] == 1:
            actual_1st = r["horse_no"]
            actual_odds_1st = r.get("odds", "")
        elif r["finish"] == 2:
            actual_2nd = r["horse_no"]
        elif r["finish"] == 3:
            actual_3rd = r["horse_no"]

    # 同着判定
    is_dead_heat = 1 if _is_dead_heat(finish_map) else 0

    # top2 / top3（同着対応: f<=2 で取得すると同着1着レースはtop2が3頭になることがある）
    # → 実際には top2 = finish<=2 (同着1着なら2頭が finish=1, finish=2 はスキップ)
    top2 = {h for h, f in finish_map.items() if f <= 2}
    top3 = {h for h, f in finish_map.items() if f <= 3}

    # 払戻
    um_pay  = payouts.get("馬連", {})
    san_pay = payouts.get("三連複", {})
    um_payout  = um_pay.get("payout", 0)  if isinstance(um_pay,  dict) else 0
    san_payout = san_pay.get("payout", 0) if isinstance(san_pay, dict) else 0
    um_actual_combo  = um_pay.get("combo",  "") if isinstance(um_pay,  dict) else ""
    san_actual_combo = san_pay.get("combo", "") if isinstance(san_pay, dict) else ""

    # チケット別
    um_tickets  = [t for t in tickets if t.get("type") == "馬連"]
    san_tickets = [t for t in tickets if t.get("type") == "三連複"]

    # 馬連的中
    um_hit = ""
    for t in um_tickets:
        if top2 and _check_umaren_hit(t.get("combo", []), top2):
            um_hit = "1"; break
    if um_hit == "" and um_tickets:
        um_hit = "0"

    # 三連複的中
    san_hit = ""
    for t in san_tickets:
        if top3 and _check_sanren_hit(t.get("combo", []), top3):
            san_hit = "1"; break
    if san_hit == "" and san_tickets:
        san_hit = "0"

    # 収支計算
    stake = (sum(t.get("stake", 100) or 100 for t in um_tickets)
           + sum(t.get("stake", 100) or 100 for t in san_tickets))
    ret   = ((um_payout  if um_hit  == "1" else 0)
           + (san_payout if san_hit == "1" else 0))
    race_profit = ret - stake if stake > 0 else ""

    # チケット文字列（最大4点・6点）
    um_combos  = [_sorted_combo(t.get("combo", [])) for t in um_tickets[:4]]
    san_combos = [_sorted_combo(t.get("combo", [])) for t in san_tickets[:6]]
    while len(um_combos)  < 4: um_combos.append("")
    while len(san_combos) < 6: san_combos.append("")

    return [
        date, race_id, venue, race_no, race_name,
        surface, distance, grade, conf, field_cnt,
        is_jra, is_backfill, is_dead_heat,
        honmei_mk, honmei_no,
        taikou_no, tannuke_no, rendashi_no, daiyou_no,
        um_actual_combo, um_hit, um_payout,
        um_combos[0], um_combos[1], um_combos[2], um_combos[3],
        san_actual_combo, san_hit, san_payout,
        san_combos[0], san_combos[1], san_combos[2],
        san_combos[3], san_combos[4], san_combos[5],
        actual_1st, actual_2nd, actual_3rd,
        actual_odds_1st, cancelled,
        stake, ret, race_profit,
    ]


def _build_horse_rows(row):
    """1レース分のhorses CSV行リストを構築して返す"""
    date    = row["date"]
    race_id = row["race_id"]
    venue   = row["venue"] or ""
    race_no = row["race_no"] or (int(race_id[-2:]) if len(race_id) >= 2 else 0)
    vc      = race_id[4:6] if len(race_id) >= 6 else ""
    is_jra  = 1 if vc in JRA_CODES else 0

    horses  = json.loads(row["horses_json"]) if row["horses_json"] else []
    order   = json.loads(row["order_json"])  if row["order_json"]  else []
    finish_map = {r["horse_no"]: r["finish"] for r in order}
    odds_map   = {r["horse_no"]: r.get("odds", 0)   for r in order}

    out = []
    for h in horses:
        hno = h["horse_no"]
        out.append([
            date, race_id, venue, race_no, is_jra,
            hno,
            h.get("horse_name", ""),
            h.get("mark", "-"),
            h.get("sex", ""),
            h.get("age", ""),
            h.get("weight", ""),
            h.get("odds", ""),
            h.get("popularity", ""),
            finish_map.get(hno, ""),
            odds_map.get(hno, ""),
        ])
    return out


PRED_HEADERS = [
    "date", "race_id", "venue", "race_no", "race_name",
    "surface", "distance", "grade", "confidence", "field_count",
    "is_jra", "is_backfill", "is_dead_heat",
    "honmei_mark", "honmei_no",
    "taikou_no", "tannuke_no", "rendashi_no", "daiyou_no",
    "umaren_combo", "umaren_hit", "umaren_payout",
    "umaren_t1_combo", "umaren_t2_combo", "umaren_t3_combo", "umaren_t4_combo",
    "sanren_combo", "sanren_hit", "sanren_payout",
    "sanren_t1", "sanren_t2", "sanren_t3", "sanren_t4",
    "sanren_t5", "sanren_t6",
    "actual_1st", "actual_2nd", "actual_3rd",
    "actual_odds_1st", "cancelled",
    "stake", "ret", "race_profit",
]

HORSE_HEADERS = [
    "date", "race_id", "venue", "race_no", "is_jra",
    "horse_no", "horse_name", "mark", "sex", "age",
    "weight", "odds", "popularity", "actual_finish", "actual_odds",
]


def export_csv(start: str, end: str, output_dir: str, split_by: str = "none"):
    os.makedirs(output_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT p.date, p.race_id, p.venue, p.race_no, p.race_name,"
        "       p.surface, p.distance, p.grade, p.confidence,"
        "       p.field_count, p.horses_json, p.tickets_json,"
        "       r.order_json, r.payouts_json, r.cancelled"
        " FROM predictions p"
        " LEFT JOIN race_results r ON p.race_id = r.race_id"
        " WHERE p.date >= ? AND p.date <= ?"
        " ORDER BY p.date, p.race_id",
        (start, end),
    ).fetchall()
    conn.close()

    print(f"対象: {len(rows)}件  ({start} 〜 {end})")

    # ── グルーピング ──────────────────────────────────────────────
    def _group_key(row):
        if split_by == "month":
            return row["date"][:7]
        elif split_by == "venue":
            return row["venue"] or "不明"
        return "all"

    grouped = defaultdict(list)
    for row in rows:
        grouped[_group_key(row)].append(row)

    s_tag = start.replace("-", "")
    e_tag = end.replace("-", "")
    total_pred_rows = 0
    total_horse_rows = 0

    for key, group_rows in sorted(grouped.items()):
        pred_data  = []
        horse_data = []
        for row in group_rows:
            pred_data.append(_build_pred_row(row, PRED_HEADERS))
            horse_data.extend(_build_horse_rows(row))

        if split_by == "none":
            pred_path  = os.path.join(output_dir, f"predictions_{s_tag}_{e_tag}.csv")
            horse_path = os.path.join(output_dir, f"horses_{s_tag}_{e_tag}.csv")
        else:
            safe_key = re.sub(r"[^\w\-]", "_", key)
            pred_path  = os.path.join(output_dir, f"predictions_{s_tag}_{e_tag}_{safe_key}.csv")
            horse_path = os.path.join(output_dir, f"horses_{s_tag}_{e_tag}_{safe_key}.csv")

        with open(pred_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(PRED_HEADERS)
            w.writerows(pred_data)

        with open(horse_path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(HORSE_HEADERS)
            w.writerows(horse_data)

        total_pred_rows  += len(pred_data)
        total_horse_rows += len(horse_data)
        print(f"  [{key}] predictions: {len(pred_data):,}行  horses: {len(horse_data):,}行")

    print(f"\n[OK] predictions 合計: {total_pred_rows:,}行")
    print(f"[OK] horses      合計: {total_horse_rows:,}行")
    print(f"     出力先: {output_dir}/")


def main():
    parser = argparse.ArgumentParser(description="予想・結果をCSVエクスポート")
    parser.add_argument("--start",    default="2025-01-01")
    parser.add_argument("--end",      default="2026-03-01")
    parser.add_argument("--output",   default=OUTPUT_DIR)
    parser.add_argument("--split-by", default="none",
                        choices=["none", "month", "venue"],
                        help="CSVの分割単位 (none/month/venue)")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  D-AI Keiba CSV エクスポート v2")
    print(f"  期間: {args.start} ～ {args.end}")
    print(f"  出力先: {args.output}")
    print(f"  分割: {args.split_by}")
    print(f"{'='*60}\n")

    export_csv(args.start, args.end, args.output, args.split_by)
    print("\n完了！")


if __name__ == "__main__":
    main()
