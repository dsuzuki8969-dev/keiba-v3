"""
pred.json の補完馬に必要な全フィールドを race_log + horses から補完する統合スクリプト。

【背景】
出馬表 scraper の構造的バグで補完した馬は基本情報すべて欠落:
- 父・母父 (sire / maternal_grandsire)
- 性齢 (sex / age)
- 騎手・厩舎 (jockey / trainer)
- 馬体重・体重増減・斤量 (horse_weight / weight_change / weight_kg)
- 各種指数 (ability_total / pace_total / tekisei_dev / jockey_dev / trainer_dev / bloodline_dev)
- 枠番 (gate_no)
- 印 (mark)
- 過去 3 走 (past_3_runs)

【修復ソース】
1. race_log: 馬名で過去走から sire / age / sex / trainer / 馬体重 等取得
2. horses: 馬名で sire/dam/bms/birth_year 取得
3. odds: 各種指数 (ability_total 等) を逆数推定

【使い方】
    python scripts/repair_pred_full_fields.py 2026-05-05
    python scripts/repair_pred_full_fields.py --all  # 全期間スキャン (要時間)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import DATABASE_PATH

VC_NAME = {
    "30": "門別", "35": "盛岡", "36": "水沢", "42": "浦和", "43": "船橋",
    "44": "大井", "45": "川崎", "46": "笠松", "47": "名古屋", "48": "名古屋",
    "50": "園田", "51": "姫路", "54": "高知", "55": "佐賀", "65": "帯広",
}

DEFAULTS = {
    "jockey_grade": "-", "trainer_grade": "-", "sire_grade": "-",
    "mgs_grade": "-", "last3f_grade": "-", "ability_reliability": "-",
    "running_style": "", "trainer_affiliation": "",
}


def calc_gate_no(horse_no, fc):
    if fc <= 0:
        return 1
    if fc <= 8:
        return horse_no
    return min(8, (horse_no - 1) * 8 // fc + 1)


def estimate_dev(odds, base=50.0):
    if not odds or odds <= 0:
        return base
    if odds <= 2:
        return base + 18
    if odds <= 4:
        return base + 10
    if odds <= 8:
        return base + 4
    if odds <= 15:
        return base
    if odds <= 30:
        return base - 5
    if odds <= 60:
        return base - 12
    return base - 20


def repair_horse(h, conn, pred_date, fc):
    """1 頭の補完馬を全フィールド補完。"""
    c = conn.cursor()
    horse_name = h.get("horse_name")
    if not horse_name:
        return False

    odds = float(h.get("odds") or 0)
    pred_year = int(pred_date[:4])

    # 1) race_log から最新基本情報
    c.execute("""
        SELECT sire_name, bms_name, jockey_name, trainer_name, weight_kg,
               horse_weight, weight_change, jockey_id, trainer_id, gate_no
        FROM race_log
        WHERE horse_name = ? ORDER BY race_date DESC LIMIT 1
    """, (horse_name,))
    r = c.fetchone()
    if r:
        keys = [k[0] for k in c.description]
        d = dict(zip(keys, r))
        if not h.get("jockey"): h["jockey"] = d.get("jockey_name") or ""
        if not h.get("jockey_name"): h["jockey_name"] = d.get("jockey_name") or ""
        if not h.get("trainer"): h["trainer"] = d.get("trainer_name") or ""
        if not h.get("jockey_id"): h["jockey_id"] = d.get("jockey_id") or ""
        if not h.get("trainer_id"): h["trainer_id"] = d.get("trainer_id") or ""
        if not h.get("weight_kg"): h["weight_kg"] = d.get("weight_kg") or 54.0
        if not h.get("horse_weight"): h["horse_weight"] = d.get("horse_weight") or 0
        if h.get("weight_change") is None: h["weight_change"] = d.get("weight_change") or 0
        if d.get("gate_no"): h["gate_no"] = d["gate_no"]

    # 2) age (NOT NULL の最新)
    if not h.get("age"):
        c.execute("SELECT age, race_date FROM race_log WHERE horse_name=? AND age IS NOT NULL AND age > 0 ORDER BY race_date DESC LIMIT 1", (horse_name,))
        r = c.fetchone()
        if r:
            past_year = int(r[1][:4])
            h["age"] = r[0] + (pred_year - past_year)

    # 3) sire/bms (NOT NULL の最新)
    if not h.get("sire"):
        c.execute("SELECT sire_name FROM race_log WHERE horse_name=? AND sire_name IS NOT NULL AND sire_name <> '' ORDER BY race_date DESC LIMIT 1", (horse_name,))
        r = c.fetchone()
        if r: h["sire"] = r[0]
    if not h.get("maternal_grandsire"):
        c.execute("SELECT bms_name FROM race_log WHERE horse_name=? AND bms_name IS NOT NULL AND bms_name <> '' ORDER BY race_date DESC LIMIT 1", (horse_name,))
        r = c.fetchone()
        if r: h["maternal_grandsire"] = r[0]

    # 4) horses テーブル (sire/dam/sex)
    c.execute("SELECT sire_name, dam_name, bms_name, sex, owner FROM horses WHERE horse_name=? LIMIT 1", (horse_name,))
    r = c.fetchone()
    if r:
        if not h.get("sire") and r[0]: h["sire"] = r[0]
        if not h.get("dam") and r[1]: h["dam"] = r[1]
        if not h.get("maternal_grandsire") and r[2]: h["maternal_grandsire"] = r[2]
        if not h.get("sex") and r[3]: h["sex"] = r[3]
        if not h.get("owner") and r[4]: h["owner"] = r[4]

    # 5) 各種指数 (odds 推定)
    est = estimate_dev(odds)
    for k in ["ability_total", "ability_max", "ability_wa", "pace_total", "tekisei_dev"]:
        if h.get(k) is None or h.get(k) == 0:
            h[k] = est
    for k in ["jockey_dev", "trainer_dev", "bloodline_dev", "blood_dev", "race_relative_dev"]:
        if h.get(k) is None or h.get(k) == 0:
            h[k] = 50.0

    # 6) 枠番
    if not h.get("gate_no"):
        h["gate_no"] = calc_gate_no(h.get("horse_no", 1), fc)

    # 7) 印 (補完馬は強制「-」)
    h["mark"] = "-"

    # 8) past_3_runs
    if not h.get("past_3_runs"):
        c.execute("""
            SELECT race_date, race_id, venue_code, surface, distance, finish_pos,
                   finish_time_sec, last_3f_sec, popularity, position_4c,
                   positions_corners, jockey_name, weight_kg, condition,
                   race_name, grade, run_dev, field_count
            FROM race_log
            WHERE horse_name = ? AND race_date < ?
            ORDER BY race_date DESC, race_id DESC LIMIT 3
        """, (horse_name, pred_date))
        past = c.fetchall()
        kk = [k[0] for k in c.description]
        past_3 = []
        for p in past:
            d = dict(zip(kk, p))
            past_3.append({
                "race_id": d.get("race_id"),
                "date": d.get("race_date"),
                "venue": VC_NAME.get(d.get("venue_code", ""), d.get("venue_code", "")),
                "class": d.get("race_name") or d.get("grade") or "",
                "condition": d.get("condition") or "",
                "distance": d.get("distance") or 0,
                "surface": d.get("surface") or "",
                "finish_pos": d.get("finish_pos") or 0,
                "field_count": d.get("field_count") or 0,
                "finish_time": d.get("finish_time_sec") or 0,
                "last_3f": d.get("last_3f_sec") or 0,
                "popularity": d.get("popularity") or 0,
                "position_4c": d.get("position_4c") or 0,
                "positions_corners": d.get("positions_corners") or "",
                "jockey": d.get("jockey_name") or "",
                "weight_kg": d.get("weight_kg") or 0,
                "speed_dev": d.get("run_dev") or 50.0,
                "speed_dev_grade": "-",
                "race_level_grade": "-",
                "margin": 0, "pace": "", "last_3f_rank": 0,
            })
        h["past_3_runs"] = past_3

    # 9) デフォルト値
    for k, v in DEFAULTS.items():
        if k not in h:
            h[k] = v

    return True


def repair_pred(date: str) -> int:
    date_key = date.replace("-", "")
    pred_path = PROJECT_ROOT / "data" / "predictions" / f"{date_key}_pred.json"
    if not pred_path.exists():
        print(f"[ERR] {pred_path} なし")
        return 0

    with open(pred_path, encoding="utf-8") as f:
        pred = json.load(f)
    pred_date = pred.get("date", date)

    conn = sqlite3.connect(DATABASE_PATH)
    repaired = 0
    for race in pred.get("races", []):
        fc = len(race.get("horses", []))
        for h in race.get("horses", []):
            if h.get("scrape_failed") or h.get("repair_source"):
                if repair_horse(h, conn, pred_date, fc):
                    repaired += 1
    conn.close()

    if repaired > 0:
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(pred, f, ensure_ascii=False, indent=2)
        print(f"[OK] {date}: {repaired} 馬補完")
    else:
        print(f"[OK] {date}: 補完対象なし")
    return repaired


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("date", nargs="?", help="YYYY-MM-DD or --all")
    ap.add_argument("--all", action="store_true", help="data/predictions/2026* 全件スキャン")
    args = ap.parse_args()

    if args.all:
        import glob
        for f in sorted(glob.glob("data/predictions/2026*_pred.json")):
            if "bak" in f or "prev" in f:
                continue
            d = Path(f).stem.replace("_pred", "")
            date_str = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            repair_pred(date_str)
    else:
        if not args.date:
            ap.error("date を指定してください or --all")
        repair_pred(args.date)


if __name__ == "__main__":
    main()
