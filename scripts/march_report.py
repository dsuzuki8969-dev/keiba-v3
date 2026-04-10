"""3月予想結果の多角的集計レポート"""
import sys, os, json, glob
from collections import defaultdict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout.reconfigure(encoding="utf-8")

PRED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "predictions")
RES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "results")

# ── データ読み込み ──
def load_month(year_month="202603"):
    records = []  # (race, horse, finish, payouts)
    pred_files = sorted(glob.glob(os.path.join(PRED_DIR, f"{year_month}*_pred.json")))
    for pf in pred_files:
        date_key = os.path.basename(pf)[:8]
        rf = os.path.join(RES_DIR, f"{date_key}_results.json")
        if not os.path.isfile(rf):
            continue
        try:
            with open(pf, "r", encoding="utf-8") as f:
                pred = json.load(f)
            with open(rf, "r", encoding="utf-8") as f:
                res = json.load(f)
        except (json.JSONDecodeError, Exception) as e:
            print(f"  スキップ: {os.path.basename(pf)} ({e.__class__.__name__})")
            continue
        for race in pred.get("races", []):
            rid = race.get("race_id", "")
            r = res.get(rid)
            if not r or not r.get("order"):
                continue
            finish_map = {o["horse_no"]: o["finish"] for o in r["order"]}
            payouts = r.get("payouts", {})
            for h in race.get("horses", []):
                hno = h.get("horse_no", 0)
                fin = finish_map.get(hno, 99)
                records.append((race, h, fin, payouts))
    return records

records = load_month()
print(f"3月集計対象: {len(records)}頭")

# ── 印別成績 ──
mark_stats = defaultdict(lambda: {"n": 0, "w": 0, "p2": 0, "p3": 0,
                                   "tan_stake": 0, "tan_ret": 0,
                                   "fuku_stake": 0, "fuku_ret": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    if not m:
        continue
    s = mark_stats[m]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    if fin <= 2: s["p2"] += 1
    if fin <= 3: s["p3"] += 1
    # 単勝回収
    s["tan_stake"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tan_ret"] += tan.get("payout", 0)
    # 複勝回収
    s["fuku_stake"] += 100
    if fin <= 3:
        fuku = pay.get("複勝", [])
        if isinstance(fuku, list):
            for fp in fuku:
                if fp.get("combo") == str(h.get("horse_no")):
                    s["fuku_ret"] += fp.get("payout", 0)

print("\n═══ 印別成績 ═══")
print(f"{'印':>2} {'件数':>5} {'勝率':>6} {'連対':>6} {'複勝':>6} {'単回収':>6} {'複回収':>6}")
mark_order = ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]
for m in mark_order:
    s = mark_stats.get(m)
    if not s or s["n"] == 0:
        continue
    wr = s["w"] / s["n"] * 100
    p2r = s["p2"] / s["n"] * 100
    p3r = s["p3"] / s["n"] * 100
    tan_roi = s["tan_ret"] / s["tan_stake"] * 100 if s["tan_stake"] else 0
    fuku_roi = s["fuku_ret"] / s["fuku_stake"] * 100 if s["fuku_stake"] else 0
    print(f"{m:>2} {s['n']:>5} {wr:>5.1f}% {p2r:>5.1f}% {p3r:>5.1f}% {tan_roi:>5.1f}% {fuku_roi:>5.1f}%")

# ── 自信度別成績（◎の的中率・回収率）──
conf_stats = defaultdict(lambda: {"n": 0, "honmei_w": 0, "honmei_p3": 0,
                                    "tan_stake": 0, "tan_ret": 0})
seen_races_conf = set()
for race, h, fin, pay in records:
    rid = race.get("race_id", "")
    conf = race.get("confidence", "")
    m = h.get("mark", "")
    if not conf:
        continue
    if m in ("◎", "◉") and rid not in seen_races_conf:
        seen_races_conf.add(rid)
        s = conf_stats[conf]
        s["n"] += 1
        if fin == 1:
            s["honmei_w"] += 1
        if fin <= 3:
            s["honmei_p3"] += 1
        s["tan_stake"] += 100
        if fin == 1:
            tan = pay.get("単勝", {})
            if isinstance(tan, dict):
                s["tan_ret"] += tan.get("payout", 0)

print("\n═══ 自信度別（◎/◉の成績）═══")
print(f"{'自信度':>4} {'件数':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>6}")
conf_order = ["SS", "S", "A", "B", "C", "D", "E"]
for c in conf_order:
    s = conf_stats.get(c)
    if not s or s["n"] == 0:
        continue
    wr = s["honmei_w"] / s["n"] * 100
    p3r = s["honmei_p3"] / s["n"] * 100
    roi = s["tan_ret"] / s["tan_stake"] * 100 if s["tan_stake"] else 0
    print(f"{c:>4} {s['n']:>5} {wr:>5.1f}% {p3r:>5.1f}% {roi:>5.1f}%")

# ── 会場別成績 ──
venue_stats = defaultdict(lambda: {"races": set(), "honmei_w": 0, "honmei_p3": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    v = race.get("venue", "")
    rid = race.get("race_id", "")
    if m in ("◎", "◉") and v:
        s = venue_stats[v]
        if rid not in s["races"]:
            s["races"].add(rid)
            if fin == 1: s["honmei_w"] += 1
            if fin <= 3: s["honmei_p3"] += 1

print("\n═══ 会場別（◎/◉の成績）═══")
print(f"{'会場':>6} {'レース':>5} {'勝率':>6} {'複勝率':>6}")
for v, s in sorted(venue_stats.items(), key=lambda x: -len(x[1]["races"])):
    n = len(s["races"])
    if n < 3:
        continue
    wr = s["honmei_w"] / n * 100
    p3r = s["honmei_p3"] / n * 100
    print(f"{v:>6} {n:>5} {wr:>5.1f}% {p3r:>5.1f}%")

# ── 馬場別（ダート/芝）──
surface_stats = defaultdict(lambda: {"races": set(), "honmei_w": 0, "honmei_p3": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    sf = race.get("surface", "")
    rid = race.get("race_id", "")
    if m in ("◎", "◉") and sf:
        s = surface_stats[sf]
        if rid not in s["races"]:
            s["races"].add(rid)
            if fin == 1: s["honmei_w"] += 1
            if fin <= 3: s["honmei_p3"] += 1

print("\n═══ 馬場別（◎/◉の成績）═══")
print(f"{'馬場':>6} {'レース':>5} {'勝率':>6} {'複勝率':>6}")
for sf, s in sorted(surface_stats.items(), key=lambda x: -len(x[1]["races"])):
    n = len(s["races"])
    wr = s["honmei_w"] / n * 100
    p3r = s["honmei_p3"] / n * 100
    print(f"{sf:>6} {n:>5} {wr:>5.1f}% {p3r:>5.1f}%")

# ── 距離帯別 ──
def dist_bucket(d):
    if d <= 1200: return "短距離(~1200)"
    if d <= 1600: return "マイル(~1600)"
    if d <= 2000: return "中距離(~2000)"
    return "長距離(2001~)"

dist_stats = defaultdict(lambda: {"races": set(), "honmei_w": 0, "honmei_p3": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    d = race.get("distance", 0)
    rid = race.get("race_id", "")
    if m in ("◎", "◉") and d:
        bucket = dist_bucket(d)
        s = dist_stats[bucket]
        if rid not in s["races"]:
            s["races"].add(rid)
            if fin == 1: s["honmei_w"] += 1
            if fin <= 3: s["honmei_p3"] += 1

print("\n═══ 距離帯別（◎/◉の成績）═══")
print(f"{'距離帯':>14} {'レース':>5} {'勝率':>6} {'複勝率':>6}")
for bucket in ["短距離(~1200)", "マイル(~1600)", "中距離(~2000)", "長距離(2001~)"]:
    s = dist_stats.get(bucket)
    if not s:
        continue
    n = len(s["races"])
    wr = s["honmei_w"] / n * 100
    p3r = s["honmei_p3"] / n * 100
    print(f"{bucket:>14} {n:>5} {wr:>5.1f}% {p3r:>5.1f}%")

# ── 人気別（◎の人気帯と的中）──
pop_stats = defaultdict(lambda: {"n": 0, "w": 0, "p3": 0, "tan_stake": 0, "tan_ret": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    pop = h.get("popularity", 0)
    if m not in ("◎", "◉") or not pop:
        continue
    if pop <= 1: bucket = "1番人気"
    elif pop <= 3: bucket = "2-3番人気"
    elif pop <= 6: bucket = "4-6番人気"
    else: bucket = "7番人気~"
    s = pop_stats[bucket]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    if fin <= 3: s["p3"] += 1
    s["tan_stake"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tan_ret"] += tan.get("payout", 0)

print("\n═══ ◎/◉の人気帯別成績 ═══")
print(f"{'人気帯':>10} {'件数':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>6}")
for bucket in ["1番人気", "2-3番人気", "4-6番人気", "7番人気~"]:
    s = pop_stats.get(bucket)
    if not s or s["n"] == 0:
        continue
    wr = s["w"] / s["n"] * 100
    p3r = s["p3"] / s["n"] * 100
    roi = s["tan_ret"] / s["tan_stake"] * 100 if s["tan_stake"] else 0
    print(f"{bucket:>10} {s['n']:>5} {wr:>5.1f}% {p3r:>5.1f}% {roi:>5.1f}%")

# ── 週別推移 ──
from datetime import datetime
week_stats = defaultdict(lambda: {"n": 0, "honmei_w": 0, "honmei_p3": 0,
                                    "tan_stake": 0, "tan_ret": 0})
seen_races_week = set()
for race, h, fin, pay in records:
    m = h.get("mark", "")
    rid = race.get("race_id", "")
    if m not in ("◎", "◉") or rid in seen_races_week:
        continue
    seen_races_week.add(rid)
    # race_idから日付 (YYYY + VV + MMDD + RR)
    date_str = rid[0:4] + "-" + rid[6:8] + "-" + rid[8:10]
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.month != 3:
            continue  # 3月以外はスキップ
        # 週の開始日（月曜）を求めて週ラベルにする
        week_start = dt - __import__("datetime").timedelta(days=dt.weekday())
        week_label = f"{week_start.strftime('%m/%d')}週"
    except:
        continue
    s = week_stats[week_label]
    s["n"] += 1
    if fin == 1: s["honmei_w"] += 1
    if fin <= 3: s["honmei_p3"] += 1
    s["tan_stake"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tan_ret"] += tan.get("payout", 0)

print("\n═══ 週別推移（◎/◉の成績）═══")
print(f"{'週':>16} {'レース':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>6}")
for wk, s in sorted(week_stats.items()):
    if s["n"] == 0:
        continue
    wr = s["honmei_w"] / s["n"] * 100
    p3r = s["honmei_p3"] / s["n"] * 100
    roi = s["tan_ret"] / s["tan_stake"] * 100 if s["tan_stake"] else 0
    print(f"{wk:>16} {s['n']:>5} {wr:>5.1f}% {p3r:>5.1f}% {roi:>5.1f}%")

# ── 券種別収支（推定）──
# tickets配列から三連複・馬連等の的中を集計
ticket_stats = defaultdict(lambda: {"n": 0, "hit": 0, "stake": 0, "ret": 0})
seen_race_tickets = set()
for race, h, fin, pay in records:
    rid = race.get("race_id", "")
    if rid in seen_race_tickets:
        continue
    seen_race_tickets.add(rid)
    tickets = race.get("tickets", [])
    if not tickets:
        continue
    order = pay  # payoutsにアクセス
    for t in tickets:
        ttype = t.get("type", "")
        if not ttype:
            continue
        s = ticket_stats[ttype]
        s["n"] += 1
        s["stake"] += t.get("cost", 100)
        # 的中判定
        result_payout = order.get(ttype)
        if result_payout:
            combos = t.get("combo", [])
            if isinstance(result_payout, dict):
                result_combos = [result_payout.get("combo", "")]
            elif isinstance(result_payout, list):
                result_combos = [rp.get("combo", "") for rp in result_payout]
            else:
                result_combos = []
            for c in (combos if isinstance(combos, list) else [combos]):
                c_str = "-".join(str(x) for x in c) if isinstance(c, (list, tuple)) else str(c)
                if c_str in result_combos:
                    s["hit"] += 1
                    matching = [rp for rp in (result_payout if isinstance(result_payout, list) else [result_payout])
                                if rp.get("combo") == c_str]
                    if matching:
                        s["ret"] += matching[0].get("payout", 0)

if ticket_stats:
    print("\n═══ 券種別概要 ═══")
    print(f"{'券種':>8} {'発行数':>6} {'的中':>5} {'的中率':>6} {'回収率':>7}")
    for tt in ["単勝", "複勝", "馬連", "ワイド", "馬単", "三連複", "三連単"]:
        s = ticket_stats.get(tt)
        if not s or s["n"] == 0:
            continue
        hr = s["hit"] / s["n"] * 100
        roi = s["ret"] / s["stake"] * 100 if s["stake"] else 0
        print(f"{tt:>8} {s['n']:>6} {s['hit']:>5} {hr:>5.1f}% {roi:>6.1f}%")

# ── JRA vs NAR ──
from data.masters.venue_master import JRA_CODES
org_stats = defaultdict(lambda: {"races": set(), "honmei_w": 0, "honmei_p3": 0,
                                   "tan_stake": 0, "tan_ret": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    rid = race.get("race_id", "")
    vc = race.get("venue_code", rid[4:6] if len(rid) >= 6 else "")
    if m not in ("◎", "◉") or rid in org_stats["_seen"]:
        continue
    org = "JRA" if str(vc) in JRA_CODES else "NAR"
    s = org_stats[org]
    if rid not in s["races"]:
        s["races"].add(rid)
        if fin == 1: s["honmei_w"] += 1
        if fin <= 3: s["honmei_p3"] += 1
        s["tan_stake"] += 100
        if fin == 1:
            tan = pay.get("単勝", {})
            if isinstance(tan, dict):
                s["tan_ret"] += tan.get("payout", 0)

print("\n═══ JRA vs NAR（◎/◉の成績）═══")
print(f"{'区分':>4} {'レース':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>6}")
for org in ["JRA", "NAR"]:
    s = org_stats.get(org)
    if not s:
        continue
    n = len(s["races"])
    if n == 0:
        continue
    wr = s["honmei_w"] / n * 100
    p3r = s["honmei_p3"] / n * 100
    roi = s["tan_ret"] / s["tan_stake"] * 100 if s["tan_stake"] else 0
    print(f"{org:>4} {n:>5} {wr:>5.1f}% {p3r:>5.1f}% {roi:>5.1f}%")

# ── 全体サマリー ──
total_races = len(seen_races_conf)
total_honmei_w = sum(s["honmei_w"] for c, s in conf_stats.items())
total_honmei_p3 = sum(s["honmei_p3"] for c, s in conf_stats.items())
total_tan_stake = sum(s["tan_stake"] for c, s in conf_stats.items())
total_tan_ret = sum(s["tan_ret"] for c, s in conf_stats.items())
print("\n═══ 3月全体サマリー ═══")
print(f"対象レース数: {total_races}")
print(f"◎/◉ 勝率: {total_honmei_w}/{total_races} = {total_honmei_w/total_races*100:.1f}%" if total_races else "")
print(f"◎/◉ 複勝率: {total_honmei_p3}/{total_races} = {total_honmei_p3/total_races*100:.1f}%" if total_races else "")
print(f"◎/◉ 単勝回収率: {total_tan_ret}/{total_tan_stake} = {total_tan_ret/total_tan_stake*100:.1f}%" if total_tan_stake else "")
