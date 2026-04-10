"""3月 オッズ乖離（期待値）分析"""
import sys, os, json, glob
from collections import defaultdict
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout.reconfigure(encoding="utf-8")

PRED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "predictions")
RES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "results")

def load_month(ym="202603"):
    records = []
    for pf in sorted(glob.glob(os.path.join(PRED_DIR, f"{ym}*_pred.json"))):
        dk = os.path.basename(pf)[:8]
        rf = os.path.join(RES_DIR, f"{dk}_results.json")
        if not os.path.isfile(rf):
            continue
        try:
            with open(pf, "r", encoding="utf-8") as f:
                pred = json.load(f)
            with open(rf, "r", encoding="utf-8") as f:
                res = json.load(f)
        except Exception:
            continue
        for race in pred.get("races", []):
            rid = race.get("race_id", "")
            r = res.get(rid)
            if not r or not r.get("order"):
                continue
            fmap = {o["horse_no"]: o["finish"] for o in r["order"]}
            pay = r.get("payouts", {})
            for h in race.get("horses", []):
                hno = h.get("horse_no", 0)
                fin = fmap.get(hno, 99)
                records.append((race, h, fin, pay))
    return records

records = load_month()

# ── 期待値(EV)計算 ──
# EV = win_prob × odds
# EV > 1.0 なら理論上プラス期待値

print("=" * 70)
print("3月 オッズ乖離（期待値）詳細分析")
print("=" * 70)

# ── 1. 全◎/◉のEV分布と成績 ──
ev_buckets = defaultdict(lambda: {"n": 0, "w": 0, "p3": 0,
                                   "tan_s": 0, "tan_r": 0,
                                   "fuku_s": 0, "fuku_r": 0})
all_ev_data = []
for race, h, fin, pay in records:
    m = h.get("mark", "")
    if m not in ("◎", "◉"):
        continue
    wp = h.get("win_prob", 0)
    odds = h.get("odds", 0)
    if not wp or not odds or odds <= 0:
        continue
    ev = wp * odds
    all_ev_data.append((ev, wp, odds, fin, h, pay, m, race))

    # バケット分類
    if ev >= 2.0:   bk = "EV≥2.0 (超割安)"
    elif ev >= 1.5: bk = "EV 1.5-2.0 (割安)"
    elif ev >= 1.2: bk = "EV 1.2-1.5 (やや割安)"
    elif ev >= 1.0: bk = "EV 1.0-1.2 (均衡)"
    elif ev >= 0.8: bk = "EV 0.8-1.0 (やや割高)"
    elif ev >= 0.5: bk = "EV 0.5-0.8 (割高)"
    else:           bk = "EV<0.5 (超割高)"

    s = ev_buckets[bk]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    if fin <= 3: s["p3"] += 1
    s["tan_s"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tan_r"] += tan.get("payout", 0)
    s["fuku_s"] += 100
    if fin <= 3:
        fuku = pay.get("複勝", [])
        if isinstance(fuku, list):
            for fp in fuku:
                if fp.get("combo") == str(h.get("horse_no")):
                    s["fuku_r"] += fp.get("payout", 0)

print(f"\n■ ◎/◉のEV帯別成績（{len(all_ev_data)}件）")
print(f"{'EV帯':>22} {'件数':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>7} {'複回収':>7} {'平均EV':>6}")
bk_order = ["EV≥2.0 (超割安)", "EV 1.5-2.0 (割安)", "EV 1.2-1.5 (やや割安)",
            "EV 1.0-1.2 (均衡)", "EV 0.8-1.0 (やや割高)", "EV 0.5-0.8 (割高)", "EV<0.5 (超割高)"]
for bk in bk_order:
    s = ev_buckets.get(bk)
    if not s or s["n"] == 0:
        continue
    wr = s["w"] / s["n"] * 100
    p3 = s["p3"] / s["n"] * 100
    tr = s["tan_r"] / s["tan_s"] * 100 if s["tan_s"] else 0
    fr = s["fuku_r"] / s["fuku_s"] * 100 if s["fuku_s"] else 0
    # 平均EV
    evs = [e[0] for e in all_ev_data if
           (bk == "EV≥2.0 (超割安)" and e[0] >= 2.0) or
           (bk == "EV 1.5-2.0 (割安)" and 1.5 <= e[0] < 2.0) or
           (bk == "EV 1.2-1.5 (やや割安)" and 1.2 <= e[0] < 1.5) or
           (bk == "EV 1.0-1.2 (均衡)" and 1.0 <= e[0] < 1.2) or
           (bk == "EV 0.8-1.0 (やや割高)" and 0.8 <= e[0] < 1.0) or
           (bk == "EV 0.5-0.8 (割高)" and 0.5 <= e[0] < 0.8) or
           (bk == "EV<0.5 (超割高)" and e[0] < 0.5)]
    avg_ev = sum(evs) / len(evs) if evs else 0
    print(f"{bk:>22} {s['n']:>5} {wr:>5.1f}% {p3:>5.1f}% {tr:>6.1f}% {fr:>6.1f}% {avg_ev:>5.2f}")

# EV≥1.0 vs EV<1.0 サマリー
ev_above = {"n": 0, "w": 0, "p3": 0, "ts": 0, "tr": 0}
ev_below = {"n": 0, "w": 0, "p3": 0, "ts": 0, "tr": 0}
for ev, wp, odds, fin, h, pay, m, race in all_ev_data:
    tgt = ev_above if ev >= 1.0 else ev_below
    tgt["n"] += 1
    if fin == 1: tgt["w"] += 1
    if fin <= 3: tgt["p3"] += 1
    tgt["ts"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            tgt["tr"] += tan.get("payout", 0)

print(f"\n{'─'*50}")
for label, s in [("EV≥1.0 (割安側)", ev_above), ("EV<1.0 (割高側)", ev_below)]:
    if s["n"] == 0:
        continue
    print(f"  {label}: {s['n']}件 勝率{s['w']/s['n']*100:.1f}% 複勝率{s['p3']/s['n']*100:.1f}% 単回収{s['tr']/s['ts']*100:.1f}%")

# ── 2. ◉限定のEV分析 ──
print(f"\n\n■ ◉限定のEV帯別成績")
tekipan_ev = defaultdict(lambda: {"n": 0, "w": 0, "p3": 0, "ts": 0, "tr": 0})
for ev, wp, odds, fin, h, pay, m, race in all_ev_data:
    if m != "◉":
        continue
    if ev >= 1.5:   bk = "EV≥1.5"
    elif ev >= 1.0: bk = "EV 1.0-1.5"
    elif ev >= 0.7: bk = "EV 0.7-1.0"
    else:           bk = "EV<0.7"
    s = tekipan_ev[bk]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    if fin <= 3: s["p3"] += 1
    s["ts"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tr"] += tan.get("payout", 0)

print(f"{'EV帯':>12} {'件数':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>7}")
for bk in ["EV≥1.5", "EV 1.0-1.5", "EV 0.7-1.0", "EV<0.7"]:
    s = tekipan_ev.get(bk)
    if not s or s["n"] == 0:
        continue
    wr = s["w"] / s["n"] * 100
    p3 = s["p3"] / s["n"] * 100
    tr = s["tr"] / s["ts"] * 100 if s["ts"] else 0
    print(f"{bk:>12} {s['n']:>5} {wr:>5.1f}% {p3:>5.1f}% {tr:>6.1f}%")

# ── 3. 人気帯×EV帯クロス ──
print(f"\n\n■ 人気帯 × EV帯 クロス集計（◎/◉、単勝回収率%）")
cross = defaultdict(lambda: {"n": 0, "w": 0, "ts": 0, "tr": 0})
for ev, wp, odds, fin, h, pay, m, race in all_ev_data:
    pop = h.get("popularity", 0)
    if pop <= 1: pk = "1人気"
    elif pop <= 3: pk = "2-3人気"
    elif pop <= 6: pk = "4-6人気"
    else: pk = "7人気~"
    ek = "EV≥1.0" if ev >= 1.0 else "EV<1.0"
    key = (pk, ek)
    s = cross[key]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    s["ts"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tr"] += tan.get("payout", 0)

print(f"{'':>10} {'EV≥1.0':>24} {'EV<1.0':>24}")
print(f"{'':>10} {'件数':>5} {'勝率':>6} {'単回収':>7}   {'件数':>5} {'勝率':>6} {'単回収':>7}")
for pk in ["1人気", "2-3人気", "4-6人気", "7人気~"]:
    a = cross.get((pk, "EV≥1.0"), {"n": 0, "w": 0, "ts": 0, "tr": 0})
    b = cross.get((pk, "EV<1.0"), {"n": 0, "w": 0, "ts": 0, "tr": 0})
    def fmt(s):
        if s["n"] == 0:
            return "    -     -       -"
        wr = s["w"] / s["n"] * 100
        roi = s["tr"] / s["ts"] * 100 if s["ts"] else 0
        return f"{s['n']:>5} {wr:>5.1f}% {roi:>6.1f}%"
    print(f"{pk:>10} {fmt(a)}   {fmt(b)}")

# ── 4. win_prob精度検証（キャリブレーション） ──
print(f"\n\n■ win_prob キャリブレーション（予測確率 vs 実際の勝率）")
cal_buckets = defaultdict(lambda: {"n": 0, "w": 0})
for race, h, fin, pay in records:
    wp = h.get("win_prob", 0)
    if not wp:
        continue
    if wp >= 0.40:   bk = "40%+"
    elif wp >= 0.30: bk = "30-40%"
    elif wp >= 0.20: bk = "20-30%"
    elif wp >= 0.15: bk = "15-20%"
    elif wp >= 0.10: bk = "10-15%"
    elif wp >= 0.05: bk = "5-10%"
    else:            bk = "<5%"
    s = cal_buckets[bk]
    s["n"] += 1
    if fin == 1: s["w"] += 1

print(f"{'予測確率帯':>10} {'件数':>6} {'実勝率':>7} {'乖離':>7}")
for bk, mid in [("<5%", 2.5), ("5-10%", 7.5), ("10-15%", 12.5),
                ("15-20%", 17.5), ("20-30%", 25.0), ("30-40%", 35.0), ("40%+", 50.0)]:
    s = cal_buckets.get(bk)
    if not s or s["n"] == 0:
        continue
    actual = s["w"] / s["n"] * 100
    diff = actual - mid
    bar = "+" if diff > 0 else ""
    print(f"{bk:>10} {s['n']:>6} {actual:>6.1f}% {bar}{diff:>5.1f}pt")

# ── 5. オッズ乖離度（odds_divergence）との関係 ──
print(f"\n\n■ odds_divergence（予測オッズとの乖離度）と成績")
div_buckets = defaultdict(lambda: {"n": 0, "w": 0, "p3": 0, "ts": 0, "tr": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    if m not in ("◎", "◉"):
        continue
    div = h.get("odds_divergence", None)
    if div is None:
        continue
    # odds_divergence > 0 = 実オッズが予測より高い（過小評価＝割安）
    if div >= 5.0:    bk = "乖離+5以上(大幅割安)"
    elif div >= 2.0:  bk = "乖離+2~5(割安)"
    elif div >= 0.0:  bk = "乖離 0~2(やや割安)"
    elif div >= -2.0: bk = "乖離-2~0(やや割高)"
    else:             bk = "乖離-2以下(割高)"
    s = div_buckets[bk]
    s["n"] += 1
    if fin == 1: s["w"] += 1
    if fin <= 3: s["p3"] += 1
    s["ts"] += 100
    if fin == 1:
        tan = pay.get("単勝", {})
        if isinstance(tan, dict):
            s["tr"] += tan.get("payout", 0)

print(f"{'乖離度帯':>20} {'件数':>5} {'勝率':>6} {'複勝率':>6} {'単回収':>7}")
for bk in ["乖離+5以上(大幅割安)", "乖離+2~5(割安)", "乖離 0~2(やや割安)",
           "乖離-2~0(やや割高)", "乖離-2以下(割高)"]:
    s = div_buckets.get(bk)
    if not s or s["n"] == 0:
        continue
    wr = s["w"] / s["n"] * 100
    p3 = s["p3"] / s["n"] * 100
    tr = s["tr"] / s["ts"] * 100 if s["ts"] else 0
    print(f"{bk:>20} {s['n']:>5} {wr:>5.1f}% {p3:>5.1f}% {tr:>6.1f}%")

# ── 6. 「もしEV≥1.0の馬だけ◉にしていたら」シミュレーション ──
print(f"\n\n■ シミュレーション: EV≥1.0の◎/◉だけに単勝100円賭けた場合")
for threshold in [0.8, 1.0, 1.2, 1.5, 2.0]:
    n = w = ts = tr = 0
    for ev, wp, odds, fin, h, pay, m, race in all_ev_data:
        if ev < threshold:
            continue
        n += 1
        ts += 100
        if fin == 1:
            w += 1
            tan = pay.get("単勝", {})
            if isinstance(tan, dict):
                tr += tan.get("payout", 0)
    if n == 0:
        continue
    wr = w / n * 100
    roi = tr / ts * 100 if ts else 0
    pnl = tr - ts
    print(f"  EV≥{threshold:.1f}: {n:>4}件 勝率{wr:>5.1f}% 単回収{roi:>6.1f}% 損益{pnl:>+7}円")

# ── 7. 全印のEV分析（◎/◉以外も含む） ──
print(f"\n\n■ 全印のEV≥1.0該当率と成績")
mark_ev = defaultdict(lambda: {"total": 0, "ev_above": 0, "ev_above_w": 0,
                                "ev_above_ts": 0, "ev_above_tr": 0})
for race, h, fin, pay in records:
    m = h.get("mark", "")
    if not m:
        continue
    wp = h.get("win_prob", 0)
    odds = h.get("odds", 0)
    if not wp or not odds or odds <= 0:
        continue
    ev = wp * odds
    s = mark_ev[m]
    s["total"] += 1
    if ev >= 1.0:
        s["ev_above"] += 1
        if fin == 1: s["ev_above_w"] += 1
        s["ev_above_ts"] += 100
        if fin == 1:
            tan = pay.get("単勝", {})
            if isinstance(tan, dict):
                s["ev_above_tr"] += tan.get("payout", 0)

print(f"{'印':>2} {'全件':>5} {'EV≥1件':>6} {'該当率':>6} {'EV≥1勝率':>8} {'EV≥1回収':>8}")
for m in ["◉", "◎", "○", "▲", "△", "★"]:
    s = mark_ev.get(m)
    if not s or s["total"] == 0:
        continue
    pct = s["ev_above"] / s["total"] * 100
    wr = s["ev_above_w"] / s["ev_above"] * 100 if s["ev_above"] else 0
    roi = s["ev_above_tr"] / s["ev_above_ts"] * 100 if s["ev_above_ts"] else 0
    print(f"{m:>2} {s['total']:>5} {s['ev_above']:>6} {pct:>5.1f}% {wr:>7.1f}% {roi:>7.1f}%")
