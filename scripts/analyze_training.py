"""調教データ成績分析"""
import sqlite3, json, sys
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
DB_PATH = "data/keiba.db"

db = sqlite3.connect(DB_PATH)
db.row_factory = sqlite3.Row
cur = db.cursor()

cur.execute("SELECT race_id, horse_no, finish_pos, tansho_odds FROM race_log WHERE finish_pos IS NOT NULL AND finish_pos > 0")
rm = {}
for row in cur:
    rm[(row["race_id"], row["horse_no"])] = {"f": row["finish_pos"], "to": row["tansho_odds"] or 0}

cur.execute("SELECT date, race_id, confidence, field_count, horses_json FROM predictions ORDER BY date, race_id")
all_h = []
for row in cur:
    rid = row["race_id"]
    try:
        horses = json.loads(row["horses_json"]) if row["horses_json"] else []
    except:
        continue
    is_jra = rid[4:6] in ["01","02","03","04","05","06","07","08","09","10"]

    # 各馬の調教スコア計算
    horse_scores = []
    for h in horses:
        hno = h.get("horse_no")
        trec = h.get("training_records") or []
        sigmas = [r.get("sigma_from_mean", 0) for r in trec
                  if isinstance(r, dict) and r.get("sigma_from_mean") is not None and r.get("sigma_from_mean") != 0]
        intensities = [r.get("intensity_label", "") for r in trec if isinstance(r, dict)]
        best_sigma = max(sigmas) if sigmas else None
        avg_sigma = sum(sigmas) / len(sigmas) if sigmas else None
        horse_scores.append((hno, avg_sigma, best_sigma, len(sigmas), intensities))

    # レース内ランク
    scored = [(hno, avg, best, cnt) for hno, avg, best, cnt, _ in horse_scores if avg is not None]
    scored.sort(key=lambda x: x[1], reverse=True)
    rank_map = {}
    for i, (hno, _, _, _) in enumerate(scored):
        rank_map[hno] = i + 1

    for hno, avg_s, best_s, cnt, intensities in horse_scores:
        if hno is None:
            continue
        key = (rid, hno)
        if key not in rm:
            continue
        rl = rm[key]
        h = next((x for x in horses if x.get("horse_no") == hno), {})
        wp = h.get("win_prob") or 0
        odds = h.get("odds")
        tp = int(rl["to"] * 100) if rl["f"] == 1 and rl["to"] > 0 else 0
        strong_count = sum(1 for il in intensities if il in ("一杯", "強め", "仕上がる"))
        all_h.append({
            "date": row["date"], "rid": rid, "is_jra": is_jra,
            "mark": h.get("mark", ""), "wp": wp, "odds": odds,
            "finish": rl["f"], "tp": tp, "pop": h.get("popularity"),
            "avg_sigma": avg_s, "best_sigma": best_s,
            "sigma_count": cnt, "train_rank": rank_map.get(hno),
            "strong_count": strong_count,
            "composite": h.get("composite") or 0,
        })
db.close()


def s(recs):
    n = len(recs)
    if n == 0:
        return None
    w = sum(1 for r in recs if r["finish"] == 1)
    p2 = sum(1 for r in recs if r["finish"] <= 2)
    p3 = sum(1 for r in recs if r["finish"] <= 3)
    t = sum(r["tp"] for r in recs)
    return {"n": n, "wr": w/n*100, "p2r": p2/n*100, "p3r": p3/n*100, "roi": t/(n*100)*100}


with_sigma = [h for h in all_h if h["avg_sigma"] is not None]
without_sigma = [h for h in all_h if h["avg_sigma"] is None]
print(f"全レコード: {len(all_h):,}件")
print(f"調教sigma付き: {len(with_sigma):,}件 ({len(with_sigma)/len(all_h)*100:.1f}%)")
print(f"調教sigmaなし: {len(without_sigma):,}件")

# ============================================================
print("\n" + "=" * 90)
print(" 1. 調教スコア(avg_sigma)帯別成績")
print("=" * 90)
bands = [(-999, -1.0, "<-1.0(低調)"), (-1.0, -0.5, "-1.0~-0.5"), (-0.5, 0, "-0.5~0"),
         (0, 0.5, "0~0.5"), (0.5, 1.0, "0.5~1.0"), (1.0, 1.5, "1.0~1.5"),
         (1.5, 2.0, "1.5~2.0"), (2.0, 999, "2.0+(絶好調)")]
for lo, hi, label in bands:
    sub = [h for h in with_sigma if lo <= h["avg_sigma"] < hi]
    st = s(sub)
    if st and st["n"] >= 30:
        print(f"  sigma {label:<15}: {st['n']:>6,}件 勝{st['wr']:>5.1f}% 連対{st['p2r']:>5.1f}% 複勝{st['p3r']:>5.1f}% 単回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 2. 調教レース内ランク別成績")
print("=" * 90)
for rank in range(1, 10):
    sub = [h for h in with_sigma if h["train_rank"] == rank]
    st = s(sub)
    if st and st["n"] >= 30:
        avg = sum(h["avg_sigma"] for h in sub) / len(sub)
        print(f"  調教{rank}位: {st['n']:>6,}件 avg_sigma={avg:>+.2f} 勝{st['wr']:>5.1f}% 連対{st['p2r']:>5.1f}% 複勝{st['p3r']:>5.1f}% 単回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 3. 調教1位 × 印 クロス分析")
print("=" * 90)
t1 = [h for h in with_sigma if h["train_rank"] == 1]
for mark in ["◉", "◎", "○", "▲", "△", "★", "☆", "×"]:
    sub = [h for h in t1 if h["mark"] == mark]
    st = s(sub)
    if st and st["n"] >= 20:
        print(f"  調教1位×{mark}: {st['n']:>5,}件 勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>6.1f}%")

print("\n  調教1位 × 人気:")
for pop in range(1, 11):
    sub = [h for h in t1 if h.get("pop") == pop]
    st = s(sub)
    if st and st["n"] >= 30:
        print(f"    {pop}番人気: {st['n']:>5,}件 勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 4. JRA/NAR別の調教ランク成績")
print("=" * 90)
for scope, fn in [("JRA", lambda h: h["is_jra"]), ("NAR", lambda h: not h["is_jra"])]:
    sub_scope = [h for h in with_sigma if fn(h)]
    print(f"\n  [{scope}] (sigma付き{len(sub_scope):,}件)")
    for rank in range(1, 6):
        sub = [h for h in sub_scope if h["train_rank"] == rank]
        st = s(sub)
        if st and st["n"] >= 30:
            avg = sum(h["avg_sigma"] for h in sub) / len(sub)
            print(f"    調教{rank}位: {st['n']:>5,}件 sigma={avg:>+.2f} 勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 5. best_sigma(最高調教値)帯別成績")
print("=" * 90)
bands2 = [(-999, 0, "<0(全て平均以下)"), (0, 0.5, "0~0.5"), (0.5, 1.0, "0.5~1.0"),
          (1.0, 1.5, "1.0~1.5"), (1.5, 2.0, "1.5~2.0"), (2.0, 3.0, "2.0~3.0"), (3.0, 999, "3.0+(飛び抜け)")]
for lo, hi, label in bands2:
    sub = [h for h in with_sigma if h["best_sigma"] is not None and lo <= h["best_sigma"] < hi]
    st = s(sub)
    if st and st["n"] >= 30:
        print(f"  best_sigma {label:<16}: {st['n']:>6,}件 勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 6. 調教1位が◎◉と一致 vs 不一致")
print("=" * 90)
top_mark_h = [h for h in with_sigma if h["mark"] in ("◉", "◎")]
match = [h for h in top_mark_h if h["train_rank"] == 1]
mismatch = [h for h in top_mark_h if h["train_rank"] is not None and h["train_rank"] != 1]
st_m = s(match)
st_mm = s(mismatch)
if st_m:
    print(f"  ◎◉かつ調教1位:   {st_m['n']:>6,}件 勝{st_m['wr']:>5.1f}% 複{st_m['p3r']:>5.1f}% 回{st_m['roi']:>6.1f}%")
if st_mm:
    print(f"  ◎◉かつ調教2位以下: {st_mm['n']:>6,}件 勝{st_mm['wr']:>5.1f}% 複{st_mm['p3r']:>5.1f}% 回{st_mm['roi']:>6.1f}%")

t1_low = [h for h in t1 if h["mark"] in ("△", "★", "☆", "×", "")]
st_low = s(t1_low)
if st_low:
    print(f"  調教1位だが△以下: {st_low['n']:>6,}件 勝{st_low['wr']:>5.1f}% 複{st_low['p3r']:>5.1f}% 回{st_low['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 7. 調教強度(intensity_label)別成績")
print("=" * 90)
# intensity_labelの分布を確認
cur2 = sqlite3.connect(DB_PATH)
cur2.row_factory = sqlite3.Row
c2 = cur2.cursor()
c2.execute("SELECT horses_json FROM predictions WHERE date >= '2025-01-01' LIMIT 3000")
intensity_map = defaultdict(int)
for row in c2:
    try:
        horses = json.loads(row["horses_json"])
    except:
        continue
    for h in horses:
        for r in (h.get("training_records") or []):
            il = r.get("intensity_label", "")
            if il:
                intensity_map[il] += 1
cur2.close()
print("  調教強度ラベル分布:")
for label, cnt in sorted(intensity_map.items(), key=lambda x: -x[1]):
    print(f"    {label}: {cnt:,}件")

# ============================================================
print("\n" + "=" * 90)
print(" 8. 調教1位 × EV帯 クロス分析")
print("=" * 90)
for ev_lo, ev_hi, ev_label in [(0, 0.8, "EV<0.8"), (0.8, 1.0, "EV 0.8-1.0"), (1.0, 1.5, "EV 1.0-1.5"), (1.5, 999, "EV>=1.5")]:
    sub = [h for h in t1 if h.get("odds") and h["wp"] > 0 and ev_lo <= h["wp"] * h["odds"] < ev_hi]
    st = s(sub)
    if st and st["n"] >= 20:
        print(f"  調教1位×{ev_label:<12}: {st['n']:>5,}件 勝{st['wr']:>5.1f}% 複{st['p3r']:>5.1f}% 回{st['roi']:>6.1f}%")

# ============================================================
print("\n" + "=" * 90)
print(" 9. 時期別カバレッジ（月別sigma付き率）")
print("=" * 90)
months = defaultdict(lambda: [0, 0])
for h in all_h:
    m = h["date"][:7]
    months[m][0] += 1
    if h["avg_sigma"] is not None:
        months[m][1] += 1
for m in sorted(months.keys()):
    if m >= "2024-06":
        total, with_t = months[m]
        pct = with_t / total * 100 if total > 0 else 0
        print(f"  {m}: {total:>5,}件中 {with_t:>5,}件 ({pct:.1f}%)")
