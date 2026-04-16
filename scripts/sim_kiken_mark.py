#!/usr/bin/env python3
"""
×印改善シミュレーション
過去pred.json + results.jsonを突き合わせ、各改善案の複勝率・対象数を比較する。

スクレイピング不要・DB不要・モデルロード不要。JSONだけで完結。
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START = "20240101"
END = "20260412"

# === 改善案 ===
PLANS = {
    "現行": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "OR", "score_threshold": 3.0, "max_per_race": 2,
    },
    "案1: AND条件化": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "AND", "score_threshold": 3.0, "max_per_race": 2,
    },
    "案2: 閾値5.0pt": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "OR", "score_threshold": 5.0, "max_per_race": 2,
    },
    "案3: 2番人気まで": {
        "pop_limit": 2, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "OR", "score_threshold": 3.0, "max_per_race": 2,
    },
    "案1+2: AND+閾値5pt": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "AND", "score_threshold": 5.0, "max_per_race": 2,
    },
    "案1+3: AND+2人気": {
        "pop_limit": 2, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "AND", "score_threshold": 3.0, "max_per_race": 2,
    },
    "案A: AND+閾値4pt": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.45, "comp_pct": 0.35,
        "logic": "AND", "score_threshold": 4.0, "max_per_race": 2,
    },
    "案B: AND+ML50%+Comp40%": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.50, "comp_pct": 0.40,
        "logic": "AND", "score_threshold": 3.0, "max_per_race": 2,
    },
    "案C: AND+閾値4pt+ML50%": {
        "pop_limit": 3, "odds_limit": 15.0,
        "ml_pct": 0.50, "comp_pct": 0.40,
        "logic": "AND", "score_threshold": 4.0, "max_per_race": 2,
    },
}


def simulate_plan(plan, races_with_finish):
    """1つの改善案をシミュレーション。races_with_finishは(race, finish_map)のリスト"""
    pop_limit = plan["pop_limit"]
    odds_limit = plan["odds_limit"]
    ml_pct = plan["ml_pct"]
    comp_pct = plan["comp_pct"]
    logic = plan["logic"]
    score_thr = plan["score_threshold"]
    max_per = plan["max_per_race"]

    stats = {"total": 0, "win": 0, "p2": 0, "p3": 0, "races_with_x": 0}
    pop_stats = {1: {"n": 0, "p3": 0}, 2: {"n": 0, "p3": 0}, 3: {"n": 0, "p3": 0}}

    for race, finish_map in races_with_finish:
        horses = race.get("horses", [])
        n = len(horses)
        if n < 4:
            continue

        # composite順位・win_prob順位を算出
        sorted_by_wp = sorted(range(n), key=lambda i: horses[i].get("win_prob", 0) or 0, reverse=True)
        sorted_by_comp = sorted(range(n), key=lambda i: horses[i].get("composite", 0) or 0, reverse=True)
        wp_rank = {idx: rank + 1 for rank, idx in enumerate(sorted_by_wp)}
        comp_rank = {idx: rank + 1 for rank, idx in enumerate(sorted_by_comp)}

        wp_threshold = max(3, int(n * ml_pct))
        comp_threshold = max(3, int(n * comp_pct))

        candidates = []
        for i, h in enumerate(horses):
            pop = h.get("popularity")
            odds = h.get("odds")
            if pop is None or pop > pop_limit:
                continue
            if odds is None or odds >= odds_limit:
                continue

            ml_low = wp_rank[i] >= wp_threshold
            comp_low = comp_rank[i] >= comp_threshold

            if logic == "AND":
                if not (ml_low and comp_low):
                    continue
            else:
                if not (ml_low or comp_low):
                    continue

            # 追加スコア（pred.jsonの tokusen_kiken_score を使用）
            tk_score = h.get("tokusen_kiken_score", 0) or 0
            if tk_score < score_thr:
                continue

            candidates.append((i, tk_score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        if candidates:
            stats["races_with_x"] += 1

        for i, _sc in candidates[:max_per]:
            h = horses[i]
            hno = h.get("horse_no")
            fp = finish_map.get(hno)
            if fp is None or fp <= 0:
                continue

            stats["total"] += 1
            if fp == 1:
                stats["win"] += 1
            if fp <= 2:
                stats["p2"] += 1
            if fp <= 3:
                stats["p3"] += 1

            pop = h.get("popularity", 0)
            if pop in pop_stats:
                pop_stats[pop]["n"] += 1
                if fp <= 3:
                    pop_stats[pop]["p3"] += 1

    return stats, pop_stats


# === メイン ===
print(f"{'='*70}")
print(f"  x印改善シミュレーション  {START} -> {END}")
print(f"{'='*70}")

t0 = time.time()

# === 結果データ全読込 ===
result_files = sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json")))
all_results = {}  # race_id -> {horse_no: finish}
loaded_r = 0
for i, fp in enumerate(result_files):
    dt = os.path.basename(fp)[:8]
    if not (START <= dt <= END):
        continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for race_id, data in d.items():
            fm = {}
            for entry in data.get("order", []):
                hno = entry.get("horse_no")
                fin = entry.get("finish")
                if hno is not None and fin is not None:
                    fm[hno] = fin
            all_results[race_id] = fm
        loaded_r += 1
    except Exception:
        pass
    if (i + 1) % 100 == 0 or i == len(result_files) - 1:
        pct = (i + 1) / len(result_files) * 100
        print(f"  [{pct:>5.1f}%] 結果 {i+1}/{len(result_files)} loaded", flush=True)

print(f"  結果データ: {loaded_r}files, {len(all_results)} races")

# === pred.json 読込 & 結果マッチ ===
pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
target_pred = [(f, os.path.basename(f)[:8]) for f in pred_files
               if START <= os.path.basename(f)[:8] <= END
               and '_prev' not in os.path.basename(f)
               and '_backup' not in os.path.basename(f)]

races_with_finish = []  # [(race_dict, finish_map), ...]
total_pred_races = 0
matched_races = 0

for i, (fp, dt) in enumerate(target_pred):
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for race in d.get("races", []):
            total_pred_races += 1
            race_id = race.get("race_id", "")
            if race_id in all_results:
                races_with_finish.append((race, all_results[race_id]))
                matched_races += 1
    except Exception:
        pass
    if (i + 1) % 100 == 0 or i == len(target_pred) - 1:
        pct = (i + 1) / len(target_pred) * 100
        print(f"  [{pct:>5.1f}%] 予想 {i+1}/{len(target_pred)} loaded", flush=True)

load_time = time.time() - t0
print(f"  予想: {total_pred_races}R, 結果マッチ: {matched_races}R, {load_time:.1f}s")

# === 現行×の実績（pred.jsonのmark='×'をそのまま集計） ===
print(f"\n{'='*70}")
print(f"  【参考】現行pred.jsonの x印実績（実データ）")
print(f"{'='*70}")
ax_total = ax_win = ax_p2 = ax_p3 = 0
ax_pop = {1: {"n": 0, "p3": 0}, 2: {"n": 0, "p3": 0}, 3: {"n": 0, "p3": 0}}
for race, finish_map in races_with_finish:
    for h in race.get("horses", []):
        if h.get("mark") == "\u00d7":
            hno = h.get("horse_no")
            fp = finish_map.get(hno)
            if fp and fp > 0:
                ax_total += 1
                if fp == 1: ax_win += 1
                if fp <= 2: ax_p2 += 1
                if fp <= 3: ax_p3 += 1
                pop = h.get("popularity", 0)
                if pop in ax_pop:
                    ax_pop[pop]["n"] += 1
                    if fp <= 3: ax_pop[pop]["p3"] += 1

if ax_total > 0:
    print(f"  x印数(結果あり): {ax_total}")
    print(f"  勝率: {ax_win/ax_total*100:.1f}%  連対率: {ax_p2/ax_total*100:.1f}%  複勝率: {ax_p3/ax_total*100:.1f}%")
    for pop in [1, 2, 3]:
        s = ax_pop[pop]
        if s["n"] > 0:
            print(f"    {pop}番人気: {s['n']}頭, 複勝率{s['p3']/s['n']*100:.1f}%")
        else:
            print(f"    {pop}番人気: 0頭")
else:
    print(f"  x印データなし")

# === 各案シミュレーション ===
print(f"\n{'='*70}")
print(f"  シミュレーション実行中...")
print(f"{'='*70}")

sim_t0 = time.time()
results = {}
plan_names = list(PLANS.keys())
for pi, name in enumerate(plan_names):
    plan = PLANS[name]
    stats, pop_s = simulate_plan(plan, races_with_finish)
    results[name] = (stats, pop_s)
    elapsed = time.time() - sim_t0
    pct = (pi + 1) / len(plan_names) * 100
    print(f"  [{pct:>5.1f}%] {name} ({elapsed:.1f}s)", flush=True)

# === 結果テーブル ===
print(f"\n{'='*70}")
print(f"  シミュレーション結果比較")
print(f"{'='*70}")
print(f"  {'案名':<28s} {'対象数':>6s} {'勝率':>6s} {'連対率':>6s} {'複勝率':>6s} {'x付R':>5s} {'判定':>4s}")
print(f"  {'-'*28} {'-'*6} {'-'*6} {'-'*6} {'-'*6} {'-'*5} {'-'*4}")
for name in plan_names:
    stats, pop_s = results[name]
    t = stats["total"]
    if t > 0:
        wr = stats["win"] / t * 100
        p2r = stats["p2"] / t * 100
        p3r = stats["p3"] / t * 100
        ok = "◎" if p3r < 15.0 else "○" if p3r < 20.0 else "△" if p3r < 25.0 else "×"
    else:
        wr = p2r = p3r = 0
        ok = "-"
    print(f"  {name:<28s} {t:>6d} {wr:>5.1f}% {p2r:>5.1f}% {p3r:>5.1f}% {stats['races_with_x']:>5d} {ok:>4s}")

# === 人気別内訳 ===
print(f"\n{'='*70}")
print(f"  人気別内訳（各案）")
print(f"{'='*70}")
for name in plan_names:
    stats, pop_s = results[name]
    parts = []
    for pop in [1, 2, 3]:
        s = pop_s[pop]
        if s["n"] > 0:
            rate = s["p3"] / s["n"] * 100
            parts.append(f"{pop}人気:{s['n']}頭 P3={rate:.0f}%")
        else:
            parts.append(f"{pop}人気:0頭")
    print(f"  {name:<28s}  {' | '.join(parts)}")

# === ベースライン比較（1-3人気の全馬複勝率） ===
print(f"\n{'='*70}")
print(f"  参考: 1-3番人気の全馬ベースライン複勝率")
print(f"{'='*70}")
base_pop = {1: {"n": 0, "p3": 0}, 2: {"n": 0, "p3": 0}, 3: {"n": 0, "p3": 0}}
for race, finish_map in races_with_finish:
    for h in race.get("horses", []):
        pop = h.get("popularity")
        if pop in base_pop:
            hno = h.get("horse_no")
            fp = finish_map.get(hno)
            if fp and fp > 0:
                base_pop[pop]["n"] += 1
                if fp <= 3:
                    base_pop[pop]["p3"] += 1
for pop in [1, 2, 3]:
    s = base_pop[pop]
    if s["n"] > 0:
        print(f"  {pop}番人気: {s['n']}頭, 複勝率 {s['p3']/s['n']*100:.1f}%")

total_base = sum(s["n"] for s in base_pop.values())
total_base_p3 = sum(s["p3"] for s in base_pop.values())
if total_base > 0:
    print(f"  全体(1-3人気): {total_base}頭, 複勝率 {total_base_p3/total_base*100:.1f}%")

total_time = time.time() - t0
print(f"\n  処理時間: {total_time:.1f}s")
