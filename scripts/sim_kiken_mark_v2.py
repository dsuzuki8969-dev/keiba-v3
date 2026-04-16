#!/usr/bin/env python3
"""
×印改善シミュレーション v2
win_probの絶対値ベースで×判定する新方式を検証。

現行方式の問題: 順位ベース(上位45%/35%)では閾値が甘すぎる
新方式: 「人気に対してwin_probが極端に低い」馬を直接検出
"""
import json, glob, os, sys, io, time
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace')

PRED_DIR = "data/predictions"
RESULT_DIR = "data/results"
START = "20240101"
END = "20260412"

# === 人気馬の期待win_prob（ベースライン） ===
# 1番人気の平均勝率≒25-30%, 2番人気≒15-18%, 3番人気≒10-13%
# ×候補: 期待値の半分以下しかMLが評価していない馬

PLANS_V2 = {
    "v2A: wp<期待値*0.5+AND+スコア3": {
        "wp_ratio": 0.50,  # 期待win_probの50%未満
        "logic": "AND", "score_thr": 3.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2B: wp<期待値*0.4+AND+スコア3": {
        "wp_ratio": 0.40,
        "logic": "AND", "score_thr": 3.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2C: wp<期待値*0.3+AND+スコア3": {
        "wp_ratio": 0.30,
        "logic": "AND", "score_thr": 3.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2D: wp<期待値*0.4+スコア4": {
        "wp_ratio": 0.40,
        "logic": "AND", "score_thr": 4.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2E: wp<期待値*0.3+スコア4": {
        "wp_ratio": 0.30,
        "logic": "AND", "score_thr": 4.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2F: wp<期待値*0.3+スコア5": {
        "wp_ratio": 0.30,
        "logic": "AND", "score_thr": 5.0,
        "pop_limit": 3, "odds_limit": 15.0,
    },
    "v2G: comp下位30%+wp<期待*0.4+AND": {
        "wp_ratio": 0.40,
        "logic": "AND_STRICT", "score_thr": 3.0,
        "pop_limit": 3, "odds_limit": 15.0,
        "comp_pct": 0.30,  # 下位30%
    },
    "v2H: comp下位25%+wp<期待*0.3": {
        "wp_ratio": 0.30,
        "logic": "AND_STRICT", "score_thr": 3.0,
        "pop_limit": 3, "odds_limit": 15.0,
        "comp_pct": 0.25,
    },
}


def simulate_v2(plan, races_with_finish, expected_wp):
    """新方式シミュレーション"""
    wp_ratio = plan["wp_ratio"]
    score_thr = plan["score_thr"]
    pop_limit = plan["pop_limit"]
    odds_limit = plan["odds_limit"]
    logic = plan["logic"]
    comp_pct_strict = plan.get("comp_pct", 0.35)

    stats = {"total": 0, "win": 0, "p2": 0, "p3": 0, "races_with_x": 0}
    pop_stats = {1: {"n": 0, "p3": 0}, 2: {"n": 0, "p3": 0}, 3: {"n": 0, "p3": 0}}

    for race, finish_map in races_with_finish:
        horses = race.get("horses", [])
        n = len(horses)
        if n < 4:
            continue

        # composite順位
        sorted_by_comp = sorted(range(n), key=lambda i: horses[i].get("composite", 0) or 0, reverse=True)
        comp_rank = {idx: rank + 1 for rank, idx in enumerate(sorted_by_comp)}
        comp_threshold = max(3, int(n * comp_pct_strict))

        candidates = []
        for i, h in enumerate(horses):
            pop = h.get("popularity")
            odds = h.get("odds")
            if pop is None or pop > pop_limit:
                continue
            if odds is None or odds >= odds_limit:
                continue

            wp = h.get("win_prob", 0) or 0
            expected = expected_wp.get(pop, 0.10)

            # 新条件: win_probが期待値のwp_ratio倍未満
            if wp >= expected * wp_ratio:
                continue

            # composite条件
            comp_low = comp_rank[i] >= comp_threshold
            if logic in ("AND", "AND_STRICT") and not comp_low:
                continue

            # 追加スコア
            tk_score = h.get("tokusen_kiken_score", 0) or 0
            if tk_score < score_thr:
                continue

            candidates.append((i, tk_score))

        candidates.sort(key=lambda x: x[1], reverse=True)
        if candidates:
            stats["races_with_x"] += 1

        for i, _sc in candidates[:2]:
            h = horses[i]
            hno = h.get("horse_no")
            fp = finish_map.get(hno)
            if fp is None or fp <= 0:
                continue

            stats["total"] += 1
            if fp == 1: stats["win"] += 1
            if fp <= 2: stats["p2"] += 1
            if fp <= 3: stats["p3"] += 1

            pop = h.get("popularity", 0)
            if pop in pop_stats:
                pop_stats[pop]["n"] += 1
                if fp <= 3: pop_stats[pop]["p3"] += 1

    return stats, pop_stats


# === メイン ===
print(f"{'='*70}")
print(f"  x印改善シミュレーション v2（win_probベース）")
print(f"  期間: {START} -> {END}")
print(f"{'='*70}")

t0 = time.time()

# 結果データ読込
result_files = sorted(glob.glob(os.path.join(RESULT_DIR, "*_results.json")))
all_results = {}
for fp in result_files:
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
    except Exception:
        pass
print(f"  結果: {len(all_results)} races")

# pred.json読込
pred_files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
races_with_finish = []
for fp in pred_files:
    bn = os.path.basename(fp)
    dt = bn[:8]
    if not (START <= dt <= END) or '_prev' in bn or '_backup' in bn:
        continue
    try:
        d = json.load(open(fp, 'r', encoding='utf-8'))
        for race in d.get("races", []):
            race_id = race.get("race_id", "")
            if race_id in all_results:
                races_with_finish.append((race, all_results[race_id]))
    except Exception:
        pass
print(f"  予想(結果付き): {len(races_with_finish)}R")

# === ベースライン: 人気別の平均win_prob（MLの期待値）===
print(f"\n  人気別ML win_prob平均を算出中...")
pop_wp_sum = {1: 0, 2: 0, 3: 0}
pop_wp_n = {1: 0, 2: 0, 3: 0}
pop_win_sum = {1: 0, 2: 0, 3: 0}
pop_total = {1: 0, 2: 0, 3: 0}
for race, finish_map in races_with_finish:
    for h in race.get("horses", []):
        pop = h.get("popularity")
        if pop in pop_wp_sum:
            wp = h.get("win_prob", 0) or 0
            pop_wp_sum[pop] += wp
            pop_wp_n[pop] += 1
            hno = h.get("horse_no")
            fp = finish_map.get(hno, 99)
            pop_total[pop] += 1
            if fp == 1:
                pop_win_sum[pop] += 1

expected_wp = {}
for pop in [1, 2, 3]:
    avg_wp = pop_wp_sum[pop] / max(1, pop_wp_n[pop])
    actual_win = pop_win_sum[pop] / max(1, pop_total[pop])
    expected_wp[pop] = avg_wp
    print(f"    {pop}番人気: 平均wp={avg_wp*100:.1f}%, 実勝率={actual_win*100:.1f}%, 馬数={pop_wp_n[pop]}")

# === シミュレーション ===
print(f"\n{'='*70}")
print(f"  シミュレーション実行中...")
print(f"{'='*70}")

sim_t0 = time.time()
results = {}
plan_names = list(PLANS_V2.keys())
for pi, name in enumerate(plan_names):
    plan = PLANS_V2[name]
    stats, pop_s = simulate_v2(plan, races_with_finish, expected_wp)
    results[name] = (stats, pop_s)
    pct = (pi + 1) / len(plan_names) * 100
    print(f"  [{pct:>5.1f}%] {name} ({time.time()-sim_t0:.1f}s)", flush=True)

# === 結果テーブル ===
print(f"\n{'='*70}")
print(f"  結果比較（v2 win_probベース方式）")
print(f"{'='*70}")
print(f"  {'案名':<38s} {'対象':>5s} {'勝率':>5s} {'連対':>5s} {'複勝':>5s} {'判定':>4s}")
print(f"  {'-'*38} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*4}")
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
    print(f"  {name:<38s} {t:>5d} {wr:>4.1f}% {p2r:>4.1f}% {p3r:>4.1f}% {ok:>4s}")

# === 人気別 ===
print(f"\n  人気別内訳:")
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
    print(f"    {name:<38s}  {' | '.join(parts)}")

# === 現行×との直接比較 ===
print(f"\n{'='*70}")
print(f"  現行×からの改善幅")
print(f"{'='*70}")
# 現行の数値
ax_total = ax_p3 = 0
for race, fm in races_with_finish:
    for h in race.get("horses", []):
        if h.get("mark") == "\u00d7":
            hno = h.get("horse_no")
            fp = fm.get(hno)
            if fp and fp > 0:
                ax_total += 1
                if fp <= 3: ax_p3 += 1
current_p3 = ax_p3 / max(1, ax_total) * 100
print(f"  現行×: {ax_total}頭, 複勝率 {current_p3:.1f}%")
for name in plan_names:
    stats, _ = results[name]
    t = stats["total"]
    if t > 0:
        p3r = stats["p3"] / t * 100
        diff = p3r - current_p3
        print(f"  {name:<38s}: {t}頭, 複勝率 {p3r:.1f}% ({diff:+.1f}pt)")

total_time = time.time() - t0
print(f"\n  処理時間: {total_time:.1f}s")
