"""
改善施策の回帰テスト — 過去データで改善前/後を比較
改善後の印付与ロジックをシミュレーションし、◎複勝率の変化を算出
"""
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PRED_DIR = Path("data/predictions")
RESULT_DIR = Path("data/results")
JRA_VENUES = {"01","02","03","04","05","06","07","08","09","10"}

# 改善後の設定値
NEW_TEKIPAN_GAP_JRA = 0.5
NEW_TEKIPAN_GAP_NAR = 3.0
NEW_TEKIPAN_WP_JRA = 0.25
NEW_TEKIPAN_WP_NAR = 0.25
NEW_TEKIPAN_P3_JRA = 0.50
NEW_TEKIPAN_P3_NAR = 0.0
NEW_KIKEN_POP_LIMIT_JRA = 3


def load_data():
    results_map = {}
    for f in sorted(RESULT_DIR.glob("*_results.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race_id, rdata in data.items():
                finish_map = {}
                for o in rdata.get("order", []):
                    finish_map[o["horse_no"]] = o.get("finish", 99)
                results_map[race_id] = finish_map
        except Exception:
            continue

    races = []
    for f in sorted(PRED_DIR.glob("*_pred.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for race in data.get("races", []):
                rid = race.get("race_id", "")
                if rid not in results_map:
                    continue
                for h in race.get("horses", []):
                    h["finish_pos"] = results_map[rid].get(h.get("horse_no"), 99)
                races.append(race)
        except Exception:
            continue
    return races


def _venue_code(race):
    vc = race.get("venue_code", "")
    if not vc:
        rid = race.get("race_id", "")
        vc = rid[4:6] if len(rid) >= 6 else ""
    return vc


def simulate_old(races):
    """現行ロジックでの印判定をシミュレーション"""
    stats = {"tekipan": {"total":0,"p3":0}, "honmei": {"total":0,"p3":0},
             "all_honmei": {"total":0,"p3":0}, "kiken": {"total":0,"fell":0}}

    for race in races:
        horses = race.get("horses", [])
        for h in horses:
            mk = h.get("mark", "")
            fp = h["finish_pos"]
            if mk == "◉":
                stats["tekipan"]["total"] += 1
                if fp <= 3: stats["tekipan"]["p3"] += 1
                stats["all_honmei"]["total"] += 1
                if fp <= 3: stats["all_honmei"]["p3"] += 1
            elif mk == "◎":
                stats["honmei"]["total"] += 1
                if fp <= 3: stats["honmei"]["p3"] += 1
                stats["all_honmei"]["total"] += 1
                if fp <= 3: stats["all_honmei"]["p3"] += 1
            elif mk == "×":
                stats["kiken"]["total"] += 1
                if fp >= 4: stats["kiken"]["fell"] += 1
    return stats


def simulate_new(races):
    """改善後ロジックでの印判定をシミュレーション"""
    stats = {"tekipan": {"total":0,"p3":0}, "honmei": {"total":0,"p3":0},
             "all_honmei": {"total":0,"p3":0}, "kiken": {"total":0,"fell":0}}

    for race in races:
        horses = race.get("horses", [])
        if not horses:
            continue
        vc = _venue_code(race)
        is_jra = vc in JRA_VENUES

        # composite順ソート
        sorted_by_comp = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
        # win_prob順ソート
        wp_top = max(horses, key=lambda h: h.get("win_prob", 0) or 0)
        comp_top = sorted_by_comp[0]

        # --- 施策1: ML合意チェック ---
        ml_agrees = (comp_top.get("horse_no") == wp_top.get("horse_no"))
        if ml_agrees:
            top = comp_top
        else:
            # win_prob1位のcompositeランク確認
            comp_rank = next(
                (i for i, h in enumerate(sorted_by_comp) if h.get("horse_no") == wp_top.get("horse_no")),
                99,
            )
            # 施策2: reliability Cチェック
            comp_rel = comp_top.get("ability_reliability", "A")
            if comp_rank <= 4 or comp_rel in ("C", "D"):
                top = wp_top
            else:
                top = comp_top

        fp = top["finish_pos"]
        second = sorted_by_comp[1] if len(sorted_by_comp) >= 2 else None
        gap = (comp_top.get("composite", 0) - second.get("composite", 0)) if second else 99.0

        # --- 施策3: ◉判定（緩和B） ---
        tekipan_gap = NEW_TEKIPAN_GAP_JRA if is_jra else NEW_TEKIPAN_GAP_NAR
        tekipan_wp = NEW_TEKIPAN_WP_JRA if is_jra else NEW_TEKIPAN_WP_NAR
        tekipan_p3 = NEW_TEKIPAN_P3_JRA if is_jra else NEW_TEKIPAN_P3_NAR

        is_tekipan = (
            gap >= tekipan_gap
            and (top.get("win_prob", 0) or 0) >= tekipan_wp
            and (top.get("place3_prob", 0) or 0) >= tekipan_p3
        )

        if is_tekipan:
            stats["tekipan"]["total"] += 1
            if fp <= 3: stats["tekipan"]["p3"] += 1
        else:
            stats["honmei"]["total"] += 1
            if fp <= 3: stats["honmei"]["p3"] += 1

        stats["all_honmei"]["total"] += 1
        if fp <= 3: stats["all_honmei"]["p3"] += 1

        # --- 施策5: ×危険馬（3人気以下限定） ---
        pop_limit = NEW_KIKEN_POP_LIMIT_JRA if is_jra else 3
        for h in horses:
            if h.get("is_tokusen_kiken"):
                pop = h.get("popularity") or 99
                if pop <= pop_limit:
                    stats["kiken"]["total"] += 1
                    if h["finish_pos"] >= 4: stats["kiken"]["fell"] += 1

    return stats


def main():
    print("="*70)
    print("D-AI Keiba v3 — 改善施策 回帰テスト")
    print("="*70)

    races = load_data()
    print(f"照合済みレース: {len(races)}")

    old = simulate_old(races)
    new = simulate_new(races)

    print("\n" + "="*70)
    print("比較結果")
    print("="*70)

    def pct(s, k):
        return s[k]["p3"] / s[k]["total"] * 100 if s[k]["total"] else 0

    def fell_pct(s):
        return s["kiken"]["fell"] / s["kiken"]["total"] * 100 if s["kiken"]["total"] else 0

    print(f"\n{'指標':>20} {'改善前':>10} {'改善後':>10} {'差分':>10}")
    print("-" * 55)

    metrics = [
        ("◉ 複勝率", pct(old, "tekipan"), pct(new, "tekipan")),
        ("◉ 件数", old["tekipan"]["total"], new["tekipan"]["total"]),
        ("◎ 複勝率", pct(old, "honmei"), pct(new, "honmei")),
        ("◎ 件数", old["honmei"]["total"], new["honmei"]["total"]),
        ("◎+◉ 複勝率", pct(old, "all_honmei"), pct(new, "all_honmei")),
        ("◎+◉ 件数", old["all_honmei"]["total"], new["all_honmei"]["total"]),
        ("× 4着以下率", fell_pct(old), fell_pct(new)),
        ("× 件数", old["kiken"]["total"], new["kiken"]["total"]),
    ]

    for label, old_v, new_v in metrics:
        if isinstance(old_v, float):
            diff = new_v - old_v
            print(f"{label:>20} {old_v:>9.1f}% {new_v:>9.1f}% {diff:>+9.1f}%")
        else:
            diff = new_v - old_v
            print(f"{label:>20} {old_v:>9} {new_v:>9} {diff:>+9}")

    print("\n" + "="*70)
    print("テスト完了")
    print("="*70)


if __name__ == "__main__":
    main()
