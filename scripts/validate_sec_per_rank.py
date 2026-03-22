"""
sec_per_rank（1頭差あたりの秒差）を data/ml/*.json から検証するスクリプト。

同レース内の馬ペアの走破タイム差÷4角位置差をペース別・面別に統計する。
"""
import sys, os, json, glob
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from collections import defaultdict
import statistics


def main():
    ml_dir = os.path.join(os.path.dirname(__file__), "..", "data", "ml")
    files = sorted(glob.glob(os.path.join(ml_dir, "*.json")))
    print(f"MLデータファイル: {len(files)}件")

    results = defaultdict(list)
    n_races = 0
    n_pairs = 0

    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)

        race_list = data.get("races", []) if isinstance(data, dict) else data if isinstance(data, list) else []
        for race in race_list:
            horses = race.get("horses", [])
            if len(horses) < 3:
                continue

            surface = race.get("surface", "")
            distance = race.get("distance", 0) or 0
            pace = race.get("pace", "M")
            is_jra = race.get("is_jra", True)

            if distance < 800:
                continue
            dist_band = "sprint" if distance < 1400 else "mile" if distance < 1800 else "middle" if distance < 2200 else "long"

            # 有効な馬のみ（走破タイム+4角位置あり）
            valid = []
            for h in horses:
                ft = h.get("finish_time_sec")
                corners = h.get("positions_corners", [])
                pos4c = corners[-1] if corners else None
                if ft and ft > 0 and pos4c and pos4c > 0:
                    valid.append({"ft": ft, "pos4c": pos4c, "l3f": h.get("last_3f_sec")})

            if len(valid) < 3:
                continue

            n_races += 1
            valid.sort(key=lambda x: x["pos4c"])

            for i in range(len(valid) - 1):
                a, b = valid[i], valid[i + 1]
                pos_diff = b["pos4c"] - a["pos4c"]
                if pos_diff <= 0:
                    continue
                time_diff = b["ft"] - a["ft"]
                spr = time_diff / pos_diff
                if -0.5 < spr < 1.5:  # 外れ値除去
                    n_pairs += 1
                    results[f"{surface}_{dist_band}_{pace}"].append(spr)
                    results[f"{surface}_ALL_{pace}"].append(spr)
                    results[f"ALL_ALL_{pace}"].append(spr)
                    results[f"ALL_ALL_ALL"].append(spr)
                    results[f"{surface}_{dist_band}_ALL"].append(spr)
                    org = "JRA" if is_jra else "NAR"
                    results[f"{org}_ALL_ALL"].append(spr)

    print(f"対象レース: {n_races} / ペア数: {n_pairs}")
    print()
    print("=" * 75)
    print(f"{'カテゴリ':<30} {'N':>8} {'平均':>8} {'中央値':>8} {'σ':>8}")
    print("-" * 75)
    for key in sorted(results.keys()):
        vals = results[key]
        if len(vals) < 20:
            continue
        mean = statistics.mean(vals)
        median = statistics.median(vals)
        stdev = statistics.stdev(vals) if len(vals) > 1 else 0
        print(f"{key:<30} {len(vals):>8} {mean:>8.4f} {median:>8.4f} {stdev:>8.4f}")

    print()
    print("現在の設定値 POSITION_SEC_BY_PACE:")
    print("  HH: 0.19, HM: 0.16, MM: 0.12, MS: 0.09, SS: 0.07 (秒/頭)")


if __name__ == "__main__":
    main()
