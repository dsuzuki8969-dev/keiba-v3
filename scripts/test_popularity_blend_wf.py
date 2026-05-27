"""
WF popularity_blend 動作確認スクリプト

wf_2025 期間の 1 日分 pred.json を使って popularity_blend の適用前後を比較。
実際の WF backtest を実行せず、既存 pred.json に対して blend のみテストする。

使い方:
  python scripts/test_popularity_blend_wf.py
  python scripts/test_popularity_blend_wf.py --wf wf_2026 --date 20250101
"""

import argparse
import copy
import json
import math
import os
import sys

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from config.settings import PREDICTIONS_DIR, DATA_DIR


def _softmax_win_probs(horses: list) -> None:
    """composite ベースの softmax で win_prob を正規化 (wf_inference.py 相当)"""
    import math
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    if not active:
        return
    composites = [h.get("composite", 50.0) for h in active]
    max_c = max(composites)
    exps = [math.exp((c - max_c) / 10.0) for c in composites]
    total = sum(exps)
    if total <= 0:
        return
    active_map = {}
    for h, e in zip(active, exps):
        active_map[h.get("horse_no", -1)] = e / total
    for h in horses:
        hno = h.get("horse_no", -1)
        if hno in active_map:
            h["win_prob"] = round(active_map[hno], 6)


def _apply_blend_to_snapshot(race: dict, pop_stats: dict) -> dict:
    """race dict のコピーに popularity_blend を適用して返す

    既存 pred.json の win_prob は P(top3) が入っているため、
    blend 前に softmax で正規化してから適用する。
    """
    race_copy = copy.deepcopy(race)
    horses = race_copy.get("horses", [])
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]

    if not active:
        return race_copy

    # 既存 pred.json は win_prob が正規化されていない場合があるため softmax で正規化
    _softmax_win_probs(horses)

    # wf_inference._apply_popularity_blend_wf を使用 (DISABLE_POPULARITY_BLEND を無視)
    scripts_dir = os.path.join(PROJECT_ROOT, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from wf_inference import _apply_popularity_blend_wf
    _apply_popularity_blend_wf(race_copy, pop_stats)
    return race_copy


def check_probability_integrity(horses: list, race_id: str) -> list:
    """確率の整合性チェック (win_prob 合計 ≒ 1.0 等)"""
    issues = []
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    if not active:
        return issues

    total_win = sum(h.get("win_prob", 0) for h in active)
    total_p2 = sum(h.get("place2_prob", 0) for h in active)
    total_p3 = sum(h.get("place3_prob", 0) for h in active)

    # win_prob 合計 ≒ 1.0 (±10% 許容)
    if abs(total_win - 1.0) > 0.10:
        issues.append(f"  [{race_id}] win_prob 合計 = {total_win:.4f} (1.0 から大きく乖離)")

    # win_prob の異常値チェック (各馬が 0〜1)
    for h in active:
        wp = h.get("win_prob", 0)
        hname = h.get("horse_name", h.get("horse_id", "?"))
        if wp < 0 or wp > 1.0:
            issues.append(f"  [{race_id}] {hname}: win_prob={wp:.4f} (範囲外 [0,1])")

    return issues


def run_test(wf_name: str, test_date: str):
    """指定 WF 期間の 1 日分で blend 前後を比較"""
    print(f"\n{'='*60}")
    print(f"WF popularity_blend 動作確認: {wf_name}")
    print(f"テスト日付: {test_date}")
    print(f"{'='*60}")

    # 期間別 stats ロード
    stats_path = os.path.join(DATA_DIR, f"popularity_rates_{wf_name}.json")
    if not os.path.exists(stats_path):
        print(f"ERROR: {stats_path} が見つかりません。")
        print(f"先に python scripts/build_popularity_stats_wf.py --period {wf_name} を実行してください。")
        return False

    with open(stats_path, "r", encoding="utf-8") as f:
        pop_stats = json.load(f)
    print(f"stats ロード完了: sample_days={pop_stats.get('sample_days')}, "
          f"total_entries={pop_stats.get('total_entries', 0):,}")

    if pop_stats.get("total_entries", 0) == 0:
        print(f"WARNING: total_entries=0 のため blend のテストをスキップします。")
        return True

    # pred.json ロード
    pred_path = os.path.join(PREDICTIONS_DIR, f"{test_date}_pred.json")
    if not os.path.exists(pred_path):
        # 近い日付を探す
        import glob as glob_module
        candidates = sorted(glob_module.glob(os.path.join(
            PREDICTIONS_DIR, f"{test_date[:4]}*_pred.json"
        )))
        if candidates:
            pred_path = candidates[0]
            test_date = os.path.basename(pred_path)[:8]
            print(f"指定日の pred.json が見つからないため {test_date} を使用")
        else:
            print(f"ERROR: pred.json が見つかりません: {pred_path}")
            return False

    with open(pred_path, "r", encoding="utf-8") as f:
        pred_data = json.load(f)

    races = pred_data.get("races", [])
    print(f"対象レース数: {len(races)}")

    # --- 各レースで blend 前後を比較 ---
    all_diffs = []
    all_issues = []
    blend_count = 0
    skip_count = 0

    for race in races:
        race_id = race.get("race_id", "")
        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]

        if not active:
            skip_count += 1
            continue

        # popularity が存在するか確認
        has_popularity = any(h.get("popularity") is not None and h.get("popularity", 0) >= 1 for h in active)
        if not has_popularity:
            skip_count += 1
            continue

        # softmax 後 (blend 前) の win_prob を記録するため、softmax だけ先に適用
        import copy as _copy
        race_softmax = _copy.deepcopy(race)
        _softmax_win_probs(race_softmax.get("horses", []))
        softmax_active = [h for h in race_softmax.get("horses", [])
                          if not h.get("is_scratched") and not h.get("scrape_failed")]
        before = {h.get("horse_no"): h.get("win_prob", 0.0) for h in softmax_active}

        # blend 適用 (コピーに)
        race_after = _apply_blend_to_snapshot(race, pop_stats)
        after_horses = race_after.get("horses", [])
        after_active = [h for h in after_horses if not h.get("is_scratched") and not h.get("scrape_failed")]

        # 変化量を計算
        for h_after in after_active:
            hno = h_after.get("horse_no")
            wp_before = before.get(hno, 0.0)
            wp_after = h_after.get("win_prob", 0.0)
            diff = wp_after - wp_before
            pop = h_after.get("popularity")
            all_diffs.append({
                "race_id": race_id,
                "horse_no": hno,
                "popularity": pop,
                "before": wp_before,
                "after": wp_after,
                "diff": diff,
            })

        # 整合性チェック
        issues = check_probability_integrity(after_active, race_id)
        all_issues.extend(issues)
        blend_count += 1

    # --- 結果サマリー ---
    print(f"\n--- 処理結果 ---")
    print(f"  blend 適用: {blend_count} races")
    print(f"  スキップ (popularity なし等): {skip_count} races")
    print(f"  整合性エラー: {len(all_issues)} 件")
    if all_issues:
        for issue in all_issues[:5]:
            print(issue)
        if len(all_issues) > 5:
            print(f"  ... (他 {len(all_issues) - 5} 件)")

    if all_diffs:
        diffs = [d["diff"] for d in all_diffs]
        print(f"\n--- win_prob 変化分布 ---")
        print(f"  エントリ数: {len(diffs)}")
        print(f"  平均変化: {sum(diffs)/len(diffs):+.6f}")
        print(f"  最大増加: {max(diffs):+.6f}")
        print(f"  最大減少: {min(diffs):+.6f}")
        print(f"  変化なし(|diff|<0.0001): {sum(1 for d in diffs if abs(d) < 0.0001)}")

        # 人気別の変化
        print(f"\n--- 人気別 win_prob 変化 ---")
        by_pop = {}
        for d in all_diffs:
            pop = d.get("popularity")
            if pop is not None and 1 <= pop <= 5:
                if pop not in by_pop:
                    by_pop[pop] = []
                by_pop[pop].append(d["diff"])
        for pop in sorted(by_pop.keys()):
            dlist = by_pop[pop]
            avg = sum(dlist) / len(dlist)
            print(f"  {pop}番人気 (n={len(dlist)}): 平均変化 {avg:+.6f}")

    print(f"\n{'='*60}")
    print(f"テスト完了: {wf_name} / {test_date}")
    print(f"整合性エラー: {'なし OK' if not all_issues else str(len(all_issues)) + '件 要確認'}")
    return len(all_issues) == 0


def main():
    parser = argparse.ArgumentParser(description="WF popularity_blend 動作確認")
    parser.add_argument("--wf", default="wf_2025",
                        choices=["wf_2024", "wf_2025", "wf_2026"],
                        help="テストする WF 期間 (default: wf_2025)")
    parser.add_argument("--date", default="20250101",
                        help="テストする日付 YYYYMMDD (default: 20250101)")
    args = parser.parse_args()

    ok = run_test(args.wf, args.date)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
