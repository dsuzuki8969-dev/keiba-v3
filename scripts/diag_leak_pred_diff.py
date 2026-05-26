# -*- coding: utf-8 -*-
"""L-1 学習リーク究明 Step 1: 本番運用 vs WF backtest pred.json の構造完全比較

目的: 同一 race で _pred.json (WF Lv3) と _pred_backup.json (3/19 本番運用) の
各馬データを完全比較し、ROI +127〜148pt の差を生むフィールドを特定する。

出力:
1. キー集合差分 (本番運用にあって WF にない / 逆)
2. composite / probs / mark の数値差
3. 学習リーク疑い特徴量の検出 (オッズ依存 / 結果由来 / 当日情報)
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PRED_DIR = PROJECT_ROOT / "data" / "predictions"


# 学習リーク疑い特徴量 (当日情報・結果由来)
LEAK_SUSPECT_KEYS = {
    "odds", "popularity", "actual_odds", "final_odds",
    "finish_pos", "finish_time", "last_3f", "corner_pos",
    "horse_weight", "body_weight_diff",
}

# 学習リークしない特徴量 (確定スタート前情報)
SAFE_KEYS = {
    "horse_no", "horse_id", "horse_name", "sex", "age",
    "jockey_id", "jockey_name", "trainer_id", "trainer_name",
    "weight_carry", "barrier",
}


def _flatten_keys(obj, prefix=""):
    """ネスト dict のキーを再帰的に flatten"""
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            full = f"{prefix}.{k}" if prefix else k
            keys.add(full)
            if isinstance(v, (dict, list)):
                keys |= _flatten_keys(v, full)
    elif isinstance(obj, list) and obj:
        keys |= _flatten_keys(obj[0], f"{prefix}[]")
    return keys


def compare_race(wf_race: dict, bk_race: dict, race_id: str):
    """1 race を完全比較"""
    print(f"\n{'=' * 100}")
    print(f"race_id = {race_id}")
    print(f"{'=' * 100}")
    print(f"WF  horses: {len(wf_race.get('horses', []))} / 本番 horses: {len(bk_race.get('horses', []))}")

    # race レベルフィールド比較
    wf_race_keys = set(wf_race.keys()) - {"horses"}
    bk_race_keys = set(bk_race.keys()) - {"horses"}
    wf_only = wf_race_keys - bk_race_keys
    bk_only = bk_race_keys - wf_race_keys
    common = wf_race_keys & bk_race_keys

    print(f"\n[race レベル] 共通={len(common)} / WF のみ={len(wf_only)} / 本番のみ={len(bk_only)}")
    if wf_only:
        print(f"  WF のみ: {sorted(wf_only)[:20]}")
    if bk_only:
        print(f"  本番のみ: {sorted(bk_only)[:20]}")

    # horse レベルフィールド比較 (TOP 馬で)
    wf_horses = wf_race.get("horses", [])
    bk_horses = bk_race.get("horses", [])
    if not wf_horses or not bk_horses:
        print("  horses 空のため詳細比較不可")
        return

    # TOP 馬 (composite 最大) で比較
    wf_top = max(wf_horses, key=lambda h: h.get("composite", 0) or 0)
    bk_top = max(bk_horses, key=lambda h: h.get("composite", 0) or 0)

    wf_h_keys = _flatten_keys(wf_top)
    bk_h_keys = _flatten_keys(bk_top)
    wf_only_h = wf_h_keys - bk_h_keys
    bk_only_h = bk_h_keys - wf_h_keys
    common_h = wf_h_keys & bk_h_keys

    print(f"\n[horse レベル] 共通={len(common_h)} / WF のみ={len(wf_only_h)} / 本番のみ={len(bk_only_h)}")
    if wf_only_h:
        print(f"  WF のみ: {sorted(wf_only_h)[:30]}")
    if bk_only_h:
        print(f"  本番のみ: {sorted(bk_only_h)[:30]}")

    # 数値差比較
    print(f"\n[TOP 馬 数値比較] WF top: {wf_top.get('horse_name','?')} (no={wf_top.get('horse_no')}) / 本番 top: {bk_top.get('horse_name','?')} (no={bk_top.get('horse_no')})")
    print(f"  composite: WF={wf_top.get('composite', 0):.2f} / 本番={bk_top.get('composite', 0):.2f}")
    print(f"  win_prob:  WF={wf_top.get('win_prob', 0):.4f} / 本番={bk_top.get('win_prob', 0):.4f}")
    print(f"  mark:      WF={wf_top.get('mark','?')} / 本番={bk_top.get('mark','?')}")

    # 同 horse_no の馬で値の差を見る
    print(f"\n[馬番マッチング比較]")
    print(f"  {'no':>3} | {'WF_name':<15} {'WF_mark':<5} {'WF_comp':>7} {'WF_prob':>7} | {'本番_name':<15} {'本番_mark':<5} {'本番_comp':>7} {'本番_prob':>7}")
    bk_by_no = {h.get("horse_no"): h for h in bk_horses}
    for wh in sorted(wf_horses, key=lambda x: x.get("horse_no", 99))[:8]:
        no = wh.get("horse_no")
        bh = bk_by_no.get(no, {})
        wn = (wh.get("horse_name", "?") or "?")[:14]
        bn = (bh.get("horse_name", "?") or "?")[:14]
        print(f"  {no:>3} | {wn:<15} {wh.get('mark','?'):<5} {wh.get('composite',0) or 0:>6.2f}  {wh.get('win_prob',0) or 0:>7.4f} | {bn:<15} {bh.get('mark','?'):<5} {bh.get('composite',0) or 0:>6.2f}  {bh.get('win_prob',0) or 0:>7.4f}")

    return wf_only_h, bk_only_h


def main():
    # 比較対象日 (本番運用 backup と WF 両方ある日)
    target_dates = ["20260101", "20260201", "20260301"]
    print("=" * 100)
    print("L-1 学習リーク究明 Step 1: pred.json 構造完全比較")
    print("=" * 100)

    all_wf_only = Counter()
    all_bk_only = Counter()

    for date in target_dates:
        wf_path = PRED_DIR / f"{date}_pred.json"
        bk_path = PRED_DIR / f"{date}_pred_backup.json"
        if not (wf_path.exists() and bk_path.exists()):
            print(f"\n--- {date}: 片方欠落 (WF={wf_path.exists()} / 本番={bk_path.exists()}) ---")
            continue

        try:
            wf_pred = json.loads(wf_path.read_text(encoding="utf-8"))
            bk_pred = json.loads(bk_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"\n--- {date}: load error {e} ---")
            continue

        wf_races = wf_pred.get("races", [])
        bk_races = bk_pred.get("races", [])
        wf_by_id = {r.get("race_id"): r for r in wf_races}
        bk_by_id = {r.get("race_id"): r for r in bk_races}
        common_rids = sorted(set(wf_by_id) & set(bk_by_id))

        print(f"\n{'#' * 100}")
        print(f"# 日付: {date}  共通 race数: {len(common_rids)}  WF only: {len(set(wf_by_id) - set(bk_by_id))}  本番 only: {len(set(bk_by_id) - set(wf_by_id))}")
        print(f"{'#' * 100}")

        # 最初の 1 race を詳細比較
        if common_rids:
            rid = common_rids[0]
            result = compare_race(wf_by_id[rid], bk_by_id[rid], rid)
            if result:
                wf_o, bk_o = result
                for k in wf_o:
                    all_wf_only[k] += 1
                for k in bk_o:
                    all_bk_only[k] += 1

    # 集計
    print(f"\n{'=' * 100}")
    print("【サマリ】全比較日にわたる horse キー差")
    print(f"{'=' * 100}")
    print(f"\n本番運用のみに存在 (WF 出力には欠落) ← 学習リーク疑い候補:")
    for k, c in all_bk_only.most_common(40):
        leak_flag = ""
        for sk in LEAK_SUSPECT_KEYS:
            if sk in k.lower():
                leak_flag = " 🚨LEAK疑"
                break
        print(f"  {c:>3}x  {k}{leak_flag}")
    print(f"\nWF のみに存在 (本番運用に欠落):")
    for k, c in all_wf_only.most_common(20):
        print(f"  {c:>3}x  {k}")


if __name__ == "__main__":
    main()
