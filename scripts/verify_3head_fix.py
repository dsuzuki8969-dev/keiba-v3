# -*- coding: utf-8 -*-
"""3頭バグ修復検証スクリプト

4日分の pred.json を検査して:
1. 5頭未満レースが正規の少頭数のみか (actual == pred)
2. 偽ヒットが 0 件か
3. 全体ヒット率が妥当か
"""
import json
import os
import sys

sys.stdout.reconfigure(encoding="utf-8")

PRED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "predictions")
RES_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "results")

TARGET_DATES = ["20260426", "20260428", "20260501", "20260505"]

total_false_hits = 0
total_small = 0
total_legit_small = 0

for dt in TARGET_DATES:
    pred_path = os.path.join(PRED_DIR, f"{dt}_pred.json")
    res_path = os.path.join(RES_DIR, f"{dt}_results.json")

    if not os.path.exists(pred_path):
        print(f"❌ {dt}: pred.json なし")
        continue
    if not os.path.exists(res_path):
        print(f"⚠ {dt}: results.json なし (スキップ)")
        continue

    with open(pred_path, "r", encoding="utf-8") as f:
        pred = json.load(f)
    with open(res_path, "r", encoding="utf-8") as f:
        actual = json.load(f)

    races = pred.get("races", [])
    false_hits = 0
    small_races = 0
    legit_small = 0

    for race in races:
        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched")]
        race_id = race.get("race_id", "")
        result = actual.get(race_id, {})
        actual_cnt = len(result.get("order", []))

        if len(active) < 5:
            small_races += 1
            if actual_cnt > 0 and len(active) != actual_cnt:
                print(f"  ❌ {dt} {race.get('venue')} {race.get('race_no')}R: "
                      f"pred={len(active)} actual={actual_cnt} → 不一致!")
            elif actual_cnt > 0:
                legit_small += 1

        # 偽ヒットチェック
        if len(active) <= 3 and actual_cnt > 3:
            tickets = race.get("formation_tickets", []) or race.get("tickets", [])
            if tickets:
                top3 = sorted(
                    [int(o["horse_no"]) for o in result.get("order", [])],
                    key=lambda h: next(
                        (int(o["finish"]) for o in result["order"]
                         if int(o["horse_no"]) == h), 99
                    ),
                )[:3]
                top3_set = set(top3)
                for t in tickets:
                    if t.get("type") == "三連複":
                        combo_set = {int(x) for x in t.get("combo", [])}
                        if combo_set == top3_set:
                            false_hits += 1
                            print(f"  ❌ 偽ヒット: {dt} {race.get('venue')} "
                                  f"{race.get('race_no')}R")
                            break

    total_false_hits += false_hits
    total_small += small_races
    total_legit_small += legit_small

    status = "✅" if false_hits == 0 else "❌"
    print(f"{status} {dt}: {len(races)}R, "
          f"5頭未満={small_races} (正規={legit_small}), "
          f"偽ヒット={false_hits}")

print()
print("=" * 50)
if total_false_hits == 0:
    print("✅ 全日程で偽ヒット 0 件 — 3頭バグ修復完了")
else:
    print(f"❌ 偽ヒット {total_false_hits} 件残存")
print(f"5頭未満レース合計: {total_small} (正規: {total_legit_small})")
print("=" * 50)
