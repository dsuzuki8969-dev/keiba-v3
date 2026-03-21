# -*- coding: utf-8 -*-
"""reassign_marks_dict / _apply_ml_composite_adj の修正テスト"""
import sys, json, copy, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.calculator.popularity_blend import reassign_marks_dict, _apply_ml_composite_adj

MARKS_ORDER = {"◉": 0, "◎": 1, "○": 2, "▲": 3, "△": 4, "★": 5}
PRED_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "predictions", "20260318_pred.json")


def test_composite_unchanged():
    """テスト1: reassign_marks_dict がcompositeを変更しない"""
    horses = [
        {"horse_no": i, "composite": 60 - i * 5, "win_prob": 0.3 - i * 0.05,
         "ml_composite_adj": 2 - i * 0.5, "mark": "", "odds": 3 + i * 2, "popularity": i}
        for i in range(1, 7)
    ]
    orig = {h["horse_no"]: h["composite"] for h in horses}
    reassign_marks_dict(horses)
    for h in horses:
        assert h["composite"] == orig[h["horse_no"]], \
            f"composite changed: #{h['horse_no']} {orig[h['horse_no']]} -> {h['composite']}"
    print("  OK: composite変更なし")


def test_marks_follow_composite_order():
    """テスト2: 印がcomposite降順に付与される"""
    horses = [
        {"horse_no": i, "composite": 60 - i * 5, "win_prob": 0.3 - i * 0.05,
         "ml_composite_adj": 2 - i * 0.5, "mark": "", "odds": 3 + i * 2, "popularity": i}
        for i in range(1, 7)
    ]
    reassign_marks_dict(horses)
    marked = [(h["composite"], h["mark"], h["horse_no"])
              for h in horses if h["mark"] in MARKS_ORDER]
    marked.sort(key=lambda x: MARKS_ORDER[x[1]])
    prev = 999
    for comp, mark, hno in marked:
        assert comp <= prev + 0.001, \
            f"印順がcomposite降順でない: {mark}(#{hno})={comp} > prev={prev}"
        prev = comp
    print("  OK: 印順 = composite降順")


def test_special_marks_preserved():
    """テスト3: ☆/×印が保持される"""
    horses = [
        {"horse_no": 1, "composite": 60, "win_prob": 0.30, "ml_composite_adj": 2.0,
         "mark": "◎", "odds": 3.0, "popularity": 1},
        {"horse_no": 2, "composite": 55, "win_prob": 0.20, "ml_composite_adj": 1.0,
         "mark": "○", "odds": 5.0, "popularity": 2},
        {"horse_no": 3, "composite": 50, "win_prob": 0.15, "ml_composite_adj": 0.5,
         "mark": "▲", "odds": 8.0, "popularity": 3},
        {"horse_no": 4, "composite": 45, "win_prob": 0.10, "ml_composite_adj": -0.5,
         "mark": "△", "odds": 12.0, "popularity": 4},
        {"horse_no": 5, "composite": 40, "win_prob": 0.08, "ml_composite_adj": -1.0,
         "mark": "☆", "odds": 50.0, "popularity": 8},
        {"horse_no": 6, "composite": 35, "win_prob": 0.05, "ml_composite_adj": -2.0,
         "mark": "×", "odds": 2.5, "popularity": 1},
        {"horse_no": 7, "composite": 52, "win_prob": 0.12, "ml_composite_adj": 0.3,
         "mark": "★", "odds": 10.0, "popularity": 5},
    ]
    reassign_marks_dict(horses)
    h5 = next(h for h in horses if h["horse_no"] == 5)
    h6 = next(h for h in horses if h["horse_no"] == 6)
    assert h5["mark"] == "☆", f"☆ lost: mark={h5['mark']}"
    assert h6["mark"] == "×", f"× lost: mark={h6['mark']}"
    print("  OK: ☆/×印が保持された")


def test_real_data_consistency():
    """テスト4: 実データ全レースでcomposite-印整合"""
    if not os.path.exists(PRED_PATH):
        print("  SKIP: pred.jsonが見つからない")
        return
    with open(PRED_PATH, "r", encoding="utf-8") as f:
        pred = json.load(f)
    mismatch = 0
    total = 0
    for race in pred.get("races", []):
        horses = copy.deepcopy(race["horses"])
        if len(horses) < 3:
            continue
        total += 1
        orig = {h["horse_no"]: h["composite"] for h in horses}
        reassign_marks_dict(horses)
        # composite不変チェック
        for h in horses:
            if abs(h["composite"] - orig[h["horse_no"]]) > 0.001:
                v = race.get("venue", "?")
                r = race.get("race_no", "?")
                print(f"  NG: {v}{r}R #{h['horse_no']} composite変更 "
                      f"{orig[h['horse_no']]:.2f} -> {h['composite']:.2f}")
                mismatch += 1
                break
        # 印順チェック
        marked = [(h["composite"], h["mark"], h["horse_no"])
                  for h in horses if h["mark"] in MARKS_ORDER]
        marked.sort(key=lambda x: MARKS_ORDER[x[1]])
        prev = 999
        for comp, mark, hno in marked:
            if comp > prev + 0.001:
                v = race.get("venue", "?")
                r = race.get("race_no", "?")
                print(f"  NG: {v}{r}R #{hno} {mark}({comp:.2f}) > prev({prev:.2f})")
                mismatch += 1
                break
            prev = comp
    if mismatch == 0:
        print(f"  OK: {total}レース全て整合")
    else:
        print(f"  NG: {mismatch}/{total}レースで不整合")


def test_apply_ml_adj_no_double_count():
    """テスト5: _apply_ml_composite_adj が二重適用しない"""
    horses = [
        {"horse_no": 1, "composite": 55.0, "win_prob": 0.30, "ml_composite_adj": 3.0},
        {"horse_no": 2, "composite": 50.0, "win_prob": 0.15, "ml_composite_adj": 1.0},
        {"horse_no": 3, "composite": 45.0, "win_prob": 0.05, "ml_composite_adj": -2.0},
    ]
    # 元のbase（composite - ml_adj）
    expected_bases = {1: 52.0, 2: 49.0, 3: 47.0}
    _apply_ml_composite_adj(horses)
    for h in horses:
        base = h.get("_composite_base")
        exp = expected_bases[h["horse_no"]]
        assert abs(base - exp) < 0.001, \
            f"#{h['horse_no']} _composite_base={base:.2f} != expected={exp:.2f}"
    print("  OK: _composite_base が既存ml_adjを正しく差し引いて算出")


def test_apply_ml_adj_idempotent():
    """テスト6: _apply_ml_composite_adj を2回呼んでも冪等"""
    horses = [
        {"horse_no": 1, "composite": 55.0, "win_prob": 0.30, "ml_composite_adj": 3.0},
        {"horse_no": 2, "composite": 50.0, "win_prob": 0.15, "ml_composite_adj": 1.0},
        {"horse_no": 3, "composite": 45.0, "win_prob": 0.05, "ml_composite_adj": -2.0},
    ]
    _apply_ml_composite_adj(horses)
    after_1st = {h["horse_no"]: h["composite"] for h in horses}
    _apply_ml_composite_adj(horses)
    for h in horses:
        assert abs(h["composite"] - after_1st[h["horse_no"]]) < 0.001, \
            f"#{h['horse_no']} 2回目で変化: {after_1st[h['horse_no']]:.2f} -> {h['composite']:.2f}"
    print("  OK: 2回呼んでも冪等（同じ結果）")


def test_urawa_r6_specific():
    """テスト7: 浦和6R — 修正前のバグケースが解消されたか"""
    if not os.path.exists(PRED_PATH):
        print("  SKIP: pred.jsonが見つからない")
        return
    with open(PRED_PATH, "r", encoding="utf-8") as f:
        pred = json.load(f)
    for race in pred.get("races", []):
        if race.get("race_no") == 6 and "浦和" in race.get("venue", ""):
            horses = copy.deepcopy(race["horses"])
            reassign_marks_dict(horses)
            # composite順でソート
            sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
            # ◎は最高composite馬であるべき
            honmei = next((h for h in horses if h["mark"] in ("◎", "◉")), None)
            assert honmei is not None, "◎/◉が見つからない"
            assert honmei["horse_no"] == sorted_h[0]["horse_no"], \
                f"◎が最高composite馬でない: ◎=#{honmei['horse_no']}({honmei['composite']:.2f}) vs " \
                f"1位=#{sorted_h[0]['horse_no']}({sorted_h[0]['composite']:.2f})"
            print(f"  OK: ◎=#{honmei['horse_no']} ({honmei['composite']:.2f}) = composite1位")
            # 印5頭のcomposite表示
            for h in sorted_h[:6]:
                m = h.get("mark", "－")
                print(f"    {m:2s} #{h['horse_no']:2d} comp={h['composite']:5.2f} "
                      f"win={h.get('win_prob',0):.4f}")
            break


if __name__ == "__main__":
    print("=== reassign_marks_dict / _apply_ml_composite_adj テスト ===\n")
    test_composite_unchanged()
    test_marks_follow_composite_order()
    test_special_marks_preserved()
    test_real_data_consistency()
    test_apply_ml_adj_no_double_count()
    test_apply_ml_adj_idempotent()
    test_urawa_r6_specific()
    print("\n=== 全テスト完了 ===")
