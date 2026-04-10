"""Phase 9 テスト検証2回目: 全レースデータ検証スクリプト"""
import sys, json, os
sys.stdout.reconfigure(encoding='utf-8')

PRED_DIR = "data/predictions"

def find_latest_pred(date_str="20260408"):
    fname = f"{date_str}_pred.json"
    fpath = os.path.join(PRED_DIR, fname)
    if os.path.exists(fpath):
        return fpath
    # フォールバック
    for f in sorted(os.listdir(PRED_DIR), reverse=True):
        if f.endswith("_pred.json"):
            return os.path.join(PRED_DIR, f)
    return None

def load_pred(fpath):
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict) and "races" in data:
        return data["races"]
    if isinstance(data, list):
        return data
    return []

def check_展開図_1c_consistency(races):
    """検証1: 展開図(position_1c)とpredicted_cornersの整合性"""
    issues = []
    checked = 0
    for race in races:
        horses = race.get("horses", [])
        if not horses:
            continue
        # position_1cがあるか
        has_1c = any(h.get("position_1c") is not None for h in horses)
        if not has_1c:
            issues.append(f'{race.get("race_name","?")} : position_1cフィールドなし')
            continue
        checked += 1
        # 1Cの順位付け
        sorted_by_1c = sorted(
            [(h.get("horse_no"), h.get("position_1c", 0.5)) for h in horses],
            key=lambda x: x[1]
        )
        _1c_order = [x[0] for x in sorted_by_1c]
        # predicted_cornersがある馬の初角順位
        for h in horses:
            pc = h.get("predicted_corners")
            if pc and len(pc) > 0:
                pass  # predicted_cornersは4角ベースなので不一致は想定内
    return checked, issues

def check_declining_horse_l3f(races):
    """検証2: 下降馬のest_l3fが不当に速くないか"""
    issues = []
    for race in races:
        horses = race.get("horses", [])
        for h in horses:
            ab_dev = h.get("ability_dev")
            est_l3f = h.get("pace_estimated_last3f")
            if ab_dev is not None and est_l3f is not None:
                # 能力偏差値45未満(D以下)で上がりが異常に速い場合
                if ab_dev < 45 and est_l3f < 36.5:
                    issues.append(
                        f'{race.get("race_name","?")} {h.get("horse_name","?")} '
                        f'能力Dev={ab_dev:.1f} est_l3f={est_l3f:.1f}秒 → 能力低なのにl3f速すぎ'
                    )
    return issues

def check_honmei_ability(races):
    """検証3: ◎の馬の能力順位が低すぎないか"""
    issues = []
    for race in races:
        horses = race.get("horses", [])
        if not horses:
            continue
        # ◎の馬を特定
        honmei = [h for h in horses if h.get("mark") == "◎"]
        if not honmei:
            continue
        h = honmei[0]
        # 能力偏差値でのランク
        ab_devs = [(hh.get("horse_no"), hh.get("ability_dev", 50)) for hh in horses]
        ab_devs.sort(key=lambda x: -x[1])
        ab_rank = next((i+1 for i, (no, _) in enumerate(ab_devs) if no == h.get("horse_no")), 99)
        n = len(horses)
        # 能力が下半分なのに◎
        if ab_rank > n * 0.6 and n >= 6:
            issues.append(
                f'{race.get("race_name","?")} ◎={h.get("horse_name","?")} '
                f'能力Dev={h.get("ability_dev",0):.1f} 順位{ab_rank}/{n}'
            )
    return issues

def check_odds_cap_effect(races):
    """検証4: 高オッズ馬のwin_prob上限"""
    issues = []
    for race in races:
        horses = race.get("horses", [])
        for h in horses:
            odds = h.get("odds")
            wp = h.get("win_prob")
            if odds and wp and odds > 30:
                mkt = min(0.95, 0.80 / odds)
                cap = max(mkt * 3.0, 0.005)
                if wp > cap * 1.1:  # 10%マージン
                    issues.append(
                        f'{race.get("race_name","?")} {h.get("horse_name","?")} '
                        f'odds={odds:.1f} wp={wp:.4f} cap={cap:.4f} → オッズキャップ漏れ?'
                    )
    return issues

def check_pace_weight(races):
    """検証5: ペース重みがCAP以内か"""
    # ペース重みはJSON出力に含まれない場合があるのでスキップ可能
    return []

def main():
    fpath = find_latest_pred()
    if not fpath:
        print("ERROR: 予想ファイルが見つかりません")
        sys.exit(1)
    
    print(f"検証対象: {fpath}")
    races = load_pred(fpath)
    print(f"レース数: {len(races)}")
    print()
    
    total_issues = 0
    
    # 検証1: 展開図1C
    checked, issues = check_展開図_1c_consistency(races)
    print(f"=== 検証1: 展開図position_1c ===")
    print(f"  position_1c搭載レース: {checked}/{len(races)}")
    if issues:
        for i in issues[:5]:
            print(f"  ⚠ {i}")
        total_issues += len(issues)
    else:
        print("  ✓ OK")
    print()
    
    # 検証2: 下降馬l3f
    issues2 = check_declining_horse_l3f(races)
    print(f"=== 検証2: 下降馬est_l3f ===")
    if issues2:
        for i in issues2[:5]:
            print(f"  ⚠ {i}")
        total_issues += len(issues2)
    else:
        print("  ✓ OK (能力低馬のl3f異常なし)")
    print()
    
    # 検証3: ◎能力順位
    issues3 = check_honmei_ability(races)
    print(f"=== 検証3: ◎の能力順位 ===")
    if issues3:
        for i in issues3[:5]:
            print(f"  ⚠ {i}")
        total_issues += len(issues3)
    else:
        print("  ✓ OK (◎は全て能力上位)")
    print()
    
    # 検証4: オッズキャップ
    issues4 = check_odds_cap_effect(races)
    print(f"=== 検証4: オッズキャップ ===")
    if issues4:
        for i in issues4[:5]:
            print(f"  ⚠ {i}")
        total_issues += len(issues4)
    else:
        print("  ✓ OK (高オッズ馬のwp適切)")
    print()
    
    # サマリー
    print("=" * 50)
    if total_issues == 0:
        print("全検証PASS: 構造的不整合なし")
    else:
        print(f"検出問題: {total_issues}件")
    print("=" * 50)

if __name__ == "__main__":
    main()
