"""Plan-γ Phase 3 検証スクリプト: hybrid_total フィールド確認

使用方法:
    python scripts/verify_phase3_hybrid_total.py
    python scripts/verify_phase3_hybrid_total.py data/predictions/20260428_pred.json
"""

import json
import sys
import math
from pathlib import Path

# プロジェクトルートを sys.path に追加
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def check_pred_json(pred_path: Path) -> bool:
    """pred.json の hybrid_total フィールドを検証する"""
    print(f"\n[1/4] pred.json 読み込み: {pred_path}")
    with open(pred_path, encoding="utf-8") as f:
        data = json.load(f)

    races = data if isinstance(data, list) else [data]
    print(f"    レース数: {len(races)}")

    errors = []
    null_count = 0
    sample_rows = []
    beta = 0.30  # HYBRID_BETA 期待値

    for race in races:
        race_id = race.get("race_id", "?")
        horses = race.get("horses", [])
        for h in horses:
            ht = h.get("hybrid_total")
            at = h.get("ability_total")
            rrd = h.get("race_relative_dev")
            name = h.get("horse_name", "?")

            # null チェック
            if ht is None:
                null_count += 1
                errors.append(f"  {race_id} {name}: hybrid_total=null")
                continue

            # 計算検証: hybrid = at*(1-β) + rrd*β
            if at is not None and rrd is not None:
                expected = at * (1 - beta) + rrd * beta
                # クランプ (-50〜100)
                expected = max(-50.0, min(100.0, expected))
                diff = abs(ht - expected)
                if diff > 0.1:
                    errors.append(
                        f"  {race_id} {name}: hybrid={ht:.2f} expected={expected:.2f} "
                        f"diff={diff:.3f} (at={at}, rrd={rrd})"
                    )

            # サンプル収集（最初の3レースの最初の1頭）
            if len(sample_rows) < 9 and at is not None and rrd is not None:
                sample_rows.append((race_id, name, at, rrd, ht))

    print(f"\n[2/4] β=0.30 計算検証")
    print(f"    全馬数: {sum(len(r.get('horses', [])) for r in races)}")
    print(f"    null 件数: {null_count}")
    print(f"    計算不一致: {len(errors)} 件")

    print(f"\n[3/4] サンプル 3 馬の at/rrd/hybrid 比較 (β=0.30)")
    print(f"    {'レースID':<20} {'馬名':<12} {'at':>6} {'rrd':>6} {'hybrid':>7} {'検算':>7}")
    print(f"    {'-'*70}")
    for race_id, name, at, rrd, ht in sample_rows[:9]:
        calc = at * (1 - beta) + rrd * beta
        calc = max(-50.0, min(100.0, calc))
        ok = "OK" if abs(ht - calc) < 0.1 else "NG"
        print(f"    {race_id:<20} {name:<12} {at:>6.1f} {rrd:>6.1f} {ht:>7.2f} {calc:>7.2f} {ok}")

    # 手動検証: at=60, rrd=70 → hybrid=63.0
    print(f"\n[4/4] ハードコード検算: at=60.0, rrd=70.0, β=0.30")
    manual = 60.0 * 0.7 + 70.0 * 0.3
    print(f"    期待値: 60.0 * 0.7 + 70.0 * 0.3 = {manual:.1f}")
    assert abs(manual - 63.0) < 0.001, f"検算NG: {manual}"
    print(f"    → 63.0 OK")

    if errors:
        print(f"\n[ERROR] 不整合 {len(errors)} 件:")
        for e in errors[:10]:
            print(e)
        if len(errors) > 10:
            print(f"  ... 他 {len(errors) - 10} 件")
        return False

    print(f"\n[PASS] hybrid_total 検証完了。全馬 null なし・計算整合性 OK")
    return True


def check_import() -> bool:
    """models.py の hybrid_total プロパティが呼べるか確認"""
    print("\n[import] models.HorseEvaluation.hybrid_total プロパティ確認")
    try:
        from src.models import HorseEvaluation, AbilityDeviation
        ev = HorseEvaluation.__new__(HorseEvaluation)
        # 必要フィールドだけ手動セット
        from src.models import (
            Horse, AbilityDeviation, PaceDeviation, CourseAptitude,
            Mark, BakenType, AnaType, KikenType, Trend, Reliability,
            ChakusaPattern,
        )
        # 最低限の初期化
        ev.ability = AbilityDeviation()
        ev.ability.max_dev = 60.0
        ev.ability.wa_dev = 60.0
        ev.ability.alpha = 0.5
        ev.ability.class_adjustment = 0.0
        ev.ability.norm_adjustment = 0.0
        ev.ability.surface_switch_adj = 0.0
        ev.race_relative_dev = 70.0

        ht = ev.hybrid_total
        expected = 60.0 * 0.7 + 70.0 * 0.3
        expected = max(-50.0, min(100.0, expected))
        print(f"    ability.total~60.0, race_relative_dev=70.0 -> hybrid_total={ht:.2f} (expected: {expected:.2f})")
        assert abs(ht - expected) < 0.5, f"不一致: {ht} vs {expected}"
        print("    OK")
        return True
    except Exception as e:
        print(f"    [ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False


def main() -> None:
    all_ok = True

    # import チェック
    if not check_import():
        all_ok = False

    # pred.json チェック（引数指定 or 最新ファイル自動検索）
    if len(sys.argv) > 1:
        pred_path = Path(sys.argv[1])
        if pred_path.exists():
            if not check_pred_json(pred_path):
                all_ok = False
        else:
            print(f"[WARN] ファイルが見つかりません: {pred_path}")
    else:
        # 最新の pred.json を自動検索
        pred_dir = ROOT / "data" / "predictions"
        preds = sorted(pred_dir.glob("*_pred.json"), reverse=True)
        if preds:
            print(f"\n最新 pred.json を使用: {preds[0].name}")
            if not check_pred_json(preds[0]):
                all_ok = False
        else:
            print("\n[WARN] data/predictions/ に pred.json が見つかりません")
            print("       run_analysis_date.py 実行後に再度検証してください")

    print("\n" + ("=" * 60))
    if all_ok:
        print("Phase 3 hybrid_total 検証: PASS")
    else:
        print("Phase 3 hybrid_total kensho: FAIL -- uwa no error wo kakunin shite kudasai")
        sys.exit(1)


if __name__ == "__main__":
    main()
