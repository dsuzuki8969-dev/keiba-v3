"""買い目指南 Phase 1 検収スクリプト

run_analysis_date.py 実行後の pred.json を読み込み、以下の観点で検証する：

  (A) Phase 1-a: col1/col2 の印制約遵守
  (B) Phase 1-a: 三連複 formation_tickets の生成
  (C) Phase 1-b: bet_decision の存在、skip 時の参考ヒモ3点
  (D) Phase 1-b: トリガミ違反 (payback < sum_stake * 1.05) が 0 件
  (E) Phase 1-b: skip 発火率が 15〜30% レンジ

使い方:
  python scripts/verify_tickets.py data/predictions/20260419_pred.json
  python scripts/verify_tickets.py                # デフォルトで今日付ファイル
"""

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# プロジェクトルートを sys.path に追加（スクリプト直接起動対応）
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import (
    ALLOWED_COL1_MARKS,
    ALLOWED_COL2_MARKS,
    BET_DECISION_THRESHOLDS,
    TORIGAMI_SAFETY_MARGIN,
)


def _mark_of(horses: list, hno: int) -> str:
    for h in horses:
        if h.get("horse_no") == hno:
            return h.get("mark", "") or "－"
    return "－"


def verify(path: Path) -> int:
    with path.open(encoding="utf-8") as f:
        data = json.load(f)

    races = data.get("races", [])
    print(f"=== {path.name} 検収: {len(races)} レース ===\n")

    # 集計器
    col1_violations = []
    col2_violations = []
    col3_unmarked_violations = []  # allow_unmarked_col3=False モードで無印が混入したら違反
    races_with_sanrenpuku = 0
    races_with_bet_decision = 0
    skip_races = 0
    skip_reasons = Counter()
    skip_missing_reference = []
    torigami_violations = []  # (venue, race_no, detail)
    payback_missing = 0
    total_eval = 0
    # Phase 1-c: tickets_by_mode
    races_with_tbm = 0
    mode_ticket_counts = {"accuracy": [], "balanced": [], "recovery": []}
    mode_empty_races = {"accuracy": 0, "balanced": 0, "recovery": 0}

    for r in races:
        venue = r.get("venue", "")
        race_no = r.get("race_no", "")
        horses = r.get("horses", []) or []
        if not horses:
            continue
        total_eval += 1

        # (A) col1/col2/col3 印制約
        fc = r.get("formation_columns", {}) or {}
        c1 = fc.get("col1", []) or []
        c2 = fc.get("col2", []) or []
        c3 = fc.get("col3", []) or []
        for hno in c1:
            mk = _mark_of(horses, hno)
            if mk not in ALLOWED_COL1_MARKS:
                col1_violations.append((venue, race_no, hno, mk))
        for hno in c2:
            mk = _mark_of(horses, hno)
            if mk not in ALLOWED_COL2_MARKS:
                col2_violations.append((venue, race_no, hno, mk))

        # (B) 三連複 formation_tickets
        ft = r.get("formation_tickets", []) or []
        if ft:
            races_with_sanrenpuku += 1

        # (C) bet_decision
        bd = r.get("bet_decision")
        if bd:
            races_with_bet_decision += 1
            if bd.get("skip"):
                skip_races += 1
                skip_reasons[bd.get("reason", "unknown")] += 1
                rt = bd.get("reference_tickets", []) or []
                if len(rt) != BET_DECISION_THRESHOLDS["reference_ticket_count"]:
                    skip_missing_reference.append((venue, race_no, len(rt)))

        # (D) トリガミ違反
        buyable = [t for t in ft if t.get("stake", 0) > 0]
        if buyable:
            sum_stake = sum(t["stake"] for t in buyable)
            safety_payback = sum_stake * TORIGAMI_SAFETY_MARGIN
            for t in buyable:
                payback = (t.get("odds") or 0) * t["stake"]
                if payback < safety_payback:
                    torigami_violations.append(
                        (venue, race_no, f"{t.get('a')}-{t.get('b')}-{t.get('c')}",
                         int(payback), int(safety_payback))
                    )
            # payback フィールド欠落
            for t in buyable:
                if "payback_if_hit" not in t:
                    payback_missing += 1
                    break

        # (F) Phase 1-c tickets_by_mode
        tbm = r.get("tickets_by_mode") or {}
        if tbm:
            races_with_tbm += 1
            for mode in ("accuracy", "balanced", "recovery"):
                lst = tbm.get(mode, []) or []
                mode_ticket_counts[mode].append(len(lst))
                if not lst:
                    mode_empty_races[mode] += 1

    # 出力
    print("=== (A) col1/col2 印制約 ===")
    if col1_violations:
        print(f"  [NG] col1 違反 {len(col1_violations)} 件:")
        for v in col1_violations[:5]:
            print(f"    {v[0]} {v[1]}R  馬#{v[2]} 印={v[3]}")
    else:
        print(f"  [OK] col1 違反 0 件")
    if col2_violations:
        print(f"  [NG] col2 違反 {len(col2_violations)} 件:")
        for v in col2_violations[:5]:
            print(f"    {v[0]} {v[1]}R  馬#{v[2]} 印={v[3]}")
    else:
        print(f"  [OK] col2 違反 0 件")

    print("\n=== (B) 三連複 formation_tickets 生成 ===")
    print(f"  三連複あり: {races_with_sanrenpuku}/{total_eval} レース"
          f"（{races_with_sanrenpuku/max(total_eval,1)*100:.1f}%）")

    print("\n=== (C) bet_decision ===")
    print(f"  bet_decision あり: {races_with_bet_decision}/{total_eval}")
    skip_rate = skip_races / max(total_eval, 1)
    print(f"  skip 発火: {skip_races}/{total_eval}（{skip_rate*100:.1f}%）")
    print(f"  skip 内訳: {dict(skip_reasons)}")
    if skip_missing_reference:
        print(f"  [WARN] 参考ヒモ点数不足 {len(skip_missing_reference)} 件:")
        for v in skip_missing_reference[:5]:
            print(f"    {v[0]} {v[1]}R  参考ヒモ={v[2]}点")
    else:
        print(f"  [OK] 参考ヒモ3点生成率 100%")

    print("\n=== (D) トリガミ違反 ===")
    if torigami_violations:
        print(f"  [NG] トリガミ違反 {len(torigami_violations)} 件:")
        for v in torigami_violations[:5]:
            print(f"    {v[0]} {v[1]}R  {v[2]}  payback={v[3]}円 < safety={v[4]}円")
    else:
        print(f"  [OK] トリガミ違反 0 件")
    if payback_missing:
        print(f"  [NG] payback_if_hit フィールド欠落: {payback_missing} レース")

    print("\n=== (F) tickets_by_mode (Phase 1-c) ===")
    print(f"  tickets_by_mode あり: {races_with_tbm}/{total_eval}"
          f"（{races_with_tbm/max(total_eval,1)*100:.1f}%）")
    for mode in ("accuracy", "balanced", "recovery"):
        counts = mode_ticket_counts[mode]
        if counts:
            avg = sum(counts) / len(counts)
            mx = max(counts)
            empty = mode_empty_races[mode]
            print(f"    {mode:>8}: 平均{avg:>4.1f}点  最大{mx:>2}点  空{empty}R")
        else:
            print(f"    {mode:>8}: (データなし)")

    print("\n=== (E) skip 発火率 判定 ===")
    if 0.15 <= skip_rate <= 0.30:
        print(f"  [OK] {skip_rate*100:.1f}% は目標 15-30% レンジ")
    elif skip_rate < 0.15:
        print(f"  [WARN] {skip_rate*100:.1f}% は過少（閾値を厳格化すべきか要検討）")
    else:
        print(f"  [WARN] {skip_rate*100:.1f}% は過剰（閾値を緩和すべきか要検討）")

    # 総合判定
    print("\n=== 総合判定 ===")
    critical_ng = (
        bool(col1_violations) or bool(col2_violations)
        or bool(torigami_violations) or payback_missing > 0
    )
    if critical_ng:
        print("  [NG] クリティカル違反あり（上記参照）")
        return 1
    else:
        print("  [OK] クリティカル違反なし")
        return 0


def main():
    if len(sys.argv) >= 2:
        p = Path(sys.argv[1])
    else:
        # デフォルト: 今日付の pred.json
        today = datetime.now().strftime("%Y%m%d")
        p = Path(f"data/predictions/{today}_pred.json")

    if not p.exists():
        print(f"ファイルが見つかりません: {p}")
        return 2
    return verify(p)


if __name__ == "__main__":
    sys.exit(main())
