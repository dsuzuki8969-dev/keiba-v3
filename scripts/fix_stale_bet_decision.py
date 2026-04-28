"""bet_decision の取消理由が残留している pred.json を修復

マスター指示 2026-04-23: 「取消馬により買い目無効」と表示されているが実際には取消馬ゼロ。
  patch_false_scratched.py が is_scratched=False に戻したが bet_decision は更新されてない。

修復ロジック:
  現在の is_scratched 状態を見て、取消馬ゼロなら:
    - bet_decision.skip_reasons から 'scratched' を削除
    - 'scratched' が唯一の理由だった場合は skip=False に戻し message クリア
    - tickets_by_mode で少なくとも 1 モードにチケットがあれば skip=False

使い方:
  python scripts/fix_stale_bet_decision.py 20260423
  python scripts/fix_stale_bet_decision.py   # 全日分
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def fix_race(r: dict) -> bool:
    """1 レース修正。変更あれば True"""
    bd = r.get("bet_decision")
    if not isinstance(bd, dict):
        return False
    if not bd.get("skip"):
        return False
    skip_reasons = bd.get("skip_reasons") or []
    if "scratched" not in skip_reasons:
        return False

    # 実際に取消馬がいるか確認
    horses = r.get("horses", [])
    actually_scratched = sum(1 for h in horses if h.get("is_scratched"))
    if actually_scratched > 0:
        return False  # 実際に取消馬がいる → OK

    # 取消馬ゼロなのに 'scratched' 理由残留 → 除去
    new_reasons = [x for x in skip_reasons if x != "scratched"]
    bd["skip_reasons"] = new_reasons
    # tickets_by_mode で買い目あるか再確認
    tbm = r.get("tickets_by_mode") or {}
    has_any_tickets = any(
        isinstance(tbm.get(m), list) and len(tbm[m]) > 0
        for m in ("fixed", "accuracy", "balanced", "recovery")
    )
    if not new_reasons and has_any_tickets:
        # 他の skip 理由なく買い目もある → 買える
        bd["skip"] = False
        bd["message"] = ""
    elif not new_reasons:
        # 取消以外のskip 理由がなく買い目も無い → メッセージを差し替え
        bd["message"] = "買い目なし"
    else:
        # 他の skip 理由が残ってる → message を他の理由から再生成
        _reason_msg = {
            "low_ev": "期待値不足",
            "low_confidence": "信頼度低",
            "no_tickets": "買い目なし",
        }
        main = new_reasons[0] if new_reasons else ""
        bd["message"] = _reason_msg.get(main, "買わない")
    r["bet_decision"] = bd
    return True


def main() -> None:
    args = sys.argv[1:]
    if args:
        targets = [args[0].replace("-", "")]
    else:
        targets = [fp.name[:8] for fp in Path("data/predictions").glob("*_pred.json") if "_prev" not in fp.name]

    total_files = 0
    total_races_fixed = 0
    for date_key in targets:
        fp = Path(f"data/predictions/{date_key}_pred.json")
        if not fp.exists():
            continue
        total_files += 1
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        changed = 0
        for r in pred.get("races", []):
            if fix_race(r):
                changed += 1
        if changed > 0:
            fp.write_text(
                json.dumps(pred, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            total_races_fixed += changed
            print(f"  {date_key}: {changed} races fixed", flush=True)

    print(f"\n完了: {total_files} files / {total_races_fixed} races fixed")


if __name__ == "__main__":
    main()
