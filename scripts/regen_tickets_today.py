"""今日の pred.json で tickets_by_mode.fixed が空なレースを再生成。

マスター指示 2026-04-23: 誤取消で tickets が空になったレース（今日17件）を復活。
build_sanrentan_tickets を使って pred.json から直接三連単チケット再構築。
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from scripts.monthly_backtest import build_sanrentan_tickets
from src.calculator.betting import SANRENTAN_SKIP_CONFIDENCES


def main() -> None:
    date_key = sys.argv[1].replace("-", "") if len(sys.argv) > 1 else "20260423"
    fp = Path(f"data/predictions/{date_key}_pred.json")
    pred = json.loads(fp.read_text(encoding="utf-8"))
    fixed_cnt = 0
    empty_cnt = 0
    for r in pred.get("races", []):
        tbm = r.get("tickets_by_mode") or {}
        fixed = tbm.get("fixed") if isinstance(tbm.get("fixed"), list) else []
        conf = r.get("confidence", "C")
        if fixed:
            continue  # 既にある
        empty_cnt += 1
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if not horses:
            continue
        n = r.get("field_count") or len(horses)
        is_jra = r.get("is_jra", False)
        # SS/C/D skip
        if conf in SANRENTAN_SKIP_CONFIDENCES:
            continue
        try:
            tickets = build_sanrentan_tickets(horses, n, is_jra)
        except Exception as e:
            print(f"  ERR {r.get('venue')}{r.get('race_no')}R: {e}")
            continue
        if tickets:
            tbm["fixed"] = tickets
            r["tickets_by_mode"] = tbm
            # bet_decision も更新
            bd = r.get("bet_decision") or {}
            if isinstance(bd, dict):
                if bd.get("skip") and not bd.get("skip_reasons"):
                    bd["skip"] = False
                    bd["message"] = ""
                    r["bet_decision"] = bd
            fixed_cnt += 1
            print(f"  fixed {r.get('venue')}{r.get('race_no')}R: {len(tickets)} tickets")

    fp.write_text(json.dumps(pred, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"\n完了: fixed={fixed_cnt} / empty_races={empty_cnt}")


if __name__ == "__main__":
    main()
