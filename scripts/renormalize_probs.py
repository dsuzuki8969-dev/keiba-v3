"""全 pred.json の確率合計を Σwin=1.0/Σp2=2.0/Σp3=3.0 に再正規化

マスター指示 2026-04-23 (keiba-reviewer 指摘):
patch_false_scratched.py 実行時の閾値 0.05 が緩く、17 レースで合計ズレ残留。
本スクリプトはそのまま全 pred.json を走査し、ズレが 1e-6 超なら再正規化する。
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def main() -> None:
    t0 = time.time()
    changed_files = 0
    changed_races = 0
    total_races = 0
    targets = [("win_prob", 1.0), ("place2_prob", 2.0), ("place3_prob", 3.0)]
    for fp in sorted(Path("data/predictions").glob("*_pred.json")):
        if "_prev" in fp.name:
            continue
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
        f_changed = False
        for r in pred.get("races", []):
            total_races += 1
            active = [h for h in r.get("horses", []) if not h.get("is_scratched")]
            if len(active) < 2:
                continue
            r_changed = False
            for pk, target in targets:
                cur_sum = sum(h.get(pk) or 0 for h in active)
                if cur_sum > 0 and abs(cur_sum - target) > 1e-6:
                    for h in active:
                        h[pk] = round(min(1.0, (h.get(pk) or 0) / cur_sum * target), 4)
                    r_changed = True
            if r_changed:
                changed_races += 1
                f_changed = True
        if f_changed:
            fp.write_text(
                json.dumps(pred, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            changed_files += 1
    print(f"完了 {time.time()-t0:.1f}s total_races={total_races} changed_races={changed_races} changed_files={changed_files}", flush=True)


if __name__ == "__main__":
    main()
