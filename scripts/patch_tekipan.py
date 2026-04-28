"""◉鉄板 付与バグ修復
過去の全 pred.json を走査し、TEKIPAN 条件を満たす軸馬の印を ◎→◉ に修正する。
マスター指示 2026-04-22: ◉ が 3年で 17件しかないバグの根本修復。

修正ロジック:
  各レースで、現在 ◎ or ◉ が付いている馬（軸馬）を対象に
  TEKIPAN 条件（gap/wp/p3/pop/EV）を再評価。満たせば ◉、外せば ◎ に統一する。

  JRA: gap>=7.0, wp>=0.25, p3>=0.70, pop<=2
  NAR: gap>=5.0, wp>=0.35, p3>=0.75, pop<=2

使い方: python scripts/patch_tekipan.py
"""
from __future__ import annotations
import io, sys, json, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")

from config.settings import (
    TEKIPAN_GAP_JRA, TEKIPAN_GAP_NAR,
    TEKIPAN_WIN_PROB_JRA, TEKIPAN_WIN_PROB_NAR,
    TEKIPAN_PLACE3_PROB_JRA, TEKIPAN_PLACE3_PROB_NAR,
    TEKIPAN_POP_MAX_JRA, TEKIPAN_POP_MAX_NAR,
    TEKIPAN_MIN_EV_JRA, TEKIPAN_MIN_EV_NAR,
)


def is_tekipan(top: dict, second_composite: float, is_jra: bool) -> bool:
    """TEKIPAN 条件判定"""
    gap_thr = TEKIPAN_GAP_JRA if is_jra else TEKIPAN_GAP_NAR
    wp_thr = TEKIPAN_WIN_PROB_JRA if is_jra else TEKIPAN_WIN_PROB_NAR
    p3_thr = TEKIPAN_PLACE3_PROB_JRA if is_jra else TEKIPAN_PLACE3_PROB_NAR
    pop_max = TEKIPAN_POP_MAX_JRA if is_jra else TEKIPAN_POP_MAX_NAR
    min_ev = TEKIPAN_MIN_EV_JRA if is_jra else TEKIPAN_MIN_EV_NAR

    comp = top.get("composite") or 0
    gap = comp - second_composite
    wp = top.get("win_prob") or 0
    p3 = top.get("place3_prob") or 0
    pop = top.get("popularity") or 99
    odds = top.get("odds") or 0
    ev = wp * odds if odds > 0 else 1.0

    ev_ok = ev >= min_ev if min_ev > 0 else True
    return gap >= gap_thr and wp >= wp_thr and p3 >= p3_thr and pop <= pop_max and ev_ok


def patch_prediction(pred: dict) -> tuple[int, int]:
    """1 日分 pred を走査し、mark を ◉/◎ 正規化して変更数を返す。
    returns: (upgraded_to_tekipan, downgraded_to_honmei)
    """
    up = 0
    down = 0
    for r in pred.get("races", []):
        horses = [h for h in r.get("horses", []) if not h.get("is_scratched")]
        if len(horses) < 2:
            continue
        is_jra = r.get("is_jra", True)

        # 現在の軸馬（◎ または ◉）を取得
        top = None
        for h in horses:
            if h.get("mark") in ("◎", "◉"):
                top = h
                break
        if not top:
            continue

        # composite 順で2位の composite を取得（top 本人を除いた最高値）
        sorted_h = sorted(horses, key=lambda h: -(h.get("composite") or 0))
        second_composite = 0.0
        for h in sorted_h:
            if h.get("horse_no") == top.get("horse_no"):
                continue
            second_composite = h.get("composite") or 0
            break

        # TEKIPAN 判定
        cur_mark = top.get("mark")
        should_tekipan = is_tekipan(top, second_composite, is_jra)
        if should_tekipan and cur_mark == "◎":
            top["mark"] = "◉"
            up += 1
        elif not should_tekipan and cur_mark == "◉":
            top["mark"] = "◎"
            down += 1
    return up, down


def main():
    t0 = time.time()
    pred_dir = Path("data/predictions")
    files = sorted(fp for fp in pred_dir.glob("*_pred.json") if "_prev" not in fp.name)
    print(f"[{time.strftime('%H:%M:%S')}] 対象 pred.json: {len(files)} 件")

    total_up = 0
    total_down = 0
    modified_files = 0

    for i, fp in enumerate(files):
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  [SKIP] {fp.name}: {e}")
            continue

        up, down = patch_prediction(pred)
        if up > 0 or down > 0:
            # ファイル書き戻し
            fp.write_text(
                json.dumps(pred, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            total_up += up
            total_down += down
            modified_files += 1

        # プログレス（50件ごと）
        if (i + 1) % 50 == 0:
            pct = (i + 1) / len(files) * 100
            el = time.time() - t0
            eta = el / (i + 1) * (len(files) - i - 1)
            print(
                f"  [{i+1}/{len(files)}] {pct:.1f}% 経過{el:.0f}s 残{eta:.0f}s "
                f"upgraded={total_up} downgraded={total_down}",
                flush=True,
            )

    print(f"\n[{time.strftime('%H:%M:%S')}] 完了: {time.time()-t0:.1f}s")
    print(f"  修正ファイル: {modified_files} / {len(files)}")
    print(f"  ◎ → ◉ : {total_up}")
    print(f"  ◉ → ◎ : {total_down}")


if __name__ == "__main__":
    main()
