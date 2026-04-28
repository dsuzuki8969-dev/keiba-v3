"""過去の pred.json で誤って is_scratched=True になった馬を元に戻す
マスター指示 2026-04-23: 部分オッズ取得失敗で取消扱いになった馬を救出。

判定: is_scratched=True だが ML予測（predicted_tansho_odds / win_prob / composite）
  を持っている → 誤検知 → False に戻し、確率を復元
"""
from __future__ import annotations
import io, json, sys, time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, ".")


def restore_horse(h: dict) -> bool:
    """is_scratched を誤検知と判断したら False に戻す。変更があれば True。"""
    if not h.get("is_scratched"):
        return False
    # ML 予測があれば誤検知
    has_pred = h.get("predicted_tansho_odds") is not None
    has_composite = (h.get("composite") or 0) > 0
    has_ability = (h.get("ability_total") or 0) > 0
    if not (has_pred or has_composite or has_ability):
        return False  # 真の取消
    # 復元
    h["is_scratched"] = False
    return True


def main() -> None:
    t0 = time.time()
    pred_dir = Path("data/predictions")
    files = sorted(fp for fp in pred_dir.glob("*_pred.json") if "_prev" not in fp.name)
    print(f"対象: {len(files)} 件", flush=True)

    total_files_changed = 0
    total_horses_restored = 0

    for i, fp in enumerate(files):
        try:
            pred = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            continue

        file_changed = 0
        for r in pred.get("races", []):
            horses = r.get("horses", [])
            for h in horses:
                if restore_horse(h):
                    file_changed += 1

        if file_changed > 0:
            # 確率の再正規化（取消解除された馬を含めて）
            # マスター指示 2026-04-23 (keiba-reviewer 指摘):
            # Σ厳守のため閾値を 0.05 → 1e-6 に厳格化（規約「Σwin=1.0」完全維持）
            for r in pred.get("races", []):
                active = [h for h in r.get("horses", []) if not h.get("is_scratched")]
                for pk, target in [("win_prob", 1.0), ("place2_prob", 2.0), ("place3_prob", 3.0)]:
                    cur_sum = sum(h.get(pk) or 0 for h in active)
                    # cur_sum が target から 1e-6 以上ずれていれば再正規化
                    if cur_sum > 0 and abs(cur_sum - target) > 1e-6:
                        for h in active:
                            h[pk] = round(min(1.0, (h.get(pk) or 0) / cur_sum * target), 4)
            fp.write_text(
                json.dumps(pred, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            total_files_changed += 1
            total_horses_restored += file_changed

        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            pct = (i + 1) / len(files) * 100
            print(
                f"  [{i+1}/{len(files)}] {pct:.1f}% "
                f"file_changed={total_files_changed} horses={total_horses_restored} "
                f"({elapsed:.0f}s)",
                flush=True,
            )

    print(f"\n完了: {time.time()-t0:.1f}秒")
    print(f"  修正ファイル: {total_files_changed}")
    print(f"  復元馬数: {total_horses_restored}")


if __name__ == "__main__":
    main()
