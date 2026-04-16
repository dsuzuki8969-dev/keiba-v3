#!/usr/bin/env python3
"""
自信度v4 再計算 — 全pred.jsonに適用

v4変更点:
  - JRA/NAR別パーセンタイル閾値（構成比 SS5% S10% A15% B35% C25% D10%）
  - SS/S人気ゲート（市場との合意確認）
  - SS硬性条件・オッズゲート撤廃
  - Eレベル廃止（D統合）

JRA閾値: SS≥0.641 S≥0.537 A≥0.438 B≥0.257 C≥0.134 D<0.134
NAR閾値: SS≥0.761 S≥0.673 A≥0.585 B≥0.390 C≥0.224 D<0.224
人気ゲート: SS pop≤1, S pop≤2（JRA/NAR共通）
"""
import json, glob, os, sys, io, time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True, errors="replace")

PRED_DIR = "data/predictions"
JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "新潟", "福島", "函館", "札幌"}

# v4閾値
THRESHOLDS = {
    "JRA": {"SS": 0.641, "S": 0.537, "A": 0.438, "B": 0.257, "C": 0.134},
    "NAR": {"SS": 0.761, "S": 0.673, "A": 0.585, "B": 0.390, "C": 0.224},
}
POP_GATE = {"SS": 1, "S": 2}  # JRA/NAR共通


def classify_v4(score, honmei_pop, is_jra):
    """v4自信度判定"""
    cat = "JRA" if is_jra else "NAR"
    th = THRESHOLDS[cat]

    # score閾値で初期レベル
    if score >= th["SS"]:
        level = "SS"
    elif score >= th["S"]:
        level = "S"
    elif score >= th["A"]:
        level = "A"
    elif score >= th["B"]:
        level = "B"
    elif score >= th["C"]:
        level = "C"
    else:
        level = "D"

    # 人気ゲート
    pop = honmei_pop if honmei_pop else 99
    if level == "SS" and pop > POP_GATE["SS"]:
        level = "S"
    if level == "S" and pop > POP_GATE["S"]:
        level = "A"

    return level


def process_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0, {}

    modified = False
    changes = {}

    for race in data.get("races", []):
        score = race.get("confidence_score", 0) or 0
        venue = race.get("venue", "")
        is_jra = venue in JRA_VENUES

        # ◎馬の人気を取得
        honmei_pop = 99
        for h in race.get("horses", []):
            if h.get("mark", "") in ("◉", "◎"):
                honmei_pop = h.get("popularity") or 99
                break

        new_level = classify_v4(score, honmei_pop, is_jra)
        old_level = race.get("confidence") or race.get("overall_confidence") or "B"

        # E→D統合
        if old_level == "E":
            old_level = "D"

        if new_level != old_level:
            race["confidence"] = new_level
            race["overall_confidence"] = new_level
            modified = True
            key = f"{old_level}→{new_level}"
            changes[key] = changes.get(key, 0) + 1

    if modified:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return modified, changes


def main():
    t0 = time.time()
    files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
    files = [f for f in files if "_prev" not in os.path.basename(f) and "_backup" not in os.path.basename(f)]

    total = len(files)
    print(f"自信度v4再計算: {total}ファイル対象")
    print(f"  JRA: SS≥0.641(pop≤1) S≥0.537(pop≤2) A≥0.438 B≥0.257 C≥0.134 D<0.134")
    print(f"  NAR: SS≥0.761(pop≤1) S≥0.673(pop≤2) A≥0.585 B≥0.390 C≥0.224 D<0.224")
    print()

    total_modified = 0
    all_changes = {}

    for i, fp in enumerate(files):
        mod, changes = process_file(fp)
        if mod:
            total_modified += 1
        for k, v in changes.items():
            all_changes[k] = all_changes.get(k, 0) + v

        if (i + 1) % 100 == 0 or i + 1 == total:
            pct = (i + 1) / total * 100
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (total - i - 1)
            total_ch = sum(all_changes.values())
            print(f"  [{pct:>5.1f}%] {i+1}/{total}  変更{total_modified}件  移動{total_ch}R  ({elapsed:.0f}s / 残{eta:.0f}s)", flush=True)

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.1f}s")
    print(f"  変更ファイル: {total_modified}/{total}")
    print(f"\n  レベル移動内訳:")
    for k in sorted(all_changes.keys()):
        print(f"    {k}: {all_changes[k]}R")


if __name__ == "__main__":
    main()
