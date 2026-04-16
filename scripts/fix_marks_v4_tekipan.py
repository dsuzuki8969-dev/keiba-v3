#!/usr/bin/env python3
"""
◉マーク v4 再計算 — 全pred.jsonに適用

v4変更点:
  - EVフィルター撤廃（低オッズ本命を排除していた問題を解消）
  - 人気フィルター追加（JRA: pop≤2, NAR: pop≤2）
  - JRA: gap 5→7, wp 0.30→0.25, p3p 0.65→0.70
  - NAR: wp 0.25→0.35, p3p 0.0→0.75

ロジック: ◉ → ◎ に降格、または ◎ → ◉ に昇格を判定
"""
import json
import glob
import os
import sys
import io
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True, errors="replace")

PRED_DIR = "data/predictions"
JRA_VENUES = {"東京", "中山", "阪神", "京都", "中京", "小倉", "新潟", "福島", "函館", "札幌"}

# v4 条件
V4 = {
    "JRA": {"gap": 7.0, "wp": 0.25, "p3p": 0.70, "pop_max": 2},
    "NAR": {"gap": 5.0, "wp": 0.35, "p3p": 0.75, "pop_max": 2},
}


def should_be_tekipan_v4(race):
    """レース内のcomposite1位が◉条件を満たすか判定"""
    venue = race.get("venue", "")
    is_jra = venue in JRA_VENUES
    cat = "JRA" if is_jra else "NAR"
    cfg = V4[cat]

    horses = race.get("horses", [])
    if len(horses) < 5:
        return None, None

    # composite降順ソート
    sh = sorted(horses, key=lambda h: h.get("composite", 0) or 0, reverse=True)
    top = sh[0]
    second = sh[1] if len(sh) >= 2 else None

    # gap（1位-2位差）
    gap = (top.get("composite", 0) or 0) - (second.get("composite", 0) or 0) if second else 99.0

    # 各条件チェック
    wp = top.get("win_prob", 0) or 0
    p3p = top.get("place3_prob", 0) or 0
    pop = top.get("popularity") or 99

    is_tekipan = (
        gap >= cfg["gap"]
        and wp >= cfg["wp"]
        and p3p >= cfg["p3p"]
        and pop <= cfg["pop_max"]
    )

    return top.get("horse_no"), is_tekipan


def process_file(filepath):
    """1ファイル処理: ◉/◎マークを再計算"""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0, 0, 0

    modified = False
    promoted = 0  # ◎→◉
    demoted = 0   # ◉→◎

    for race in data.get("races", []):
        horses = race.get("horses", [])
        target_hno, should_tekipan = should_be_tekipan_v4(race)
        if target_hno is None:
            continue

        for h in horses:
            hno = h.get("horse_no")
            mark = h.get("mark", "")

            if hno == target_hno:
                # このhorse_noが◉対象
                if should_tekipan and mark != "◉":
                    # ◎→◉昇格（◎以外から◉にはしない。◎のみ対象）
                    if mark == "◎":
                        h["mark"] = "◉"
                        modified = True
                        promoted += 1
                elif not should_tekipan and mark == "◉":
                    # ◉→◎降格
                    h["mark"] = "◎"
                    modified = True
                    demoted += 1
            else:
                # 別の馬が◉を持っている場合は◎に降格
                if mark == "◉":
                    h["mark"] = "◎"
                    modified = True
                    demoted += 1

    if modified:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return modified, promoted, demoted


def main():
    t0 = time.time()
    files = sorted(glob.glob(os.path.join(PRED_DIR, "*_pred.json")))
    files = [f for f in files if "_prev" not in os.path.basename(f) and "_backup" not in os.path.basename(f)]

    total = len(files)
    print(f"◉v4再計算: {total}ファイル対象")
    print(f"  JRA: gap≥{V4['JRA']['gap']:.0f} wp≥{V4['JRA']['wp']:.0%} p3p≥{V4['JRA']['p3p']:.0%} pop≤{V4['JRA']['pop_max']}")
    print(f"  NAR: gap≥{V4['NAR']['gap']:.0f} wp≥{V4['NAR']['wp']:.0%} p3p≥{V4['NAR']['p3p']:.0%} pop≤{V4['NAR']['pop_max']}")
    print(f"  EVフィルター: 撤廃")
    print()

    total_modified = 0
    total_promoted = 0
    total_demoted = 0

    for i, fp in enumerate(files):
        mod, prom, dem = process_file(fp)
        if mod:
            total_modified += 1
        total_promoted += prom
        total_demoted += dem

        if (i + 1) % 100 == 0 or i + 1 == total:
            pct = (i + 1) / total * 100
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (total - i - 1)
            print(
                f"  [{pct:>5.1f}%] {i+1}/{total}  "
                f"変更{total_modified}件  昇格{total_promoted}  降格{total_demoted}  "
                f"({elapsed:.0f}s / 残{eta:.0f}s)",
                flush=True,
            )

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.1f}s")
    print(f"  変更ファイル: {total_modified}/{total}")
    print(f"  ◎→◉昇格: {total_promoted}頭")
    print(f"  ◉→◎降格: {total_demoted}頭")


if __name__ == "__main__":
    main()
