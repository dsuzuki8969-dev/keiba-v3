"""全 pred.json の全レースに対して印・買い目を composite 基準で強制再割当

wf_inference.py が更新できなかったレース (ML JSON マッチ失敗 / horse_id 空等) に
旧バイアス印が残存している問題を一括修正する。

処理内容:
  1. 全 pred.json ファイルを走査
  2. 全レースの全馬について composite 降順で印 (◎○▲△★☆) を再割当
  3. softmax win_prob を composite 基準で再計算
  4. confidence 別に三連複チケットを再生成
  5. 変更があったファイルのみ書き戻し
"""

import json
import math
import os
import sys
import time
from glob import glob
from itertools import combinations

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREDICTIONS_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")

MARK_ORDER = ["◎", "○", "▲", "△", "★", "☆"]
PATTERN_MAP = {"SS": "E", "S": "C", "A": "C", "B": "D", "C": "D", "D": "D", "E": "skip"}
M_PRIME_FORMAT = "M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)"


def reassign_marks(horses):
    """composite 順位で印を再割り当て"""
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    active.sort(key=lambda h: h.get("composite", 0), reverse=True)
    for h in horses:
        h["mark"] = ""
    for i, h in enumerate(active):
        if i < len(MARK_ORDER):
            h["mark"] = MARK_ORDER[i]


def softmax_win_probs(horses):
    """composite ベースの softmax で win_prob を再計算"""
    active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
    if not active:
        return
    composites = [h.get("composite", 50.0) for h in active]
    max_c = max(composites)
    exps = [math.exp((c - max_c) / 10.0) for c in composites]
    total = sum(exps)
    if total <= 0:
        return
    probs = [e / total for e in exps]
    active_map = {}
    for h, p in zip(active, probs):
        active_map[h.get("horse_no", -1)] = p
    for h in horses:
        hno = h.get("horse_no", -1)
        if hno in active_map:
            h["win_prob"] = round(active_map[hno], 6)
        elif h.get("is_scratched") or h.get("scrape_failed"):
            h["win_prob"] = 0.0


def regenerate_tickets(horses, confidence):
    """三連複チケット再生成 (M' 戦略)"""
    mark_to_no = {}
    for h in horses:
        m = h.get("mark", "")
        if m and m not in ("", "-", "－", "×"):
            mark_to_no[m] = h.get("horse_no")

    pivot_no = mark_to_no.get("◎")
    if pivot_no is None:
        return []

    taikou_no = mark_to_no.get("○")
    tannuke_no = mark_to_no.get("▲")
    rendashi_no = mark_to_no.get("△")
    rendashi2_no = mark_to_no.get("★")
    ana_no = mark_to_no.get("☆")

    partners = [n for n in [taikou_no, tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
    if len(partners) < 2:
        return []

    tickets = []
    if confidence == "SS":
        if taikou_no is None:
            return []
        thirds = [n for n in [tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
        for t_no in thirds:
            combo = sorted([pivot_no, taikou_no, t_no])
            tickets.append({"type": "三連複", "combo": combo, "pattern": "M'-E", "stake": 100})
    elif confidence in ("S", "A"):
        seconds = [n for n in [taikou_no, tannuke_no] if n is not None]
        all_thirds = [n for n in [taikou_no, tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
        seen = set()
        for s_no in seconds:
            for t_no in all_thirds:
                if t_no == pivot_no or t_no == s_no:
                    continue
                combo = tuple(sorted([pivot_no, s_no, t_no]))
                if combo in seen:
                    continue
                seen.add(combo)
                tickets.append({"type": "三連複", "combo": list(combo), "pattern": "M'-C", "stake": 100})
    else:
        for p1, p2 in combinations(partners, 2):
            combo = sorted([pivot_no, p1, p2])
            tickets.append({"type": "三連複", "combo": combo, "pattern": "M'-D", "stake": 100})

    return tickets


def process_file(fpath, dry_run=False):
    """1ファイルの全レースを処理"""
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    races_updated = 0
    marks_changed = 0

    for race in data.get("races", []):
        horses = race.get("horses", [])
        if not horses:
            continue

        # 現在の印を記録
        old_marks = {h.get("horse_no"): h.get("mark", "") for h in horses}

        # 印を composite 基準で再割当
        reassign_marks(horses)

        # 印が変わったか確認
        new_marks = {h.get("horse_no"): h.get("mark", "") for h in horses}
        changed = any(old_marks.get(k) != new_marks.get(k) for k in set(old_marks) | set(new_marks))
        if changed:
            marks_changed += 1

        # win_prob 再計算
        softmax_win_probs(horses)

        # active馬が少なすぎるレースはチケットをスキップ (偽的中防止)
        active = [h for h in horses if not h.get("is_scratched") and not h.get("scrape_failed")]
        if len(active) < 4 and len(horses) >= 5:
            race["tickets"] = []
            race["formation_tickets"] = []
            pat = "skip"
            race["tickets_by_mode"] = {
                "fixed": [],
                "accuracy": [],
                "balanced": [],
                "recovery": [],
                "_meta": {
                    "format": M_PRIME_FORMAT,
                    "confidence": "E",
                    "pattern": pat,
                    "skipped": True,
                    "skip_reason": f"degenerate: active={len(active)}/{len(horses)}",
                    "race_ev_ratio": 0.0,
                },
            }
            races_updated += 1
            continue

        # confidence 取得
        confidence = race.get("overall_confidence", "") or "B"
        tbm = race.get("tickets_by_mode", {})
        meta = tbm.get("_meta", {})
        if meta.get("confidence"):
            confidence = meta["confidence"]

        # tickets 再生成
        new_tickets = regenerate_tickets(horses, confidence)
        race["tickets"] = new_tickets
        race["formation_tickets"] = []

        pat = PATTERN_MAP.get(confidence, "D")
        race["tickets_by_mode"] = {
            "fixed": new_tickets,
            "accuracy": [],
            "balanced": [],
            "recovery": [],
            "_meta": {
                "format": M_PRIME_FORMAT,
                "confidence": confidence,
                "pattern": pat,
                "skipped": pat == "skip",
                "skip_reason": "E rank" if pat == "skip" else "",
                "race_ev_ratio": 0.0,
            },
        }

        races_updated += 1

    if races_updated > 0 and not dry_run:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return races_updated, marks_changed


def main():
    import argparse
    parser = argparse.ArgumentParser(description="全 pred.json 印・買い目強制再割当")
    parser.add_argument("--start", default="20240101", help="開始日 (YYYYMMDD)")
    parser.add_argument("--end", default="20261231", help="終了日 (YYYYMMDD)")
    parser.add_argument("--dry-run", action="store_true", help="書き込みせず件数のみ表示")
    args = parser.parse_args()

    files = sorted(glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    target_files = []
    for fp in files:
        fname = os.path.basename(fp)
        date_part = fname.replace("_pred.json", "")
        if date_part.isdigit() and args.start <= date_part <= args.end:
            target_files.append(fp)

    print(f"対象ファイル: {len(target_files)} ({args.start} - {args.end})")
    if args.dry_run:
        print("(dry-run モード)")

    t0 = time.time()
    total_races = 0
    total_marks_changed = 0
    total_files_modified = 0

    for i, fp in enumerate(target_files):
        races, marks_changed = process_file(fp, dry_run=args.dry_run)
        total_races += races
        total_marks_changed += marks_changed
        if marks_changed > 0:
            total_files_modified += 1

        if (i + 1) % 50 == 0 or i == len(target_files) - 1:
            elapsed = time.time() - t0
            pct = (i + 1) / len(target_files) * 100
            print(f"  [{i+1}/{len(target_files)}] {pct:.0f}% - "
                  f"races={total_races}, marks_changed={total_marks_changed}, "
                  f"files_modified={total_files_modified}, elapsed={elapsed:.1f}s")

    elapsed = time.time() - t0
    print(f"\n完了: {elapsed:.1f}s")
    print(f"  全レース数: {total_races}")
    print(f"  印変更レース数: {total_marks_changed} ({total_marks_changed/max(total_races,1)*100:.1f}%)")
    print(f"  変更ファイル数: {total_files_modified}/{len(target_files)}")


if __name__ == "__main__":
    main()
