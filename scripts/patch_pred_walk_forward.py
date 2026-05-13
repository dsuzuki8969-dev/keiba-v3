"""pred.json Walk-Forward パッチスクリプト

Phase 1: odds_consistency_adj を 0 にして composite 再計算
Phase 2: odds + ml_composite_adj + market_anchor_adj 全除去 + チケット再生成

全 pred.json を直接書き換える。バックアップオプションあり。
"""

import argparse
import json
import math
import os
import shutil
import sys
from glob import glob
from itertools import combinations

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PREDICTIONS_DIR = os.path.join(PROJECT_ROOT, "data", "predictions")


def softmax_win_probs(horses: list) -> None:
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


MARK_ORDER = ["◎", "○", "▲", "△", "★", "☆"]


def reassign_marks(horses: list) -> None:
    """composite 順位で印を再割り当て (上位6頭: ◎○▲△★☆)"""
    active = [
        h for h in horses
        if not h.get("is_scratched") and not h.get("scrape_failed")
    ]
    active.sort(key=lambda h: h.get("composite", 0), reverse=True)

    for h in horses:
        h["mark"] = ""

    for i, h in enumerate(active):
        if i < len(MARK_ORDER):
            h["mark"] = MARK_ORDER[i]


def regenerate_tickets(horses: list, confidence: str) -> list:
    """新しい印に基づいて三連複チケットを再生成する。

    confidence に応じたパターン:
      SS → 絞り: ◎+○ 軸 → {▲,△,★,☆} (最大4点)
      S/A → 中: ◎ 軸, 2列{○,▲}, 3列{○,▲,△,★,☆} (最大7点)
      B/C/D/E → 広: ◎ 軸 + C({○,▲,△,★,☆}, 2) (最大10点)
    """
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
        # 絞り: ◎+○ 軸 → 各{▲,△,★,☆}
        if taikou_no is None:
            return []
        thirds = [n for n in [tannuke_no, rendashi_no, rendashi2_no, ana_no] if n is not None]
        for t_no in thirds:
            combo = sorted([pivot_no, taikou_no, t_no])
            tickets.append({
                "type": "三連複",
                "combo": combo,
                "pattern": "M'-E",
                "stake": 100,
            })
    elif confidence in ("S", "A"):
        # 中: ◎ 軸, second={○,▲}, third={○,▲,△,★,☆}
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
                tickets.append({
                    "type": "三連複",
                    "combo": list(combo),
                    "pattern": "M'-C",
                    "stake": 100,
                })
    else:
        # 広: ◎ 軸 + C(partners, 2)
        for p1, p2 in combinations(partners, 2):
            combo = sorted([pivot_no, p1, p2])
            tickets.append({
                "type": "三連複",
                "combo": combo,
                "pattern": "M'-D",
                "stake": 100,
            })

    return tickets


def patch_pred_file(fpath: str, remove_odds_adj: bool = True,
                    remove_ml_adj: bool = False,
                    remove_market_anchor: bool = False,
                    regen_tickets: bool = False,
                    dry_run: bool = False) -> dict:
    """1ファイルのパッチ。変更統計を返す。"""
    with open(fpath, "r", encoding="utf-8") as f:
        data = json.load(f)

    stats = {
        "horses": 0,
        "odds_adj_removed": 0,
        "ml_adj_removed": 0,
        "market_adj_removed": 0,
        "composite_changed": 0,
        "mark_changed": 0,
        "tickets_regenerated": 0,
    }

    for race in data.get("races", []):
        old_marks = {h.get("horse_no"): h.get("mark", "") for h in race.get("horses", [])}

        for h in race.get("horses", []):
            stats["horses"] += 1
            old_composite = h.get("composite", 50.0)
            delta = 0.0

            if remove_odds_adj:
                oa = h.get("odds_consistency_adj", 0) or 0
                if abs(oa) > 0.001:
                    delta += oa
                    h["odds_consistency_adj"] = 0.0
                    stats["odds_adj_removed"] += 1

            if remove_ml_adj:
                ma = h.get("ml_composite_adj", 0) or 0
                if abs(ma) > 0.001:
                    delta += ma
                    h["ml_composite_adj"] = 0.0
                    stats["ml_adj_removed"] += 1

            if remove_market_anchor:
                mka = h.get("market_anchor_adj", 0) or 0
                if abs(mka) > 0.001:
                    delta += mka
                    h["market_anchor_adj"] = 0.0
                    stats["market_adj_removed"] += 1

            if abs(delta) > 0.001:
                h["composite"] = round(old_composite - delta, 4)
                stats["composite_changed"] += 1

        reassign_marks(race.get("horses", []))
        softmax_win_probs(race.get("horses", []))

        for h in race.get("horses", []):
            hno = h.get("horse_no")
            if old_marks.get(hno, "") != h.get("mark", ""):
                stats["mark_changed"] += 1

        if regen_tickets:
            confidence = race.get("confidence", "B") or "B"
            new_tickets = regenerate_tickets(race.get("horses", []), confidence)
            race["tickets"] = new_tickets
            race["formation_tickets"] = []
            # M' 戦略パターンマップ
            _pattern_map = {"SS": "E", "S": "C", "A": "C", "B": "D", "C": "D", "D": "D", "E": "skip"}
            _pat = _pattern_map.get(confidence, "D")
            race["tickets_by_mode"] = {
                "fixed": new_tickets,
                "accuracy": [],
                "balanced": [],
                "recovery": [],
                "_meta": {
                    "format": f"M': 自信度別 三連複 (SS=E/S=C/A=C/B/C/D=D/E=skip)",
                    "confidence": confidence,
                    "pattern": _pat,
                    "skipped": _pat == "skip",
                    "skip_reason": "E rank" if _pat == "skip" else "",
                    "race_ev_ratio": 0.0,
                },
            }
            race["overall_confidence"] = confidence
            stats["tickets_regenerated"] += 1

    if not dry_run:
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    return stats


def main():
    parser = argparse.ArgumentParser(description="pred.json Walk-Forward パッチ")
    parser.add_argument("--start", default="2024-01-01", help="開始日")
    parser.add_argument("--end", default="2026-05-11", help="終了日")
    parser.add_argument("--backup", action="store_true", help="変更前にバックアップ作成")
    parser.add_argument("--dry-run", action="store_true", help="書き込みせず統計のみ表示")
    parser.add_argument("--phase", type=int, default=1, choices=[1, 2, 3],
                        help="Phase 1: odds除去のみ, Phase 2: odds+ML+market全除去, Phase 3: Phase2+チケット再生成")
    args = parser.parse_args()

    start_fname = args.start.replace("-", "")
    end_fname = args.end.replace("-", "")

    files = sorted(glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
    targets = [
        f for f in files
        if start_fname <= os.path.basename(f)[:8] <= end_fname
    ]

    print(f"対象 pred.json: {len(targets)} ファイル ({args.start} ～ {args.end})")
    if args.dry_run:
        print("(dry-run: 書き込みなし)")

    if args.backup and not args.dry_run:
        backup_dir = os.path.join(PREDICTIONS_DIR, "_backup_pre_wf")
        os.makedirs(backup_dir, exist_ok=True)
        print(f"バックアップ先: {backup_dir}")
        for f in targets:
            shutil.copy2(f, os.path.join(backup_dir, os.path.basename(f)))
        print(f"  {len(targets)} ファイルバックアップ完了")

    total = {
        "files": 0,
        "horses": 0,
        "odds_adj_removed": 0,
        "ml_adj_removed": 0,
        "market_adj_removed": 0,
        "composite_changed": 0,
        "mark_changed": 0,
        "tickets_regenerated": 0,
    }

    for i, fpath in enumerate(targets):
        s = patch_pred_file(
            fpath,
            remove_odds_adj=(args.phase >= 1),
            remove_ml_adj=(args.phase >= 2),
            remove_market_anchor=(args.phase >= 2),
            regen_tickets=(args.phase >= 3),
            dry_run=args.dry_run,
        )
        total["files"] += 1
        for k in s:
            total[k] = total.get(k, 0) + s[k]

        if (i + 1) % 100 == 0 or i == len(targets) - 1:
            pct = (i + 1) / len(targets) * 100
            removed = total['odds_adj_removed'] + total['ml_adj_removed'] + total['market_adj_removed']
            print(f"  [{i+1}/{len(targets)}] {pct:.0f}% — 除去: {removed:,}, "
                  f"印変更: {total['mark_changed']:,}")

    n = max(total["horses"], 1)
    print(f"\n=== パッチ完了 (Phase {args.phase}) ===")
    print(f"  ファイル: {total['files']}")
    print(f"  馬数: {total['horses']:,}")
    print(f"  odds_adj 除去: {total['odds_adj_removed']:,} ({total['odds_adj_removed']/n*100:.1f}%)")
    if args.phase >= 2:
        print(f"  ml_adj 除去:   {total['ml_adj_removed']:,} ({total['ml_adj_removed']/n*100:.1f}%)")
        print(f"  market_adj 除去: {total['market_adj_removed']:,} ({total['market_adj_removed']/n*100:.1f}%)")
    print(f"  composite 変更: {total['composite_changed']:,}")
    print(f"  印変更: {total['mark_changed']:,} ({total['mark_changed']/n*100:.1f}%)")
    if args.phase >= 3:
        print(f"  チケット再生成: {total['tickets_regenerated']:,} レース")


if __name__ == "__main__":
    main()
