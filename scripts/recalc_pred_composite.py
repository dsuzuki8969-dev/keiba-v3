# -*- coding: utf-8 -*-
"""
施策#4/#6 反映: pred.json のcomposite再計算 + マーク再付与 + チケット再生成

フルパイプライン再実行（数時間）の代わりに、格納済み偏差値から
新しいcomposite重み・TEKIPAN閾値で直接再計算する。

変更される値:
  - composite (新重みで再計算)
  - mark (新TEKIPAN閾値で再付与)
  - tickets / tickets_by_mode (新マークで再生成)
  - overall_confidence (新compositeで再計算)
  - pace_weight_applied (新CAPを記録)

変更されない値 (フルパイプライン再実行が必要):
  - ability_total (#2 VENUE_SPEED_TABLEの反映には再実行が必要)
  - pace_total (#5 坂データの反映には再実行が必要)
  - jockey_dev / trainer_dev (#3 ベイズ収縮の反映には再実行が必要)
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import (
    get_composite_weights,
    DEVIATION,
    TEKIPAN_GAP_JRA, TEKIPAN_GAP_NAR,
    TEKIPAN_WIN_PROB_JRA, TEKIPAN_WIN_PROB_NAR,
    TEKIPAN_WIN_PROB_NAR_BY_FIELD,
    TEKIPAN_PLACE3_PROB_JRA, TEKIPAN_PLACE3_PROB_NAR,
    TEKIPAN_POP_MAX_JRA, TEKIPAN_POP_MAX_NAR,
    _CALIB_VC_TO_NAME,
)

pred_dir = Path("data/predictions")
MARKS_PRIORITY = ["◉", "◎", "○", "▲", "△", "★", "☆", "×", ""]
MIN_WP_HONMEI = 0.05


def venue_name_from_code(vc: str) -> str:
    return _CALIB_VC_TO_NAME.get(vc, "")


def recalc_composite(h: dict, w: dict) -> float:
    """格納済み偏差値から新重みでcompositeを再計算"""
    ability = h.get("ability_total", 50.0) or 50.0
    pace = h.get("pace_total", 50.0) or 50.0
    course = h.get("course_total", 50.0) or 50.0
    jockey = h.get("jockey_dev", 50.0) or 50.0
    trainer = h.get("trainer_dev", 50.0) or 50.0
    bloodline = h.get("bloodline_dev", 50.0) or 50.0
    training = h.get("training_dev", 50.0) or 50.0

    # 調教ボーナス
    tm = 1.0
    if training > 50:
        tm = 1.0 + (training - 50) * 0.006

    v = (
        ability * w["ability"] * tm
        + pace * w["pace"] * tm
        + course * w["course"]
        + jockey * w.get("jockey", 0.10)
        + trainer * w.get("trainer", 0.05)
        + bloodline * w.get("bloodline", 0.05)
    )

    # 補正項
    v += h.get("odds_consistency_adj", 0.0) or 0.0
    v += h.get("ml_composite_adj", 0.0) or 0.0

    return max(DEVIATION["composite"]["min"], min(DEVIATION["composite"]["max"], v))


def assign_marks_simple(horses: list, is_jra: bool) -> list:
    """簡易版マーク付与（pred.jsonの格納値ベース）"""
    n = len(horses)
    if n == 0:
        return horses

    # compositeソート（降順）
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)

    # 全馬のマークをリセット
    for h in sorted_h:
        h["mark"] = ""

    top = sorted_h[0]

    # --- Step 0: ML合意チェック ---
    wp_top = sorted(horses, key=lambda h: h.get("win_prob", 0), reverse=True)[0]
    if top["horse_no"] != wp_top["horse_no"]:
        if (top.get("win_prob", 0) or 0) < MIN_WP_HONMEI:
            # composite1位のwpが低すぎ→wp1位に◎
            top = wp_top
            sorted_h = [top] + [h for h in sorted_h if h["horse_no"] != top["horse_no"]]

    # --- Step 1: ◉ or ◎ ---
    tekipan_gap = TEKIPAN_GAP_JRA if is_jra else TEKIPAN_GAP_NAR
    if is_jra:
        tekipan_wp = TEKIPAN_WIN_PROB_JRA
    else:
        if n <= 8:
            tekipan_wp = TEKIPAN_WIN_PROB_NAR_BY_FIELD["small"]
        elif n <= 12:
            tekipan_wp = TEKIPAN_WIN_PROB_NAR_BY_FIELD["medium"]
        else:
            tekipan_wp = TEKIPAN_WIN_PROB_NAR_BY_FIELD["large"]
    tekipan_p3 = TEKIPAN_PLACE3_PROB_JRA if is_jra else TEKIPAN_PLACE3_PROB_NAR
    tekipan_pop = TEKIPAN_POP_MAX_JRA if is_jra else TEKIPAN_POP_MAX_NAR

    second = sorted_h[1] if len(sorted_h) >= 2 else None
    gap = (top.get("composite", 0) - second.get("composite", 0)) if second else 99.0
    top_wp = top.get("win_prob", 0) or 0
    top_p3 = top.get("place3_prob", 0) or 0
    top_pop = top.get("popularity", 99) or 99

    is_tekipan = (
        gap >= tekipan_gap
        and top_wp >= tekipan_wp
        and top_p3 >= tekipan_p3
        and top_pop <= tekipan_pop
    )
    top["mark"] = "◉" if is_tekipan else "◎"

    # --- Step 2: ○▲△★ ---
    min_wps = {"○": 0.02, "▲": 0.01, "△": 0.005, "★": 0.0}
    remaining_marks = ["○", "▲", "△", "★"]
    for h in sorted_h[1:]:
        if not remaining_marks:
            break
        if h["mark"]:
            continue
        mark = remaining_marks[0]
        wp = h.get("win_prob", 0) or 0
        if wp >= min_wps[mark]:
            h["mark"] = mark
            remaining_marks.pop(0)

    # 5印保証
    for mark in remaining_marks:
        for h in sorted_h[1:]:
            if not h["mark"]:
                h["mark"] = mark
                break
        remaining_marks = [m for m in remaining_marks if m not in [h2.get("mark", "") for h2 in sorted_h]]

    # --- Step 3: ☆ (穴馬) ---
    for h in sorted_h:
        if not h["mark"] and h.get("is_tokusen"):
            h["mark"] = "☆"
            break

    # --- Step 4: × (危険馬) ---
    main_marks = {"◉", "◎", "○", "▲", "△", "★"}
    for h in sorted_h:
        if h.get("is_tokusen_kiken") and h.get("mark", "") not in main_marks:
            h["mark"] = "×"

    return horses


def calc_confidence(horses: list, is_jra: bool) -> str:
    """簡易版confidence計算"""
    from config.settings import (
        CONFIDENCE_THRESHOLDS_JRA, CONFIDENCE_THRESHOLDS_NAR,
        CONFIDENCE_GAP_DIVISOR_JRA, CONFIDENCE_GAP_DIVISOR_NAR,
        CONFIDENCE_WP_GATE_SS_JRA, CONFIDENCE_WP_GATE_SS_NAR,
        CONFIDENCE_GAP_GATE_SS_JRA, CONFIDENCE_GAP_GATE_SS_NAR,
        CONFIDENCE_WP_GATE_S_JRA, CONFIDENCE_WP_GATE_S_NAR,
        CONFIDENCE_GAP_GATE_S_JRA, CONFIDENCE_GAP_GATE_S_NAR,
    )
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)
    if len(sorted_h) < 2:
        return "D"

    top = sorted_h[0]
    second = sorted_h[1]
    gap = top.get("composite", 0) - second.get("composite", 0)
    top_wp = top.get("win_prob", 0) or 0
    top_p3 = top.get("place3_prob", 0) or 0

    gap_div = CONFIDENCE_GAP_DIVISOR_JRA if is_jra else CONFIDENCE_GAP_DIVISOR_NAR
    thresholds = CONFIDENCE_THRESHOLDS_JRA if is_jra else CONFIDENCE_THRESHOLDS_NAR

    # 6信号スコア (簡易版: 主要4信号)
    gap_norm = min(gap / gap_div, 1.0) if gap_div > 0 else 0.0
    wp_norm = min(top_wp / 0.30, 1.0)
    p3_norm = min(top_p3 / 0.80, 1.0)
    mark_signal = 1.0 if top.get("mark") == "◉" else 0.7 if top.get("mark") == "◎" else 0.3

    score = (gap_norm * 0.30 + wp_norm * 0.25 + p3_norm * 0.20 + mark_signal * 0.25)

    # ランク判定
    for rank in ["SS", "S", "A", "B", "C"]:
        if score >= thresholds[rank]:
            # SS/Sゲート
            if rank == "SS":
                wp_gate = CONFIDENCE_WP_GATE_SS_JRA if is_jra else CONFIDENCE_WP_GATE_SS_NAR
                gap_gate = CONFIDENCE_GAP_GATE_SS_JRA if is_jra else CONFIDENCE_GAP_GATE_SS_NAR
                if top_wp < wp_gate or gap < gap_gate:
                    rank = "S"
            if rank == "S":
                wp_gate = CONFIDENCE_WP_GATE_S_JRA if is_jra else CONFIDENCE_WP_GATE_S_NAR
                gap_gate = CONFIDENCE_GAP_GATE_S_JRA if is_jra else CONFIDENCE_GAP_GATE_S_NAR
                if top_wp < wp_gate or gap < gap_gate:
                    rank = "A"
            return rank
    return "D"


def generate_m_prime_tickets(horses: list, confidence: str) -> list:
    """M'戦略の三連複チケット生成（簡易版）"""
    from config.settings import STAKE_DEFAULT

    if confidence == "D":
        return []

    stake = STAKE_DEFAULT.get(confidence, 0)
    if stake <= 0:
        return []

    # col1: ◉◎のみ
    col1_marks = {"◉", "◎"}
    # col2: ◉◎○▲△★☆
    col2_marks = {"◉", "◎", "○", "▲", "△", "★", "☆"}
    # col3: 印付き馬
    col3_marks = {"◉", "◎", "○", "▲", "△", "★", "☆"}

    col1 = [h["horse_no"] for h in horses if h.get("mark", "") in col1_marks]
    col2 = [h["horse_no"] for h in horses if h.get("mark", "") in col2_marks]
    col3 = [h["horse_no"] for h in horses if h.get("mark", "") in col3_marks]

    if not col1 or len(col2) < 2:
        return []

    # フォーメーション生成
    tickets = []
    seen = set()
    for c1 in col1:
        for c2 in col2:
            if c2 == c1:
                continue
            for c3 in col3:
                if c3 == c1 or c3 == c2:
                    continue
                combo = tuple(sorted([c1, c2, c3]))
                if combo not in seen:
                    seen.add(combo)
                    tickets.append({
                        "type": "三連複",
                        "combo": list(combo),
                        "pattern": f"M'-recalc",
                        "stake": 100,
                    })

    # 最大15点に制限
    tickets = tickets[:15]
    return tickets


def process_file(fp: Path) -> dict:
    """1ファイル処理、統計を返す"""
    data = json.loads(fp.read_text(encoding="utf-8"))
    races = data.get("races", [])
    stats = {"races": 0, "comp_changed": 0, "mark_changed": 0}

    for race in races:
        is_jra = race.get("is_jra", True)
        venue_code = race.get("venue_code", "")
        venue_name = venue_name_from_code(venue_code) or race.get("venue", "")
        surface = race.get("surface", "")
        distance = race.get("distance", 0)
        field_count = race.get("field_count", 0)
        horses = race.get("horses", [])
        stats["races"] += 1

        # 新重みでcomposite再計算
        w = get_composite_weights(
            venue_name,
            surface=surface,
            field_size=field_count or len(horses),
            distance=distance,
        )

        for h in horses:
            old_comp = h.get("composite", 0)
            new_comp = recalc_composite(h, w)
            if abs(new_comp - old_comp) > 0.01:
                stats["comp_changed"] += 1
            h["composite"] = round(new_comp, 2)
            h["pace_weight_applied"] = round(w.get("pace", 0.30), 3)

        # 旧マークを保存
        old_marks = {h["horse_no"]: h.get("mark", "") for h in horses}

        # マーク再付与
        assign_marks_simple(horses, is_jra)

        for h in horses:
            if h.get("mark", "") != old_marks.get(h["horse_no"], ""):
                stats["mark_changed"] += 1

        # confidence再計算
        race["overall_confidence"] = calc_confidence(horses, is_jra)

        # チケット再生成
        tickets = generate_m_prime_tickets(horses, race["overall_confidence"])
        race["tickets"] = tickets
        # tickets_by_mode の _meta を更新
        tbm = race.get("tickets_by_mode", {})
        if "_meta" in tbm:
            tbm["_meta"]["confidence"] = race["overall_confidence"]
        race["tickets_by_mode"] = tbm

    fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return stats


def main():
    files = sorted(pred_dir.glob("2026*_pred.json"))
    files = [f for f in files if "_prev" not in f.name and ".bak" not in f.name]
    total = len(files)
    print(f"対象: {total} ファイル")

    total_stats = defaultdict(int)
    for i, fp in enumerate(files):
        stats = process_file(fp)
        for k, v in stats.items():
            total_stats[k] += v
        if (i + 1) % 20 == 0 or i + 1 == total:
            pct = (i + 1) / total * 100
            print(f"  [{i+1}/{total}] {pct:.0f}% - composite変更: {total_stats['comp_changed']}, mark変更: {total_stats['mark_changed']}")

    print(f"\n完了: {total_stats['races']}レース処理")
    print(f"  composite変更: {total_stats['comp_changed']}馬")
    print(f"  mark変更: {total_stats['mark_changed']}馬")


if __name__ == "__main__":
    main()
