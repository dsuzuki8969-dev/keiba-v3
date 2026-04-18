"""
pace.total / composite / win_prob / 印 / predicted_corners を
「予想ゴールタイム差」ベースで再計算するスクリプト（最小影響化の原則に従い
フルパイプライン再実行不要、pred.json を直接書き換える）。

【背景】
  pace.total は last3f_eval / position_balance 等の離散評価コンポーネントで
  構成されており、実数ベースの "予想ゴールタイム差" を直接反映していない。
  その結果、ML が推定した estimated_last3f / estimated_pos_4c から導かれる
  ゴール時点の着順と、印(総合指数)の結論が乖離する問題が恒常的に発生していた。

【処理内容】
  1) 予想ゴールタイム = (pos_4c - 1) × 0.15秒 + estimated_last3f
     フィールド内 Z-score → goal_time_dev (偏差値50基準)
  2) pace_total_new = pace_total_old × 0.5 + goal_time_dev × 0.5
  3) composite 再計算 (venue 別 6因子重み)
  4) estimate_three_win_rates() で win/place2/place3 再計算
  5) 印再割当 (composite 上位から ◎○▲△★, 8頭以下は4頭目まで)
  6) predicted_corners を composite 連動で再計算
     1角: estimated_pos_1c そのまま
     4角: ML推定 × 0.4 + composite順位 × 0.6 (composite寄り)
  7) 取消馬は確率0・印空・predicted_corners空

使い方:
  python scripts/fix_pace_goal_time.py                # 全日付
  python scripts/fix_pace_goal_time.py 2026-04-18     # 特定日
  python scripts/fix_pace_goal_time.py --dry-run      # 書き込みなし
"""
import argparse
import json
import statistics
import sys
from pathlib import Path

# プロジェクトルートを path へ
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import get_composite_weights  # noqa: E402
from src.calculator.jockey_trainer import estimate_three_win_rates  # noqa: E402

PRED_DIR = Path(__file__).resolve().parent.parent / "data" / "predictions"

VENUE_NAME_TO_CODE = {
    "札幌": "01", "函館": "02", "福島": "03", "新潟": "04",
    "東京": "05", "中山": "06", "中京": "07", "京都": "08",
    "阪神": "09", "小倉": "10",
    "門別": "30", "盛岡": "35", "水沢": "36", "浦和": "42",
    "船橋": "43", "大井": "44", "川崎": "45", "金沢": "46",
    "笠松": "47", "名古屋": "48", "園田": "50", "姫路": "51",
    "高知": "54", "佐賀": "55", "帯広": "65",
}
JRA_CODES = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10"}

# 1番手差の秒換算（標準値、pace_course.py の POSITION_SEC_PER_RANK と整合）
SEC_PER_RANK = 0.15

# pace_total を旧値と ゴールタイム偏差値 で何対何で blend するか
PACE_BLEND_RATIO = 0.5  # 0.5 = 5:5 blend

# predicted_corners の 4角順位で ML vs composite をどう blend するか (脚質別)
# ML推定 pos4c と composite順位を blend する比率 (1.0 = 全部ML)
# 逃げ馬はMLを尊重（ポジション争いが重要）、追込馬はcomposite強め
CORNER4_ML_RATIO_BY_STYLE = {
    "逃げ": 0.80,   # ML 80% : composite 20%
    "先行": 0.60,   # ML 60% : composite 40%
    "差し": 0.45,   # ML 45% : composite 55%
    "追込": 0.35,   # ML 35% : composite 65%
    "":     0.55,   # デフォルト
}


def _compute_goal_time_devs(active_horses):
    """予想ゴールタイムからフィールド内偏差値を算出して返す"""
    goal_times = []
    for h in active_horses:
        pos4c = h.get("pace_estimated_pos4c")
        l3f = h.get("pace_estimated_last3f")
        if pos4c is None or l3f is None or l3f <= 0:
            goal_times.append(None)
        else:
            # 予想ゴールタイム = 先頭からの4角着差 + 直線上がり3F
            gt = (pos4c - 1) * SEC_PER_RANK + l3f
            goal_times.append(gt)

    valid = [g for g in goal_times if g is not None]
    if len(valid) < 2:
        return [50.0] * len(active_horses)

    avg = statistics.mean(valid)
    std = statistics.stdev(valid) if len(valid) >= 2 else 0.3
    std = max(0.2, std)  # ゼロ除算防止 & 過剰感度抑制

    devs = []
    for gt in goal_times:
        if gt is None:
            devs.append(50.0)
        else:
            # 速い(gt小)ほど高偏差値
            z = (avg - gt) / std
            dev = 50.0 + z * 10.0
            devs.append(max(20.0, min(100.0, dev)))
    return devs


def _assign_marks(sorted_active):
    """composite 上位から印を割当 (◎○▲△★, 穴馬候補で☆)"""
    marks = ["◎", "○", "▲", "△", "★"]
    for h in sorted_active:
        h["mark"] = ""
    for i, h in enumerate(sorted_active[: min(5, len(sorted_active))]):
        h["mark"] = marks[i]
    # 穴馬候補: 6-8位 & 人気7番人気以下 → ☆
    for h in sorted_active[5:8]:
        pop = h.get("popularity")
        if pop is not None and pop >= 7:
            h["mark"] = "☆"
            break
    # 危険馬候補: 人気1-3位で composite 下位50% → ×
    n = len(sorted_active)
    if n >= 6:
        lower_half = sorted_active[n // 2:]
        for h in lower_half:
            pop = h.get("popularity")
            if pop is not None and 1 <= pop <= 3 and not h.get("mark"):
                h["mark"] = "×"
                break


def _recalc_predicted_corners(active, corner_count):
    """composite 連動で predicted_corners を再計算"""
    n = len(active)
    if n < 2:
        return

    horse_positions = []  # [(horse_no, pos_1c_norm, pos_4c_norm)]
    # composite 順位 (1位=最上位)
    sorted_comp = sorted((h["composite"] for h in active), reverse=True)

    for h in active:
        pos_1c_raw = h.get("estimated_pos_1c")
        if pos_1c_raw is not None and n > 1:
            pos_1c = max(0.0, min(1.0, (pos_1c_raw - 1) / (n - 1)))
        else:
            pos_1c = h.get("position_initial", 0.5) or 0.5

        pos_4c_ml_raw = h.get("pace_estimated_pos4c")
        if pos_4c_ml_raw is not None and n > 1:
            pos_4c_ml = max(0.0, min(1.0, (pos_4c_ml_raw - 1) / (n - 1)))
        else:
            style = h.get("running_style", "") or "先行"
            shift = {"逃げ": 0.06, "先行": 0.00, "差し": -0.15, "追込": -0.25}.get(style, 0.0)
            pos_4c_ml = max(0.0, min(1.0, pos_1c + shift))

        # composite 順位を 0.0-1.0 正規化
        comp = h["composite"]
        comp_rank = sorted_comp.index(comp) + 1
        pos_4c_comp = max(0.0, min(1.0, (comp_rank - 1) / (n - 1))) if n > 1 else 0.5

        # 脚質別 blend 比率: 逃げは ML 優先、追込は composite 優先
        style = h.get("running_style", "") or ""
        ml_ratio = CORNER4_ML_RATIO_BY_STYLE.get(style, 0.55)
        pos_4c = pos_4c_ml * ml_ratio + pos_4c_comp * (1.0 - ml_ratio)
        horse_positions.append((h["horse_no"], pos_1c, pos_4c))

    # 各コーナーを線形補間して順位化
    if corner_count <= 2:
        corner_indices = [2, 3]
    else:
        corner_indices = [0, 1, 2, 3]

    corners_per_horse = {}
    for ci in corner_indices:
        if corner_count <= 2:
            t = 0.5 if ci == 2 else 1.0
        else:
            t = ci / 3.0

        scores = []
        for hno, p1, p4 in horse_positions:
            score = p1 * (1.0 - t) + p4 * t
            scores.append((hno, score))
        scores.sort(key=lambda x: x[1])
        for rank, (hno, _) in enumerate(scores, 1):
            corners_per_horse.setdefault(hno, []).append(rank)

    for h in active:
        ranks = corners_per_horse.get(h["horse_no"], [])
        h["predicted_corners"] = "-".join(str(r) for r in ranks) if ranks else ""

        # 脚質整合性チェック: 1角位置と running_style の矛盾補正
        if ranks:
            first = ranks[0]
            rs = h.get("running_style", "")
            if rs == "逃げ" and first >= 3:
                h["running_style"] = "先行"
            elif rs in ("差し", "追込") and first == 1:
                h["running_style"] = "逃げ"
            elif rs == "追込" and first <= 2:
                h["running_style"] = "先行"


def recalc_race(race, is_jra: bool, stats: dict):
    horses = race.get("horses", [])
    if not horses or race.get("is_banei"):
        return

    active = [h for h in horses if not h.get("is_scratched")]
    if len(active) < 2:
        return

    venue = race.get("venue", "")
    surface = race.get("surface", "")
    distance = race.get("distance", 0)
    field_size = len(active)

    # ---- Step 1: 予想ゴールタイム偏差値 ----
    goal_devs = _compute_goal_time_devs(active)

    # ---- Step 2: pace_total を blend ----
    for h, gdev in zip(active, goal_devs):
        old = h.get("pace_total", 50.0)
        new = old * (1.0 - PACE_BLEND_RATIO) + gdev * PACE_BLEND_RATIO
        h["pace_total_prev"] = round(old, 2)
        h["pace_goal_time_dev"] = round(gdev, 2)
        h["pace_total"] = round(max(20.0, min(100.0, new)), 2)

    # ---- Step 3: composite 再計算 ----
    w = get_composite_weights(venue, surface=surface, field_size=field_size, distance=distance)

    for h in active:
        ab = h.get("ability_total", 50.0) or 50.0
        pc = h.get("pace_total", 50.0) or 50.0
        co = h.get("course_total", 50.0) or 50.0
        jdev = h.get("jockey_dev", 50.0) or 50.0
        tdev = h.get("trainer_dev", 50.0) or 50.0
        bdev = h.get("bloodline_dev", 50.0) or 50.0
        ml_adj = h.get("ml_composite_adj", 0.0) or 0.0

        new_comp = (
            ab * w["ability"]
            + pc * w["pace"]
            + co * w["course"]
            + jdev * w.get("jockey", 0.10)
            + tdev * w.get("trainer", 0.05)
            + bdev * w.get("bloodline", 0.05)
            + ml_adj
        )
        h["composite_prev"] = round(h.get("composite", 0.0), 2)
        h["composite"] = round(max(20.0, min(100.0, new_comp)), 2)

    # ---- Step 4: win_prob / place2 / place3 再計算 ----
    all_comp = [h["composite"] for h in active]
    all_pace = [h["pace_total"] for h in active]
    all_course = [h.get("course_total", 50.0) or 50.0 for h in active]
    fc = len(active)

    for h in active:
        win, p2, p3 = estimate_three_win_rates(
            h["composite"], all_comp,
            pace_score=h["pace_total"],
            course_score=h.get("course_total", 50.0) or 50.0,
            all_pace_scores=all_pace,
            all_course_scores=all_course,
            field_count=fc,
            is_jra=is_jra,
        )
        h["win_prob"] = float(win)
        h["place2_prob"] = float(p2)
        h["place3_prob"] = float(p3)

    # 正規化: sum=1.0 / min(n,2) / min(n,3)
    for pk, ts in [("win_prob", 1.0),
                   ("place2_prob", min(fc, 2)),
                   ("place3_prob", min(fc, 3))]:
        total = sum(h[pk] for h in active)
        if total > 0:
            for h in active:
                h[pk] = round(min(1.0, h[pk] / total * ts), 4)
        else:
            for h in active:
                h[pk] = 0.0

    # 勝率<連対率<複勝率 整合補正
    for h in active:
        h["place2_prob"] = max(h["place2_prob"], h["win_prob"])
        h["place3_prob"] = max(h["place3_prob"], h["place2_prob"])

    # ---- Step 5: 印再割当 ----
    sorted_active = sorted(active, key=lambda x: -x["composite"])
    _assign_marks(sorted_active)

    # ---- Step 6: predicted_corners 再計算 ----
    corner_count = race.get("corner_count", 4)
    _recalc_predicted_corners(active, corner_count)

    # ---- Step 7: 取消馬クリア ----
    for h in horses:
        if h.get("is_scratched"):
            h["win_prob"] = 0.0
            h["place2_prob"] = 0.0
            h["place3_prob"] = 0.0
            h["mark"] = ""
            h["predicted_corners"] = ""

    stats["races"] += 1
    stats["horses"] += len(active)


def main():
    parser = argparse.ArgumentParser(description="pace/composite/印/予想通過順を ゴールタイム差 ベースで再計算")
    parser.add_argument("date", nargs="?", default=None, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.date:
        dk = args.date.replace("-", "")
        files = [PRED_DIR / f"{dk}_pred.json"]
    else:
        files = sorted(
            (f for f in PRED_DIR.glob("*_pred.json")
             if "_prev" not in f.name and ".bak" not in f.name),
            reverse=True, key=lambda p: p.name)

    print(f"=== fix_pace_goal_time 開始 {len(files)}ファイル ===")
    if args.dry_run:
        print("[DRY-RUN モード]")

    grand = {"races": 0, "horses": 0}

    for i, fp in enumerate(files, 1):
        if not fp.exists():
            print(f"[{i}/{len(files)}] {fp.name}: 存在しない")
            continue

        try:
            data = json.load(open(fp, "r", encoding="utf-8"))
        except Exception as e:
            print(f"[{i}/{len(files)}] {fp.name}: 読込失敗 {e}")
            continue

        s = {"races": 0, "horses": 0}
        for race in data.get("races", []):
            venue = race.get("venue", "")
            is_jra = VENUE_NAME_TO_CODE.get(venue, "") in JRA_CODES
            recalc_race(race, is_jra, s)

        if not args.dry_run and s["races"] > 0:
            try:
                json.dump(data, open(fp, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"  保存失敗: {e}")

        pct = (i / len(files)) * 100
        print(f"[{i}/{len(files)}] ({pct:.1f}%) {fp.name} races={s['races']} horses={s['horses']}")
        for k in grand:
            grand[k] += s[k]

    print(f"\n=== 完了 ===  総レース={grand['races']}, 総頭数={grand['horses']}")


if __name__ == "__main__":
    main()
