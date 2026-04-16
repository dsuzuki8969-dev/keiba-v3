"""
全頭診断用グレード算出ユーティリティ

偏差値ベースの統一グレード体系（7段階）:
  SS: ≥65   S: 61-64.9   A: 56-60.9
  B: 49-55.9   C: 44-48.9   D: 39-43.9   E: <39

目標分布 (N(52.5, 6.4) ベース):
  SS=2.5%, S=7.5%, A=20%, B=40%, C=20%, D=7.5%, E=2.5%

各セクション（騎手/調教師/血統/コース適性）の詳細項目を
SS〜E のグレードに変換して返す。
"""

from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# 共通ユーティリティ
# ============================================================

_GRADE_THRESHOLDS = [
    (65.0, "SS"),
    (61.0, "S"),
    (56.0, "A"),
    (49.0, "B"),
    (44.0, "C"),
    (39.0, "D"),
]


def dev_to_grade(value: Optional[float]) -> str:
    """偏差値 → SS/S/A/B/C/D/E"""
    if value is None:
        return "—"
    for threshold, grade in _GRADE_THRESHOLDS:
        if value >= threshold:
            return grade
    return "E"



def rate_to_dev(rate: float, mean: float = 0.10, sigma: float = 0.05) -> float:
    """勝率/複勝率 → 偏差値変換 (Z-score × 10 + 50)"""
    if sigma <= 0:
        return 50.0
    return (rate - mean) / sigma * 10.0 + 50.0


def rate_to_grade(
    rate: float,
    mean: float = 0.10,
    sigma: float = 0.05,
    min_samples: int = 5,
    sample_n: int = 0,
) -> str:
    """勝率 → グレード (サンプル数不足なら "—")"""
    if sample_n < min_samples:
        return "—"
    return dev_to_grade(rate_to_dev(rate, mean, sigma))


def _weighted_avg(records: List[Tuple[float, int]]) -> Optional[float]:
    """(dev, sample_n) のリストからサンプル加重平均を算出"""
    total_w = sum(n for _, n in records)
    if total_w == 0:
        return None
    return sum(d * n for d, n in records) / total_w


def compute_category_deviation(
    factor_rates: Dict[str, Optional[float]],
    factor_runs: Dict[str, int],
    factor_weights: Dict[str, float],
    base_mean: float = 0.10,
    base_sigma: float = 0.05,
    min_runs: int = 3,
) -> Optional[float]:
    """複数ファクターの複勝率から加重平均偏差値を算出。

    1. 各ファクターの rate → rate_to_dev(rate, mean, sigma)
    2. サンプル数 × ファクター重みで加重平均
    3. 20-100 クランプ

    Args:
        factor_rates: {"overall": 0.35, "venue": 0.28, ...} (None=データなし)
        factor_runs: {"overall": 100, "venue": 15, ...}
        factor_weights: {"overall": 1.0, "venue": 0.8, ...}
        base_mean: 母集団平均（複勝率）
        base_sigma: 母集団標準偏差
        min_runs: 最低サンプル数（これ未満のファクターは無視）
    """
    # ベイズ縮小推定の定数: k_shrink走で実績と母集団平均が半々の重み
    # 少数サンプルでの偏差値跳ね上がりを防止（5走100%→dev116 のような異常値を抑制）
    k_shrink = 15
    w_sum = 0.0
    dev_sum = 0.0
    for key, rate in factor_rates.items():
        if rate is None:
            continue
        runs = factor_runs.get(key, 0)
        if runs < min_runs:
            continue
        weight = factor_weights.get(key, 0.0)
        if weight <= 0:
            continue
        # ベイズ縮小: サンプル少→母集団平均寄り、サンプル多→実績寄り
        rate_adj = (rate * runs + base_mean * k_shrink) / (runs + k_shrink)
        dev = rate_to_dev(rate_adj, base_mean, base_sigma)
        w = weight * min(runs, 100)  # サンプル数上限100で飽和（過大重み防止）
        w_sum += w
        dev_sum += dev * w
    if w_sum == 0:
        return None
    result = dev_sum / w_sum
    return max(20.0, min(100.0, result))


# G5: 血統 rate_to_dev 用の距離帯×面別パラメータ
# 距離分類: sprint ≤1400m, mile 1401-1799m, middle 1800-2199m, long ≥2200m
_BLOODLINE_RATE_PARAMS: Dict[Tuple[str, str], Dict[str, float]] = {
    ("sprint", "芝"):    {"mean": 0.28, "sigma": 0.12},
    ("sprint", "ダート"): {"mean": 0.26, "sigma": 0.10},
    ("mile", "芝"):      {"mean": 0.25, "sigma": 0.10},
    ("middle", "芝"):    {"mean": 0.24, "sigma": 0.09},
    ("long", "芝"):      {"mean": 0.22, "sigma": 0.08},
}
# デフォルト（キーがない場合のフォールバック）
_BLOODLINE_RATE_DEFAULT = {"mean": 0.25, "sigma": 0.10}


def _bloodline_rate_to_dev(
    rate: float,
    dist_bucket: str = "",
    surface: str = "",
) -> float:
    """血統用: 距離帯×面別パラメータで rate_to_dev を呼ぶ

    dist_bucket/surface の情報がない場合はデフォルトパラメータを使用。
    """
    params = _BLOODLINE_RATE_PARAMS.get((dist_bucket, surface), _BLOODLINE_RATE_DEFAULT)
    return rate_to_dev(rate, mean=params["mean"], sigma=params["sigma"])


def _parse_course_id(course_id: str) -> Tuple[str, str, int]:
    """course_id ("05_芝_1600") → (venue_code, surface, distance)"""
    parts = course_id.split("_")
    if len(parts) >= 3:
        try:
            return parts[0], parts[1], int(parts[2])
        except (ValueError, IndexError):
            pass
    return "", "", 0


def _distance_bucket(distance: int) -> str:
    """距離をバケット化"""
    if distance < 1400:
        return "sprint"
    if distance < 1800:
        return "mile"
    if distance < 2200:
        return "middle"
    return "long"


# ============================================================
# コースマスター情報のルックアップ
# ============================================================


def _build_course_lookup(all_courses) -> Dict[str, Any]:
    """all_courses → {venue_code: {straight_m, corner_count, ...}} 辞書を構築"""
    lookup = {}
    for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
        key = f"{c.venue_code}_{c.surface}_{c.distance}"
        lookup[key] = c
    return lookup


def _get_venue_straight(all_courses, venue_code: str) -> Optional[int]:
    """指定競馬場の代表的な直線距離を返す"""
    straights = [c.straight_m for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses) if c.venue_code == venue_code and c.straight_m]
    return max(straights) if straights else None


def _get_venue_corner_count(all_courses, venue_code: str) -> Optional[int]:
    """指定競馬場の代表的なコーナー数を返す"""
    corners = [c.corner_count for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses) if c.venue_code == venue_code and c.corner_count]
    if not corners:
        return None
    # 最頻値を返す
    from collections import Counter
    return Counter(corners).most_common(1)[0][0]


def _run_venue_code(run) -> str:
    """過去走の course_id から venue_code を抽出"""
    cid = getattr(run, "course_id", "") or ""
    if not cid:
        return ""
    parts = cid.split("_")
    return parts[0] if parts else ""


def _grade_from_past_runs(past_runs, filter_fn, min_runs: int = 1) -> str:
    """過去走のうちフィルタ条件に合致する走の成績からグレードを算出

    パフォーマンス = (頭数 - 着順) / (頭数 - 1)  →  1.0=1着, 0.0=最下位
    平均パフォーマンスを偏差値変換してグレード化。
    """
    filtered = [r for r in (past_runs or []) if filter_fn(r)]
    if len(filtered) < min_runs:
        return "—"
    perfs = []
    for run in filtered:
        fc = getattr(run, "field_count", 0) or 1
        fp = getattr(run, "finish_pos", 0) or fc
        if fc <= 1:
            continue
        perfs.append((fc - fp) / (fc - 1))
    if not perfs:
        return "—"
    avg = sum(perfs) / len(perfs)
    return dev_to_grade(rate_to_dev(avg, mean=0.50, sigma=0.20))


def _get_course_info(all_courses, course_id: str):
    """course_id から CourseMaster を取得（dict or list 両対応）"""
    if isinstance(all_courses, dict):
        return all_courses.get(course_id)
    return None


# ============================================================
# 騎手詳細グレード (11項目)
# ============================================================


def compute_jockey_detail_grades(
    jockey_stats,
    race_info,
    all_courses,
    horse_popularity: Optional[int] = None,
    trainer_stats=None,
    running_style=None,
    gate_no: int = 0,
    field_count: int = 0,
) -> Dict[str, str]:
    """
    騎手詳細の11項目グレードを算出。

    Returns: {
        "dev", "venue", "similar_venue", "straight", "corner",
        "surface", "distance", "same_cond", "style", "gate",
        "stable_synergy"
    }
    """
    result = {k: "—" for k in [
        "dev", "venue", "similar_venue", "straight", "corner",
        "surface", "distance", "same_cond", "style", "gate",
        "stable_synergy",
    ]}

    if not jockey_stats:
        return result

    # ── 騎手偏差値 ──
    is_upper = horse_popularity is not None and horse_popularity <= 3
    dev = jockey_stats.get_deviation(is_upper)
    result["dev"] = dev_to_grade(dev)

    cr = jockey_stats.course_records or {}

    # RaceInfo.course (CourseMaster) から属性を取得
    _course = getattr(race_info, "course", None)
    venue_code = getattr(_course, "venue_code", "") if _course else ""
    surface = getattr(_course, "surface", "") if _course else ""
    distance = getattr(_course, "distance", 0) if _course else 0
    course_id = getattr(_course, "course_id", "") if _course else ""

    # ── cr (course_records) が存在する場合のみ、詳細計算 ──
    target_straight = _get_venue_straight(all_courses, venue_code) if venue_code else None

    if cr:
        # ── 競馬場実績 ── (同venue_codeの全course_recordsの加重平均)
        venue_recs = []
        for cid, rec in cr.items():
            vc, _, _ = _parse_course_id(cid)
            if vc == venue_code and rec.get("sample_n", 0) >= 1:
                venue_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
        avg = _weighted_avg(venue_recs)
        if avg is not None:
            result["venue"] = dev_to_grade(avg)

        # ── 類似競馬場実績 ── (同じ直線長帯・坂タイプの競馬場の実績を集約)
        if target_straight is not None:
            sim_recs = []
            for cid, rec in cr.items():
                vc, _, _ = _parse_course_id(cid)
                if vc == venue_code:
                    continue  # 自場は除外
                s = _get_venue_straight(all_courses, vc)
                if s is not None and abs(s - target_straight) <= 80 and rec.get("sample_n", 0) >= 1:
                    sim_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            avg = _weighted_avg(sim_recs)
            if avg is not None:
                result["similar_venue"] = dev_to_grade(avg)

        # ── 直線相性 ── (直線距離400m+のコースでの実績)
        long_straight_recs = []
        for cid, rec in cr.items():
            vc, _, _ = _parse_course_id(cid)
            s = _get_venue_straight(all_courses, vc)
            if s is not None:
                is_long = s >= 400
                target_is_long = target_straight is not None and target_straight >= 400
                if is_long == target_is_long and rec.get("sample_n", 0) >= 1:
                    long_straight_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
        avg = _weighted_avg(long_straight_recs)
        if avg is not None:
            result["straight"] = dev_to_grade(avg)

        # ── コーナー相性 ── (コーナー数が同じコースでの実績)
        target_corners = _get_venue_corner_count(all_courses, venue_code)
        if target_corners is not None:
            corner_recs = []
            for cid, rec in cr.items():
                vc, _, _ = _parse_course_id(cid)
                cc = _get_venue_corner_count(all_courses, vc)
                if cc is not None and cc == target_corners and rec.get("sample_n", 0) >= 1:
                    corner_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            avg = _weighted_avg(corner_recs)
            if avg is not None:
                result["corner"] = dev_to_grade(avg)

        # ── コース実績（芝・ダート） ──
        surface_recs = []
        for cid, rec in cr.items():
            _, surf, _ = _parse_course_id(cid)
            if surf == surface and rec.get("sample_n", 0) >= 1:
                surface_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
        avg = _weighted_avg(surface_recs)
        if avg is not None:
            result["surface"] = dev_to_grade(avg)

        # ── 距離実績 ──
        target_bucket = _distance_bucket(distance)
        dist_recs = []
        for cid, rec in cr.items():
            _, _, d = _parse_course_id(cid)
            if d > 0 and _distance_bucket(d) == target_bucket and rec.get("sample_n", 0) >= 1:
                dist_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
        avg = _weighted_avg(dist_recs)
        if avg is not None:
            result["distance"] = dev_to_grade(avg)

        # ── 同条件実績 ── (exact course_id + condition_records の馬場状態別成績)
        exact = cr.get(course_id)
        exact_dev = exact.get("all_dev", 50.0) if exact and exact.get("sample_n", 0) >= 1 else None

        # condition_records（馬場状態別成績）も参照
        _track_cond_attr_j = "track_condition_turf" if "芝" in surface else "track_condition_dirt"
        track_condition = getattr(race_info, _track_cond_attr_j, "") or ""
        cond_records = getattr(jockey_stats, "condition_records", None) or {}
        cond_dev = None
        if track_condition and cond_records:
            cond_data = cond_records.get(track_condition)
            if cond_data and cond_data.get("runs", 0) >= 5:
                cond_rate = cond_data.get("wins", 0) / cond_data["runs"]
                cond_dev = rate_to_dev(cond_rate, mean=0.10, sigma=0.05)

        # course_record と condition_records を加重平均
        if exact_dev is not None and cond_dev is not None:
            # コース完全一致を重視しつつ馬場状態も加味
            result["same_cond"] = dev_to_grade(exact_dev * 0.6 + cond_dev * 0.4)
        elif exact_dev is not None:
            result["same_cond"] = dev_to_grade(exact_dev)
        elif cond_dev is not None:
            result["same_cond"] = dev_to_grade(cond_dev)

        # ── 脚質実績 ── (脚質に合った騎乗能力: 脚質と相性の良いコースでの実績)
        if running_style:
            from src.models import RunningStyle
            style_val = running_style.value if hasattr(running_style, "value") else str(running_style)
            style_recs = []
            if style_val in ("逃げ", "先行"):
                for cid, rec in cr.items():
                    _, _, d = _parse_course_id(cid)
                    if d > 0 and d <= 1800 and rec.get("sample_n", 0) >= 1:
                        style_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            elif style_val in ("差し", "追込", "マクリ"):
                for cid, rec in cr.items():
                    _, _, d = _parse_course_id(cid)
                    if d > 0 and d >= 1600 and rec.get("sample_n", 0) >= 1:
                        style_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            else:
                for cid, rec in cr.items():
                    if rec.get("sample_n", 0) >= 1:
                        style_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            avg = _weighted_avg(style_recs)
            if avg is not None:
                result["style"] = dev_to_grade(avg)

        # ── 枠実績 ── (枠位置に合ったコースでの実績)
        if gate_no > 0 and field_count > 0:
            gate_recs = []
            is_inner = gate_no <= max(field_count // 3, 2)
            is_outer = gate_no >= field_count - max(field_count // 3, 2) + 1
            for cid, rec in cr.items():
                vc, _, _ = _parse_course_id(cid)
                s = _get_venue_straight(all_courses, vc)
                if s is None or rec.get("sample_n", 0) < 1:
                    continue
                if is_inner and s < 400:
                    gate_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
                elif is_outer and s >= 400:
                    gate_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
                elif not is_inner and not is_outer:
                    gate_recs.append((rec.get("all_dev", 50.0), rec["sample_n"]))
            avg = _weighted_avg(gate_recs)
            if avg is not None:
                result["gate"] = dev_to_grade(avg)

    # ── 厩舎相性 ── (cr 不要)
    if trainer_stats and hasattr(trainer_stats, "jockey_combo"):
        combo = trainer_stats.jockey_combo.get(jockey_stats.jockey_id, {})
        runs = combo.get("runs", 0)
        wins = combo.get("wins", 0)
        if runs >= 3:
            win_rate = wins / runs
            # n=2でSS判定リスクを回避するため min_samples=3 に引き上げ
            result["stable_synergy"] = rate_to_grade(win_rate, mean=0.10, sigma=0.05, min_samples=3, sample_n=runs)

    # ── 残り"—"のフィールドを dev ベースで補完（情報不足分は中央寄りに抑制） ──
    if result["dev"] != "—":
        # dev の偏差値数値を取得
        _base_dev_val = jockey_stats.get_deviation(True) if jockey_stats else 50.0
        # 情報不足の項目は、dev を 50（C）に 15% 引き寄せた値を使う
        # 旧: 0.7/0.3 では差が縮まりすぎたため緩和
        _shrunk_dev = _base_dev_val * 0.85 + 50.0 * 0.15
        _shrunk_grade = dev_to_grade(_shrunk_dev)
        for key in result:
            if result[key] == "—":
                result[key] = _shrunk_grade

    return result


# ============================================================
# 調教師詳細グレード (11項目)
# ============================================================


def compute_trainer_detail_grades(
    trainer_stats,
    race_info,
    all_courses,
    jockey_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    調教師詳細の11項目グレードを算出。
    """
    result = {k: "—" for k in [
        "dev", "venue", "similar_venue", "straight", "corner",
        "surface", "distance", "same_cond", "style", "gate",
        "jockey_synergy",
    ]}

    if not trainer_stats:
        return result

    # ── 調教師偏差値 ──
    dev = getattr(trainer_stats, "deviation", None)
    if dev is not None:
        result["dev"] = dev_to_grade(dev)

    # ── 競馬場実績 ── (good_venues / bad_venues)
    venue = getattr(race_info, "venue", "")
    good = getattr(trainer_stats, "good_venues", []) or []
    bad = getattr(trainer_stats, "bad_venues", []) or []
    if venue in good:
        result["venue"] = "A"
    elif venue in bad:
        result["venue"] = "D"
    elif good or bad:
        result["venue"] = "C"  # 中立

    # ── 類似競馬場 ── (得意競馬場と同じ直線長帯なら B)
    _course_t = getattr(race_info, "course", None)
    venue_code = getattr(_course_t, "venue_code", "") if _course_t else ""
    target_straight = _get_venue_straight(all_courses, venue_code)
    if target_straight is not None and good:
        # 得意競馬場のコードを取得
        venue_name_to_code = {}
        for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
            venue_name_to_code[c.venue] = c.venue_code
        has_similar = False
        for gv in good:
            gv_code = venue_name_to_code.get(gv, "")
            gv_straight = _get_venue_straight(all_courses, gv_code)
            if gv_straight is not None and abs(gv_straight - target_straight) <= 80:
                has_similar = True
                break
        if has_similar:
            result["similar_venue"] = "B"

    # ── 直線相性 ── (得意競馬場の直線距離特性から推定)
    if target_straight is not None and good:
        venue_name_to_code_2 = {}
        for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
            venue_name_to_code_2[c.venue] = c.venue_code
        has_long_straight_good = False
        has_short_straight_good = False
        for gv in good:
            gv_code = venue_name_to_code_2.get(gv, "")
            gv_straight = _get_venue_straight(all_courses, gv_code)
            if gv_straight is not None:
                if gv_straight >= 400:
                    has_long_straight_good = True
                else:
                    has_short_straight_good = True
        target_is_long = target_straight >= 400
        if target_is_long and has_long_straight_good:
            result["straight"] = "A"
        elif not target_is_long and has_short_straight_good:
            result["straight"] = "A"
        elif target_is_long and has_short_straight_good:
            result["straight"] = "C"
        elif not target_is_long and has_long_straight_good:
            result["straight"] = "C"
        else:
            result["straight"] = "C"
    elif dev is not None:
        result["straight"] = dev_to_grade(dev)

    # ── コーナー相性 ── (得意競馬場のコーナー特性から推定)
    target_corners = None
    if venue_code:
        target_corners = _get_venue_corner_count(all_courses, venue_code)
    if target_corners is not None and good:
        if "venue_name_to_code_2" not in dir():
            venue_name_to_code_2 = {}
            for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
                venue_name_to_code_2[c.venue] = c.venue_code
        has_matching_corners = False
        for gv in good:
            gv_code = venue_name_to_code_2.get(gv, "")
            gv_corners = _get_venue_corner_count(all_courses, gv_code)
            if gv_corners is not None and gv_corners == target_corners:
                has_matching_corners = True
                break
        if has_matching_corners:
            result["corner"] = "A"
        else:
            result["corner"] = "C"
    elif dev is not None:
        result["corner"] = dev_to_grade(dev)

    # ── コース/距離/同条件 ── (condition_records から推定)
    cr = getattr(trainer_stats, "condition_records", {}) or {}
    surface = getattr(_course_t, "surface", "") if _course_t else ""
    if surface and cr:
        # 芝/ダートの条件レコードがあれば
        surface_key = "芝" if "芝" in surface else "ダート"
        # condition_records は {"良": {wins, runs}, ...} 形式
        total_wins = sum(r.get("wins", 0) for r in cr.values())
        total_runs = sum(r.get("runs", 0) for r in cr.values())
        if total_runs >= 3:
            win_rate = total_wins / total_runs
            result["surface"] = rate_to_grade(win_rate, mean=0.10, sigma=0.05, min_samples=2, sample_n=total_runs)

    # ── 距離実績 ── (得意競馬場の距離レンジから推定)
    distance = getattr(_course_t, "distance", 0) if _course_t else 0
    if distance and good:
        if "venue_name_to_code_2" not in dir():
            venue_name_to_code_2 = {}
            for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
                venue_name_to_code_2[c.venue] = c.venue_code
        target_bucket = _distance_bucket(distance)
        # 得意競馬場の距離レンジからバケットマッチ判定
        has_dist_match = False
        for gv in good:
            gv_code = venue_name_to_code_2.get(gv, "")
            for c in (all_courses.values() if isinstance(all_courses, dict) else all_courses):
                if c.venue_code == gv_code and _distance_bucket(c.distance) == target_bucket:
                    has_dist_match = True
                    break
            if has_dist_match:
                break
        if has_dist_match:
            result["distance"] = "A" if venue in good else "B"
        else:
            result["distance"] = "C"
    elif dev is not None:
        result["distance"] = dev_to_grade(dev)

    # ── 同条件実績 ── (condition_records から馬場状態マッチ)
    # race_info.condition は "3歳未勝利" 等のレース条件、馬場状態ではない
    # 馬場状態（良/稍重/重/不良）を使用
    _track_cond_attr_t = "track_condition_turf" if "芝" in surface else "track_condition_dirt"
    condition = getattr(race_info, _track_cond_attr_t, "") or ""
    if condition and cr:
        cond_data = cr.get(condition, {})
        cond_runs = cond_data.get("runs", 0)
        cond_wins = cond_data.get("wins", 0)
        if cond_runs >= 2:
            win_rate = cond_wins / cond_runs
            result["same_cond"] = rate_to_grade(win_rate, mean=0.10, sigma=0.05, min_samples=2, sample_n=cond_runs)
        elif result["surface"] != "—":
            result["same_cond"] = result["surface"]
    elif dev is not None:
        result["same_cond"] = dev_to_grade(dev)

    # ── 脚質実績 ── (調教師の得意パターンからの推定)
    break_type = getattr(trainer_stats, "break_type", "")
    if break_type == "叩き良化型":
        # 叩き良化型は差し/追込馬の管理に長けている傾向
        result["style"] = "B"
    elif break_type == "初戦型":
        # 初戦から仕上げる → 脚質を問わず安定
        result["style"] = "B"
    elif dev is not None:
        result["style"] = dev_to_grade(dev)
    else:
        result["style"] = "C"

    # ── 枠実績 ── (一般的な調教師能力ベース)
    if dev is not None:
        result["gate"] = dev_to_grade(dev)
    else:
        result["gate"] = "C"

    # ── 騎手相性 ──
    if jockey_id:
        combo = getattr(trainer_stats, "jockey_combo", {}) or {}
        jc = combo.get(jockey_id, {})
        runs = jc.get("runs", 0)
        wins = jc.get("wins", 0)
        if runs >= 3:
            win_rate = wins / runs
            # n=2でSS判定リスクを回避するため min_samples=3 に引き上げ
            result["jockey_synergy"] = rate_to_grade(win_rate, mean=0.10, sigma=0.05, min_samples=3, sample_n=runs)

    return result


# ============================================================
# 血統詳細グレード (12項目)
# ============================================================


def compute_bloodline_detail_grades(
    bloodline_db: Optional[Dict],
    sire_id: Optional[str],
    mgs_id: Optional[str],
    race_info,
    all_courses=None,
    jockey_dev: Optional[float] = None,
    sire_name: Optional[str] = None,
    mgs_name: Optional[str] = None,
) -> Dict[str, str]:
    """
    血統詳細の12項目グレードを算出。
    """
    result = {k: "—" for k in [
        "sire_dev", "mgs_dev", "venue", "similar_venue",
        "straight", "corner", "surface", "distance",
        "same_cond", "style", "gate", "jockey_synergy",
    ]}

    if not bloodline_db:
        return result

    # sire_id/mgs_id が空なら名前でフォールバック（キャッシュに sire_id 未保存の馬対策）
    if not sire_id and sire_name:
        sire_id = sire_name
    if not mgs_id and mgs_name:
        mgs_id = mgs_name

    _course_b = getattr(race_info, "course", None)
    surface = getattr(_course_b, "surface", "") if _course_b else ""
    distance = getattr(_course_b, "distance", 0) if _course_b else 0
    # track_condition: 馬場状態（良/稍重/重/不良）— RaceInfo に直接ある属性を使用
    _track_cond_attr = "track_condition_turf" if "芝" in surface else "track_condition_dirt"
    condition = getattr(race_info, _track_cond_attr, "良") or "良"
    dist_bucket = _distance_bucket(distance)

    def _compute_grades_for_ancestor(ancestor_id: str, db_key: str, ancestor_name: str = None) -> Tuple[str, str, str, str]:
        """1頭分の (総合dev_grade, surface_grade, distance_grade, same_cond_grade)"""
        _db = bloodline_db.get(db_key, {})
        data = (_db.get(ancestor_id) if ancestor_id else None) or (_db.get(ancestor_name) if ancestor_name else None) or {}
        if not data:
            return "—", "—", "—", "—"

        dist_data = data.get("distance", {})
        cond_data = data.get("course_condition", {})

        # キー解決ヘルパー: タプルキーと文字列キー("a|b")の両方に対応
        def _split_key(key):
            if isinstance(key, tuple):
                return key
            if isinstance(key, str) and "|" in key:
                parts = key.split("|")
                return tuple(parts)
            return (key, "")

        # 総合偏差値: 全データの加重平均（n=2でSS判定リスク回避のため min_samples=3）
        # G5: 距離帯×面別パラメータで rate_to_dev を呼ぶ
        all_recs = []
        for key, stats in dist_data.items():
            runs = stats.get("runs", 0)
            pr = stats.get("place_rate", 0)
            if runs >= 3:
                _bkt, _srf = _split_key(key)
                all_recs.append((_bloodline_rate_to_dev(pr, _bkt, _srf), runs))
        for key, stats in cond_data.items():
            runs = stats.get("runs", 0)
            pr = stats.get("place_rate", 0)
            if runs >= 3:
                # cond_data のキーは (surface, condition) 形式 → 面情報を抽出
                _cond_surf, _ = _split_key(key)
                all_recs.append((_bloodline_rate_to_dev(pr, "", _cond_surf), runs))
        overall = _weighted_avg(all_recs) if all_recs else None
        overall_grade = dev_to_grade(overall)

        # コース実績 (surface) — G5: 距離帯×面別パラメータ
        surface_grade = "—"
        surface_recs = []
        for key, stats in dist_data.items():
            bucket, surf = _split_key(key)
            if surf == surface and stats.get("runs", 0) >= 3:
                pr = stats.get("place_rate", 0)
                surface_recs.append((_bloodline_rate_to_dev(pr, bucket, surf), stats["runs"]))
        avg = _weighted_avg(surface_recs)
        if avg is not None:
            surface_grade = dev_to_grade(avg)

        # 距離実績（タプルキーと文字列キーの両方を検索）— G5
        distance_grade = "—"
        dist_key_tuple = (dist_bucket, surface)
        dist_key_str = f"{dist_bucket}|{surface}"
        dist_stats = dist_data.get(dist_key_tuple) or dist_data.get(dist_key_str)
        if dist_stats and dist_stats.get("runs", 0) >= 3:
            pr = dist_stats.get("place_rate", 0)
            distance_grade = dev_to_grade(_bloodline_rate_to_dev(pr, dist_bucket, surface))

        # 同条件 (surface + condition)（タプルキーと文字列キーの両方を検索）— G5
        same_cond_grade = "—"
        cond_key_tuple = (surface, condition)
        cond_key_str = f"{surface}|{condition}"
        cond_stats = cond_data.get(cond_key_tuple) or cond_data.get(cond_key_str)
        if cond_stats and cond_stats.get("runs", 0) >= 3:
            pr = cond_stats.get("place_rate", 0)
            # 同条件は距離帯不明なのでデフォルトパラメータ（面のみ考慮）
            same_cond_grade = dev_to_grade(_bloodline_rate_to_dev(pr, "", surface))

        return overall_grade, surface_grade, distance_grade, same_cond_grade

    # 父馬（sire_id が空なら sire_name でフォールバック）
    if sire_id or sire_name:
        sg, surf_g, dist_g, cond_g = _compute_grades_for_ancestor(sire_id, "sire", sire_name)
        result["sire_dev"] = sg
        # surface/distance/same_cond は父馬70%+母父30%で最終グレードを決めるが、
        # ここでは個別表示なので父馬分をそのまま使う
        if result["surface"] == "—":
            result["surface"] = surf_g
        if result["distance"] == "—":
            result["distance"] = dist_g
        if result["same_cond"] == "—":
            result["same_cond"] = cond_g

    # 母父馬（mgs_id が空なら mgs_name でフォールバック）
    if mgs_id or mgs_name:
        mg, surf_g, dist_g, cond_g = _compute_grades_for_ancestor(mgs_id, "bms", mgs_name)
        result["mgs_dev"] = mg
        # 母父の結果で補完 (父馬データなしの場合のみ)
        if result["surface"] == "—":
            result["surface"] = surf_g
        if result["distance"] == "—":
            result["distance"] = dist_g
        if result["same_cond"] == "—":
            result["same_cond"] = cond_g

    # ── 競馬場実績 ── (種牡馬の当該面×距離帯パフォーマンスから推定)
    venue_code = getattr(_course_b, "venue_code", "") if _course_b else ""
    if result["venue"] == "—" and (sire_id or mgs_id):
        venue_grade = _bloodline_venue_grade(
            bloodline_db, sire_id, mgs_id, surface, distance, all_courses, venue_code
        )
        if venue_grade != "—":
            result["venue"] = venue_grade

    # ── 類似競馬場 ── (近接距離帯での種牡馬パフォーマンスから推定)
    if result["similar_venue"] == "—" and (sire_id or mgs_id):
        # 全距離帯のsurface実績を集約（venue単体より広い範囲）
        sim_grade = _bloodline_surface_all_grade(bloodline_db, sire_id, mgs_id, surface)
        if sim_grade != "—":
            result["similar_venue"] = sim_grade

    # ── 直線相性 ── (短距離種牡馬=短直線向き、長距離種牡馬=長直線向き)
    if result["straight"] == "—" and (sire_id or mgs_id):
        target_straight = None
        if all_courses and venue_code:
            target_straight = _get_venue_straight(all_courses, venue_code)
        if target_straight is not None:
            is_long = target_straight >= 400
            # 長直線 → 長距離バケット, 短直線 → 短距離バケット
            ref_bucket = "long" if is_long else "sprint"
            result["straight"] = _bloodline_bucket_grade(
                bloodline_db, sire_id, mgs_id, ref_bucket, surface
            )

    # ── コーナー相性 ── (コーナー多い=長距離向き, 少ない=短距離向き)
    if result["corner"] == "—" and (sire_id or mgs_id):
        target_corners = None
        if all_courses and venue_code:
            target_corners = _get_venue_corner_count(all_courses, venue_code)
        if target_corners is not None:
            ref_bucket = "middle" if target_corners >= 4 else "sprint"
            result["corner"] = _bloodline_bucket_grade(
                bloodline_db, sire_id, mgs_id, ref_bucket, surface
            )

    # ── 脚質傾向 ── (短距離種牡馬→先行型, 長距離種牡馬→差し型)
    if result["style"] == "—" and (sire_id or mgs_id):
        # sprint/mile実績 vs middle/long実績で脚質傾向を判断
        sprint_g = _bloodline_bucket_grade(bloodline_db, sire_id, mgs_id, "sprint", surface)
        long_g = _bloodline_bucket_grade(bloodline_db, sire_id, mgs_id, "long", surface)
        # どちらか良い方 = 得意脚質寄りの適性
        result["style"] = sprint_g if sprint_g != "—" else long_g

    # ── 枠実績 ── (種牡馬の全般的な適応力ベース)
    if result["gate"] == "—":
        # 父馬・母父馬の総合偏差値をベースに
        if result["sire_dev"] != "—":
            result["gate"] = result["sire_dev"]
        elif result["mgs_dev"] != "—":
            result["gate"] = result["mgs_dev"]

    # ── 騎手相性 ── (騎手偏差値 × 種牡馬偏差値の組み合わせ)
    if result["jockey_synergy"] == "—" and jockey_dev is not None:
        sire_overall = result["sire_dev"] if result["sire_dev"] != "—" else None
        mgs_overall = result["mgs_dev"] if result["mgs_dev"] != "—" else None
        blood_grade = sire_overall or mgs_overall
        if blood_grade:
            # 騎手・血統の両方をスコア化して平均
            jockey_g = dev_to_grade(jockey_dev)
            _g2d = {"SS": 70, "S": 63, "A": 58.5, "B": 52.5, "C": 46.5, "D": 41.5, "E": 35}
            j_score = _g2d.get(jockey_g, 50)
            b_score = _g2d.get(blood_grade, 50)
            synergy_dev = (j_score + b_score) / 2
            result["jockey_synergy"] = dev_to_grade(synergy_dev)

    # データがない項目は "—" のまま残す（過大評価を防止）
    # sire_dev/mgs_dev 自体が "—" の場合のみ、もう一方で補完
    if result["sire_dev"] == "—" and result["mgs_dev"] != "—":
        result["sire_dev"] = result["mgs_dev"]
    elif result["mgs_dev"] == "—" and result["sire_dev"] != "—":
        result["mgs_dev"] = result["sire_dev"]

    return result


# ============================================================
# 血統ヘルパー関数
# ============================================================


def _bloodline_venue_grade(
    bloodline_db, sire_id, mgs_id, surface, distance, all_courses, venue_code
) -> str:
    """種牡馬の当該競馬場でのパフォーマンスからグレードを推定。
    venue_distance データ（過去走フォールバック）があれば venue_code で絞り込み、
    なければ従来通り距離×面のデータにフォールバックする。"""
    if not bloodline_db:
        return "—"
    dist_bucket = _distance_bucket(distance) if distance else ""
    # G6: 長距離(>=2200m)では母父の影響を大きくする
    sire_w = 0.60 if distance >= 2200 else 0.70
    mgs_w = 1.0 - sire_w

    recs = []
    for ancestor_id, db_key in [(sire_id, "sire"), (mgs_id, "bms")]:
        if not ancestor_id:
            continue
        data = bloodline_db.get(db_key, {}).get(ancestor_id, {})
        if not data:
            continue
        weight = sire_w if db_key == "sire" else mgs_w

        # まず venue_distance（会場別集計）から検索
        venue_data = data.get("venue_distance", {})
        if venue_code and venue_data:
            # 同会場・同面の全距離バケットを集約（会場での総合力）
            venue_recs = []
            for key, stats in venue_data.items():
                if isinstance(key, tuple) and len(key) == 3:
                    vc, bucket, surf = key
                elif isinstance(key, str) and "|" in key:
                    parts = key.split("|")
                    vc, bucket, surf = parts[0], parts[1], parts[2] if len(parts) >= 3 else ""
                else:
                    continue
                if vc == venue_code and surf == surface and stats.get("runs", 0) >= 1:
                    pr = stats.get("place_rate", 0)
                    venue_recs.append((_bloodline_rate_to_dev(pr, bucket, surf), stats["runs"]))
            if venue_recs:
                avg = _weighted_avg(venue_recs)
                if avg is not None:
                    total_runs = sum(r[1] for r in venue_recs)
                    recs.append((avg, total_runs * weight))
                continue  # venue データがあれば距離×面フォールバック不要

        # フォールバック: 従来の距離×面データ
        dist_data = data.get("distance", {})
        target_key = (dist_bucket, surface)
        target_key_str = f"{dist_bucket}|{surface}"
        stats = dist_data.get(target_key) or dist_data.get(target_key_str)
        if stats and stats.get("runs", 0) >= 3:
            pr = stats.get("place_rate", 0)
            recs.append((_bloodline_rate_to_dev(pr, dist_bucket, surface), stats["runs"] * weight))

    avg = _weighted_avg(recs)
    return dev_to_grade(avg) if avg is not None else "—"


def _bloodline_surface_all_grade(bloodline_db, sire_id, mgs_id, surface) -> str:
    """種牡馬の当該面での全距離帯パフォーマンスを集約"""
    if not bloodline_db:
        return "—"
    recs = []
    for ancestor_id, db_key in [(sire_id, "sire"), (mgs_id, "bms")]:
        if not ancestor_id:
            continue
        data = bloodline_db.get(db_key, {}).get(ancestor_id, {})
        if not data:
            continue
        dist_data = data.get("distance", {})
        for key, stats in dist_data.items():
            if isinstance(key, tuple):
                bucket, surf = key
            elif isinstance(key, str) and "|" in key:
                bucket, surf = key.split("|")
            else:
                bucket, surf = key, ""
            if surf == surface and stats.get("runs", 0) >= 3:
                pr = stats.get("place_rate", 0)
                # G6: 父/母父の比率を固定で使用（距離情報なしのため標準比率）
                weight = 0.70 if db_key == "sire" else 0.30
                # G5: 距離帯×面別パラメータ
                recs.append((_bloodline_rate_to_dev(pr, bucket, surf), stats["runs"] * weight))
    avg = _weighted_avg(recs)
    return dev_to_grade(avg) if avg is not None else "—"


def _bloodline_bucket_grade(bloodline_db, sire_id, mgs_id, bucket, surface) -> str:
    """種牡馬の指定距離バケット×面のパフォーマンスグレード"""
    if not bloodline_db:
        return "—"
    # G6: バケットが"long"なら長距離扱い（母父比率を大きくする）
    is_long_dist = (bucket == "long")
    recs = []
    for ancestor_id, db_key in [(sire_id, "sire"), (mgs_id, "bms")]:
        if not ancestor_id:
            continue
        data = bloodline_db.get(db_key, {}).get(ancestor_id, {})
        if not data:
            continue
        dist_data = data.get("distance", {})
        target_key = (bucket, surface)
        target_key_str = f"{bucket}|{surface}"
        stats = dist_data.get(target_key) or dist_data.get(target_key_str)
        if stats and stats.get("runs", 0) >= 3:
                pr = stats.get("place_rate", 0)
                # G6: 長距離では母父の影響を大きくする
                sire_w = 0.60 if is_long_dist else 0.70
                mgs_w = 1.0 - sire_w
                weight = sire_w if db_key == "sire" else mgs_w
                # G5: 距離帯×面別パラメータ
                recs.append((_bloodline_rate_to_dev(pr, bucket, surface), stats["runs"] * weight))
    avg = _weighted_avg(recs)
    return dev_to_grade(avg) if avg is not None else "—"


# ============================================================
# コース適性詳細グレード (7項目)
# ============================================================


def compute_course_detail_grades(
    course_aptitude,
    race_info,
    all_courses,
    past_runs=None,
) -> Dict[str, str]:
    """
    コース適性詳細の7項目をグレード化。
    既存のスコアをグレードに変換する。
    past_runs が与えられた場合、直線/コーナー/距離の適性を過去走実績から算出。
    """
    result = {k: "—" for k in [
        "venue", "similar_venue", "straight", "corner",
        "surface", "distance", "same_cond",
    ]}

    if not course_aptitude:
        return result

    # コース実績 → 競馬場実績 (-5〜+5のスコアを偏差値に変換)
    # G4: サンプル数による信頼度加重（少サンプルは中央寄りに抑制）
    cr = getattr(course_aptitude, "course_record", None)
    if cr is not None:
        cr_n = getattr(course_aptitude, "course_record_n", 0) or 0
        cr_confidence = min(1.0, cr_n / 10.0)  # サンプル10で信頼度100%
        result["same_cond"] = dev_to_grade(50.0 + cr * 3.0 * cr_confidence)

    # 競馬場適性 → venue
    # G4: サンプル数による信頼度加重
    va = getattr(course_aptitude, "venue_aptitude", None)
    if va is not None:
        va_n = getattr(course_aptitude, "venue_aptitude_n", 0) or 0
        va_confidence = min(1.0, va_n / 10.0)  # サンプル10で信頼度100%
        result["venue"] = dev_to_grade(50.0 + va * 3.0 * va_confidence)

    # 騎手コース → surface (間接的)
    jc = getattr(course_aptitude, "jockey_course", None)
    if jc is not None:
        result["surface"] = dev_to_grade(50.0 + jc * 4.0)

    # venue_contrib_level → similar_venue
    level = getattr(course_aptitude, "venue_contrib_level", "")
    level_map = {"Quartet+": "SS", "Trio": "S", "Pair": "A", "Solo": "B"}
    if level in level_map:
        result["similar_venue"] = level_map[level]

    # ── 過去走ベースの追加グレード ──
    _course_ca = getattr(race_info, "course", None)
    venue_code = getattr(_course_ca, "venue_code", "") if _course_ca else ""
    distance = getattr(_course_ca, "distance", 0) if _course_ca else 0

    if past_runs and all_courses:
        # ── 直線相性 ── (同直線長帯のコースでの馬の成績)
        target_straight = _get_venue_straight(all_courses, venue_code) if venue_code else None
        if target_straight is not None and result["straight"] == "—":
            target_is_long = target_straight >= 400
            g = _grade_from_past_runs(
                past_runs,
                lambda run: (
                    _run_venue_code(run) != "" and
                    (lambda s: s is not None and (s >= 400) == target_is_long)(
                        _get_venue_straight(all_courses, _run_venue_code(run))
                    )
                ),
            )
            if g != "—":
                result["straight"] = g

        # ── コーナー相性 ── (同コーナー数のコースでの馬の成績)
        target_corners = _get_venue_corner_count(all_courses, venue_code) if venue_code else None
        if target_corners is not None and result["corner"] == "—":
            g = _grade_from_past_runs(
                past_runs,
                lambda run: (
                    _run_venue_code(run) != "" and
                    _get_venue_corner_count(all_courses, _run_venue_code(run)) == target_corners
                ),
            )
            if g != "—":
                result["corner"] = g

        # ── 距離実績 ── (同距離帯での馬の成績)
        if distance > 0 and result["distance"] == "—":
            target_bucket = _distance_bucket(distance)
            g = _grade_from_past_runs(
                past_runs,
                lambda run: (
                    getattr(run, "distance", 0) > 0 and
                    _distance_bucket(run.distance) == target_bucket
                ),
            )
            if g != "—":
                result["distance"] = g

    # フォールバック: 過去走が少なくても venue/surface があればそれを使う
    base_dev = 50.0
    if va is not None:
        base_dev = 50.0 + va * 3.0
    for key in ["straight", "corner", "distance"]:
        if result[key] == "—":
            result[key] = dev_to_grade(base_dev)

    # similar_venue のフォールバック
    if result["similar_venue"] == "—" and va is not None:
        result["similar_venue"] = dev_to_grade(50.0 + va * 2.0)

    return result


# ============================================================
# プロフィール用グレード
# ============================================================


def compute_profile_grades(
    jockey_stats,
    trainer_stats,
    ability,
    horse_popularity: Optional[int] = None,
    bloodline_db: Optional[Dict] = None,
    sire_id: Optional[str] = None,
    mgs_id: Optional[str] = None,
    sire_name: Optional[str] = None,
    mgs_name: Optional[str] = None,
) -> Dict[str, str]:
    """
    プロフィール表示用の騎手/調教師/父/母父/馬主グレードを算出。
    """
    result = {
        "jockey_grade": "—",
        "trainer_grade": "—",
        "sire_grade": "—",
        "mgs_grade": "—",
        "owner_grade": "—",
        # 数値（偏差値）も返す
        "jockey_dev": None,
        "trainer_dev": None,
        "sire_dev": None,
        "mgs_dev": None,
    }

    # 騎手
    if jockey_stats:
        is_upper = horse_popularity is not None and horse_popularity <= 3
        jdev = jockey_stats.get_deviation(is_upper)
        result["jockey_grade"] = dev_to_grade(jdev)
        # 4象限すべて50.0はデフォルト（データ未取得） → Noneにしてgradeフォールバックを使わせる
        _all_default = (
            jockey_stats.upper_long_dev == 50.0
            and jockey_stats.upper_short_dev == 50.0
            and jockey_stats.lower_long_dev == 50.0
            and jockey_stats.lower_short_dev == 50.0
        )
        result["jockey_dev"] = None if _all_default else round(max(20.0, min(100.0, jdev)), 1)

    # 調教師
    if trainer_stats:
        dev = getattr(trainer_stats, "deviation", None)
        if dev is not None:
            result["trainer_grade"] = dev_to_grade(dev)
            # デフォルト50.0（データ不足） → Noneにしてgradeフォールバックを使わせる
            result["trainer_dev"] = None if dev == 50.0 else round(max(20.0, min(100.0, dev)), 1)

    # 父馬（sire_id が空なら sire_name でフォールバックルックアップ）
    sire_dev_val = None
    if bloodline_db:
        _sire_db = bloodline_db.get("sire", {})
        sire_data = (
            (_sire_db.get(sire_id) if sire_id else None)
            or (_sire_db.get(sire_name) if sire_name else None)
            or {}
        )
        if sire_data:
            all_recs = []
            for _dk, stats in sire_data.get("distance", {}).items():
                if stats.get("runs", 0) >= 2:
                    pr = stats.get("place_rate", 0)
                    # G5: キーから距離帯×面を抽出して適切なパラメータを使用
                    if isinstance(_dk, tuple):
                        _p_bkt, _p_srf = _dk
                    elif isinstance(_dk, str) and "|" in _dk:
                        _p_bkt, _p_srf = _dk.split("|")
                    else:
                        _p_bkt, _p_srf = str(_dk), ""
                    all_recs.append((_bloodline_rate_to_dev(pr, _p_bkt, _p_srf), stats["runs"]))
            avg = _weighted_avg(all_recs)
            if avg is not None:
                result["sire_grade"] = dev_to_grade(avg)
                result["sire_dev"] = round(max(20.0, min(100.0, avg)), 1)
                sire_dev_val = avg

    # 母父馬（mgs_id が空なら mgs_name でフォールバックルックアップ）
    mgs_dev_val = None
    if bloodline_db:
        _bms_db = bloodline_db.get("bms", {})
        mgs_data = (
            (_bms_db.get(mgs_id) if mgs_id else None)
            or (_bms_db.get(mgs_name) if mgs_name else None)
            or {}
        )
        if mgs_data:
            all_recs = []
            for _dk, stats in mgs_data.get("distance", {}).items():
                if stats.get("runs", 0) >= 2:
                    pr = stats.get("place_rate", 0)
                    # G5: キーから距離帯×面を抽出して適切なパラメータを使用
                    if isinstance(_dk, tuple):
                        _p_bkt, _p_srf = _dk
                    elif isinstance(_dk, str) and "|" in _dk:
                        _p_bkt, _p_srf = _dk.split("|")
                    else:
                        _p_bkt, _p_srf = str(_dk), ""
                    all_recs.append((_bloodline_rate_to_dev(pr, _p_bkt, _p_srf), stats["runs"]))
            avg = _weighted_avg(all_recs)
            if avg is not None:
                result["mgs_grade"] = dev_to_grade(avg)
                result["mgs_dev"] = round(max(20.0, min(100.0, avg)), 1)
                mgs_dev_val = avg

    # 血統総合偏差値（父と母父の平均）、[20, 100] にクランプ
    bl_vals = [v for v in [sire_dev_val, mgs_dev_val] if v is not None]
    if bl_vals:
        raw_bl = sum(bl_vals) / len(bl_vals)
        result["bloodline_dev"] = round(max(20.0, min(100.0, raw_bl)), 1)
    else:
        result["bloodline_dev"] = None

    # 馬主 — データなし
    # result["owner_grade"] = "—"

    return result


# ============================================================
# 展開詳細の追加フィールド算出
# ============================================================


def compute_pace_extras(
    evaluations,
    horse_no: int,
    gate_no: int,
) -> Dict[str, Any]:
    """
    展開詳細の追加フィールドを算出。
    - gate_neighbors: 枠の並び（隣接馬の脚質）
    - estimated_last3f_rank: 推定上がり順位
    - last3f_grade: 上がり3F評価
    """
    result: Dict[str, Any] = {
        "gate_neighbors": "—",
        "estimated_last3f_rank": None,
        "last3f_grade": "—",
    }

    if not evaluations:
        return result

    # 枠の並び: 隣接馬の脚質を取得
    ev_by_gate = {}
    for ev in evaluations:
        g = getattr(ev.horse, "gate_no", None) or getattr(ev, "gate_no", None)
        if g:
            style = getattr(ev.pace, "running_style", None)
            style_label = ""
            if style:
                style_label = getattr(style, "value", str(style))
                # 短縮: 逃げ→逃, 先行→先, 差し→差, 追込→追, マクリ→マ
                short = {"逃げ": "逃", "先行": "先", "差し": "差", "追込": "追", "マクリ": "マ"}
                style_label = short.get(style_label, style_label[:1] if style_label else "?")
            ev_by_gate[g] = style_label

    neighbors = []
    for g in [gate_no - 1, gate_no, gate_no + 1]:
        if g in ev_by_gate and g != gate_no:
            neighbors.append(ev_by_gate[g])
        elif g == gate_no:
            neighbors.append(ev_by_gate.get(g, "?"))
    result["gate_neighbors"] = "-".join(neighbors) if neighbors else "—"

    # 推定上がり順位
    last3f_list = []
    for ev in evaluations:
        l3f = getattr(ev.pace, "estimated_last3f", None)
        hno = getattr(ev.horse, "horse_no", 0)
        if l3f is not None:
            last3f_list.append((l3f, hno))
    if last3f_list:
        last3f_list.sort(key=lambda x: x[0])
        for rank, (_, hno) in enumerate(last3f_list, 1):
            if hno == horse_no:
                result["estimated_last3f_rank"] = rank
                break

    # 上がり3F評価 (推定上がり3F → 偏差値的にグレード化)
    for ev in evaluations:
        if getattr(ev.horse, "horse_no", 0) == horse_no:
            l3f_eval = getattr(ev.pace, "last3f_eval", None)
            if l3f_eval is not None:
                # last3f_eval は -8〜+8 のスコア → 偏差値変換
                result["last3f_grade"] = dev_to_grade(50.0 + l3f_eval * 2.5)
            break

    return result


# ============================================================
# 能力詳細の追加フィールド算出
# ============================================================


def compute_ability_extras(
    horse,
    training_records=None,
    ability_trend=None,
) -> Dict[str, Any]:
    """
    能力詳細の追加フィールド。
    - popularity_trend: 過去人気推移
    - condition_signal: 仕上げ気配
    """
    result: Dict[str, Any] = {
        "popularity_trend": "—",
        "condition_signal": "—",
    }

    # 過去人気推移: past_runs の popularity を見る
    past_runs = getattr(horse, "past_runs", []) or []
    pops = [getattr(pr, "popularity_at_race", None) for pr in past_runs[:3]]
    pops = [p for p in pops if p is not None and p > 0]
    if len(pops) >= 2:
        # 最近 → 過去の順なので、pops[0]が最新
        if pops[0] < pops[-1]:
            result["popularity_trend"] = "↗ 上昇"
        elif pops[0] > pops[-1]:
            result["popularity_trend"] = "↘ 下降"
        else:
            result["popularity_trend"] = "→ 安定"

    # 仕上げ気配: 調教強度 + トレンドの組み合わせ
    if training_records:
        # training_records は List[TrainingRecord]
        if len(training_records) > 0:
            latest = training_records[0]
            intensity = getattr(latest, "intensity_label", "")
            if intensity in ("猛時計", "強め"):
                if ability_trend in ("上昇", "急上昇"):
                    result["condition_signal"] = "仕上万全"
                else:
                    result["condition_signal"] = "仕上良好"
            elif intensity in ("一杯", "やや速い"):
                result["condition_signal"] = "仕上順調"
            elif intensity in ("通常",):
                result["condition_signal"] = "平常"
            elif intensity in ("やや軽め", "軽め"):
                if ability_trend in ("下降", "急下降"):
                    result["condition_signal"] = "仕上不安"
                else:
                    result["condition_signal"] = "軽め調整"

    return result
