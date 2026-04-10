"""
展開分析の統合モジュール
ペース・枠・並び・コーナー通過・ラップを総合して有利脚質を判定
"""

import statistics
from typing import List, Optional, Tuple

from src.models import CourseMaster, Horse, PaceType, PastRun, RunningStyle


def calc_lineup(horses: List[Horse]) -> List[Tuple[int, int, int]]:
    """
    枠×馬番から想定スタート並びを算出。
    Returns: [(馬番, 枠番, 想定1角順位)], 内枠ほど前、同枠は馬番順
    """
    # 枠順でソート（内枠が前）、同枠は馬番でソート
    sorted_h = sorted(horses, key=lambda h: (h.gate_no, h.horse_no))
    return [(h.horse_no, h.gate_no, i + 1) for i, h in enumerate(sorted_h)]


def _default_pace_times(
    surface: str, distance: int, pace_type: PaceType,
) -> Tuple[float, float]:
    """馬場・距離・ペースタイプから現実的な前半3F・後半3Fのデフォルト値を返す"""

    # 基準タイム（MMペース時）: 馬場×距離帯
    # 実データ（ML data 803日分の中央値）に基づく
    if surface == "ダート":
        if distance <= 1000:
            base_front, base_last = 34.5, 37.5
        elif distance <= 1200:
            base_front, base_last = 36.0, 39.0
        elif distance <= 1400:
            base_front, base_last = 37.5, 40.5
        elif distance <= 1600:
            base_front, base_last = 36.0, 40.5
        elif distance <= 1800:
            base_front, base_last = 37.0, 40.0
        elif distance <= 2100:
            base_front, base_last = 37.5, 40.5
        else:
            base_front, base_last = 39.0, 40.5
    else:  # 芝
        if distance <= 1000:
            base_front, base_last = 33.0, 35.0
        elif distance <= 1200:
            base_front, base_last = 34.0, 35.0
        elif distance <= 1400:
            base_front, base_last = 35.0, 35.0
        elif distance <= 1600:
            base_front, base_last = 35.0, 35.0
        elif distance <= 2000:
            base_front, base_last = 36.0, 35.5
        elif distance <= 2400:
            base_front, base_last = 36.5, 35.5
        else:
            base_front, base_last = 37.0, 36.5

    # ペース補正（非対称: ハイペースは後半の消耗が大きい）
    pace_adj = {
        PaceType.H: (-1.1, 1.35),    # (HH+HM)/2
        PaceType.M: (0.0, 0.0),
        PaceType.S: (1.1, -0.75),    # (MS+SS)/2
    }.get(pace_type, (0.0, 0.0))

    return (base_front + pace_adj[0], base_last + pace_adj[1])


def estimate_pace_times_from_runs(
    course_id: str,
    pace_type: PaceType,
    all_runs: List[PastRun],
    surface: str = "芝",
    distance: int = 1600,
) -> Tuple[float, float]:
    """
    過去走の first_3f / last_3f からコース別の前半3F・後半3Fを推定。
    surface, distance を考慮して現実的なデフォルト値を使用。
    """
    # 1. 同一コース（venue+surface+distance）の過去走
    same_course = [r for r in all_runs if r.course_id and r.course_id == course_id]

    # 2. フォールバック: 同一馬場・近距離（±200m）の過去走
    if not same_course:
        same_course = [
            r for r in all_runs
            if r.surface == surface and abs(r.distance - distance) <= 200
        ]

    if not same_course:
        return _default_pace_times(surface, distance, pace_type)

    first_3f_list = [r.first_3f_sec for r in same_course if r.first_3f_sec is not None]
    last_3f_list = [r.last_3f_sec for r in same_course if r.last_3f_sec > 0]

    # データ不足時はデフォルトにフォールバック
    if not first_3f_list and not last_3f_list:
        return _default_pace_times(surface, distance, pace_type)

    # デフォルト値を馬場・距離から取得（片方のデータが無い場合のフォールバック）
    default_front, default_last = _default_pace_times(surface, distance, PaceType.M)
    front = statistics.mean(first_3f_list) if first_3f_list else default_front
    last = statistics.mean(last_3f_list) if last_3f_list else default_last

    # ペース補正
    pace_corr_front = {
        PaceType.H: -1.1,    # (HH+HM)/2
        PaceType.M: 0,
        PaceType.S: 1.1,     # (MS+SS)/2
    }.get(pace_type, 0)
    # 非対称: ハイペースの後半消耗は大きい
    pace_corr_last = {
        PaceType.H: 1.35,    # (HH+HM)/2
        PaceType.M: 0,
        PaceType.S: -0.75,   # (MS+SS)/2
    }.get(pace_type, 0)

    return (front + pace_corr_front, last + pace_corr_last)


def judge_favorable_style(
    pace_type: PaceType,
    course: CourseMaster,
    leaders: List[int],
    front_horses: List[int],
    mid_horses: List[int],
    rear_horses: List[int],
    lineup: List[Tuple[int, int, int]],
    front_3f_est: float,
    last_3f_est: float,
) -> Tuple[str, str]:
    """
    ペース・コース・枠・並び・脚質構成から有利な脚質とその根拠を判定。
    Returns: (favorable_style, favorable_style_reason)
    """
    pv = pace_type.value if pace_type else "M"

    # 1. ペースベースの基本判定
    if pv == "H":
        base_style = "差し・追い込み"
        base_reason = "ハイペース消耗戦で前が潰れる想定"
    elif pv == "M":
        base_style = "先行〜差し"
        base_reason = "力通りの平均ペース"
    else:  # S
        base_style = "先行・逃げ"
        base_reason = "スローペースで前残り有効"

    # 2. コース補正（直線長・坂・コーナー）
    course_factors = []
    if course.straight_m >= 420:
        course_factors.append("長直線で末脚が生きる")
        if pv == "H":
            base_style = "差し・追い込み"
    elif course.straight_m <= 300:
        course_factors.append("短直線で前残り寄り")
        if base_style in ("差し・追い込み", "差し・中団〜後方待機"):
            base_style = "先行〜差し"

    if course.slope_type == "急坂":
        course_factors.append("急坂で先行消耗")
        if "先行" in base_style and pv != "S":
            if "差し" in base_style:
                # "先行〜差し" → "差し" （重複を防ぐ）
                base_style = "差し"
            else:
                base_style = base_style.replace("先行", "差し").replace("・逃げ", "")

    if course.corner_type == "小回り":
        course_factors.append("小回りで前有利")
        if "差し" in base_style and "先行" not in base_style:
            # 差し系→先行〜差しに統一（"先行〜差し・中団〜後方待機"等の複合文字列を防ぐ）
            base_style = "先行〜差し"

    # 3. 枠・並び補正（内枠がスタートで前につけやすい）
    inner_count = sum(1 for _, w, _ in lineup[: len(lineup) // 2] if w <= 3)
    if inner_count >= 2 and pv == "S":
        course_factors.append("内枠馬が前取りしやすい")

    # 4. 脚質構成の矛盾チェック
    if len(rear_horses) >= 4 and pv == "S":
        course_factors.append(f"後方待機{len(rear_horses)}頭はスローでは届きにくい可能性")

    # 根拠統合
    reason_parts = [base_reason]
    if course_factors:
        reason_parts.append("。" + "。".join(course_factors))
    reason = "".join(reason_parts)

    # 数値根拠を追記
    reason += f" 前半{front_3f_est:.1f}秒・後半{last_3f_est:.1f}秒想定。"

    return base_style, reason


def classify_style_from_corners(runs: List[PastRun]) -> Optional[RunningStyle]:
    """
    全コーナー通過順位から脚質を判定。
    positions_corners がある場合に使用。パターン:
    - 1角で前(0.2以下)かつ全角安定→逃げ/先行
    - 1角後方→4角前方の推移→差し
    - 全角後方→追い込み
    """
    if not runs:
        return None
    runs_with_corners = [
        r
        for r in runs[:5]
        if getattr(r, "positions_corners", None) and len(r.positions_corners) >= 2
    ]
    if not runs_with_corners:
        return None

    positions_list = [r.positions_corners for r in runs_with_corners]
    fc = runs_with_corners[0].field_count or 16

    # 各走の1角・最終角の相対位置
    rel_first = []
    rel_last = []
    changes = []
    for pos in positions_list:
        if len(pos) >= 1:
            rel_first.append(pos[0] / fc)
        if len(pos) >= 1:
            rel_last.append(pos[-1] / fc)
        if len(pos) >= 2:
            changes.append(pos[-1] - pos[0])  # 正=後方から前方へ

    avg_first = statistics.mean(rel_first) if rel_first else 0.5
    avg_last = statistics.mean(rel_last) if rel_last else 0.5
    avg_change = statistics.mean(changes) if changes else 0

    # 差し馬: 1角後方から4角で前方へ (avg_change < -1)
    if avg_change <= -1.5 and avg_first >= 0.5:
        return RunningStyle.SASHIKOMI
    # 逃げ: 1角で最前
    if avg_first <= 0.20:
        return RunningStyle.NIGASHI
    # 先行: 1角で前
    if avg_first <= 0.35:
        return RunningStyle.SENKOU
    # 追い込み: 4角でも後方
    if avg_last >= 0.7:
        return RunningStyle.OIKOMI
    # 差し
    if avg_first >= 0.4 and avg_last <= 0.6:
        return RunningStyle.SASHIKOMI

    return None
