"""
前半3F(秒)からペースタイプ(H/M/S)を推定

馬の戦績テーブルには「35.7-36.0」形式のペース列はあるがH/M/Sラベルがない。
距離・馬場ごとの閾値で前半3Fからハイ/ミドル/スローを推定する。
"""

from typing import Optional

from src.models import PaceType

# 距離バケット別の前半3F閾値(秒)
# (H境界, M-S境界): first_3f < H境界 → H, H境界 <= f < M-S → M, >= M-S → S
# 芝: 短距離は速いペースで34秒台=H、36秒台=M、38秒以上=S
# ダート: 芝より0.5〜1秒遅め
PACE_THRESHOLDS = {
    "sprint": {  # 〜1400m
        "芝": (34.5, 36.5),
        "ダート": (35.0, 37.5),
    },
    "mile": {  # 1400〜1800m
        "芝": (35.0, 37.0),
        "ダート": (35.5, 38.0),
    },
    "middle": {  # 1800〜2200m
        "芝": (36.0, 38.0),
        "ダート": (36.5, 39.0),
    },
    "long": {  # 2200m〜
        "芝": (37.0, 39.0),
        "ダート": (37.5, 40.0),
    },
}


def _distance_bucket(distance: int) -> str:
    if distance < 1400:
        return "sprint"
    if distance < 1800:
        return "mile"
    if distance < 2200:
        return "middle"
    return "long"


def infer_pace_from_first3f(
    distance: int,
    surface: str,
    first_3f_sec: Optional[float],
) -> Optional[PaceType]:
    """
    前半3F(秒)からペースタイプを推定

    Args:
        distance: レース距離(m)
        surface: 芝 or ダート
        first_3f_sec: 前半3Fの秒数（NoneならNoneを返す）

    Returns:
        PaceType (HH/HM/MM/MS/SS) または None
        netkeiba表記H/M/Sに合わせて HH/MM/SS の3段階で返すことも可能。
        5段階: 閾値より速い→HH/HM、中間→MM、遅い→MS/SS
    """
    if first_3f_sec is None or first_3f_sec <= 0:
        return None
    bucket = _distance_bucket(distance)
    surf_norm = "芝" if "芝" in surface else "ダート"
    th = PACE_THRESHOLDS.get(bucket, PACE_THRESHOLDS["mile"])
    h_bound, s_bound = th.get(surf_norm, (35.0, 37.0))
    if first_3f_sec < h_bound:
        return PaceType.HH
    if first_3f_sec < s_bound:
        return PaceType.MM
    return PaceType.SS


def normalize_pace_to_3level(pace: Optional[PaceType]) -> str:
    """
    5段階ペースをH/M/Sの3段階に正規化（集計用）
    """
    if pace is None:
        return "M"
    v = pace.value if pace else "MM"
    if v in ("HH", "HM"):
        return "H"
    if v in ("SS", "MS"):
        return "S"
    return "M"
