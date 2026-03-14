"""
レースデータキャッシュ — fetch_race() 結果の JSON 永続化

目的:
  予想再生成時にネット競馬への再スクレイピングを不要にする。
  1レース16頭 × 1.5秒/馬 ≒ 24秒 の待ち時間を 0.01秒以下に短縮。

キャッシュ先: data/cache/races/{race_id}.json
TTL:
  - 当日レース: 2時間（オッズ・馬体重が変動するため）
  - 過去レース: 30日
"""

import json
import os
import time
from dataclasses import asdict, fields
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.log import get_logger
from src.models import CourseMaster, Horse, PastRun, PaceType, RaceInfo

logger = get_logger(__name__)

# ============================================================
# 定数
# ============================================================

_CACHE_VERSION = 1
_TTL_TODAY = 2 * 3600       # 当日レース: 2時間
_TTL_PAST = 30 * 86400      # 過去レース: 30日
_TTL_HISTORY_ONLY = 7 * 86400  # 過去走のみキャッシュ: 7日

try:
    from config.settings import DATA_DIR
    RACE_CACHE_DIR = os.path.join(DATA_DIR, "cache", "races")
except Exception:
    RACE_CACHE_DIR = os.path.join(
        os.path.dirname(__file__), "..", "..", "data", "cache", "races"
    )


# ============================================================
# シリアライズ: dataclass → dict
# ============================================================

def _serialize_course(c: CourseMaster) -> dict:
    """CourseMaster → JSON-safe dict"""
    return {
        "venue": c.venue,
        "venue_code": c.venue_code,
        "distance": c.distance,
        "surface": c.surface,
        "direction": c.direction,
        "straight_m": c.straight_m,
        "corner_count": c.corner_count,
        "corner_type": c.corner_type,
        "first_corner": c.first_corner,
        "slope_type": c.slope_type,
        "inside_outside": c.inside_outside,
        "is_jra": c.is_jra,
    }


def _serialize_past_run(r: PastRun) -> dict:
    """PastRun → JSON-safe dict"""
    return {
        "race_date": r.race_date,
        "venue": r.venue,
        "course_id": r.course_id,
        "distance": r.distance,
        "surface": r.surface,
        "condition": r.condition,
        "class_name": r.class_name,
        "grade": r.grade,
        "field_count": r.field_count,
        "gate_no": r.gate_no,
        "horse_no": r.horse_no,
        "jockey": r.jockey,
        "weight_kg": r.weight_kg,
        "position_4c": r.position_4c,
        "finish_pos": r.finish_pos,
        "finish_time_sec": r.finish_time_sec,
        "last_3f_sec": r.last_3f_sec,
        "margin_behind": r.margin_behind,
        "margin_ahead": r.margin_ahead,
        "pace": r.pace.value if r.pace else None,
        "horse_weight": r.horse_weight,
        "weight_change": r.weight_change,
        "positions_corners": r.positions_corners,
        "first_3f_sec": r.first_3f_sec,
        "jockey_id": r.jockey_id,
        "trainer_id": r.trainer_id,
        "is_generation": r.is_generation,
        "race_level_dev": r.race_level_dev,
        "tansho_odds": r.tansho_odds,
        "popularity_at_race": r.popularity_at_race,
    }


def _serialize_horse(h: Horse) -> dict:
    """Horse → JSON-safe dict"""
    return {
        "horse_id": h.horse_id,
        "horse_name": h.horse_name,
        "sex": h.sex,
        "age": h.age,
        "color": h.color,
        "trainer": h.trainer,
        "trainer_id": h.trainer_id,
        "owner": h.owner,
        "breeder": h.breeder,
        "sire": h.sire,
        "dam": h.dam,
        "sire_id": h.sire_id,
        "dam_id": h.dam_id,
        "maternal_grandsire_id": h.maternal_grandsire_id,
        "maternal_grandsire": h.maternal_grandsire,
        "race_date": h.race_date,
        "venue": h.venue,
        "race_no": h.race_no,
        "gate_no": h.gate_no,
        "horse_no": h.horse_no,
        "jockey": h.jockey,
        "jockey_id": h.jockey_id,
        "weight_kg": h.weight_kg,
        "base_weight_kg": h.base_weight_kg,
        "odds": h.odds,
        "popularity": h.popularity,
        "horse_weight": h.horse_weight,
        "weight_change": h.weight_change,
        "prev_jockey": h.prev_jockey,
        "trainer_affiliation": h.trainer_affiliation,
        "past_runs": [_serialize_past_run(r) for r in h.past_runs],
        # 公式モード用: 馬プロフ/馬詳細リンクコード
        "_profile_cname": getattr(h, "_profile_cname", ""),
        "_lineage_code": getattr(h, "_lineage_code", ""),
    }


def _serialize_race_info(r: RaceInfo) -> dict:
    """RaceInfo → JSON-safe dict"""
    d = {
        "race_id": r.race_id,
        "race_date": r.race_date,
        "venue": r.venue,
        "race_no": r.race_no,
        "race_name": r.race_name,
        "grade": r.grade,
        "condition": getattr(r, "condition", ""),
        "field_count": r.field_count,
        "weather": r.weather,
        "post_time": r.post_time,
        "track_condition_turf": r.track_condition_turf,
        "track_condition_dirt": r.track_condition_dirt,
    }
    if r.course:
        d["course"] = _serialize_course(r.course)
    else:
        d["course"] = None
    # optional attrs
    for attr in ("is_jra",):
        if hasattr(r, attr):
            d[attr] = getattr(r, attr)
    return d


# ============================================================
# デシリアライズ: dict → dataclass
# ============================================================

def _deserialize_course(d: dict) -> CourseMaster:
    return CourseMaster(
        venue=d["venue"],
        venue_code=d["venue_code"],
        distance=d["distance"],
        surface=d["surface"],
        direction=d["direction"],
        straight_m=d["straight_m"],
        corner_count=d["corner_count"],
        corner_type=d["corner_type"],
        first_corner=d.get("first_corner", ""),
        slope_type=d["slope_type"],
        inside_outside=d["inside_outside"],
        is_jra=d.get("is_jra", True),
    )


def _deserialize_past_run(d: dict) -> PastRun:
    pace_val = d.get("pace")
    pace = PaceType(pace_val) if pace_val else None
    return PastRun(
        race_date=d["race_date"],
        venue=d["venue"],
        course_id=d["course_id"],
        distance=d["distance"],
        surface=d["surface"],
        condition=d["condition"],
        class_name=d["class_name"],
        grade=d["grade"],
        field_count=d["field_count"],
        gate_no=d["gate_no"],
        horse_no=d["horse_no"],
        jockey=d["jockey"],
        weight_kg=d["weight_kg"],
        position_4c=d["position_4c"],
        finish_pos=d["finish_pos"],
        finish_time_sec=d["finish_time_sec"],
        last_3f_sec=d["last_3f_sec"],
        margin_behind=d["margin_behind"] or 0.0,
        margin_ahead=d["margin_ahead"] or 0.0,
        pace=pace,
        horse_weight=d.get("horse_weight"),
        weight_change=d.get("weight_change"),
        positions_corners=d.get("positions_corners", []),
        first_3f_sec=d.get("first_3f_sec"),
        jockey_id=d.get("jockey_id", ""),
        trainer_id=d.get("trainer_id", ""),
        is_generation=d.get("is_generation", False),
        race_level_dev=d.get("race_level_dev"),
        tansho_odds=d.get("tansho_odds"),
        popularity_at_race=d.get("popularity_at_race"),
    )


def _deserialize_horse(d: dict) -> Horse:
    past_runs = [_deserialize_past_run(r) for r in d.get("past_runs", [])]
    h = Horse(
        horse_id=d["horse_id"],
        horse_name=d["horse_name"],
        sex=d["sex"],
        age=d["age"],
        color=d.get("color", ""),
        trainer=d["trainer"],
        trainer_id=d["trainer_id"],
        owner=d.get("owner", ""),
        breeder=d.get("breeder", ""),
        sire=d.get("sire", ""),
        dam=d.get("dam", ""),
        sire_id=d.get("sire_id", ""),
        dam_id=d.get("dam_id", ""),
        maternal_grandsire_id=d.get("maternal_grandsire_id", ""),
        maternal_grandsire=d.get("maternal_grandsire", ""),
        race_date=d.get("race_date", ""),
        venue=d.get("venue", ""),
        race_no=d.get("race_no", 0),
        gate_no=d.get("gate_no", 0),
        horse_no=d.get("horse_no", 0),
        jockey=d.get("jockey", ""),
        jockey_id=d.get("jockey_id", ""),
        weight_kg=d.get("weight_kg", 55.0),
        base_weight_kg=d.get("base_weight_kg", 55.0),
        odds=d.get("odds"),
        popularity=d.get("popularity"),
        horse_weight=d.get("horse_weight"),
        weight_change=d.get("weight_change"),
        prev_jockey=d.get("prev_jockey", ""),
        trainer_affiliation=d.get("trainer_affiliation", ""),
        past_runs=past_runs,
    )
    # 公式モード用: 馬プロフ/馬詳細リンクコード復元
    pcn = d.get("_profile_cname", "")
    if pcn:
        h._profile_cname = pcn
    lc = d.get("_lineage_code", "")
    if lc:
        h._lineage_code = lc
    return h


def _deserialize_race_info(d: dict) -> RaceInfo:
    course = None
    if d.get("course"):
        course = _deserialize_course(d["course"])
    ri = RaceInfo(
        race_id=d["race_id"],
        race_date=d["race_date"],
        venue=d["venue"],
        race_no=d["race_no"],
        race_name=d["race_name"],
        grade=d.get("grade", ""),
        condition=d.get("condition", ""),
        course=course,
        field_count=d.get("field_count", 0),
        weather=d.get("weather", ""),
        post_time=d.get("post_time", ""),
        track_condition_turf=d.get("track_condition_turf", ""),
        track_condition_dirt=d.get("track_condition_dirt", ""),
    )
    if "is_jra" in d:
        ri.is_jra = d["is_jra"]
    return ri


# ============================================================
# キャッシュ読み書き
# ============================================================

def _cache_path(race_id: str, cache_dir: str = None) -> str:
    d = cache_dir or RACE_CACHE_DIR
    return os.path.join(d, f"{race_id}.json")


def _is_fresh(cache_file: str, race_date: str) -> bool:
    """キャッシュファイルの有効期限チェック"""
    try:
        age = time.time() - os.path.getmtime(cache_file)
    except OSError:
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    if race_date == today:
        return age < _TTL_TODAY
    else:
        return age < _TTL_PAST


def save_race_cache(
    race_id: str,
    race_info: RaceInfo,
    horses: List[Horse],
    cache_dir: str = None,
) -> str:
    """
    fetch_race() の結果を JSON キャッシュとして保存する。

    Returns: 保存先ファイルパス
    """
    d = cache_dir or RACE_CACHE_DIR
    os.makedirs(d, exist_ok=True)
    fp = _cache_path(race_id, d)

    # past_runsが空の場合、既存キャッシュにpast_runsがあれば上書きしない（データ劣化防止）
    new_has_history = any(
        (getattr(h, "past_runs", None) or []) for h in horses
    )
    if not new_has_history and os.path.exists(fp):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                old = json.load(f)
            old_has_history = any(
                h.get("past_runs") for h in old.get("horses", [])
            )
            if old_has_history:
                logger.debug(
                    "キャッシュ上書きスキップ（既存にpast_runsあり、新規になし）: %s", race_id
                )
                return fp
        except Exception:
            pass

    payload = {
        "_cache_version": _CACHE_VERSION,
        "_saved_at": datetime.now().isoformat(),
        "race_info": _serialize_race_info(race_info),
        "horses": [_serialize_horse(h) for h in horses],
    }

    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        logger.debug("レースキャッシュ保存: %s (%d頭)", race_id, len(horses))
    except Exception as e:
        logger.warning("レースキャッシュ保存失敗: %s → %s", race_id, e)
        return ""
    return fp


def load_race_cache(
    race_id: str,
    cache_dir: str = None,
    ignore_ttl: bool = False,
) -> Optional[Tuple[RaceInfo, List[Horse]]]:
    """
    キャッシュから RaceInfo + List[Horse] を復元する。

    Args:
        race_id: netkeibaのrace_id (12桁)
        cache_dir: キャッシュディレクトリ（省略時はデフォルト）
        ignore_ttl: True の場合 TTL を無視して期限切れでも読み込む

    Returns:
        (RaceInfo, List[Horse]) or None
    """
    fp = _cache_path(race_id, cache_dir)
    if not os.path.exists(fp):
        return None

    try:
        with open(fp, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        logger.debug("レースキャッシュ読み込み失敗: %s", race_id)
        return None

    # バージョンチェック
    if payload.get("_cache_version", 0) != _CACHE_VERSION:
        logger.debug("キャッシュバージョン不一致: %s", race_id)
        return None

    ri_data = payload.get("race_info")
    if not ri_data:
        return None

    # TTL チェック
    if not ignore_ttl:
        race_date = ri_data.get("race_date", "")
        if not _is_fresh(fp, race_date):
            logger.debug("キャッシュ期限切れ: %s", race_id)
            return None

    try:
        race_info = _deserialize_race_info(ri_data)
        horses = [_deserialize_horse(h) for h in payload.get("horses", [])]
        # 頭数が極端に少ないキャッシュは不良データの可能性 → 破棄
        if len(horses) <= 2:
            logger.debug("キャッシュ頭数不足(%d頭) → 破棄: %s", len(horses), race_id)
            return None
        logger.debug("レースキャッシュ復元: %s (%d頭)", race_id, len(horses))
        return race_info, horses
    except Exception as e:
        logger.warning("レースキャッシュ復元失敗: %s → %s", race_id, e)
        return None


def invalidate_race_cache(race_id: str, cache_dir: str = None) -> bool:
    """指定レースのキャッシュを削除する"""
    fp = _cache_path(race_id, cache_dir)
    try:
        if os.path.exists(fp):
            os.remove(fp)
            return True
    except OSError:
        pass
    return False


def purge_expired_cache(cache_dir: str = None) -> int:
    """期限切れキャッシュを一括削除"""
    d = cache_dir or RACE_CACHE_DIR
    if not os.path.isdir(d):
        return 0
    removed = 0
    for name in os.listdir(d):
        if not name.endswith(".json"):
            continue
        fp = os.path.join(d, name)
        race_id = name[:-5]
        # 簡易: 30日超えたファイルを削除
        try:
            age = time.time() - os.path.getmtime(fp)
            if age > _TTL_PAST:
                os.remove(fp)
                removed += 1
        except OSError:
            pass
    return removed
