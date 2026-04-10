"""
改善用DB構築
馬体重・騎手コンビ・クラス落差・休み明け・オッズ整合性・血統×距離×馬場・ペース別の各DB
"""

import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from src.log import get_logger
from src.models import Horse, PastRun

logger = get_logger(__name__)

# ============================================================
# 血統×距離×馬場適性（父馬・母父馬）
# ============================================================


def _distance_bucket(distance: int) -> str:
    """距離をバケット化"""
    if distance < 1400:
        return "sprint"
    if distance < 1800:
        return "mile"
    if distance < 2200:
        return "middle"
    return "long"


def _normalize_condition(c: str) -> str:
    """馬場状態の表記揺れを正規化"""
    m = {"良": "良", "稍": "稍重", "稍重": "稍重", "重": "重", "不": "不良", "不良": "不良"}
    return m.get(c, "良")


def _tuple_key_to_str(d: dict) -> dict:
    """タプルキーの辞書を文字列キーに変換（JSON保存用）"""
    return {"|".join(str(x) for x in k) if isinstance(k, tuple) else k: v for k, v in d.items()}


def _str_key_to_tuple(d: dict) -> dict:
    """文字列キーをタプルキーに復元（JSONロード用）"""
    result = {}
    for k, v in d.items():
        if "|" in k:
            result[tuple(k.split("|"))] = v
        else:
            result[k] = v
    return result


def _serialize_cache_entry(entry: dict) -> dict:
    """キャッシュエントリのタプルキーを文字列に変換して保存可能にする"""
    return {
        field: _tuple_key_to_str(data) if isinstance(data, dict) else data
        for field, data in entry.items()
    }


def _deserialize_cache_entry(entry: dict) -> dict:
    """キャッシュエントリの文字列キーをタプルに復元する"""
    return {
        field: _str_key_to_tuple(data) if isinstance(data, dict) else data
        for field, data in entry.items()
    }


def _load_bloodline_cache(cache_path: Optional[str]) -> dict:
    """キャッシュファイルを読み込み。{bid: {"sire_distance":…, "sire_cc":…, "bms_distance":…, "bms_cc":…}}"""
    if not cache_path or not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, encoding="utf-8") as f:
            raw = json.load(f)
        # 文字列キーをタプルキーに復元
        return {bid: _deserialize_cache_entry(entry) for bid, entry in raw.items()}
    except Exception:
        return {}


def _save_bloodline_cache(cache_path: Optional[str], cache: dict) -> None:
    if not cache_path:
        return
    try:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        # タプルキーを文字列に変換してからJSON保存
        serializable = {bid: _serialize_cache_entry(entry) for bid, entry in cache.items()}
        tmp = cache_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False)
        os.replace(tmp, cache_path)
    except Exception:
        logger.debug("bloodline cache save failed", exc_info=True)


def build_bloodline_db(
    horses: List[Horse],
    netkeiba_client=None,
    cache_path: Optional[str] = None,
) -> Dict[str, dict]:
    """
    父馬・母父馬×距離×馬場別の勝率・複勝率
    netkeiba_client があれば type=2(距離別) + type=1(コース・馬場別) をスクレイプ
    キャッシュ済みIDはネットアクセスをスキップし高速化する。
    Returns: {"sire": {id: {distance: {...}, course_condition: {...}}}, "bms": {...}}
    """
    sire_distance: Dict[str, Dict[Tuple[str, str], dict]] = {}
    sire_cc: Dict[str, Dict[Tuple[str, str], dict]] = {}
    bms_distance: Dict[str, Dict[Tuple[str, str], dict]] = {}
    bms_cc: Dict[str, Dict[Tuple[str, str], dict]] = {}

    # ID が空の場合は名前をキーとして使用（スクレイプ失敗でも名前ベースで集計可能にする）
    def _sire_key(h) -> str:
        return getattr(h, "sire_id", "") or getattr(h, "sire", "") or ""

    def _mgs_key(h) -> str:
        return getattr(h, "maternal_grandsire_id", "") or getattr(h, "maternal_grandsire", "") or ""

    sire_ids = {_sire_key(h) for h in horses} - {""}
    mgs_ids = {_mgs_key(h) for h in horses} - {""}
    all_ids = sire_ids | mgs_ids

    # キャッシュ読み込み：既取得IDはスキップ
    cache = _load_bloodline_cache(cache_path)
    uncached_ids = {bid for bid in all_ids if bid not in cache}

    if netkeiba_client and uncached_ids:
        try:
            import time

            from src.scraper.netkeiba import REQUEST_INTERVAL
            from src.scraper.sire_stats import (
                fetch_sire_course_condition_stats,
                fetch_sire_distance_stats,
            )

            for bid in uncached_ids:
                dist_data = fetch_sire_distance_stats(bid, netkeiba_client)
                time.sleep(REQUEST_INTERVAL)
                cc_data = fetch_sire_course_condition_stats(bid, netkeiba_client)
                time.sleep(REQUEST_INTERVAL)
                cache[bid] = {
                    "sire_distance": dist_data.get("sire", {}),
                    "sire_cc": cc_data.get("sire", {}),
                    "bms_distance": dist_data.get("bms", {}),
                    "bms_cc": cc_data.get("bms", {}),
                }
            _save_bloodline_cache(cache_path, cache)
        except Exception:
            logger.debug("bloodline scrape failed", exc_info=True)

    # キャッシュからデータを展開
    for bid in all_ids:
        entry = cache.get(bid, {})
        if bid in sire_ids:
            if entry.get("sire_distance"):
                sire_distance[bid] = entry["sire_distance"]
            if entry.get("sire_cc"):
                sire_cc[bid] = entry["sire_cc"]
        if bid in mgs_ids:
            if entry.get("bms_distance"):
                bms_distance[bid] = entry["bms_distance"]
            if entry.get("bms_cc"):
                bms_cc[bid] = entry["bms_cc"]

    fallback_dist: Dict[str, Dict[Tuple[str, str], dict]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})
    )
    for h in horses:
        # ID が空の場合は名前をキーとして代用
        s_key = _sire_key(h)
        m_key = _mgs_key(h)
        for r in getattr(h, "past_runs", []) or []:
            bucket = _distance_bucket(r.distance)
            key = (bucket, r.surface)
            for bid in (s_key, m_key):
                if not bid:
                    continue
                rec = fallback_dist[bid][key]
                rec["runs"] += 1
                if r.finish_pos == 1:
                    rec["wins"] += 1
                if r.finish_pos <= 3:
                    rec["places"] += 1
    for bid, buckets in fallback_dist.items():
        for k, rec in buckets.items():
            n = rec["runs"]
            rec["win_rate"] = rec["wins"] / n if n else 0
            rec["place_rate"] = rec["places"] / n if n else 0

    def _merge_dist(web_data: dict, fb_key: str) -> dict:
        """web データとフォールバックをマージ。同一キーは web データ優先、不足キーは fallback で補完"""
        merged: dict = {}
        fb = dict(fallback_dist.get(fb_key, {}))
        merged.update(fb)
        if web_data:
            merged.update(web_data)  # web データが優先（より信頼性高い）
        return merged

    result = {"sire": {}, "bms": {}}
    for h in horses:
        s = _sire_key(h)
        m = _mgs_key(h)
        if s:
            result["sire"][s] = {
                "distance": _merge_dist(sire_distance.get(s, {}), s),
                "course_condition": sire_cc.get(s, {}),
            }
        if m:
            result["bms"][m] = {
                "distance": _merge_dist(bms_distance.get(m, {}), m),
                "course_condition": bms_cc.get(m, {}),
            }
    return result


def calc_bloodline_adjustment(
    horse: Horse,
    distance: int,
    surface: str,
    bloodline_db: Dict,
    condition: str = "良",
) -> float:
    """
    父馬・母父馬×距離×馬場適性に基づく補正 (-5.0〜+5.0 pt)
    bloodline_db: {"sire": {id: {distance: {...}, course_condition: {...}}}, "bms": {...}}
    旧形式 {id: {key: stats}} にも対応
    """
    bucket = _distance_bucket(distance)
    dist_key = (bucket, surface)
    dist_key_str = f"{bucket}|{surface}"
    cond_norm = _normalize_condition(condition)
    cc_key = (surface, cond_norm)
    cc_key_str = f"{surface}|{cond_norm}"
    adj = 0.0
    # ID が空の場合は名前をキーとして代用（build_bloodline_db と同方式）
    sire_id = getattr(horse, "sire_id", "") or getattr(horse, "sire", "") or ""
    mgs_id = (
        getattr(horse, "maternal_grandsire_id", "")
        or getattr(horse, "maternal_grandsire", "")
        or ""
    )

    # 父 7：母父 3 の重み付け / 最低サンプル：sire=3走, bms=2走
    ROLE_WEIGHTS = {"sire": 0.7, "bms": 0.3}
    ROLE_MIN_RUNS = {"sire": 3, "bms": 2}

    def _eval_stats(rec: dict, min_runs: int = 3) -> float:
        runs = rec.get("runs", 0)
        if runs < min_runs:
            return 0.0
        wr = rec.get("win_rate", 0)
        pr = rec.get("place_rate", 0)
        # サンプル数が少ない場合は評価値を縮小（信頼性補正）
        reliability = min(1.0, runs / 10.0)
        a = 0.0
        # 平均的な勝率は約10%（フラット競馬）。12%以上でプラス、7%以下でマイナス
        if wr >= 0.12:
            a += min(1.5, (wr - 0.08) * 10)
        elif wr < 0.07 and runs >= 5:
            a -= min(1.0, (0.07 - wr) * 10)
        if pr >= 0.38 and a >= 0:
            a += 0.2
        elif pr < 0.25 and a <= 0:
            a -= 0.1
        return a * reliability

    def _eval_role(bid: str, role: str) -> float:
        if not bid:
            return 0.0
        min_runs = ROLE_MIN_RUNS.get(role, 3)
        db = bloodline_db.get(role, {})
        entry = db.get(bid, {})
        if isinstance(entry, dict) and "distance" in entry:
            dist_d = entry.get("distance", {})
            rec = dist_d.get(dist_key) or dist_d.get(dist_key_str, {})
            score = _eval_stats(rec, min_runs) * 0.6
            cc_d = entry.get("course_condition", {})
            rec_cc = cc_d.get(cc_key) or cc_d.get(cc_key_str, {})
            score += _eval_stats(rec_cc, min_runs) * 0.4
        else:
            rec = entry.get(dist_key, {}) if isinstance(entry, dict) else {}
            score = _eval_stats(rec, min_runs)
        return score

    for role, bid in [("sire", sire_id), ("bms", mgs_id)]:
        adj += _eval_role(bid, role) * ROLE_WEIGHTS.get(role, 0.5)

    # 新馬・未勝利でも血統情報から大きめの補正を許容（±5.0pt）
    return max(-5.0, min(5.0, adj))


# ============================================================
# ペース別成績（馬・騎手・調教師・父馬・母父馬）
# ============================================================


def ensure_pace_on_past_runs(horses: List[Horse]) -> None:
    """
    PastRunにpaceがない場合、first_3f_secから推定して付与する（in-place）
    """
    try:
        from src.utils.pace_inference import infer_pace_from_first3f
    except ImportError:
        return
    for h in horses:
        for r in getattr(h, "past_runs", []) or []:
            if r.pace is None and r.first_3f_sec is not None:
                # course_idからvenue_code抽出（"44_ダート_1600"→"44"）
                _vc = r.course_id.split("_")[0] if getattr(r, "course_id", None) else None
                pace = infer_pace_from_first3f(r.distance, r.surface, r.first_3f_sec, venue_code=_vc)
                if pace is not None:
                    object.__setattr__(r, "pace", pace)


def build_pace_stats_db(horses: List[Horse]) -> Dict[str, Dict[str, Dict[str, dict]]]:
    """
    ペース別(H/M/S)の勝率・複勝率を集計
    Returns: {
      "horse": {horse_id: {pace: {wins, places, runs, win_rate, place_rate}}},
      "jockey": {jockey_id: {...}},
      "trainer": {trainer_id: {...}},
      "sire": {sire_id: {...}},
      "bms": {mgs_id: {...}},
    }
    """
    from src.utils.pace_inference import normalize_pace_to_3level

    ensure_pace_on_past_runs(horses)

    result = {
        "horse": defaultdict(lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})),
        "jockey": defaultdict(lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})),
        "trainer": defaultdict(lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})),
        "sire": defaultdict(lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})),
        "bms": defaultdict(lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})),
    }
    for h in horses:
        sire_id = getattr(h, "sire_id", "") or ""
        mgs_id = getattr(h, "maternal_grandsire_id", "") or ""
        for r in getattr(h, "past_runs", []) or []:
            pace_key = normalize_pace_to_3level(r.pace)
            jid = getattr(r, "jockey_id", "") or ""
            tid = getattr(r, "trainer_id", "") or ""
            for entity, eid in [
                ("horse", h.horse_id),
                ("jockey", jid),
                ("trainer", tid),
                ("sire", sire_id),
                ("bms", mgs_id),
            ]:
                if not eid:
                    continue
                rec = result[entity][eid][pace_key]
                rec["runs"] += 1
                if r.finish_pos == 1:
                    rec["wins"] += 1
                if r.finish_pos <= 3:
                    rec["places"] += 1
    for entity in result:
        for eid, paces in result[entity].items():
            for pk, rec in paces.items():
                n = rec["runs"]
                rec["win_rate"] = rec["wins"] / n if n else 0
                rec["place_rate"] = rec["places"] / n if n else 0
    return {k: dict(v) for k, v in result.items()}


def calc_pace_adjustment(
    horse: Horse,
    pace_type,
    pace_db: Dict[str, Dict[str, Dict[str, dict]]],
) -> float:
    """
    ペース別成績に基づく補正 (-1.5〜+1.5 pt)
    今回予測ペースで実績が良い馬・騎手・血統にプラス
    pace_type: PaceType (HH/HM/MM/MS/SS) → H/M/S に正規化して照合
    """
    if not pace_type or not pace_db:
        return 0.0
    try:
        from src.utils.pace_inference import normalize_pace_to_3level

        pred_key = normalize_pace_to_3level(pace_type)
    except Exception:
        return 0.0
    adj = 0.0
    # 馬のペース別成績
    horse_rec = pace_db.get("horse", {}).get(horse.horse_id, {}).get(pred_key, {})
    if horse_rec.get("runs", 0) >= 5:
        wr = horse_rec.get("win_rate", 0)
        if wr >= 0.20:
            adj += min(1.0, (wr - 0.12) * 8)
        elif wr < 0.08 and horse_rec.get("runs", 0) >= 10:
            adj -= 0.5
    # 騎手のペース別成績
    jid = getattr(horse, "jockey_id", "") or ""
    jockey_rec = pace_db.get("jockey", {}).get(jid, {}).get(pred_key, {})
    if jockey_rec.get("runs", 0) >= 20:
        wr = jockey_rec.get("win_rate", 0)
        if wr >= 0.15:
            adj += 0.3
        elif wr < 0.08:
            adj -= 0.3
    # 血統のペース別成績（新馬・未勝利で有効）
    sire_id = getattr(horse, "sire_id", "") or ""
    mgs_id = getattr(horse, "maternal_grandsire_id", "") or ""
    if sire_id:
        rec = pace_db.get("sire", {}).get(sire_id, {}).get(pred_key, {})
        if rec.get("runs", 0) >= 15 and rec.get("win_rate", 0) >= 0.12:
            adj += 0.2
    if mgs_id:
        rec = pace_db.get("bms", {}).get(mgs_id, {}).get(pred_key, {})
        if rec.get("runs", 0) >= 15 and rec.get("win_rate", 0) >= 0.12:
            adj += 0.2
    return max(-1.5, min(1.5, adj))


# ============================================================
# 騎手×馬コンビ成績
# ============================================================


def build_jockey_horse_combo_db(horses: List[Horse]) -> Dict[str, Dict[str, dict]]:
    """
    馬×騎手別の勝率・複勝率を集計
    combo_db: {horse_id: {jockey_id: {wins, places, runs, win_rate, place_rate}}}
    """
    result: Dict[str, Dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"wins": 0, "places": 0, "runs": 0})
    )
    for h in horses:
        for r in getattr(h, "past_runs", []) or []:
            jid = getattr(r, "jockey_id", None) or ""
            if not jid:
                continue
            rec = result[h.horse_id][jid]
            rec["runs"] += 1
            if r.finish_pos == 1:
                rec["wins"] += 1
            if r.finish_pos <= 3:
                rec["places"] += 1
    for hid, jockeys in result.items():
        for jid, rec in jockeys.items():
            n = rec["runs"]
            rec["win_rate"] = rec["wins"] / n if n else 0
            rec["place_rate"] = rec["places"] / n if n else 0
    return dict(result)


def get_combo_adjustment(combo_db: Dict, horse_id: str, jockey_id: str) -> float:
    """
    騎手×馬コンビ成績に基づく補正 (-2〜+3 pt)
    10走以上で勝率30%超→+2, 20%未満→-1
    """
    by_horse = combo_db.get(horse_id, {})
    rec = by_horse.get(jockey_id, {})
    runs = rec.get("runs", 0)
    if runs < 5:
        return 0.0
    wr = rec.get("win_rate", 0)
    pr = rec.get("place_rate", 0)
    if runs >= 10 and wr >= 0.30:
        return min(3.0, 1.5 + (wr - 0.25) * 10)
    if runs >= 10 and wr < 0.15:
        return max(-2.0, -1.0 - (0.15 - wr) * 5)
    if runs >= 5 and pr >= 0.5:
        return 0.5
    return 0.0


# ============================================================
# クラス落差補正
# ============================================================

CLASS_ORDER = [
    "新馬",
    "未勝利",
    "1勝",
    "2勝",
    "3勝",
    "OP",
    "JpnIII",
    "G3",
    "JpnII",
    "G2",
    "JpnI",
    "G1",
]


def get_class_diff_index(race_grade: str, last_grade: str) -> int:
    """race_grade - last_grade の格差。正=格上挑戦、負=格落ち"""
    try:
        return CLASS_ORDER.index(race_grade) - CLASS_ORDER.index(last_grade)
    except ValueError:
        return 0


def calc_class_adjustment(race_grade: str, last_grade: str) -> float:
    """
    クラス落差補正 (-2〜+2 pt)
    格上挑戦→プラス、格落ち→マイナス
    """
    diff = get_class_diff_index(race_grade, last_grade)
    if diff >= 2:
        return min(2.0, diff * 0.8)
    if diff == 1:
        return 0.5
    if diff <= -2:
        return max(-2.0, diff * 0.5)
    if diff == -1:
        return -0.5
    return 0.0


# ============================================================
# 休み明けの精密判定
# ============================================================


def get_days_since_last_run(runs: List[PastRun], race_date: str) -> Optional[int]:
    """直近走から今回レースまでの日数"""
    if not runs:
        return None
    try:
        ref = datetime.strptime(race_date, "%Y-%m-%d")
        latest = max(datetime.strptime(r.race_date, "%Y-%m-%d") for r in runs if r.race_date)
        return (ref - latest).days
    except Exception:
        return None


def calc_break_adjustment(
    days_break: Optional[int],
    trainer_recovery_break: float,
    is_long_break: bool,
) -> float:
    """
    休み明け補正 (-1〜+2 pt) — 精密化版
    5段階日数帯 × 厩舎回収率テーブル
    """
    if days_break is None:
        return 0.0

    # 連闘 (1〜14日): 疲労・調整不足リスク
    if days_break <= 14:
        return -0.5

    # 短期間隔 (15〜27日): 標準
    if days_break <= 27:
        return 0.0

    # 標準間隔 (28〜59日): 通常ローテ、補正なし
    if days_break <= 59:
        return 0.0

    # 中期休養 (60〜89日): 休み明けだが短め
    if days_break <= 89:
        if trainer_recovery_break >= 115:
            return 1.0
        if trainer_recovery_break >= 100:
            return 0.4
        return -0.3

    # 長期休養 (90〜179日): 主要な休み明けゾーン
    if days_break <= 179:
        if trainer_recovery_break >= 120:
            return 2.0
        if trainer_recovery_break >= 110:
            return 1.3
        if trainer_recovery_break >= 100:
            return 0.6
        return -0.5

    # 超長期休養 (180日以上): 怪我明けの可能性
    if trainer_recovery_break >= 120:
        return 1.5
    if trainer_recovery_break >= 110:
        return 0.5
    return -1.0


# ============================================================
# 馬体重・増減の定量評価
# ============================================================


def calc_weight_change_adjustment(
    weight_change: Optional[int], horse_weight: Optional[int]
) -> float:
    """
    馬体重変動補正の拡張 (-2.5〜+1.5 pt)
    既存の composite 補正を強化。馬体重が分かる場合の追加補正。

    【減量ペナルティが増量より大きい理由】
    - 減量（マイナス方向）は体調不良・ストレス・輸送疲れ等の
      ネガティブな原因が多く、パフォーマンスへの悪影響が直接的。
      特に大幅減量は疾病や過度の調教による消耗を示唆する。
    - 増量（プラス方向）は成長期の若馬では自然な変化であり、
      休養明けの体力回復を示すこともある。大幅増量でも
      「太め残り」程度で走れるケースがあるため、減量ほど
      致命的ではない。
    - 統計的にも、大幅減量馬の勝率低下幅は大幅増量馬の
      勝率低下幅より大きいことが知られている。
    """
    if weight_change is None:
        return 0.0
    base = 0.0
    # --- 増量側: 成長や回復の可能性があるため減量よりペナルティ軽め ---
    if weight_change >= 20:
        base = -2.0
    elif weight_change >= 16:
        base = -1.5
    elif weight_change >= 8:
        base = -0.5
    # --- 減量側: 体調不良の可能性が高く、より厳しいペナルティ ---
    elif weight_change <= -20:
        base = -2.5  # 増量 +20 の -2.0 より大きい
    elif weight_change <= -16:
        base = -2.0  # 増量 +16 の -1.5 より大きい
    elif weight_change <= -8:
        base = -0.5
    elif 2 <= weight_change <= 6 and horse_weight and horse_weight < 480:
        base = 0.5  # 軽い馬の適度な増量=仕上がり良好のサイン
    return base


# ============================================================
# オッズ整合性スコア
# ============================================================


def calc_odds_consistency_score(
    model_composite: float,
    all_composites: List[float],
    odds: Optional[float],
) -> float:
    """
    モデルと市場の乖離スコア (-2〜+2)
    モデル勝率が市場より高い=過小評価=プラス。
    段階的に係数を変更し、大きな乖離シグナルを適切に反映する。
    """
    import math as _math
    if odds is None or odds < 1.1:
        return 0.0
    market_prob = 1.0 / odds
    # softmaxベースのモデル確率推定（10のべき乗→自然指数で穏やかに）
    exps = [_math.exp((c - 50) / 10.0) for c in all_composites]
    total = sum(exps)
    model_prob = _math.exp((model_composite - 50) / 10.0) / total if total > 0 else 0.01
    diff = model_prob - market_prob
    abs_diff = abs(diff)
    # 不感帯: 8%未満の乖離は無視（旧5%）
    if abs_diff < 0.08:
        return 0.0
    # 段階的スコアリング: 最大±2.0に縮小（旧±4.0）
    if abs_diff >= 0.20:
        adj = 2.0
    elif abs_diff >= 0.10:
        adj = min(1.8, abs_diff * 12)
    elif abs_diff >= 0.08:
        adj = min(1.0, abs_diff * 10)
    else:
        adj = abs_diff * 10
    sign = 1 if diff > 0 else -1
    return sign * adj
