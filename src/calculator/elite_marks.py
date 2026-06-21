"""
elite_marks.py — 印体系刷新 Phase 2+3 共有選定ヘルパー

◉(鉄板) 選定: 全レースの本命(mark∈{◎,◉})を win_prob 降順で top_n 抽出
穴馬   選定: dashboard._scan_today_predictions と同一ロジックを共有関数化
日次適用: apply_daily_elite_marks でレースリストに反映

2026-06-21 マスター承認済仕様
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# ============================================================
# 定数
# ============================================================

# 本命印セット (compute_danso_columns と同一定義)
_HONMEI_MARKS = {"◉", "◎"}

# ばんえい判定キーワード
_BANEI_VENUE = "帯広"

# 除外レース種別キーワード（メイクデビュー・障害）
_NO_BET_KEYWORDS = ("メイクデビュー", "障害")


# ============================================================
# 内部ヘルパー
# ============================================================

def _is_banei(race: Dict[str, Any]) -> bool:
    """ばんえい(帯広)レースか判定する。"""
    venue = race.get("venue", "") or ""
    return _BANEI_VENUE in venue


def _is_no_bet_race(race: Dict[str, Any]) -> bool:
    """メイクデビュー・障害レースか判定する。"""
    name = race.get("name", "") or race.get("race_name", "") or ""
    return any(kw in name for kw in _NO_BET_KEYWORDS)


def _get_honmei(horses: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """レース内の本命馬(mark∈{◉,◎})を返す。複数存在する場合は先頭を使用。"""
    for h in horses:
        if h.get("mark", "") in _HONMEI_MARKS:
            return h
    return None


# ============================================================
# Public API
# ============================================================

def select_pivot_honmei(
    races_flat: List[Dict[str, Any]],
    top_n: int = 5,
) -> List[Tuple[str, int, float]]:
    """全レースから鉄板候補(本命 win_prob 降順 top_n)を選定する。

    Parameters
    ----------
    races_flat : List[Dict]
        各要素 = {"race_id": str, "race": dict, "horses": list}
        race dict に "name" / "venue" キーがあること。
        horse dict に "mark", "win_prob", "horse_no" があること。
    top_n : int
        抽出頭数 (デフォルト 5)

    Returns
    -------
    List[Tuple[str, int, float]]
        [(race_id, horse_no, win_prob), ...] win_prob 降順
    """
    candidates: List[Tuple[str, int, float]] = []

    for item in races_flat:
        race = item.get("race", {})
        race_id = item.get("race_id", "")
        horses = item.get("horses", [])

        # ばんえい・新馬・障害は除外
        if _is_banei(race) or _is_no_bet_race(race):
            continue

        honmei = _get_honmei(horses)
        if honmei is None:
            continue

        # 取消馬は◉選定から除外
        if honmei.get("is_scratched", False):
            continue

        win_prob = float(honmei.get("win_prob") or 0.0)
        horse_no = int(honmei.get("horse_no") or 0)
        candidates.append((race_id, horse_no, win_prob))

    # win_prob 降順ソート → top_n（同値時は race_id で決定論的タイブレーク）
    candidates.sort(key=lambda x: (-x[2], x[0]))
    return candidates[:top_n]


def select_dark_horses(
    races_flat: List[Dict[str, Any]],
    top_n: int = 5,
) -> List[Tuple[str, int, float]]:
    """全レースから厳選穴馬(妙味スコア降順 top_n)を選定する。

    ロジックは dashboard._scan_today_predictions L1013-1085 と同一。
    係数は config/settings.py から読み込む（発散防止）。
    オッズ上限なし・無印馬のみ(★☆も除外)・1-3人気除外。

    Parameters
    ----------
    races_flat : List[Dict]
        各要素 = {"race_id": str, "race": dict, "horses": list, "is_jra": bool}
    top_n : int
        抽出頭数 (デフォルト 5)

    Returns
    -------
    List[Tuple[str, int, float]]
        [(race_id, horse_no, miryoku), ...] miryoku 降順
    """
    from config import settings as _s

    candidates: List[Tuple[str, int, float]] = []

    for item in races_flat:
        race = item.get("race", {})
        race_id = item.get("race_id", "")
        horses = item.get("horses", [])
        is_jra = bool(item.get("is_jra", False))

        # ばんえい・新馬・障害は除外
        if _is_banei(race) or _is_no_bet_race(race):
            continue

        for h in horses:
            # 取消馬は穴選定から除外
            if h.get("is_scratched", False):
                continue

            mk = h.get("mark", "")
            odds_val = float(h.get("odds") or 0)
            ts = float(h.get("tokusen_score") or 0)
            ana_sc = float(h.get("ana_score") or 0)
            comp = float(h.get("composite") or 0)
            course = float(h.get("course_total") or 0)
            p3 = float(h.get("place3_prob") or 0)
            pop = int(h.get("popularity") or 0)

            # 無印判定: ◉◎○▲△★☆×穴 以外、odds≥10、1-3人気除外
            # "穴" を除外することで再実行時に既存穴馬を二重選定しない（冪等性確保）
            is_ana = (
                mk not in ("◉", "◎", "○", "▲", "△", "★", "☆", "×", "穴")
                and odds_val >= 10.0
                and pop not in (1, 2, 3)
            )
            if not is_ana:
                continue

            # 妙味スコア（dashboard と同一式）
            miryoku = round(
                _s.MIRYOKU_W_TOKUSEN   * ts
                + _s.MIRYOKU_W_COMPOSITE * (comp - 45) / 10
                + _s.MIRYOKU_W_COURSE    * (course - 45) / 10
                + _s.MIRYOKU_W_ANA       * ana_sc / 5
                + _s.MIRYOKU_W_PLACE3    * p3 * 10
                + _s.MIRYOKU_W_JRA       * (1 if is_jra else 0),
                2,
            )

            # C グレード以上のみ対象（dashboard と同一閾値）
            if miryoku < _s.MIRYOKU_GRADE_C:
                continue

            horse_no = int(h.get("horse_no") or 0)
            candidates.append((race_id, horse_no, miryoku))

    # 妙味降順 → top_n（同値時は race_id でタイブレーク=決定論的）
    candidates.sort(key=lambda x: (-x[2], x[0]))
    return candidates[:top_n]


def apply_daily_elite_marks(
    races_flat: List[Dict[str, Any]],
    pivot_top_n: int = 5,
    dark_top_n: int = 5,
) -> Dict[str, Any]:
    """pred.json のレースリストに ◉/穴 印を後処理で付与する。

    処理フロー:
    1. select_pivot_honmei → 本命 win_prob 降順 top_n
       - 該当レース本命: mark を "◉" に設定
       - 非該当の {◎,◉} 本命: mark を "◎" に統一
    2. select_dark_horses → 妙味 top_n
       - 該当馬: mark を "穴" に設定

    Parameters
    ----------
    races_flat : List[Dict]
        各要素 = {"race_id": str, "race": dict, "horses": list, "is_jra": bool}
        horses の各要素は直接書き換えられる（in-place）。

    Returns
    -------
    Dict[str, Any]
        {
            "pivot": [(race_id, horse_no), ...],  # ◉ 付与リスト
            "ana":   [(race_id, horse_no), ...],  # 穴 付与リスト
        }
    """
    # ── Step1: ◉/◎ 正規化 ──
    pivot_list = select_pivot_honmei(races_flat, top_n=pivot_top_n)
    pivot_set: set = {(rid, no) for rid, no, _ in pivot_list}

    pivot_results: List[Tuple[str, int]] = []

    for item in races_flat:
        race_id = item.get("race_id", "")
        horses = item.get("horses", [])
        for h in horses:
            mk = h.get("mark", "")
            if mk not in _HONMEI_MARKS:
                continue
            horse_no = int(h.get("horse_no") or 0)
            if (race_id, horse_no) in pivot_set:
                h["mark"] = "◉"
                pivot_results.append((race_id, horse_no))
            else:
                # ◉ → ◎ に落とす（既に ◎ なら変更なし）
                h["mark"] = "◎"

    # ── Step2: 穴印付与 ──
    # Step1 で ◉ に変わった馬は元々 {◎,◉} だったので穴候補の無印条件を満たさない。
    # select_dark_horses は "穴" を除外条件に含むため、Step1 後に呼んでも安全
    # （Step1 前後で穴候補集合は変わらない）。
    dark_list = select_dark_horses(races_flat, top_n=dark_top_n)
    dark_set: set = {(rid, no) for rid, no, _ in dark_list}

    ana_results: List[Tuple[str, int]] = []

    for item in races_flat:
        race_id = item.get("race_id", "")
        horses = item.get("horses", [])
        for h in horses:
            horse_no = int(h.get("horse_no") or 0)
            if (race_id, horse_no) in dark_set:
                h["mark"] = "穴"
                ana_results.append((race_id, horse_no))

    return {
        "pivot": pivot_results,
        "ana":   ana_results,
    }
