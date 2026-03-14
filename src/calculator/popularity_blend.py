"""
人気別実績統計ブレンドモジュール

勝率・連対率・複勝率を以下の3要素でブレンドする:
  1. JRA/NAR全体の人気別 (or オッズレンジ別) 勝率
  2. 競馬場別の人気別 (or オッズレンジ別) 勝率
  3. MLモデル予測勝率（現在の win_prob 等）

特徴:
  - ベイズ縮小推定: 小サンプル競馬場は全体平均に引き寄せ
  - オッズレンジ別統計: オッズがある場合は人気ではなくオッズレンジで統計を引く
  - 頭数別補正: 少頭数/中頭数/多頭数で基準確率を調整
  - 動的alpha: モデル確信度に応じてブレンド比率を適応調整
"""

import json
import os
from typing import Dict, List, Optional, Tuple

from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# モジュールレベルキャッシュ
# ============================================================
_STATS_CACHE: Optional[dict] = None
_STATS_LOADED = False

# ============================================================
# 定数
# ============================================================
# ベイズ縮小の閾値（この頭数以上あれば競馬場統計をフル信頼）
SHRINKAGE_THRESHOLD_JRA = 3000
SHRINKAGE_THRESHOLD_NAR = 5000

# 動的alpha の範囲
ALPHA_MODEL_MIN = 0.33   # 拮抗レース時のモデル重み
ALPHA_MODEL_MAX = 0.50   # 一強レース時のモデル重み
CONFIDENCE_GAP = 0.15    # この勝率差でモデル最大信頼

# 頭数区分
FIELD_SIZE_BINS = {
    "small": (1, 8),
    "medium": (9, 14),
    "large": (15, 99),
}

# オッズレンジ
ODDS_RANGES = [
    (1.0, 1.9, "1.0-1.9"),
    (2.0, 2.9, "2.0-2.9"),
    (3.0, 4.9, "3.0-4.9"),
    (5.0, 9.9, "5.0-9.9"),
    (10.0, 19.9, "10.0-19.9"),
    (20.0, 49.9, "20.0-49.9"),
    (50.0, 9999.0, "50.0+"),
]


def load_popularity_stats(path: Optional[str] = None) -> Optional[dict]:
    """統計テーブルをロード（モジュールキャッシュ付き）"""
    global _STATS_CACHE, _STATS_LOADED
    if _STATS_LOADED:
        return _STATS_CACHE

    if path is None:
        from config.settings import DATA_DIR
        path = os.path.join(DATA_DIR, "popularity_rates.json")

    if not os.path.exists(path):
        logger.warning("人気別統計テーブルが見つかりません: %s", path)
        _STATS_LOADED = True
        _STATS_CACHE = None
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            _STATS_CACHE = json.load(f)
        _STATS_LOADED = True
        logger.info(
            "人気別統計テーブルロード完了: %s日分, %s件",
            _STATS_CACHE.get("sample_days", "?"),
            f"{_STATS_CACHE.get('total_entries', 0):,}",
        )
        return _STATS_CACHE
    except Exception as e:
        logger.error("統計テーブルのロードに失敗: %s", e)
        _STATS_LOADED = True
        _STATS_CACHE = None
        return None


def _get_shrunk_rate(
    venue_rate: float,
    venue_n: int,
    org_rate: float,
    threshold: int,
) -> float:
    """ベイズ縮小推定: サンプル数が少ない場合は全体平均に引き寄せる"""
    shrinkage = min(venue_n / threshold, 1.0)
    return shrinkage * venue_rate + (1 - shrinkage) * org_rate


def _odds_range_key(odds: float) -> str:
    """オッズからレンジキーを取得"""
    for lo, hi, key in ODDS_RANGES:
        if lo <= odds <= hi:
            return key
    return "50.0+"


def _field_size_bin(n: int) -> str:
    """頭数から区分キーを取得"""
    for key, (lo, hi) in FIELD_SIZE_BINS.items():
        if lo <= n <= hi:
            return key
    return "large"


def _lookup_rates(
    stats: dict,
    org: str,
    venue: str,
    popularity: int,
    odds: Optional[float],
    field_count: int,
) -> Tuple[float, float, float, float, float, float]:
    """統計テーブルから全体レート・競馬場レートを取得

    Returns:
        (org_win, org_top2, org_top3, venue_win, venue_top2, venue_top3)
        競馬場レートはベイズ縮小済み
    """
    default_win = 1.0 / max(field_count, 1)
    default_top2 = min(2.0 / max(field_count, 1), 1.0)
    default_top3 = min(3.0 / max(field_count, 1), 1.0)

    threshold = SHRINKAGE_THRESHOLD_JRA if org == "JRA" else SHRINKAGE_THRESHOLD_NAR

    # --- オッズレンジ優先、なければ人気別 ---
    use_odds_range = odds is not None and odds > 0

    if use_odds_range:
        range_key = _odds_range_key(odds)
        odds_data = stats.get("by_odds_range", {})
        org_data = odds_data.get(org, {}).get("_overall", {}).get(range_key, {})
        venue_data = odds_data.get(org, {}).get(venue, {}).get(range_key, {})
    else:
        org_data = {}
        venue_data = {}

    # オッズレンジにデータがない場合は人気別にフォールバック
    if not org_data:
        pop_str = str(min(popularity, 18))
        pop_data = stats.get("by_popularity", {})
        org_data = pop_data.get(org, {}).get("_overall", {}).get(pop_str, {})
        venue_data = pop_data.get(org, {}).get(venue, {}).get(pop_str, {})

    # 全体レート
    org_win = org_data.get("win", default_win)
    org_top2 = org_data.get("top2", default_top2)
    org_top3 = org_data.get("top3", default_top3)

    # 競馬場レート（ベイズ縮小）
    venue_n = venue_data.get("n", 0)
    venue_win = _get_shrunk_rate(
        venue_data.get("win", org_win), venue_n, org_win, threshold
    )
    venue_top2 = _get_shrunk_rate(
        venue_data.get("top2", org_top2), venue_n, org_top2, threshold
    )
    venue_top3 = _get_shrunk_rate(
        venue_data.get("top3", org_top3), venue_n, org_top3, threshold
    )

    # --- 頭数補正 ---
    fs_bin = _field_size_bin(field_count)
    fs_data = stats.get("by_field_size", {}).get(org, {}).get(fs_bin, {})
    pop_str = str(min(popularity, 18))
    fs_entry = fs_data.get(pop_str, {})
    # 全体の人気別統計との比率で補正
    overall_pop = stats.get("by_popularity", {}).get(org, {}).get("_overall", {}).get(pop_str, {})
    if fs_entry and overall_pop and overall_pop.get("win", 0) > 0:
        # 頭数区分の比率を求める（例: small 1番人気41%/全体34% = 1.21倍）
        win_ratio = fs_entry.get("win", org_win) / max(overall_pop.get("win", org_win), 0.001)
        top2_ratio = fs_entry.get("top2", org_top2) / max(overall_pop.get("top2", org_top2), 0.001)
        top3_ratio = fs_entry.get("top3", org_top3) / max(overall_pop.get("top3", org_top3), 0.001)
        # 補正を控えめに適用（50%ブレンド: 1.0に近づける）
        win_ratio = 0.5 + 0.5 * win_ratio
        top2_ratio = 0.5 + 0.5 * top2_ratio
        top3_ratio = 0.5 + 0.5 * top3_ratio

        org_win *= win_ratio
        org_top2 *= top2_ratio
        org_top3 *= top3_ratio
        venue_win *= win_ratio
        venue_top2 *= top2_ratio
        venue_top3 *= top3_ratio

    return org_win, org_top2, org_top3, venue_win, venue_top2, venue_top3


def blend_probabilities(
    evaluations: list,
    venue_name: str,
    is_jra: bool,
    field_count: int,
    stats: dict,
) -> None:
    """全馬の確率を統計テーブルとブレンドする（in-place 更新）

    動的alpha:
    - モデル確信度(1位-2位の勝率gap)に応じてブレンド比率を調整
    - gap大(一強) → モデル重視
    - gap小(拮抗) → 統計重視
    """
    org = "JRA" if is_jra else "NAR"

    # モデル確信度の計算（1位-2位の勝率差）
    all_wp = sorted([ev.win_prob for ev in evaluations], reverse=True)
    gap = (all_wp[0] - all_wp[1]) if len(all_wp) >= 2 else 0

    # 動的alpha: gap が大きいほどモデル信頼度を上げる
    confidence = min(1.0, gap / CONFIDENCE_GAP)
    alpha_model = ALPHA_MODEL_MIN + confidence * (ALPHA_MODEL_MAX - ALPHA_MODEL_MIN)
    alpha_stats = 1.0 - alpha_model
    # 統計内の分配: 競馬場60%、全体40%
    alpha_org = alpha_stats * 0.4
    alpha_venue = alpha_stats * 0.6

    blended_count = 0

    for ev in evaluations:
        pop = getattr(ev.horse, "popularity", None)
        if pop is None or pop < 1:
            continue

        odds = getattr(ev.horse, "odds", None) or getattr(ev.horse, "tansho_odds", None)

        org_win, org_top2, org_top3, ven_win, ven_top2, ven_top3 = _lookup_rates(
            stats, org, venue_name, pop, odds, field_count
        )

        ev.win_prob = (
            alpha_model * ev.win_prob
            + alpha_org * org_win
            + alpha_venue * ven_win
        )
        ev.place2_prob = (
            alpha_model * ev.place2_prob
            + alpha_org * org_top2
            + alpha_venue * ven_top2
        )
        ev.place3_prob = (
            alpha_model * ev.place3_prob
            + alpha_org * org_top3
            + alpha_venue * ven_top3
        )

        blended_count += 1

    if blended_count > 0:
        logger.debug(
            "人気別統計ブレンド完了: %d/%d頭 (alpha_model=%.2f, org=%s, venue=%s)",
            blended_count, len(evaluations), alpha_model, org, venue_name,
        )


def blend_probabilities_dict(
    horses: List[dict],
    venue_name: str,
    is_jra: bool,
    field_count: int,
    stats: dict,
) -> None:
    """dict版ブレンド（dashboard.py のリアルタイム更新用）

    HorseEvaluation ではなく dict を直接操作する。
    """
    org = "JRA" if is_jra else "NAR"

    # モデル確信度
    all_wp = sorted([h.get("win_prob", 0) for h in horses], reverse=True)
    gap = (all_wp[0] - all_wp[1]) if len(all_wp) >= 2 else 0

    confidence = min(1.0, gap / CONFIDENCE_GAP)
    alpha_model = ALPHA_MODEL_MIN + confidence * (ALPHA_MODEL_MAX - ALPHA_MODEL_MIN)
    alpha_stats = 1.0 - alpha_model
    alpha_org = alpha_stats * 0.4
    alpha_venue = alpha_stats * 0.6

    for h in horses:
        pop = h.get("popularity")
        if pop is None or pop < 1:
            continue

        odds = h.get("odds")

        org_win, org_top2, org_top3, ven_win, ven_top2, ven_top3 = _lookup_rates(
            stats, org, venue_name, pop, odds, field_count
        )

        h["win_prob"] = (
            alpha_model * h.get("win_prob", 1.0 / field_count)
            + alpha_org * org_win
            + alpha_venue * ven_win
        )
        h["place2_prob"] = (
            alpha_model * h.get("place2_prob", 2.0 / field_count)
            + alpha_org * org_top2
            + alpha_venue * ven_top2
        )
        h["place3_prob"] = (
            alpha_model * h.get("place3_prob", 3.0 / field_count)
            + alpha_org * org_top3
            + alpha_venue * ven_top3
        )

    # 正規化（勝率合計=1.0, 連対率合計≈2.0, 複勝率合計≈3.0）
    _normalize_dict_probs(horses, field_count)


def _normalize_dict_probs(horses: List[dict], field_count: int) -> None:
    """dict版の確率正規化"""
    total_win = sum(h.get("win_prob", 0) for h in horses)
    total_p2 = sum(h.get("place2_prob", 0) for h in horses)
    total_p3 = sum(h.get("place3_prob", 0) for h in horses)

    target_win = 1.0
    target_p2 = min(2.0, field_count * 1.0)
    target_p3 = min(3.0, field_count * 1.0)

    if total_win > 0:
        for h in horses:
            h["win_prob"] = h.get("win_prob", 0) / total_win * target_win
    if total_p2 > 0:
        for h in horses:
            h["place2_prob"] = h.get("place2_prob", 0) / total_p2 * target_p2
    if total_p3 > 0:
        for h in horses:
            h["place3_prob"] = h.get("place3_prob", 0) / total_p3 * target_p3


def reassign_marks_dict(horses: List[dict]) -> None:
    """dict版の印再割り当て（リアルタイムオッズ更新用）

    composite 降順で ◉/◎/○/▲/△/★ を付与。
    ☆(穴馬)・×(危険馬)は維持。
    """
    MARK_SEQUENCE = ["◎", "○", "▲", "△", "★"]
    TEKIPAN_GAP = 3.0  # 1位-2位のcomposite差がこれ以上なら◉

    # 既存の☆/×をメモ
    special_marks = {}
    for h in horses:
        m = h.get("mark", "")
        if m in ("☆", "×"):
            special_marks[h.get("horse_no")] = m

    # composite 降順ソート（総合指数順で印付け）
    sorted_h = sorted(horses, key=lambda h: h.get("composite", 0), reverse=True)

    # 全印クリア
    for h in horses:
        h["mark"] = ""

    # ☆/×を復元
    for h in horses:
        hno = h.get("horse_no")
        if hno in special_marks:
            h["mark"] = special_marks[hno]

    # composite順に印を付与（☆/×が既についている馬はスキップ）
    mark_idx = 0
    for i, h in enumerate(sorted_h):
        if h.get("mark"):
            continue
        if mark_idx == 0:
            # 1位: ◉ or ◎
            c1 = h.get("composite", 0)
            c2 = sorted_h[1].get("composite", 0) if len(sorted_h) > 1 else 0
            h["mark"] = "◉" if (c1 - c2) >= TEKIPAN_GAP else "◎"
            mark_idx += 1
        elif mark_idx <= len(MARK_SEQUENCE):
            h["mark"] = MARK_SEQUENCE[mark_idx - 1]
            mark_idx += 1
        else:
            break
