"""
競馬場類似度マトリクス（全24場）

レース結果に本質的に影響する4因子のみで類似度を算出する:
  1. 直線距離 — 差し・追込が届くかの境界線
  2. ゴール前の坂 — パワー型かスピード型かの分水嶺
  3. スタート〜初角距離 — 先行争いの激しさ＝ペースの決まり方
  4. 3〜4角の形状 — 大回り/小回り/スパイラル＝器用さの要否

用途:
  - 未経験コースでの能力推定（類似場の実績を参照）
  - コース適性の補完（Aコースの実績がないときBコースで代替）

使い方:
  from data.masters.venue_similarity import get_venue_similarity, get_similar_venues
  sim = get_venue_similarity("東京", "阪神")  # 0.0〜1.0
  top3 = get_similar_venues("東京", n=3)       # [("阪神", 0.82), ...]
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from data.masters.course_master import ALL_COURSES


# ============================================================
# 競馬場プロファイル: 本質4因子を抽出
# ============================================================

_FIRST_CORNER_SCORE = {"短い": 0.0, "平均": 0.5, "長い": 1.0, "直線のみ": 1.5}
_CORNER_TYPE_SCORE = {"大回り": 1.0, "スパイラル": 0.5, "小回り": 0.0}
_SLOPE_SCORE = {"急坂": 1.0, "軽坂": 0.5, "坂なし": 0.0}


@dataclass
class VenueProfile:
    venue: str
    venue_code: str
    is_jra: bool
    has_turf: bool
    has_dirt: bool
    # --- 本質4因子 ---
    avg_straight_m: float       # 1. 直線距離
    slope_type: str             # 2. ゴール前の坂
    first_corner_score: float   # 3. スタート〜初角（0=短い, 1=長い）
    corner_type_dominant: str   # 4. 3-4角形状
    # --- 補足 ---
    max_straight_m: int
    direction: str
    inside_outside: bool
    n_courses: int
    distances: List[int]


def _build_venue_profiles() -> Dict[str, VenueProfile]:
    """全24場のプロファイルを構築"""
    venue_courses: Dict[str, list] = defaultdict(list)
    for c in ALL_COURSES:
        venue_courses[c.venue].append(c)

    profiles = {}
    for venue, courses in venue_courses.items():
        straights = [c.straight_m for c in courses]
        surfaces = {c.surface for c in courses}
        directions = {c.direction for c in courses}
        io = {c.inside_outside for c in courses}

        corner_types = [c.corner_type for c in courses]
        ct_count = Counter(corner_types)
        dominant_ct = ct_count.most_common(1)[0][0]

        slopes = {c.slope_type for c in courses}
        slope = "急坂" if "急坂" in slopes else ("軽坂" if "軽坂" in slopes else "坂なし")

        # first_corner_m（実距離）が利用可能なら定量的に算出、なければ定性スコア
        fc_m_values = [c.first_corner_m for c in courses if c.first_corner_m > 0]
        if fc_m_values:
            avg_fc_m = sum(fc_m_values) / len(fc_m_values)
            avg_fc = min(1.5, avg_fc_m / 500.0)  # 0m→0.0, 500m→1.0
        else:
            fc_scores = [_FIRST_CORNER_SCORE.get(c.first_corner, 0.5) for c in courses]
            avg_fc = sum(fc_scores) / len(fc_scores)

        profiles[venue] = VenueProfile(
            venue=venue,
            venue_code=courses[0].venue_code,
            is_jra=courses[0].is_jra,
            has_turf="芝" in surfaces,
            has_dirt="ダート" in surfaces,
            avg_straight_m=sum(straights) / len(straights),
            slope_type=slope,
            first_corner_score=avg_fc,
            corner_type_dominant=dominant_ct,
            max_straight_m=max(straights),
            direction="両" if len(directions) > 1 else directions.pop(),
            inside_outside=("内" in io or "外" in io),
            n_courses=len(courses),
            distances=sorted({c.distance for c in courses}),
        )
    return profiles


# ============================================================
# 4因子ベクトル → ユークリッド距離ベースの類似度
# ============================================================


def _venue_to_vector(p: VenueProfile) -> List[float]:
    """本質4因子のみの特徴ベクトル

    各因子を0〜1にスケーリングし、等しい重みで比較する。
    コサイン類似度ではなくユークリッド距離ベースに変更
    （全次元が同スケールなので方向より距離が適切）。
    """
    return [
        p.avg_straight_m / 550,                            # 1. 直線距離 (200〜525m → 0.36〜0.95)
        _SLOPE_SCORE.get(p.slope_type, 0.0),               # 2. ゴール前の坂 (0/0.5/1)
        p.first_corner_score,                              # 3. 初角までの距離 (0〜1)
        _CORNER_TYPE_SCORE.get(p.corner_type_dominant, 0.5), # 4. 3-4角形状 (0/0.5/1)
    ]


def _euclidean_similarity(a: List[float], b: List[float]) -> float:
    """ユークリッド距離を0〜1の類似度に変換

    4次元・各0〜1の空間なので最大距離は2.0。
    similarity = 1 - (dist / max_dist) で正規化。
    """
    dist = math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
    max_dist = math.sqrt(len(a))
    return max(0.0, 1.0 - dist / max_dist)


# ============================================================
# 類似度マトリクス生成
# ============================================================

import threading as _threading

_PROFILES: Optional[Dict[str, VenueProfile]] = None
_SIMILARITY_MATRIX: Optional[Dict[str, Dict[str, float]]] = None
_BUILD_LOCK = _threading.Lock()


def _ensure_built():
    global _PROFILES, _SIMILARITY_MATRIX
    # Double-checked locking パターン
    if _SIMILARITY_MATRIX is not None:
        return
    with _BUILD_LOCK:
        if _SIMILARITY_MATRIX is not None:
            return
        _PROFILES = _build_venue_profiles()
        vectors = {v: _venue_to_vector(p) for v, p in _PROFILES.items()}
        venues = sorted(_PROFILES.keys())
        _matrix = {}  # 一時辞書に構築してから atomic にグローバル代入
        for v1 in venues:
            _matrix[v1] = {}
            for v2 in venues:
                _matrix[v1][v2] = round(_euclidean_similarity(vectors[v1], vectors[v2]), 4)
        _SIMILARITY_MATRIX = _matrix


def get_venue_similarity(venue_a: str, venue_b: str) -> float:
    """2場間の類似度を返す（0.0〜1.0）"""
    _ensure_built()
    return _SIMILARITY_MATRIX.get(venue_a, {}).get(venue_b, 0.0)


def get_similar_venues(venue: str, n: int = 5) -> List[Tuple[str, float]]:
    """指定場に類似する競馬場を上位n件返す（自身を除く）"""
    _ensure_built()
    row = _SIMILARITY_MATRIX.get(venue, {})
    ranked = sorted(
        [(v, s) for v, s in row.items() if v != venue],
        key=lambda x: -x[1],
    )
    return ranked[:n]


def get_all_profiles() -> Dict[str, VenueProfile]:
    _ensure_built()
    return dict(_PROFILES)


def get_full_matrix() -> Dict[str, Dict[str, float]]:
    _ensure_built()
    return dict(_SIMILARITY_MATRIX)


# ============================================================
# CLI: 全場類似度マトリクスを表示
# ============================================================

if __name__ == "__main__":
    _ensure_built()

    try:
        from rich.console import Console
        from rich.table import Table
        console = Console()
    except ImportError:
        console = None

    venues_jra = [v for v, p in sorted(_PROFILES.items(), key=lambda x: x[1].venue_code) if p.is_jra]
    venues_nar = [v for v, p in sorted(_PROFILES.items(), key=lambda x: x[1].venue_code) if not p.is_jra]
    venues_ordered = venues_jra + venues_nar

    if console:
        console.print("\n[bold]競馬場類似度マトリクス（全24場）[/]\n")

        # プロファイル一覧（本質4因子）
        pt = Table(title="競馬場プロファイル（本質4因子）")
        pt.add_column("場", style="cyan", width=6)
        pt.add_column("区分", width=4)
        pt.add_column("直線距離", justify="right", width=7)
        pt.add_column("坂", width=4)
        pt.add_column("初角距離", width=6)
        pt.add_column("3-4角", width=8)
        _FC_LABEL = {0.0: "短い", 0.5: "平均", 1.0: "長い", 1.5: "直線"}
        for v in venues_ordered:
            p = _PROFILES[v]
            fc_val = round(p.first_corner_score * 2) / 2
            fc_label = _FC_LABEL.get(fc_val, f"{p.first_corner_score:.1f}")
            pt.add_row(
                v,
                "JRA" if p.is_jra else "地方",
                f"{p.avg_straight_m:.0f}m",
                p.slope_type,
                fc_label,
                p.corner_type_dominant,
            )
        console.print(pt)

        # 各場のTOP5類似場
        console.print("\n[bold]各競馬場の類似場 TOP5[/]\n")
        st = Table(title="類似場ランキング")
        st.add_column("場", style="bold cyan", width=6)
        for i in range(1, 6):
            st.add_column(f"#{i}", width=12)
        for v in venues_ordered:
            top5 = get_similar_venues(v, n=5)
            cells = [f"{name} {sim:.0%}" for name, sim in top5]
            st.add_row(v, *cells)
        console.print(st)

        # ヒートマップ（テキスト版）
        console.print("\n[bold]全場ペアワイズ類似度[/]（上位のみ抜粋）\n")
        pairs = []
        for i, v1 in enumerate(venues_ordered):
            for v2 in venues_ordered[i + 1:]:
                pairs.append((v1, v2, get_venue_similarity(v1, v2)))
        pairs.sort(key=lambda x: -x[2])

        ht = Table(title="類似度 TOP30 ペア")
        ht.add_column("#", width=3)
        ht.add_column("場A", width=6)
        ht.add_column("場B", width=6)
        ht.add_column("類似度", justify="right", width=8)
        ht.add_column("バー", width=20)
        for i, (a, b, s) in enumerate(pairs[:30], 1):
            bar_len = int(s * 20)
            bar = "[green]" + "#" * bar_len + "[/]" + "-" * (20 - bar_len)
            ht.add_row(str(i), a, b, f"{s:.1%}", bar)
        console.print(ht)

        # 最も異なるペア
        console.print("\n[bold]最も異なるペア TOP10[/]\n")
        bt = Table()
        bt.add_column("#", width=3)
        bt.add_column("場A", width=6)
        bt.add_column("場B", width=6)
        bt.add_column("類似度", justify="right", width=8)
        for i, (a, b, s) in enumerate(pairs[-10:], 1):
            bt.add_row(str(i), a, b, f"{s:.1%}")
        console.print(bt)

    else:
        print("=" * 60)
        print("競馬場類似度マトリクス（全24場）")
        print("=" * 60)
        for v in venues_ordered:
            top5 = get_similar_venues(v, n=5)
            top_str = ", ".join(f"{name}({sim:.0%})" for name, sim in top5)
            print(f"  {v}: {top_str}")
