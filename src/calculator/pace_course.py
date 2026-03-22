"""
競馬解析マスターシステム v3.0 - 展開・コース適性計算層
F章: 展開偏差値 (ペース予測、脚質分類、上がり3F、展開偏差値反映)
G章: コース適性偏差値 (コース構造、枠順バイアス、脚質バイアス)
"""

import math
import statistics
from typing import Dict, List, Optional, Tuple

from config.settings import (
    COURSE_BASE,
    PACE_BASE,
)
from src.models import (
    CourseAptitude,
    CourseMaster,
    Horse,
    JockeyStats,
    PaceDeviation,
    PaceType,
    PastRun,
    RunningStyle,
)


# ============================================================
# F-0: フィールド脚質分布の制約付き分類
# ============================================================

# 脚質分布の制約テーブル: (逃げ最小, 逃げ最大, 先行率, 差し率, 追込上限率)
_STYLE_DIST_CONSTRAINTS = {
    "small":  (1, 1, 0.25, 0.35, 0.25),   # ≤8頭
    "medium": (1, 2, 0.25, 0.30, 0.25),   # 9-12頭
    "large":  (1, 2, 0.25, 0.30, 0.25),   # 13-18頭
}


def _get_constraint_key(n: int) -> str:
    if n <= 8:
        return "small"
    if n <= 12:
        return "medium"
    return "large"


def _calc_raw_position_score(
    horse: Horse,
    course: CourseMaster,
    jockey_cache: Optional[Dict] = None,
) -> float:
    """各馬の「生の位置スコア」を算出（0.0=最前方, 1.0=最後方）

    優先度:
    1. 同距離帯(±200m)・同馬場の過去走で cornerデータあり
    2. cornerデータがある直近5走
    3. 全直近5走
    4. デフォルト 0.45
    """
    runs = horse.past_runs
    if not runs:
        # 初出走馬: 騎手の位置取り傾向を反映
        if jockey_cache and hasattr(horse, 'jockey') and horse.jockey:
            jockey_pos = jockey_cache.get(horse.jockey)
            if jockey_pos is not None:
                return jockey_pos
        return 0.50  # 中立値（0.45は前寄りすぎ）

    dist = course.distance
    surf = course.surface

    # 優先1: 同距離帯・同馬場でcornerデータがある走（直近10走内）
    same_cond_positions = []
    for r in runs[:10]:
        if r.positions_corners and r.relative_position is not None:
            if r.surface == surf and abs(r.distance - dist) <= 200:
                same_cond_positions.append(r.relative_position)
    if len(same_cond_positions) >= 2:
        base_score = statistics.mean(same_cond_positions)
    else:
        # 優先2: cornerデータがある直近5走
        corner_positions = [r.relative_position for r in runs[:5]
                           if r.relative_position is not None and r.positions_corners]
        if corner_positions:
            base_score = statistics.mean(corner_positions)
        else:
            # 優先3: 全直近5走
            all_positions = [r.relative_position for r in runs[:5]
                            if r.relative_position is not None]
            if all_positions:
                base_score = statistics.mean(all_positions)
            else:
                return 0.45

    # ---- 条件変化による補正 ----
    adj = 0.0

    # 距離変更補正
    if runs:
        prev_dist = runs[0].distance
        dist_diff = dist - prev_dist
        if dist_diff <= -200:
            adj -= 0.05  # 距離短縮 → 前に行きやすい
        elif dist_diff >= 200:
            adj += 0.05  # 距離延長 → 後方になりやすい

    # クラス変動補正（grade encode: 高い方が上位クラス）
    if runs:
        _grade_order = {"新馬": 0, "未勝利": 1, "1勝": 2, "2勝": 3, "3勝": 4,
                        "OP": 5, "L": 5, "G3": 6, "G2": 7, "G1": 8,
                        "C3": 0, "C2": 1, "C1": 2, "B3": 3, "B2": 4, "B1": 5, "A2": 6, "A1": 7}
        cur_grade = getattr(course, "grade", "") or ""
        prev_grade = runs[0].grade or ""
        cur_g = _grade_order.get(cur_grade, 3)
        prev_g = _grade_order.get(prev_grade, 3)
        if cur_g > prev_g:
            adj += 0.03  # クラス上昇 → 前に出にくい
        elif cur_g < prev_g:
            adj -= 0.03  # クラス下降 → 前に出やすい

    # 騎手の位置取り傾向による補正
    if jockey_cache and horse.is_jockey_change and runs:
        new_jockey_pos = jockey_cache.get(horse.jockey_id)
        # 前走騎手のIDを過去走から取得
        prev_jockey_id = runs[0].jockey_id if runs[0].jockey_id else None
        if prev_jockey_id:
            old_jockey_pos = jockey_cache.get(prev_jockey_id)
        else:
            old_jockey_pos = None
        if new_jockey_pos is not None and old_jockey_pos is not None:
            diff = (new_jockey_pos - old_jockey_pos) * 0.5
            adj += max(-0.15, min(0.15, diff))  # ±0.15制限

    return max(0.0, min(1.0, base_score + adj))


def classify_field_styles(
    horses: List[Horse],
    past_runs_map: Optional[Dict[str, List[PastRun]]] = None,
    course: Optional[CourseMaster] = None,
    jockey_cache: Optional[Dict] = None,
) -> Dict[int, Tuple[RunningStyle, float]]:
    """フィールド全体の脚質分布を制約付きで分類

    各馬の生の位置スコアを算出し、フィールド内ランクソートして
    頭数に応じた分布制約を適用する。

    Args:
        horses: 出走馬リスト
        past_runs_map: 馬ID→過去走マップ（未使用、Horse.past_runsを直接参照）
        course: コース情報（距離・馬場で過去走フィルタに使用）
        jockey_cache: 騎手ID→平均位置取りスコア

    Returns:
        {horse_no: (RunningStyle, raw_position_score)}
    """
    n = len(horses)
    if n == 0:
        return {}

    # ---- Step 1: 各馬の生の位置スコア算出 ----
    scores: List[Tuple[int, float]] = []
    for h in horses:
        if course is not None:
            raw = _calc_raw_position_score(h, course, jockey_cache)
        else:
            # courseなしフォールバック: 直近5走平均
            rels = [r.relative_position for r in h.past_runs[:5]
                    if r.relative_position is not None]
            raw = statistics.mean(rels) if rels else 0.45
        scores.append((h.horse_no, raw))

    # ---- Step 2: フィールド内分布制約 ----
    # スコア昇順ソート（小さい=前方）
    scores.sort(key=lambda x: x[1])

    key = _get_constraint_key(n)
    min_nige, max_nige, senkou_rate, sashi_rate, oikomi_max_rate = _STYLE_DIST_CONSTRAINTS[key]

    # 各脚質の枠数を算出
    n_senkou = max(1, math.ceil(n * senkou_rate))
    n_sashi = max(1, math.ceil(n * sashi_rate))
    n_oikomi_max = max(0, math.floor(n * oikomi_max_rate))

    # 逃げ馬数の決定: 最前方は必ず逃げ。2番目もスコアが近ければ逃げ
    n_nige = min_nige
    if n >= 2 and max_nige >= 2:
        top_score = scores[0][1]
        second_score = scores[1][1] if len(scores) > 1 else 999
        if second_score - top_score <= 0.05:
            n_nige = 2

    # 残りを割り当て
    remaining = n - n_nige
    # 先行・差し・追込に割り当て（追込上限を適用）
    n_oikomi = min(n_oikomi_max, max(0, remaining - n_senkou - n_sashi))
    # 先行と差しは残りを均等に近く分配
    allocated_senkou = min(n_senkou, remaining - n_oikomi)
    allocated_sashi = remaining - allocated_senkou - n_oikomi

    # 割り当て枠の確定（7分類版）
    # 先行枠を「先行+好位」に、差し枠を「中団+差し」に分割
    result: Dict[int, Tuple[RunningStyle, float]] = {}
    # 先行を前半=先行、後半=好位に分割
    senkou_half = max(1, (allocated_senkou + 1) // 2)
    # 差しを前半=中団、後半=差しに分割
    sashi_count = allocated_sashi
    chuudan_half = max(1, (sashi_count + 1) // 2) if sashi_count >= 2 else 0

    for idx, (hno, raw_score) in enumerate(scores):
        if idx < n_nige:
            style = RunningStyle.NIGASHI
        elif idx < n_nige + senkou_half:
            style = RunningStyle.SENKOU
        elif idx < n_nige + allocated_senkou:
            style = RunningStyle.KOUI
        elif idx < n_nige + allocated_senkou + chuudan_half:
            style = RunningStyle.CHUUDAN
        elif idx < n - n_oikomi:
            style = RunningStyle.SASHIKOMI
        else:
            style = RunningStyle.OIKOMI
        result[hno] = (style, raw_score)

    return result


def normalize_field_positions(
    raw_positions: Dict[int, float],
    field_count: int,
) -> Dict[int, float]:
    """全馬の推定位置をフィールド内で正規化

    制約:
    - 全馬の相対順序を維持
    - 等間隔に近い分布に補正（生の値70% + 理想分布30%）
    - 先頭馬は0.05〜0.15にクランプ
    - 最後方馬は0.80〜0.95にクランプ
    """
    n = len(raw_positions)
    if n <= 1:
        return dict(raw_positions)

    # ランクソート（小→大）
    sorted_items = sorted(raw_positions.items(), key=lambda x: x[1])

    # 理想分布: 等間隔に配置（0.05〜0.90範囲）
    ideal = [0.05 + (0.85 * i / (n - 1)) for i in range(n)]

    # 生の値70% + 理想分布30%の加重平均
    result: Dict[int, float] = {}
    for idx, (hno, raw_val) in enumerate(sorted_items):
        blended = raw_val * 0.70 + ideal[idx] * 0.30
        # 先頭馬のクランプ
        if idx == 0:
            blended = max(0.05, min(0.15, blended))
        # 最後方馬のクランプ
        elif idx == n - 1:
            blended = max(0.80, min(0.95, blended))
        else:
            blended = max(0.05, min(0.95, blended))
        result[hno] = blended

    return result


# ============================================================
# F-1: ペース予測
# ============================================================


class PacePredictor:
    """
    ペーススコア = 基礎点 + コース傾向補正 + 逃げ馬実効スコア補正 + 先行密度補正
    5段階(HH/HM/MM/MS/SS)

    案A: course_pace_tendency（DB実績）で基礎点を補正
    案B: _calc_escape_strength() で逃げ馬の「実際に逃げられるか」を評価
    """

    def predict_pace(
        self,
        entries: List[Horse],
        past_runs_map: Dict[str, List[PastRun]],
        course: CourseMaster,
        course_pace_tendency: Optional[Dict] = None,
        field_styles: Optional[Dict[int, Tuple[RunningStyle, float]]] = None,
    ) -> Tuple[PaceType, float, List[int], float, float]:
        """
        Returns: (PaceType, ペーススコア, 逃げ候補馬番リスト, 先行馬率, 逃げ実効スコア合計)
        """
        leaders: List[int] = []
        front_count = 0
        total = len(entries)

        for horse in entries:
            # field_styles（制約付き分類）があればそれを使用
            if field_styles and horse.horse_no in field_styles:
                style = field_styles[horse.horse_no][0]
            else:
                runs = past_runs_map.get(horse.horse_id, [])
                style = self._classify_style(runs)
            if style == RunningStyle.NIGASHI:
                leaders.append(horse.horse_no)
            elif style in (RunningStyle.SENKOU,):
                front_count += 1

        # ── 基礎点 (50 = MM) ──────────────────────────────────────────
        base = 50.0

        # ── 案A: コース別ペース傾向補正 ─────────────────────────────────
        if course_pace_tendency:
            key = (str(course.venue_code), str(course.surface), int(course.distance))
            tendency = course_pace_tendency.get(key)
            if tendency and tendency["race_cnt"] >= 10:
                hist_escape = tendency["escape_rate"]
                hist_front  = tendency["front_rate"]
                # 逃げ馬比率が高いコース → ハイペース傾向
                if hist_escape >= 0.18:      # 約3頭/16頭以上が逃げ馬傾向
                    base += 4
                elif hist_escape >= 0.12:    # 約2頭/16頭
                    base += 2
                elif hist_escape <= 0.04:    # ほぼ逃げ馬なし傾向
                    base -= 3
                # 先行密集コース
                if hist_front >= 0.45:
                    base += 2
                elif hist_front <= 0.20:
                    base -= 2

        # ── 案B: 逃げ馬の実効スコア合計 ─────────────────────────────────
        escape_total_strength = 0.0
        escape_strengths: Dict[int, float] = {}  # horse_no → strength
        for horse in entries:
            if horse.horse_no not in leaders:
                continue
            runs = past_runs_map.get(horse.horse_id, [])
            s = self._calc_escape_strength(
                runs,
                gate_no=getattr(horse, "gate_no", None),
                field_count=total,
            )
            escape_strengths[horse.horse_no] = s
            escape_total_strength += s

        max_escape_strength = max(escape_strengths.values()) if escape_strengths else 0.0

        # 逃げ馬補正（実効スコアベース）
        if escape_total_strength >= 1.8:   # 実質2頭以上の強い逃げ馬
            base += 8
        elif escape_total_strength >= 1.2:
            base += 5
        elif escape_total_strength >= 0.6:
            base += 2
        elif len(leaders) == 0:
            # F2: 逃げ馬不在時のペース予測改善
            # 先行馬が密集している場合はミドルペース寄りにすべき
            front_ratio_esc = front_count / max(total, 1)
            base -= 2 if front_ratio_esc >= 0.40 else 4  # 先行密集→ミドル寄り

        # 先行馬密度補正
        front_ratio = (len(leaders) + front_count) / max(total, 1)
        if front_ratio >= 0.5:
            base += 5
        elif front_ratio >= 0.35:
            base += 2
        elif front_ratio <= 0.15:
            base -= 3

        # コース形状補正
        if course.straight_m >= 400:
            base -= 2  # 長い直線→スロー化傾向
        if course.corner_type == "小回り":
            base += 3  # 小回りはペース上がりやすい

        # 逃げ馬の過去前半3Fが速い→ハイペース寄り
        first_3f_list = []
        for horse in entries:
            if horse.horse_no not in leaders:
                continue
            runs = past_runs_map.get(horse.horse_id, [])
            for r in runs[:3]:
                if getattr(r, "first_3f_sec", None) is not None and r.first_3f_sec < 36:
                    first_3f_list.append(r.first_3f_sec)
        if first_3f_list and statistics.mean(first_3f_list) < 35.5:
            base += 2  # 速い前半実績

        pace_type = self._score_to_type(base)
        front_horse_rate = (len(leaders) + front_count) / max(total, 1)

        return pace_type, base, leaders, front_horse_rate, max_escape_strength

    def _calc_escape_strength(
        self,
        runs: List[PastRun],
        gate_no: Optional[int] = None,
        field_count: int = 16,
    ) -> float:
        """
        案B: 逃げ馬の「実際にハナを取れる」可能性スコア（0.0〜1.0）

        内枠ほど外からカバーされにくく逃げやすい。
        過去走で実際に前目を取れていた率も重要。
        """
        # 内枠ボーナス（1枠=1.0, 8枠≒0）
        if gate_no and 1 <= gate_no <= 8:
            gate_factor = 1.0 - (gate_no - 1) / 7.0
        else:
            gate_factor = 0.5

        # 過去走で1角前目（相対位置0.20以下）を取れた率
        if runs:
            # F4: relative_position の Null安全処理
            valid = [r for r in runs[:10] if r.relative_position is not None]
            escape_runs = sum(1 for r in valid if r.relative_position <= 0.20)
            past_escape_rate = escape_runs / max(len(valid), 1)
            # 直近の状態（前走で逃げ/先行していたか）
            recent_rp = runs[0].relative_position if runs[0].relative_position is not None else 0.5
            recent_bonus = 0.15 if recent_rp <= 0.25 else 0.0
        else:
            past_escape_rate = 0.5
            recent_bonus = 0.0

        return gate_factor * 0.35 + past_escape_rate * 0.50 + recent_bonus

    def _score_to_type(self, score: float) -> PaceType:
        if score >= 62:
            return PaceType.HH
        if score >= 56:
            return PaceType.HM
        if score >= 44:
            return PaceType.MM
        if score >= 38:
            return PaceType.MS
        return PaceType.SS

    def _classify_style(self, runs: List[PastRun]) -> RunningStyle:
        """F-2: 全コーナー通過順位があればそれを使用、なければ4角相対位置から脚質分類"""
        from src.calculator.pace_analysis import classify_style_from_corners

        style = classify_style_from_corners(runs)
        if style is not None:
            return style
        # フォールバック: 4角相対位置
        if not runs:
            return RunningStyle.SENKOU
        # F4: relative_position の Null安全処理
        rel_positions = [r.relative_position for r in runs[:5]
                         if r.relative_position is not None]
        if not rel_positions:
            return RunningStyle.SENKOU
        avg_pos = statistics.mean(rel_positions)
        return _rel_pos_to_style(avg_pos)


# ============================================================
# F-2: 脚質分類（詳細版）
# ============================================================


def _rel_pos_to_style(rel_pos: float) -> "RunningStyle":
    """相対位置(0.0-1.0)から脚質を判定（7分類版）"""
    if rel_pos <= 0.10:
        return RunningStyle.NIGASHI
    if rel_pos <= 0.22:
        return RunningStyle.SENKOU
    if rel_pos <= 0.38:
        return RunningStyle.KOUI
    if rel_pos <= 0.55:
        return RunningStyle.CHUUDAN
    if rel_pos <= 0.72:
        return RunningStyle.SASHIKOMI
    return RunningStyle.OIKOMI


class StyleClassifier:
    """
    位置取り×末脚の2軸・9分類 + マクリ
    基本脚質 + 対応脚質の2層構造
    """

    # 末脚タイプ閾値 (上がり3F偏差との比較)
    LAST3F_EXPLOSIVE = -0.5  # 平均より0.5秒速い = 爆発末脚
    LAST3F_STEADY = 0.5  # 平均より0.5秒遅い = 末脚非依存型

    # F1: 芝/ダート別末脚閾値スケール
    # ダートは構造的に上がりが遅いため、芝と同じ閾値を使うべきではない
    LAST3F_SCALE = {"芝": 1.0, "ダート": 0.65}

    def classify(self, runs: List[PastRun], surface: str = "芝") -> Dict:
        """
        Args:
            runs: 過去走リスト
            surface: "芝" or "ダート" — 末脚閾値のスケールに使用 (F1)
        Returns: {
            "basic": RunningStyle,
            "adaptive": [RunningStyle, ...],
            "last3f_type": str,
            "position_variability": float,
        }
        """
        if not runs:
            return {
                "basic": RunningStyle.SENKOU,
                "adaptive": [RunningStyle.SENKOU],
                "last3f_type": "安定中位末脚",
                "position_variability": 0.5,
            }

        # F4: cornerデータがある走のみ使用（position_4c=finish_posフォールバックは脚質判定に不適）
        rel_positions = [r.relative_position for r in runs[:5]
                         if r.relative_position is not None and r.positions_corners]
        # cornerデータなし走のフォールバック: position_4cを使うが信頼度低
        if not rel_positions:
            rel_positions = [r.relative_position for r in runs[:5]
                             if r.relative_position is not None]
        if not rel_positions:
            return {
                "basic": RunningStyle.SENKOU,
                "adaptive": [RunningStyle.SENKOU],
                "last3f_type": "安定中位末脚",
                "position_variability": 0.5,
            }
        avg_pos = statistics.mean(rel_positions)
        variability = statistics.stdev(rel_positions) if len(rel_positions) >= 2 else 0.0

        basic = self._pos_to_style(avg_pos)
        last3f_type = self._classify_last3f(runs, surface=surface)

        # 適応脚質: ±0.15以内のポジションで好走しているスタイル
        adaptive = set()
        for r in runs[:5]:
            # F4: relative_position の Null安全処理
            if r.finish_pos <= 3 and r.relative_position is not None:
                adaptive.add(self._pos_to_style(r.relative_position))
        if not adaptive:
            adaptive.add(basic)

        return {
            "basic": basic,
            "adaptive": list(adaptive),
            "last3f_type": last3f_type,
            "position_variability": variability,
        }

    def _pos_to_style(self, rel_pos: float) -> RunningStyle:
        return _rel_pos_to_style(rel_pos)

    def _classify_last3f(self, runs: List[PastRun], surface: str = "芝") -> str:
        """末脚プロファイル (F-0)

        F1: 芝/ダート別末脚閾値スケール適用。
        ダートは構造的に上がりが遅いため、閾値を LAST3F_SCALE で緩和する。
        例: ダート(scale=0.65) で爆発末脚の閾値は 34.0 → 34.0 + (37.0-34.0)*(1-0.65) = 35.05
        """
        last3f_list = [r.last_3f_sec for r in runs if r.last_3f_sec > 0]
        if not last3f_list:
            return "安定中位末脚"

        avg = statistics.mean(last3f_list)

        # F1: ダートは上がりが構造的に遅いため閾値を緩和
        # scale=1.0(芝): 閾値そのまま, scale=0.65(ダート): 閾値を上方向にシフト
        scale = self.LAST3F_SCALE.get(surface, 1.0)
        # 基準値 37.0 秒（末脚非依存型の境界）からの差分にスケールを適用
        base_ref = 37.0
        thr_explosive = base_ref - (base_ref - 34.0) * scale   # 芝:34.0, ダート:35.05
        thr_steady    = base_ref - (base_ref - 35.5) * scale   # 芝:35.5, ダート:36.025
        thr_weak      = base_ref + (37.0 - base_ref) * scale   # 芝:37.0, ダート:37.0（変わらず）

        if avg <= thr_explosive:
            return "爆発末脚"
        if avg <= thr_steady:
            return "堅実末脚"
        if avg >= thr_weak:
            return "末脚非依存型"
        return "安定中位末脚"


# ============================================================
# F-3: 上がり3F評価
# ============================================================


class Last3FEvaluator:
    """
    ペース別上がり3F実績テーブル(5段階)
    タイムと順位の両方保持。
    ML モデル (Last3FPredictor) がロード済みなら優先的に使用する。
    """

    def __init__(self, pace_last3f_db: Dict, ml_predictor=None):
        """
        pace_last3f_db: {course_id: {PaceType.value: [last3f_times]}}
        ml_predictor: src.ml.last3f_model.Last3FPredictor or None
        """
        self.db = pace_last3f_db
        self.ml_predictor = ml_predictor

    def get_baseline(self, course_id: str, pace_type: PaceType) -> Optional[float]:
        """ペース別・コース別の基準上がり3Fを返す"""
        course_data = self.db.get(course_id, {})
        times = course_data.get(pace_type.value, [])
        return statistics.mean(times) if times else None

    def estimate_last3f(
        self,
        horse_runs: List[PastRun],
        pace_type: PaceType,
        course_id: str,
        last3f_type: str,
        horse=None,
        race_info=None,
        pace_context: Optional[Dict] = None,
        field_avg_last3f: Optional[float] = None,
    ) -> float:
        """
        F-3: 馬固有の推定上がり3Fを算出
        ML モデルがあれば優先使用、なければルールベースにフォールバック。
        """
        if self.ml_predictor and horse is not None and race_info is not None:
            ml_pred = self.ml_predictor.predict(horse, race_info, pace_context)
            if ml_pred is not None:
                is_dirt_ml = "ダート" in course_id
                # ML予測もコース種別の現実的範囲にクランプ（芝:32-40秒、ダート:35-42秒）
                lo_ml, hi_ml = (35.0, 42.0) if is_dirt_ml else (32.0, 40.0)
                return max(lo_ml, min(hi_ml, ml_pred))

        # ルールベース: 過去のペース別実績から推定
        type_correction = {
            "爆発末脚": -0.5,
            "堅実末脚": -0.2,
            "安定中位末脚": 0.0,
            "末脚非依存型": 0.4,
        }.get(last3f_type, 0.0)

        # ペース補正 (ハイペースほど上がりが遅くなる)
        pace_correction = {
            PaceType.HH: 0.8,
            PaceType.HM: 0.4,
            PaceType.MM: 0.0,
            PaceType.MS: -0.4,
            PaceType.SS: -0.8,
        }.get(pace_type, 0.0)

        is_dirt = "ダート" in course_id
        surface_default = 37.5 if is_dirt else 35.5

        if not horse_runs and field_avg_last3f is not None:
            # 初出走馬: 同レース他馬の推定上がり3F平均を基準にする
            base_last3f = field_avg_last3f
        else:
            _db_baseline = self.get_baseline(course_id, pace_type)
            base_last3f = _db_baseline or surface_default
        if is_dirt and not self.get_baseline(course_id, pace_type):
            # ダートの補正幅は芝より小さい（末脚差が出にくい）
            type_correction *= 0.6
            pace_correction *= 0.6
        estimated = base_last3f + type_correction + pace_correction
        # 現実的な範囲に収める（芝: 32-40秒、ダート: 35-42秒）
        lo, hi = (35.0, 42.0) if is_dirt else (32.0, 40.0)
        return max(lo, min(hi, estimated))


# ============================================================
# F-4: 展開偏差値への反映
# ============================================================


class PaceDeviationCalculator:
    """
    推定ゴール差 = (基準上がり3F - 自分の上がり3F) - 位置取り秒差
    変換係数5.0で偏差値化 (F-4)
    ML位置取り予測モデル(PositionPredictor)があれば優先使用。
    """

    POSITION_SEC_PER_RANK = 0.12  # 位置取り1頭差のタイム差(秒) 暫定・フォールバック

    # 案F-2: 面×ペース別・位置取り秒差テーブル
    # 2026-03-22 実データ検証: data/ml 58254レース 296226ペアから統計
    # 芝は位置差がタイム差にほとんど影響しない（中央値≒0）
    # ダートは位置差がタイム差に大きく影響（中央値0.2-0.3秒）
    POSITION_SEC_BY_PACE_TURF = {
        PaceType.HH: 0.07,   # 芝H: 実測mean=0.066
        PaceType.HM: 0.08,
        PaceType.MM: 0.08,   # 芝M: 実測mean=0.077-0.108
        PaceType.MS: 0.10,
        PaceType.SS: 0.10,   # 芝S: 実測mean=0.105-0.126
    }
    POSITION_SEC_BY_PACE_DIRT = {
        PaceType.HH: 0.33,   # ダートH: 実測mean=0.333
        PaceType.HM: 0.34,
        PaceType.MM: 0.30,   # ダートM: 実測mean=0.280-0.362
        PaceType.MS: 0.28,
        PaceType.SS: 0.33,   # ダートS: 実測mean=0.329
    }
    # 後方互換用フォールバック（芝とダートの中間値）
    POSITION_SEC_BY_PACE = {
        PaceType.HH: 0.20,
        PaceType.HM: 0.18,
        PaceType.MM: 0.15,
        PaceType.MS: 0.14,
        PaceType.SS: 0.15,
    }

    # 案F-4: コース別 base_score 変換係数のデフォルト
    BASE_SCORE_COEFF_DEFAULT = 5.0
    BASE_SCORE_COEFF_MIN = 3.5
    BASE_SCORE_COEFF_MAX = 7.0

    def __init__(
        self,
        position_sec_per_rank_db: Optional[Dict[str, float]] = None,
        position_predictor=None,
        last3f_sigma_db: Optional[Dict] = None,
    ):
        self.position_sec_per_rank_db = position_sec_per_rank_db or {}
        self.position_predictor = position_predictor
        self.last3f_sigma_db = last3f_sigma_db or {}  # 案F-4

    def calc(
        self,
        horse: Horse,
        style_info: Dict,
        pace_type: PaceType,
        last3f_evaluator: Last3FEvaluator,
        course: CourseMaster,
        gate_bias: float,
        jockey_pace_score: float,
        field_count: int = 16,
        race_info=None,
        pace_context: Optional[Dict] = None,
        field_baseline_override: Optional[float] = None,
        override_position: Optional[float] = None,
    ) -> PaceDeviation:
        """
        PaceDeviationを算出する

        Args:
            gate_bias: G-2で算出した枠順バイアス
            jockey_pace_score: H-3で算出した騎手展開影響スコア
            race_info: RaceInfo (ML推定用、Noneならルールベース)
            pace_context: {"n_front": int, "front_ratio": float} (ML推定用)
            field_baseline_override: 出走馬のest_last3f平均（DBにデータがない場合に使用）
            override_position: フィールド正規化済みの推定位置（0.0-1.0）
        """
        basic_style = style_info.get("basic", RunningStyle.SENKOU)
        last3f_type = style_info.get("last3f_type", "安定中位末脚")
        variability = style_info.get("position_variability", 0.3)

        # 推定位置取り（4角番手相対値）
        # override_position（フィールド正規化済み）があればそれを使用
        if override_position is not None:
            est_position = override_position
        else:
            est_position = None
            if self.position_predictor and self.position_predictor.is_available and race_info is not None:
                est_position = self.position_predictor.predict(horse, race_info, pace_context)
            if est_position is None:
                est_position = self._estimate_position(basic_style, pace_type)

        # 推定上がり3F (MLモデルがあればMLを優先)
        est_last3f = last3f_evaluator.estimate_last3f(
            horse.past_runs,
            pace_type,
            course.course_id,
            last3f_type,
            horse=horse,
            race_info=race_info,
            field_avg_last3f=field_baseline_override,
            pace_context=pace_context,
        )
        _baseline_db = last3f_evaluator.get_baseline(course.course_id, pace_type)
        if _baseline_db is not None:
            baseline_last3f = _baseline_db
        elif field_baseline_override is not None:
            # DBにデータがない場合: フィールド全馬のest_last3f平均を基準にする
            baseline_last3f = field_baseline_override
        else:
            # フォールバック: ダートは芝より上がり3Fが遅い
            baseline_last3f = 40.5 if "ダート" in course.course_id else 36.0

        # 位置取り秒差（面×ペース別の実データ検証値を使用）
        fc = max(1, field_count or 16)
        pos_rank = int(est_position * fc)
        is_dirt_course = "ダート" in course.course_id
        _sec_table = self.POSITION_SEC_BY_PACE_DIRT if is_dirt_course else self.POSITION_SEC_BY_PACE_TURF
        sec_per_rank = _sec_table.get(pace_type, self.POSITION_SEC_PER_RANK)
        # コース別較正値があればそれを優先
        if course.course_id in self.position_sec_per_rank_db:
            sec_per_rank = self.position_sec_per_rank_db[course.course_id]
        position_sec = pos_rank * sec_per_rank

        # F3: ダート砂被りペナルティ
        # ダートのスローペースで外枠（gate_no >= 5）は砂被りで位置取りが悪化
        _gate_no = getattr(horse, "gate_no", None)
        if (course.surface == "ダート"
                and pace_type in (PaceType.MS, PaceType.SS)
                and _gate_no is not None and _gate_no >= 5):
            position_sec += 0.02  # 外枠砂被り

        # 推定ゴール差
        goal_diff = (baseline_last3f - est_last3f) - position_sec

        # 案F-4: コース別の上がり3F σ から変換係数を動的決定
        # σ小（差がつきにくい）→ 係数大（感度UP）
        # σ大（差がつきやすい）→ 係数小（過剰感度抑制）
        _sigma_key = (str(course.venue_code), str(course.surface), int(course.distance))
        _sigma_info = self.last3f_sigma_db.get(_sigma_key)
        if _sigma_info and _sigma_info.get("cnt", 0) >= 20:
            _sigma = _sigma_info["sigma"]
            # σ=0.8秒 → 係数5.0 が基準
            # σ小→係数UP, σ大→係数DOWN
            _base_coeff = max(
                self.BASE_SCORE_COEFF_MIN,
                min(self.BASE_SCORE_COEFF_MAX, 4.0 / max(0.5, _sigma))
            )
        else:
            _base_coeff = self.BASE_SCORE_COEFF_DEFAULT
        base_score = PACE_BASE + goal_diff * _base_coeff

        # F-0 各コンポーネント算出
        # 案F-3: 末脚の変動係数(CV = σ/mean)を計算
        import statistics as _stat
        _l3f_times = [r.last_3f_sec for r in horse.past_runs[:5]
                      if hasattr(r, 'last_3f_sec') and r.last_3f_sec > 0]
        if len(_l3f_times) >= 3:
            _mean = _stat.mean(_l3f_times)
            _std = _stat.stdev(_l3f_times)
            last3f_cv = _std / _mean if _mean > 0 else 0.0
        else:
            last3f_cv = 0.0
        last3f_eval = self._calc_last3f_score(last3f_type, pace_type, last3f_cv=last3f_cv)
        pos_balance = self._calc_position_balance(est_position, last3f_type, pace_type)
        course_style_bias = self._calc_course_style_bias(course, basic_style)

        # 表示用running_styleはest_position（ML予測 or ルールベース推定）から導出
        # basic_style（過去走の着順ベース）はcornerデータ不在時に不正確なため
        display_style = _rel_pos_to_style(est_position)

        result = PaceDeviation(
            base_score=base_score,
            last3f_eval=last3f_eval,
            position_balance=pos_balance,
            gate_bias=gate_bias,
            course_style_bias=course_style_bias,
            jockey_pace=jockey_pace_score,
            estimated_position_4c=est_position,
            estimated_last3f=est_last3f,
            running_style=display_style,
        )
        return result

    def _estimate_position(self, style: RunningStyle, pace: PaceType) -> float:
        """推定4角相対位置 (0.0=先頭, 1.0=最後方)"""
        base = {
            RunningStyle.NIGASHI: 0.05,
            RunningStyle.SENKOU: 0.20,
            RunningStyle.KOUI: 0.32,
            RunningStyle.CHUUDAN: 0.45,
            RunningStyle.SASHIKOMI: 0.60,
            RunningStyle.OIKOMI: 0.80,
            RunningStyle.MACURI: 0.50,
        }.get(style, 0.40)

        # ペース補正
        pace_corr = {
            PaceType.HH: -0.05,  # ハイペースは前が消耗→差し馬有利だが位置取り自体は変わらない
            PaceType.SS: +0.05,  # スローは前残り
        }.get(pace, 0.0)

        return max(0.0, min(1.0, base + pace_corr))

    def _calc_last3f_score(self, last3f_type: str, pace: PaceType,
                           last3f_cv: float = 0.0) -> float:
        """❶末脚評価 (-8〜+8)

        案F-3: 末脚安定性（変動係数 CV = σ/mean）を考慮
        - CV低（安定末脚）はボーナス
        - CV高（ムラ型末脚）はペナルティ
        """
        type_score = {
            "爆発末脚": 7,
            "堅実末脚": 3,
            "安定中位末脚": 0,
            "末脚非依存型": -4,
        }.get(last3f_type, 0)

        # ペース×末脚タイプの相性
        if pace in (PaceType.HH, PaceType.HM) and last3f_type == "爆発末脚":
            type_score = min(8, type_score + 2)
        if pace in (PaceType.SS, PaceType.MS) and last3f_type == "末脚非依存型":
            type_score = min(3, type_score + 3)  # スローなら前有利

        # 案F-3: 末脚安定性補正 (-2〜+2)
        # CV < 0.03: 非常に安定 → +2
        # CV 0.03〜0.06: 安定 → +1
        # CV 0.06〜0.10: 普通 → 0
        # CV > 0.10: ムラ型 → -1〜-2
        if last3f_cv > 0:
            if last3f_cv < 0.03:
                stability_bonus = 2.0
            elif last3f_cv < 0.06:
                stability_bonus = 1.0
            elif last3f_cv < 0.10:
                stability_bonus = 0.0
            elif last3f_cv < 0.15:
                stability_bonus = -1.0
            else:
                stability_bonus = -2.0
            type_score += stability_bonus

        return max(-8, min(8, float(type_score)))

    def _calc_position_balance(self, pos: float, last3f_type: str, pace: PaceType) -> float:
        """❷位置取り×末脚バランス (-8〜+8)

        案F-1: 3値（-5/0/+6）から連続スコアへ
        - ペース強度 × 後方位置 × 末脚適性の積でスコア計算
        - 中間値が適切に評価されるよう改善
        """
        # ペース強度: HH=2.0, HM=1.5, MM=1.0, MS=0.5, SS=0.0
        pace_strength = {
            PaceType.HH: 2.0,
            PaceType.HM: 1.5,
            PaceType.MM: 1.0,
            PaceType.MS: 0.5,
            PaceType.SS: 0.0,
        }.get(pace, 1.0)

        # 末脚適性スコア（ハイペース向き末脚ほど高い）
        last3f_fitness = {
            "爆発末脚":     1.0,
            "堅実末脚":     0.6,
            "安定中位末脚": 0.2,
            "末脚非依存型": -0.4,
        }.get(last3f_type, 0.0)

        # ── ハイペース × 後方待機 × 末脚型 ──
        # pos=0.5以上かつ爆発末脚のHHが最高点（元の+6に相当）
        if pace_strength >= 1.5 and last3f_fitness > 0:
            # 後方度合い（0.5〜1.0 を 0〜1 にスケール）
            rear_factor = max(0.0, (pos - 0.3) / 0.7)
            score = pace_strength * last3f_fitness * rear_factor * 4.0
            # 最大 +8 にクリップ
            return max(-8.0, min(8.0, score))

        # ── スロー × 追い込み（不利）: 前方分岐より先に評価 ──
        if pace_strength <= 1.0 and pos >= 0.65 and last3f_fitness < 0:
            rear_penalty = (pos - 0.65) / 0.35  # 0.65〜1.0 → 0〜1
            score = -rear_penalty * 5.0 * (1.0 - pace_strength)
            return max(-8.0, min(8.0, score))

        # ── スロー × 前方待機（逃げ・先行有利）──
        if pace_strength <= 0.5:
            # 前方度合い（0〜0.4 を 1〜0 にスケール）
            front_factor = max(0.0, (0.4 - pos) / 0.4)
            # 末脚非依存型は前残りが得意 → ボーナス（前にいる場合のみ）
            style_bonus = 0.5 if last3f_fitness < 0 and pos <= 0.4 else 0.0
            score = (front_factor + style_bonus) * 5.0
            return max(-8.0, min(8.0, score))

        # ── その他（中間ペース・中位置取り）──
        # MM で後方追込型は軽微なボーナス
        if pace == PaceType.MM and pos >= 0.5 and last3f_fitness > 0.5:
            return min(3.0, last3f_fitness * 2.0)

        return 0.0

    def _calc_course_style_bias(self, course: CourseMaster, style: RunningStyle) -> float:
        """❹コース脚質バイアス (-5〜+5)"""
        score = 0.0
        # 直線長い = 差し有利
        if (
            course.straight_m >= 420 and style in (RunningStyle.SASHIKOMI, RunningStyle.OIKOMI)
        ) or (course.straight_m <= 300 and style in (RunningStyle.NIGASHI, RunningStyle.SENKOU)):
            score += 3.0

        # 急坂 = 前残り困難
        if course.slope_type == "急坂" and style in (RunningStyle.NIGASHI, RunningStyle.SENKOU):
            score -= 2.0

        # 小回り = 前有利
        if course.corner_type == "小回り" and style in (RunningStyle.NIGASHI, RunningStyle.SENKOU):
            score += 2.0

        return max(-5.0, min(5.0, score))


# ============================================================
# G-0: コース適性偏差値
# ============================================================


class CourseAptitudeCalculator:
    """
    コース適性偏差値の計算
    枠順・脚質バイアスはF-4に一本化（二重計上防止）
    """

    # 類似度のべき乗: 高類似場を重視するための指数
    _SIM_POWER = 2.0
    # 貢献に含める最低類似度 (40%未満はノイズ)
    _SIM_THRESHOLD = 0.40
    # 逆回り割引: 左回り実績→右回り評価（またはその逆）の場合の重み倍率
    _DIRECTION_DISCOUNT = 0.65

    def calc(
        self,
        horse: Horse,
        course: CourseMaster,
        jockey_stats: Optional[JockeyStats],
        all_courses: Dict[str, CourseMaster],
    ) -> CourseAptitude:
        """CourseAptitudeを算出"""
        # ❶コース実績
        course_record_score = self._calc_course_record(horse.past_runs, course.course_id)
        # コース実績のサンプル数（グレード信頼度加重用）
        same_runs = [r for r in horse.past_runs if r.course_id == course.course_id]
        course_record_n = len(same_runs)

        # ❷競馬場適性 (4因子類似度ベース)
        venue_score, contrib_level = self._calc_venue_aptitude(
            horse.past_runs, course,
        )
        # 競馬場適性のサンプル数: 同venue_codeの過去走数
        venue_code = course.venue_code
        venue_n = sum(1 for r in horse.past_runs
                      if r.course_id and r.course_id.split("_")[0] == venue_code)

        # ❸騎手コース影響 (H-2から)
        jockey_course_score = self._calc_jockey_course(jockey_stats, course.course_id, all_courses)

        return CourseAptitude(
            base_score=COURSE_BASE,
            course_record=course_record_score,
            course_record_n=course_record_n,
            venue_aptitude=venue_score,
            venue_aptitude_n=venue_n,
            venue_contrib_level=contrib_level,
            jockey_course=jockey_course_score,
        )

    def _calc_course_record(self, runs: List[PastRun], course_id: str) -> float:
        """❶コース実績 (-5〜+5): 完全一致 + 距離帯補間（案G-1/G-2）"""
        same_runs = [r for r in runs if r.course_id == course_id]

        # 案G-2: 完全一致がない場合、同競馬場×同馬場×近距離で補間
        if not same_runs:
            # course_id の形式: "venue_surface_distance" (例: "01_芝_1600")
            parts = course_id.split("_")
            if len(parts) >= 3:
                try:
                    target_venue   = parts[0]
                    target_surface = parts[1]
                    target_dist    = int(parts[2])
                    # 同場・同馬場・±200m以内の実績を距離差に応じた重みで集計
                    near_runs = []
                    near_weights = []
                    for r in runs:
                        r_parts = r.course_id.split("_") if r.course_id else []
                        if len(r_parts) < 3:
                            continue
                        if r_parts[0] != target_venue or r_parts[1] != target_surface:
                            continue
                        try:
                            r_dist = int(r_parts[2])
                            dist_diff = abs(r_dist - target_dist)
                            if dist_diff == 0:
                                continue  # 完全一致は上の same_runs で処理済み
                            if dist_diff <= 200:
                                # 距離差が小さいほど重み大: 100m差=0.5, 200m差=0.25
                                w = max(0.1, 0.75 - dist_diff / 400.0)
                                near_runs.append(r)
                                near_weights.append(w)
                        except ValueError:
                            continue

                    if near_runs:
                        # 重み付き複勝率
                        w_place3 = sum(w for r, w in zip(near_runs, near_weights)
                                       if r.finish_pos <= 3)
                        w_total  = sum(near_weights)
                        w_rate   = w_place3 / w_total if w_total > 0 else 0.0
                        n_near   = len(near_runs)
                        if w_rate >= 0.6:
                            raw = 3.0   # 近距離高実績（満点より抑える）
                        elif w_rate >= 0.4:
                            raw = 1.5
                        elif w_rate >= 0.2:
                            raw = 0.5
                        elif w_rate >= 0.05:
                            raw = -0.5
                        else:
                            raw = -2.0
                        # 近距離補間は信頼度を抑える（最大70%）
                        rel = min(0.7, 0.3 + n_near * 0.1)
                        return round(raw * rel, 2)
                except (ValueError, IndexError):
                    pass
            return 0.0

        # 完全一致がある場合（案G-1: サンプル数信頼度スケーリング）
        n = len(same_runs)
        place3 = sum(1 for r in same_runs if r.finish_pos <= 3)
        place3_rate = place3 / n

        if place3_rate >= 0.7:
            raw = 5.0
        elif place3_rate >= 0.5:
            raw = 3.0
        elif place3_rate >= 0.3:
            raw = 1.0
        elif place3_rate >= 0.1:
            raw = -1.0
        else:
            raw = -3.0

        # 案G-1: サンプル数による信頼度スケーリング
        # 1走: 40%, 2走: 55%, 3走: 70%, 4走: 85%, 5走以上: 100%
        reliability = min(1.0, 0.25 + n * 0.15)
        return round(raw * reliability, 2)

    def _calc_venue_aptitude(
        self,
        runs: List[PastRun],
        target: CourseMaster,
    ) -> Tuple[float, str]:
        """❷競馬場適性 (-5〜+5): 4因子類似度 × 回り方向による重み付け評価

        本質4因子 = 直線距離 / ゴール前の坂 / 初角距離 / 3-4角形状
        で競馬場の構造類似度を算出し、類似場での成績を重み付け合算。

        回り方向（左/右）は馬体の利き脚に関わる固有適性のため、
        逆回り場の実績は _DIRECTION_DISCOUNT (0.65) で割り引く。
        「両」（大井等）はどちらにもペナルティなし。

        重み = similarity^2 × direction_factor:
          同一場・同回り (sim=1.00, dir=1.0)  → weight 1.00
          類似場・同回り (sim=0.90, dir=1.0)  → weight 0.81
          類似場・逆回り (sim=0.90, dir=0.65) → weight 0.53

        Returns: (score, contrib_level)
        """
        from data.masters.venue_similarity import get_all_profiles, get_venue_similarity

        if not runs:
            return 0.0, ""

        target_venue = target.venue
        target_surface = target.surface
        target_direction = target.direction

        profiles = get_all_profiles()

        weighted_sum = 0.0
        total_weight = 0.0
        contributing_venues: set = set()

        for r in runs[:20]:
            if r.surface != target_surface:
                continue

            if r.course_id == target.course_id:
                continue

            run_venue = r.venue
            sim = 1.0 if run_venue == target_venue else get_venue_similarity(target_venue, run_venue)

            if sim < self._SIM_THRESHOLD:
                continue

            run_profile = profiles.get(run_venue)
            run_direction = run_profile.direction if run_profile else "両"
            if target_direction == run_direction or "両" in (target_direction, run_direction):
                dir_factor = 1.0
            else:
                # 案G-3: 脚質依存の逆回り割引
                # 逃げ・先行型は回り方向に敏感（0.55）
                # 差し・追込型は比較的方向に依存しない（0.75）
                # F4: relative_position の Null安全処理
                _valid_rp = [rx.relative_position for rx in runs[:5]
                             if rx.relative_position is not None]
                avg_pos = (sum(_valid_rp) / len(_valid_rp)
                           if _valid_rp else 0.5)
                if avg_pos <= 0.25:        # 逃げ・先行型
                    dir_factor = 0.55
                elif avg_pos <= 0.55:      # 中団型
                    dir_factor = self._DIRECTION_DISCOUNT  # 0.65（現行値）
                else:                      # 差し・追込型
                    dir_factor = 0.75

            weight = (sim ** self._SIM_POWER) * dir_factor

            if r.finish_pos <= 3:
                perf = 1.0
            elif r.finish_pos <= 5:
                perf = 0.0
            else:
                perf = -0.5

            weighted_sum += weight * perf
            total_weight += weight
            contributing_venues.add(run_venue)

        if total_weight == 0:
            return 0.0, ""

        raw = weighted_sum / total_weight

        n_venues = len(contributing_venues)
        if n_venues <= 1:
            level = "Solo"
        elif n_venues == 2:
            level = "Pair"
        elif n_venues == 3:
            level = "Trio"
        else:
            level = "Quartet+"

        # 案G-4: スケーリング改善 + Solo/Pair時の信頼度割引
        # raw の実用範囲は [-0.5, 1.0] → [-5, +5] にスケール
        # Solo（1場のみ）は信頼度を60%に抑える
        scale = 5.0 / 0.75  # ≈ 6.67（既存値を維持しつつ信頼度を調整）
        if n_venues <= 1:
            scale *= 0.60   # Solo: 信頼度60%
        elif n_venues == 2:
            scale *= 0.80   # Pair: 信頼度80%

        score = max(-5.0, min(5.0, raw * scale))

        return round(score, 2), level

    def _calc_jockey_course(
        self,
        jockey: Optional[JockeyStats],
        course_id: str,
        all_courses: Dict[str, CourseMaster],
    ) -> float:
        """❸騎手コース影響 (-3〜+3) H-2（案G-5: 類似コース補間の改善）"""
        if jockey is None:
            return 0.0

        # 優先度1: 該当コースの直接実績（サンプル10以上）
        rec = jockey.course_records.get(course_id)
        if rec and rec.get("sample_n", 0) >= 10:
            dev = rec.get("all_dev", 50.0)
            return max(-3.0, min(3.0, (dev - 50.0) / 5.0 * 1.5))

        # 優先度2: 類似コースプール（サンプル5以上）
        # 案G-5: 類似度×log(サンプル数+1)の加重平均（旧: 類似度のみ）
        target_course = all_courses.get(course_id)
        if target_course is None:
            return 0.0

        import math
        weighted_sum = 0.0
        total_weight = 0.0
        for cid, crec in jockey.course_records.items():
            c = all_courses.get(cid)
            n = crec.get("sample_n", 0)
            if c and n >= 5:
                sim = target_course.similarity_score(c)
                # 案G-5: 重み = 類似度 × log(サンプル数+1)（サンプル多いほど重視）
                weight = (sim / 7.5) * math.log(n + 1)
                dev_contrib = (crec.get("all_dev", 50.0) - 50.0) * weight
                weighted_sum += dev_contrib
                total_weight += weight

        if total_weight == 0:
            return 0.0

        avg = weighted_sum / total_weight
        return max(-3.0, min(3.0, avg / 5.0))


# ============================================================
# G-2: 枠順バイアス
# ============================================================


def calc_gate_bias(
    horse_no: int,
    field_count: int,
    course: CourseMaster,
    gate_bias_db: Optional[Dict] = None,
    gate_no: Optional[int] = None,
) -> float:
    """
    G-2: 枠順バイアス (-5〜+5)
    gate_no（枠番）があれば枠番別DBを使用、なければ馬番→ゾーンでルールベース。
    gate_bias_db: {"venue_surface": {gate_no: bias}} 枠番1-8でキー
    """
    if field_count <= 7:
        return 0.0

    if field_count >= 10:
        n_zones = 5
    elif field_count >= 8:
        n_zones = 3
    else:
        return 0.0

    # データ駆動: 枠番別DBがあれば gate_no で検索
    # DB 値が 0.0 の場合はサンプル不足とみなしてルールベースにフォールスルー
    if gate_bias_db:
        key = f"{course.venue_code}_{course.surface}"
        venue_biases = gate_bias_db.get(key, {})
        g = gate_no if gate_no and 1 <= gate_no <= 8 else None
        if g is not None and g in venue_biases and venue_biases[g] != 0.0:
            return venue_biases[g]

    # フォールバック: 馬番からゾーン算出
    zone = int((horse_no - 1) / (field_count / n_zones))
    zone = min(zone, n_zones - 1)

    # ルールベース（フォールバック）
    if course.surface == "芝" and n_zones == 5:
        bias_map = {0: 2.0, 1: 1.0, 2: 0.0, 3: -1.0, 4: -2.0}
    elif course.surface == "ダート" and n_zones == 5:
        bias_map = {0: -1.5, 1: -0.5, 2: 0.5, 3: 1.0, 4: 1.5}
    else:
        bias_map = {z: 0.0 for z in range(n_zones)}

    return bias_map.get(zone, 0.0)


# ============================================================
# G-3: コース適性（脚質バイアス）
# ============================================================


def calc_style_bias_for_course(
    horse: Horse,
    course: CourseMaster,
    course_stats_db: Dict,
) -> float:
    """
    G-3: 4位置グループ(先頭/前方/中団/後方)別複勝率
    course_stats_db: {course_id: {"front": 0.45, "mid": 0.30, ...}}
    """
    stats = course_stats_db.get(course.course_id, {})
    if not stats:
        return 0.0

    # 馬の基本脚質を推定
    runs = horse.past_runs[:5]
    if not runs:
        return 0.0

    # F4: relative_position の Null安全処理
    valid_rp = [r.relative_position for r in runs if r.relative_position is not None]
    if not valid_rp:
        return 0.0
    avg_pos = statistics.mean(valid_rp)
    if avg_pos <= 0.2:
        group = "front"
    elif avg_pos <= 0.45:
        group = "mid_front"
    elif avg_pos <= 0.7:
        group = "mid"
    else:
        group = "rear"

    # そのグループの複勝率と全体平均の差
    group_rate = stats.get(group, 0.33)
    avg_rate = stats.get("average", 0.33)

    diff = group_rate - avg_rate
    return max(-5.0, min(5.0, diff * 15.0))  # ±5ptにスケール
