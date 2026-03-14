"""
競馬解析マスターシステム v3.0 - 騎手・厩舎・AI検知層
H章: 騎手偏差値・コース適性・乗り替わり
J章: 厩舎力・調教評価・組み合わせ
I章: 穴馬検知・危険馬検知
"""

from typing import Dict, List, Optional, Tuple

from config.settings import (
    ANA_SCORE_A,
    ANA_SCORE_B,
    KIKEN_ML_ENDORSE_MID,
    KIKEN_ML_ENDORSE_R3,
    KIKEN_ML_GUARD_RANK,
    KIKEN_SCORE_A,
    KIKEN_SCORE_B,
    TRAINING_EMOJI,
    TRAINING_INTENSITY,
)
from src.models import (
    AnaType,
    BakenType,
    Horse,
    HorseEvaluation,
    JockeyStats,
    KikenType,
    KishuPattern,
    TrainerStats,
    TrainingRecord,
)

# ============================================================
# H-3: 騎手乗り替わり評価
# ============================================================


class JockeyChangeEvaluator:
    """
    乗り替わり理由6分類 + テン乗りペナルティ + 騎手×馬過去成績
    """

    TENPORI_PENALTY = -1.0  # テン乗りペナルティ

    # 乗り替わりパターンの騎手展開影響スコア調整
    PATTERN_ADJUSTMENT = {
        KishuPattern.A: +1.5,  # 戦略的強化
        KishuPattern.B: +0.5,  # 戦術的
        KishuPattern.C: 0.0,  # ローテ都合
        KishuPattern.D: -0.5,  # 調教目的
        KishuPattern.E: -2.0,  # 見切り
        KishuPattern.F: 0.0,  # 事情不明
    }

    def evaluate(
        self,
        horse: Horse,
        new_jockey: JockeyStats,
        race_grade: str,
        combo_db: Optional[Dict] = None,
        prev_jockey_stats: Optional["JockeyStats"] = None,
    ) -> Tuple[Optional[KishuPattern], float, float]:
        """
        Returns:
            (KishuPattern, テン乗りペナルティ込み騎手展開影響スコア, 勝負気配への加算点)
        """
        if not horse.is_jockey_change:
            # 継続騎乗: ペナルティなし、コンビ成績のみ反映
            combo_adj = 0.0
            if combo_db:
                from src.scraper.improvement_dbs import get_combo_adjustment

                combo_adj = get_combo_adjustment(combo_db, horse.horse_id, new_jockey.jockey_id)
            return None, combo_adj, 0.0

        # 計算層は事実→パターン分類は簡易ヒューリスティック
        pattern = self._infer_pattern(horse, new_jockey, race_grade, prev_jockey_stats)

        # テン乗りペナルティ
        past_combo = self._get_combo_record(horse, new_jockey.jockey_id)
        has_combo = len(past_combo) >= 1

        tenpori = 0.0 if has_combo else self.TENPORI_PENALTY

        # パターン補正
        pattern_adj = self.PATTERN_ADJUSTMENT.get(pattern, 0.0)

        # 騎手×馬コンビ成績補正 (DB駆動)
        combo_adj = 0.0
        if combo_db:
            from src.scraper.improvement_dbs import get_combo_adjustment

            combo_adj = get_combo_adjustment(combo_db, horse.horse_id, new_jockey.jockey_id)

        # 勝負気配スコアへの貢献 (J-2連動)
        shobu_contrib = 1.0 if pattern == KishuPattern.A else 0.0

        total_score = tenpori + pattern_adj + combo_adj
        return pattern, total_score, shobu_contrib

    def _infer_pattern(
        self,
        horse: Horse,
        new_jockey: JockeyStats,
        grade: str,
        prev_jockey: Optional["JockeyStats"] = None,
    ) -> KishuPattern:
        """AI層が理由を推定 (Phase 1.1: C/D/E判定追加)"""
        new_dev = new_jockey.upper_long_dev
        prev_dev = prev_jockey.upper_long_dev if prev_jockey else None

        # パターンA: 明確な強化乗り替わり (偏差値60以上 or 前走比+8以上)
        if new_dev >= 60:
            return KishuPattern.A
        if prev_dev is not None and new_dev - prev_dev >= 8:
            return KishuPattern.A

        # パターンE: 見切り (前走騎手偏差値が新騎手を大幅上回る)
        if prev_dev is not None and prev_dev - new_dev >= 8:
            return KishuPattern.E

        # パターンB: 減量騎手の戦術的起用 (一般戦・若い馬)
        if new_dev < 48 and ("新馬" in grade or "未勝利" in grade or "1勝" in grade):
            return KishuPattern.B

        # パターンD: 調教目的 (3歳以下新馬・未勝利 × 中偏差値騎手への乗り替わり)
        horse_age = getattr(horse, "age", None)
        if horse_age and horse_age <= 3 and ("新馬" in grade or "未勝利" in grade):
            if new_dev < 52:
                return KishuPattern.D

        # パターンC: ローテ都合 (偏差値変化が小さい: ±5以内)
        if prev_dev is not None and abs(new_dev - prev_dev) <= 5:
            return KishuPattern.C

        return KishuPattern.F

    def _get_combo_record(self, horse: Horse, jockey_id: str) -> List:
        """騎手×馬の過去成績"""
        return [r for r in horse.past_runs if getattr(r, "jockey_id", None) == jockey_id]


# ============================================================
# H-1: 騎手偏差値算出
# ============================================================


def calc_jockey_deviation(
    jockey: JockeyStats,
    horse_popularity: Optional[int],
) -> float:
    """
    H-1: 人気別偏差値を返す
    上位人気(1-3番人気)か否かで切り替え
    """
    is_upper = (horse_popularity or 99) <= 3
    return jockey.get_deviation(is_upper)


# ============================================================
# J-4: 調教強度判定
# ============================================================


class TrainingEvaluator:
    """
    Phase 1: 生データ + 厩舎別平時比較(±σで追切強度判定)
    """

    def evaluate(
        self,
        records: List[TrainingRecord],
        trainer_baseline: Dict,
        # trainer_baseline: {course: {"mean_3f": 35.5, "std_3f": 0.4}}
    ) -> List[TrainingRecord]:
        """
        各調教記録にintensity_labelとsigma_from_meanを付与して返す
        """
        enriched = []
        for rec in records:
            baseline = trainer_baseline.get(rec.course, {})
            if not baseline or "mean_3f" not in baseline:
                rec.sigma_from_mean = 0.0
                # intensity_label はスクレイパー由来のまま維持（競馬ブック等）
                enriched.append(rec)
                continue

            mean_3f = baseline["mean_3f"]
            std_3f = baseline.get("std_3f", 0.5)
            last_3f = self._get_last3f(rec.splits, mean_3f)
            sigma = (mean_3f - last_3f) / std_3f if std_3f > 0 else 0.0

            rec.sigma_from_mean = sigma
            rec.intensity_label = self._sigma_to_label(sigma)
            enriched.append(rec)

        return enriched

    def _get_last3f(self, splits: dict, default: float) -> float:
        """3F(600m)タイムを取得。キー形式: 3F, 600, "600m" 等に対応"""
        for key in ("3F", "3f", 600, "600", "600m"):
            if key in splits:
                try:
                    return float(splits[key])
                except (TypeError, ValueError):
                    pass
        return default

    def _sigma_to_label(self, sigma: float) -> str:
        for label, (lo, hi) in TRAINING_INTENSITY.items():
            if lo <= sigma < hi:
                return label
        return "通常"

    @staticmethod
    def format_intensity(rec: TrainingRecord) -> str:
        """出力フォーマット用文字列"""
        emoji = TRAINING_EMOJI.get(rec.intensity_label, "→")
        splits_str = " ".join(f"{k}-{v:.1f}" for k, v in sorted(rec.splits.items()))
        return f"{emoji}{rec.intensity_label} {rec.course} {splits_str}"


# ============================================================
# J-2: 勝負気配スコア算出
# ============================================================


def calc_shobu_score(
    horse: Horse,
    trainer: TrainerStats,
    jockey: JockeyStats,
    jockey_change_pattern: Optional[KishuPattern],
    is_long_break: bool,
    grade: str,
    last_grade: str,
    days_since_last_run: Optional[int] = None,
) -> float:
    """
    J-2: 勝負気配スコア
    (初コンビ/騎手強化/格上げ/厩舎好調/休み明け回収率高/休み明け精密判定)
    4以上→「🔺勝負気配」
    """
    score = 0.0

    # 騎手強化
    if jockey_change_pattern == KishuPattern.A:
        score += 2.0

    # 初コンビ (テン乗り)
    if horse.is_jockey_change:
        score += 0.5

    # 格上げ (クラス昇級の好走パターン)
    class_order = ["新馬", "未勝利", "1勝", "2勝", "3勝", "OP", "G3", "G2", "G1"]
    try:
        if class_order.index(grade) > class_order.index(last_grade):
            score += 1.5
    except ValueError:
        pass

    # 厩舎好調
    if trainer.short_momentum == "好調":
        score += 1.5

    # 休み明け回収率高 (従来)
    if is_long_break and trainer.recovery_break >= 120:
        score += 1.5

    # 休み明けの精密判定 (日数帯×厩舎休み明け実績)
    if days_since_last_run is not None:
        from src.scraper.improvement_dbs import calc_break_adjustment

        score += calc_break_adjustment(days_since_last_run, trainer.recovery_break, is_long_break)

    # 調教師偏差値補正 (高偏差値厩舎の信頼度加算)
    trainer_dev = getattr(trainer, "deviation", 50.0)
    if trainer_dev >= 62:
        score += 1.5
    elif trainer_dev >= 56:
        score += 0.8
    elif trainer_dev <= 40:
        score -= 0.5

    return score


# ============================================================
# I-1: 穴馬検知
# ============================================================


def calc_ana_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> Tuple[float, AnaType]:
    """
    I-1: 穴馬スコア算出
    三連率ギャップ(最大+6pt) + 13項目
    8pt以上→穴A / 5-7pt→穴B / それ以下→該当なし
    """
    horse = eval_result.horse
    # 実オッズがない場合は予測オッズを使い、人気順位も推定する
    eff_odds = eval_result.effective_odds
    if eff_odds is None or eff_odds < 10.0:
        return 0.0, AnaType.NONE
    real_pop = eval_result.horse.popularity
    if real_pop is not None:
        if real_pop < 5:
            return 0.0, AnaType.NONE
    else:
        # 予測オッズから推定人気順
        sorted_effs = sorted([e.effective_odds for e in all_evals if e.effective_odds])
        est_pop = sorted_effs.index(eff_odds) + 1 if eff_odds in sorted_effs else 99
        if est_pop < 5:
            return 0.0, AnaType.NONE

    score = 0.0

    # 三連率ギャップ (最大+6pt)
    score += _calc_probability_gap_score(eval_result, all_evals)

    # 13項目検知
    # 1. トレンド上昇
    from src.models import Trend

    if eval_result.ability.trend in (Trend.RAPID_UP, Trend.UP):
        score += 1.5

    # 2. 着差評価指数がプラス
    if eval_result.ability.chakusa_index_avg > 0.5:
        score += 1.0

    # 3. コース初出走でも類似コース高実績
    if eval_result.course.course_record == 0.0 and eval_result.course.shape_compatibility >= 2.0:
        score += 1.0

    # 4. 休み明け×厩舎初戦型
    if eval_result.trainer_stats and eval_result.trainer_stats.break_type == "初戦型":
        if eval_result.trainer_stats.recovery_break >= 100:
            score += 1.0

    # 5. 騎手強化
    if eval_result.jockey_change_pattern == KishuPattern.A:
        score += 1.0

    # 6. 勝負気配スコア高
    if eval_result.shobu_score >= 4:
        score += 1.5

    # 7. 展開が味方 (展開偏差値が全馬上位25%)
    sorted_pace = sorted(all_evals, key=lambda e: e.pace.total, reverse=True)
    rank_pace = next(
        (i + 1 for i, e in enumerate(sorted_pace) if e.horse.horse_id == horse.horse_id),
        len(all_evals),
    )
    if rank_pace <= max(1, len(all_evals) // 4):
        score += 1.5

    # 8. 一発型
    if eval_result.baken_type == BakenType.IPPATSU:
        score += 0.5

    # 9. ML複勝率がルール複勝率を大幅に上回る（過小評価検知）
    ml_p = getattr(eval_result, "ml_place_prob", None)
    if ml_p is not None:
        rule_p = eval_result.place3_prob
        ml_gap = ml_p - rule_p
        if ml_gap >= 0.15:
            score += 2.0
        elif ml_gap >= 0.08:
            score += 1.0

    # タイプ分類
    if score >= ANA_SCORE_A:
        # 穴A: 能力が高い×オッズが過小評価
        if eval_result.composite >= 55:
            return score, AnaType.ANA_A
        return score, AnaType.ANA_B
    if score >= ANA_SCORE_B:
        return score, AnaType.ANA_B

    return score, AnaType.NONE


def _calc_probability_gap_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> float:
    """三連率ギャップスコア (最大6pt) — 精密化版"""
    import math

    odds = eval_result.effective_odds
    if odds is None:
        return 0.0

    theoretical_place3 = eval_result.place3_prob

    # 市場評価: 単勝オッズ→複勝確率への経験的変換
    # 単勝1倍台は複勝80%超、10倍は複勝25%程度という経験則に基づくべき乗変換
    market_place3 = min(0.90, 1.0 / (odds ** 0.65) * 0.7)

    gap = theoretical_place3 - market_place3
    if gap >= 0.30:
        return 6.0
    if gap >= 0.20:
        return 4.0
    if gap >= 0.12:
        return 2.5
    if gap >= 0.06:
        return 1.0
    return 0.0


# ============================================================
# I-1b: 特選穴馬スコア (Cohen's d ベース重み付き)
# ============================================================

def calc_tokusen_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> float:
    """
    特選穴馬スコア算出（最大16pt、閾値7pt）

    Cohen's d に基づく重み:
      course_record (d=1.17): 最大4.0pt
      course_total  (d=0.85): 最大3.0pt
      composite     (d=0.68): 最大2.5pt
      place3_prob   (d=0.54): 最大2.0pt
      odds_consistency (d=0.50): 最大1.5pt
      ability_trend (lift 3.5x): 最大2.0pt
      pace_last3f_eval (d=0.46): 最大1.0pt
    """
    from config.settings import TOKUSEN_ODDS_THRESHOLD

    eff_odds = eval_result.effective_odds
    if eff_odds is None or eff_odds < TOKUSEN_ODDS_THRESHOLD:
        return 0.0

    score = 0.0
    factor_hits = 0  # 主要4因子のうちスコア獲得した数

    # --- 1. course_record (d=1.17) — コース実績偏差値 ---
    cr = eval_result.course.course_record
    if cr >= 58:
        score += 4.0
        factor_hits += 1
    elif cr >= 52:
        score += 2.5
        factor_hits += 1
    elif cr >= 45:
        score += 1.5
        factor_hits += 1

    # --- 2. course_total (d=0.85) — コース総合偏差値 ---
    ct = eval_result.course.total
    if ct >= 57:
        score += 3.0
        factor_hits += 1
    elif ct >= 52:
        score += 1.5
        factor_hits += 1

    # --- 3. composite (d=0.68) — 総合指数 ---
    comp = eval_result.composite
    if comp >= 55:
        score += 2.5
        factor_hits += 1
    elif comp >= 50:
        score += 1.0
        factor_hits += 1

    # --- 4. place3_prob (d=0.54) — 複勝率推定 ---
    field_count = len(all_evals) or 1
    base_p3 = 3.0 / field_count
    p3 = eval_result.place3_prob
    if p3 >= base_p3 * 1.8:
        score += 2.0
        factor_hits += 1
    elif p3 >= base_p3 * 1.3:
        score += 1.0
        factor_hits += 1

    # --- 5. odds_consistency_adj (d=0.50) — オッズ整合性 ---
    oc = eval_result.odds_consistency_adj
    if oc >= 2.0:
        score += 1.5
    elif oc >= 0.5:
        score += 0.5

    # --- 6. ability_trend (lift 3.5x) — 近走トレンド ---
    from src.models import Trend
    trend = eval_result.ability.trend
    if trend == Trend.RAPID_UP:
        score += 2.0
    elif trend == Trend.UP:
        score += 1.5

    # --- 7. pace_last3f_eval (d=0.46) — 上がり3F評価 ---
    last3f = getattr(eval_result.pace, "last3f_eval", 0.0)
    if last3f >= 55:
        score += 1.0

    # 追加条件: 主要4因子(course_record/course_total/composite/place3_prob)のうち
    # 2つ以上でスコア獲得していないと特選対象外
    if factor_hits < 2:
        return 0.0

    return score


# ============================================================
# I-2: 危険な人気馬の検知
# ============================================================


def calc_kiken_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> Tuple[float, KikenType]:
    """
    I-2: 危険スコア算出（ML考慮版）
    5pt以上→危険A / 3-4pt→危険B / それ以下→該当なし

    改善点:
    - win_prob rank による早期除外ガード（ML高評価馬の保護）
    - ML endorsement による負のスコア（ML中位評価馬の保護）
    - 展開(pace)とcompositeの二重カウント軽減
    - 旧Item8廃止（ml_place_probはブレンド後にクリアされ常にNoneだった）
    """
    horse = eval_result.horse
    # 実オッズがない場合は予測オッズを使い、人気順位も推定する
    eff_odds = eval_result.effective_odds
    if eff_odds is None or eff_odds >= 10.0:
        return 0.0, KikenType.NONE
    real_pop = eval_result.horse.popularity
    if real_pop is not None:
        if real_pop > 3:
            return 0.0, KikenType.NONE
    else:
        # 予測オッズから推定人気順
        sorted_effs = sorted([e.effective_odds for e in all_evals if e.effective_odds])
        est_pop = sorted_effs.index(eff_odds) + 1 if eff_odds in sorted_effs else 99
        if est_pop > 3:
            return 0.0, KikenType.NONE

    # ---- 事前計算: composite rank ----
    sorted_comp = sorted(all_evals, key=lambda e: e.composite, reverse=True)
    rank = next(
        (i + 1 for i, e in enumerate(sorted_comp) if e.horse.horse_id == horse.horse_id),
        len(all_evals),
    )

    # ---- 事前計算: win_prob rank ----
    sorted_wp = sorted(all_evals, key=lambda e: e.win_prob, reverse=True)
    rank_wp = next(
        (i + 1 for i, e in enumerate(sorted_wp) if e.horse.horse_id == horse.horse_id),
        len(all_evals),
    )

    # ---- 早期除外ガード ----
    # (A) composite上位3頭は除外（既存）
    if rank <= 3:
        return 0.0, KikenType.NONE
    # (B) win_prob上位N頭は除外（ML+ルールのブレンド確率が高い馬は危険馬と矛盾）
    if rank_wp <= KIKEN_ML_GUARD_RANK:
        return 0.0, KikenType.NONE

    score = 0.0
    n = len(all_evals)

    # ---- 0. ML endorsement（負のスコア）----
    # win_prob rank が中位以上ならMLがある程度の評価 → 減点で保護
    if rank_wp == 3:
        score += KIKEN_ML_ENDORSE_R3      # -1.5
    elif rank_wp <= max(2, n // 2):
        score += KIKEN_ML_ENDORSE_MID     # -0.5

    # ---- 1. 総合偏差値が下位 ----
    if rank >= 4:
        score += 2.0

    # ---- 2. トレンド下降 ----
    from src.models import Trend

    if eval_result.ability.trend in (Trend.RAPID_DOWN, Trend.DOWN):
        score += 1.5

    # ---- 3. 展開が向かない（二重カウント軽減版）----
    sorted_pace = sorted(all_evals, key=lambda e: e.pace.total, reverse=True)
    rank_pace = next(
        (i + 1 for i, e in enumerate(sorted_pace) if e.horse.horse_id == horse.horse_id),
        n,
    )
    if rank_pace >= n * 0.75:
        if rank >= n * 0.75:
            # compositeも下位 → paceの悪さは既にcompositeに含まれている
            score += 0.5
        else:
            # compositeは中位なのにpaceだけ悪い → 独立した展開リスク
            score += 1.5

    # ---- 4. 着差評価指数が低い ----
    if eval_result.ability.chakusa_index_avg < -0.5:
        score += 1.0

    # ---- 5. データ信頼度C ----
    from src.models import Reliability

    if eval_result.ability.reliability == Reliability.C:
        score += 1.0

    # ---- 6. コース初出走かつ類似実績もない ----
    if eval_result.course.course_record <= -2.0 and eval_result.course.shape_compatibility <= 0.0:
        score += 1.5

    # ---- 7. 騎手降板(見切りパターン) ----
    if eval_result.jockey_change_pattern == KishuPattern.E:
        score += 2.0

    # （旧Item8廃止: ml_place_probはブレンド後Noneクリアで常に無効だった。
    #   ML評価の役割はItem0の負スコアと早期除外ガードに統合済み）

    # タイプ分類
    if score >= KIKEN_SCORE_A:
        return score, KikenType.KIKEN_A
    if score >= KIKEN_SCORE_B:
        return score, KikenType.KIKEN_B

    return score, KikenType.NONE


# ============================================================
# I-1: 三連率推定
# ============================================================


def estimate_three_win_rates(
    composite: float,
    all_composites: List[float],
    pace_score: float = 50.0,
    course_score: float = 50.0,
    all_pace_scores: Optional[List[float]] = None,
    all_course_scores: Optional[List[float]] = None,
) -> Tuple[float, float, float]:
    """
    総合偏差値から勝率・連対率・複勝率を独立推定。

    【案A】 連対・複勝を勝率の単純倍数ではなく独立シグナルで推定。
      - 連対率: composite + pace補正（展開適性が連対に独立して寄与）
      - 複勝率: composite + pace補正 + course補正（適性の広さが複勝に寄与）

    【案C】 temperatureをレース拮抗度で動的調整。
      - 拮抗レース(composite std小) → 高温（差をつけすぎない）
      - 一強レース(composite std大) → 低温（有力馬に確率を集中）

    Returns: (win_prob, top2_prob, top3_prob)
    """
    import math
    import statistics as _st

    n = len(all_composites)
    if n == 0:
        return 0.0, 0.0, 0.0

    # ---- 温度キャリブレーション: 実績統計ベンチマーク適合 ----
    # 実績データ: ◎(一般1人気)=勝率33.4%, ◉(一強1人気)=勝率50.6%
    # 1位-2位ギャップから目標勝率を決定し、温度を逆算
    _sorted_comp = sorted(all_composites, reverse=True)
    gap_1_2 = (_sorted_comp[0] - _sorted_comp[1]) if n >= 2 else 0.0
    if gap_1_2 >= 5.0:
        _target_top = 0.55  # ◉レベルの一強（近似誤差補正込み）
    elif gap_1_2 >= 1.0:
        # 線形補間: gap=1→0.20, gap=5→0.55
        _target_top = 0.20 + (gap_1_2 - 1.0) * 0.0875
    else:
        _target_top = max(1.5 / n, 0.12)  # 拮抗レース

    # 1位と中央値のギャップからtemperatureを逆算
    _median_comp = _sorted_comp[n // 2] if n >= 2 else _sorted_comp[0]
    _gap_to_med = _sorted_comp[0] - _median_comp
    _target_ratio = _target_top * n / max(0.01, 1.0 - _target_top)
    if _target_ratio > 1.0 and _gap_to_med > 0.5:
        temp_win = _gap_to_med / math.log(_target_ratio)
    else:
        temp_win = 8.0  # 拮抗レースフォールバック

    # 安全クランプ（極端な温度を防止）
    temp_win = max(3.0, min(10.0, temp_win))

    # ---- 勝率: composite softmax ----
    exp_win = [math.exp((c - 50) / temp_win) for c in all_composites]
    total_win = sum(exp_win)
    if total_win == 0:
        return 1 / n, min(1.0, 2 / n), min(1.0, 3 / n)

    own_idx = all_composites.index(composite) if composite in all_composites else 0
    win_prob = exp_win[own_idx] / total_win

    # ---- 案A: 連対率 — composite + pace補正 ----
    # pace_score が高い馬は展開上有利 → composite以上に連対しやすい
    if all_pace_scores and len(all_pace_scores) == n:
        try:
            mean_pace = _st.mean(all_pace_scores)
            top2_eff = [c + (p - mean_pace) * 0.3
                        for c, p in zip(all_composites, all_pace_scores)]
        except Exception:
            top2_eff = list(all_composites)
    else:
        top2_eff = list(all_composites)

    temp_top2 = temp_win + 1.4  # 連対（実績: ◎52.2%, ◉71.3%）
    exp_top2 = [math.exp((c - 50) / temp_top2) for c in top2_eff]
    total_top2 = sum(exp_top2)
    place2_prob = (exp_top2[own_idx] / total_top2 * 2.0) if total_top2 > 0 else 2 / n

    # ---- 案A: 複勝率 — composite + pace補正 + course補正 ----
    # course_score が高い馬はコース適性が高く、複勝圏に粘りやすい
    if all_pace_scores and all_course_scores and len(all_pace_scores) == n and len(all_course_scores) == n:
        try:
            mean_pace   = _st.mean(all_pace_scores)
            mean_course = _st.mean(all_course_scores)
            top3_eff = [
                c + (p - mean_pace) * 0.2 + (co - mean_course) * 0.2
                for c, p, co in zip(all_composites, all_pace_scores, all_course_scores)
            ]
        except Exception:
            top3_eff = list(all_composites)
    else:
        top3_eff = list(all_composites)

    temp_top3 = temp_win + 2.6  # 複勝（実績: ◎64.6%, ◉81.6%）
    exp_top3 = [math.exp((c - 50) / temp_top3) for c in top3_eff]
    total_top3 = sum(exp_top3)
    place3_prob = (exp_top3[own_idx] / total_top3 * 3.0) if total_top3 > 0 else 3 / n

    place2_prob = min(0.80, place2_prob)
    place3_prob = min(0.85, place3_prob)

    return win_prob, place2_prob, place3_prob
