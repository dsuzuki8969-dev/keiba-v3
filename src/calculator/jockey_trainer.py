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
            # 競馬ブックの生テキストがあればそちらを優先（"通常"以外）
            if not rec.intensity_label or rec.intensity_label == "通常":
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

    # 9. 予測複勝率が市場複勝率を大幅に上回る（過小評価検知）
    # 旧ロジック: ml_place_prob を使用していたが、ブレンド後に None クリアされるため常に無効だった
    # 新ロジック: place3_prob（ML+ルールブレンド済み）と市場確率（オッズベース）の差分で判定
    blended_p3 = eval_result.place3_prob
    if blended_p3 is not None and eff_odds is not None:
        # 市場確率: オッズから経験的に複勝確率を推定
        market_p3 = min(0.90, 1.0 / (eff_odds ** 0.65) * 0.7)
        ml_gap = blended_p3 - market_p3
        if ml_gap >= 0.15:
            score += 2.5
        elif ml_gap >= 0.08:
            score += 1.5

    # タイプ分類
    if score >= ANA_SCORE_A:
        # 穴A: 能力が高い×オッズが過小評価
        if eval_result.composite >= 50:
            return score, AnaType.ANA_A
        return score, AnaType.ANA_B
    if score >= ANA_SCORE_B:
        return score, AnaType.ANA_B

    return score, AnaType.NONE


def _calc_probability_gap_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> float:
    """三連率ギャップスコア (最大6pt) — H13: 実績データ参照版"""
    odds = eval_result.effective_odds
    if odds is None:
        return 0.0

    theoretical_place3 = eval_result.place3_prob

    # H13: 市場確率の推定を改善 — 実績統計があればそちらを優先
    market_place3 = _estimate_market_place3(odds)

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


def _estimate_market_place3(odds: float) -> float:
    """オッズから市場複勝確率を推定（H13: 実績統計優先）"""
    try:
        from src.calculator.popularity_blend import load_popularity_stats, _odds_range_key
        stats = load_popularity_stats()
        if stats:
            range_key = _odds_range_key(odds)
            # JRA/NAR両方を参照し、データがある方を使用
            for org in ("JRA", "NAR"):
                data = stats.get("by_odds_range", {}).get(org, {}).get("_overall", {}).get(range_key, {})
                if data and "top3" in data:
                    return data["top3"]
    except Exception:
        pass
    # フォールバック: 旧式の経験則的変換
    return min(0.90, 1.0 / (odds ** 0.65) * 0.7)


# ============================================================
# I-1b: 特選穴馬スコア (Cohen's d ベース重み付き)
# ============================================================

def calc_tokusen_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
) -> float:
    """
    特選穴馬スコア算出（最大10pt、閾値3pt）

    top5外の穴馬（☆候補）向けスコアリング。
    ML win_prob を主軸に据え、compositeは除外（top5除外と重複するため）。

    因子:
      win_prob     (主軸): 最大3.5pt
      course_record: 最大2.0pt
      course_total:  最大1.5pt
      place3_prob:   最大1.5pt
      ability_trend: 最大1.0pt
    """
    from config.settings import TOKUSEN_ODDS_THRESHOLD

    eff_odds = eval_result.effective_odds
    if eff_odds is None or eff_odds < TOKUSEN_ODDS_THRESHOLD:
        return 0.0

    # ML win_prob が穴馬として有望か
    wp = eval_result.win_prob
    if wp < 0.04:
        return 0.0

    score = 0.0

    # --- 1. win_prob (主軸) — ML予測勝率 ---
    if wp >= 0.08:
        score += 3.5
    elif wp >= 0.06:
        score += 2.5
    elif wp >= 0.04:
        score += 1.5

    # --- 2. course_record — コース実績偏差値 ---
    cr = eval_result.course.course_record
    if cr >= 52:
        score += 2.0
    elif cr >= 45:
        score += 1.0

    # --- 3. course_total — コース総合偏差値 ---
    ct = eval_result.course.total
    if ct >= 52:
        score += 1.5

    # --- 4. place3_prob — 複勝率推定 ---
    field_count = len(all_evals) or 1
    base_p3 = 3.0 / field_count
    p3 = eval_result.place3_prob
    if p3 >= base_p3 * 1.5:
        score += 1.5
    elif p3 >= base_p3 * 1.2:
        score += 0.5

    # --- 5. ability_trend — 近走トレンド ---
    from src.models import Trend
    trend = eval_result.ability.trend
    if trend in (Trend.RAPID_UP, Trend.UP):
        score += 1.0

    return score


# ============================================================
# I-1b: 特選危険馬の検知
# ============================================================


def calc_tokusen_kiken_score(
    eval_result: "HorseEvaluation",
    all_evals: List["HorseEvaluation"],
    is_jra: bool = True,
) -> float:
    """
    特選危険馬スコア算出（ML×composite二重否定方式・JRA/NAR分離）

    コンセプト: 「人気なのにMLもルールベースも低評価」の馬を特定。
    2つの独立した評価軸が一致して「来ない」と言っている馬だけを捕捉。

    必須条件（JRA/NAR別閾値）:
      ① 人気 ≤ pop_limit かつ odds < odds_limit（人気馬である）
      ② win_prob_rank ≥ 頭数×ml_pct（MLが低評価）
      ③ composite_rank ≥ 頭数×comp_pct（ルールベースも低評価）

    JRA: ②③はOR条件（どちらか一方で通過。大頭数ANDは構造的矛盾）
    NAR: ②③はAND条件（小頭数で両方一致が妥当。現行維持）

    追加スコア（必須条件通過後）:
      前走大敗（8着以下）      : +2pt
      連続凡走（直近3走中2走+） : +2pt
      同馬場複勝率20%未満       : +2pt
      騎手グレードD以下         : +1pt
      過去勝率5%未満            : +1pt
      長期休み明け（120日+）    : +1pt

    閾値: 合計 ≥ 3.0pt で × 確定
    目標: × 印の勝率 < 5.0%、複勝率 < 15.0%
    """
    from config.settings import (
        TOKUSEN_KIKEN_POP_LIMIT_JRA, TOKUSEN_KIKEN_POP_LIMIT_NAR,
        TOKUSEN_KIKEN_ODDS_LIMIT_JRA, TOKUSEN_KIKEN_ODDS_LIMIT_NAR,
        TOKUSEN_KIKEN_ML_RANK_PCT_JRA, TOKUSEN_KIKEN_ML_RANK_PCT_NAR,
        TOKUSEN_KIKEN_COMP_RANK_PCT_JRA, TOKUSEN_KIKEN_COMP_RANK_PCT_NAR,
    )

    pop_limit = TOKUSEN_KIKEN_POP_LIMIT_JRA if is_jra else TOKUSEN_KIKEN_POP_LIMIT_NAR
    odds_limit = TOKUSEN_KIKEN_ODDS_LIMIT_JRA if is_jra else TOKUSEN_KIKEN_ODDS_LIMIT_NAR
    ml_pct = TOKUSEN_KIKEN_ML_RANK_PCT_JRA if is_jra else TOKUSEN_KIKEN_ML_RANK_PCT_NAR
    comp_pct = TOKUSEN_KIKEN_COMP_RANK_PCT_JRA if is_jra else TOKUSEN_KIKEN_COMP_RANK_PCT_NAR

    horse = eval_result.horse
    n = len(all_evals)
    if n < 4:
        return 0.0

    # ---- 必須条件①: 人気馬である（pop_limit以内 & odds_limit未満）----
    eff_odds = eval_result.effective_odds
    if eff_odds is None or eff_odds >= odds_limit:
        return 0.0

    real_pop = horse.popularity
    if real_pop is not None:
        if real_pop > pop_limit:
            return 0.0
    else:
        # 予測オッズから推定人気順
        sorted_effs = sorted(
            [e.effective_odds for e in all_evals if e.effective_odds]
        )
        est_pop = (
            sorted_effs.index(eff_odds) + 1 if eff_odds in sorted_effs else 99
        )
        if est_pop > pop_limit:
            return 0.0

    # ---- 必須条件②③: ML低評価 / composite低評価 ----
    sorted_wp = sorted(all_evals, key=lambda e: e.win_prob, reverse=True)
    rank_wp = next(
        (
            i + 1
            for i, e in enumerate(sorted_wp)
            if e.horse.horse_id == horse.horse_id
        ),
        n,
    )
    wp_threshold = max(3, int(n * ml_pct))  # 最低3位以下

    sorted_comp = sorted(all_evals, key=lambda e: e.composite, reverse=True)
    rank_comp = next(
        (
            i + 1
            for i, e in enumerate(sorted_comp)
            if e.horse.horse_id == horse.horse_id
        ),
        n,
    )
    comp_threshold = max(3, int(n * comp_pct))  # 最低3位以下

    ml_low = rank_wp >= wp_threshold
    comp_low = rank_comp >= comp_threshold

    if is_jra:
        # JRA: ML低評価 OR composite低評価（どちらか一方で通過）
        # 大頭数ではAND条件が構造的に厳しすぎる（0.4%通過→機能不全）
        if not (ml_low or comp_low):
            return 0.0
    else:
        # NAR: 現行維持（AND — 小頭数で両方一致が妥当）
        if not (ml_low and comp_low):
            return 0.0

    # ---- 必須条件を全て通過 → 追加スコアリング ----
    score = 0.0

    # --- 1. 前走大敗（8着以下）: +2pt ---
    if hasattr(horse, "past_runs") and horse.past_runs:
        prev_fp = getattr(horse.past_runs[0], "finish_pos", None)
        if prev_fp is not None and prev_fp >= 8:
            score += 2.0

    # --- 2. 連続凡走（直近3走中2走以上が着外=4着以下）: +2pt ---
    if hasattr(horse, "past_runs") and horse.past_runs:
        recent = horse.past_runs[:3]
        poor_count = sum(
            1 for r in recent
            if getattr(r, "finish_pos", None) and r.finish_pos >= 4
        )
        if len(recent) >= 2 and poor_count >= 2:
            score += 2.0

    # --- 3. 同馬場複勝率が低い（20%未満）: +2pt ---
    same_surf_rate = getattr(eval_result, "_same_surf_place_rate", None)
    if same_surf_rate is None:
        # 過去走からの計算フォールバック
        if hasattr(horse, "past_runs") and horse.past_runs:
            cur_surface = None
            for e in all_evals:
                if hasattr(e, "_race_surface"):
                    cur_surface = e._race_surface
                    break
            if cur_surface:
                same_runs = [
                    r
                    for r in horse.past_runs
                    if getattr(r, "surface", None) == cur_surface
                    and getattr(r, "finish_pos", None)
                ]
                if same_runs:
                    same_surf_rate = sum(
                        1 for r in same_runs if r.finish_pos <= 3
                    ) / len(same_runs)
    if same_surf_rate is not None and same_surf_rate < 0.20:
        score += 2.0

    # --- 4. 騎手グレードD以下: +1pt ---
    jockey_grade = getattr(eval_result, "jockey_grade", None)
    if jockey_grade in ("D", "E"):
        score += 1.0

    # --- 5. 過去勝率5%未満: +1pt ---
    if hasattr(horse, "past_runs") and horse.past_runs:
        wins = sum(
            1 for r in horse.past_runs
            if getattr(r, "finish_pos", None) == 1
        )
        total = len(horse.past_runs)
        if total >= 3 and (wins / total) < 0.05:
            score += 1.0

    # --- 6. 長期休み明け（120日以上）: +1pt ---
    days_off = getattr(eval_result.ability, "days_since_last", None)
    if days_off is None and hasattr(horse, "past_runs") and horse.past_runs:
        last_date = getattr(horse.past_runs[0], "date", None)
        if last_date and hasattr(horse, "_race_date"):
            try:
                from datetime import datetime
                d1 = datetime.strptime(str(horse._race_date), "%Y-%m-%d")
                d0 = datetime.strptime(str(last_date), "%Y-%m-%d")
                days_off = (d1 - d0).days
            except Exception:
                pass
    if days_off is not None and days_off >= 120:
        score += 1.0

    return score


# ============================================================
# I-2: 危険な人気馬の検知（旧方式 — 特選危険馬に段階的移行中）
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

# テーブルキャッシュ（モジュールレベル）
_RANK_TABLE: Optional[dict] = None
_RANK_TABLE_LOADED = False


def _load_rank_table() -> Optional[dict]:
    """順位ベース確率テーブルをロード（初回のみ）"""
    global _RANK_TABLE, _RANK_TABLE_LOADED
    if _RANK_TABLE_LOADED:
        return _RANK_TABLE
    import json
    import os
    from config.settings import RANK_PROBABILITY_TABLE_PATH
    if os.path.exists(RANK_PROBABILITY_TABLE_PATH):
        try:
            with open(RANK_PROBABILITY_TABLE_PATH, "r", encoding="utf-8") as f:
                _RANK_TABLE = json.load(f)
        except Exception:
            _RANK_TABLE = None
    _RANK_TABLE_LOADED = True
    return _RANK_TABLE


def _field_group_key(n: int) -> str:
    """頭数→グループキー"""
    if n <= 8:
        return "small"
    if n <= 14:
        return "medium"
    return "large"


def estimate_three_win_rates(
    composite: float,
    all_composites: List[float],
    pace_score: float = 50.0,
    course_score: float = 50.0,
    all_pace_scores: Optional[List[float]] = None,
    all_course_scores: Optional[List[float]] = None,
    field_count: int = 0,
    is_jra: bool = True,
) -> Tuple[float, float, float]:
    """
    総合偏差値から勝率・連対率・複勝率を推定。

    Phase 11: テーブルベース + gap補正方式。
    テーブル未存在時はsoftmaxフォールバック。

    Returns: (win_prob, top2_prob, top3_prob)
    """
    from config.settings import USE_RANK_TABLE

    n = len(all_composites)
    if n == 0:
        return 0.0, 0.0, 0.0

    # テーブルベース方式を試行
    if USE_RANK_TABLE:
        table = _load_rank_table()
        if table is not None:
            result = _estimate_from_rank_table(
                composite, all_composites, table,
                pace_score, course_score,
                all_pace_scores, all_course_scores,
                field_count or n, is_jra,
            )
            if result is not None:
                return result

    # フォールバック: 旧softmax方式
    return _estimate_softmax(
        composite, all_composites,
        pace_score, course_score,
        all_pace_scores, all_course_scores,
    )


def _estimate_from_rank_table(
    composite: float,
    all_composites: List[float],
    table: dict,
    pace_score: float,
    course_score: float,
    all_pace_scores: Optional[List[float]],
    all_course_scores: Optional[List[float]],
    field_count: int,
    is_jra: bool,
) -> Optional[Tuple[float, float, float]]:
    """テーブルベースの三連率推定"""
    import statistics as _st
    from config.settings import (
        RANK_GAP_THRESHOLD_STRONG,
        RANK_GAP_MULT_MAX,
        RANK_GAP_FLAT_FACTOR_MAX,
    )

    n = len(all_composites)
    org = "JRA" if is_jra else "NAR"

    # composite順位を算出
    sorted_comp = sorted(all_composites, reverse=True)
    # 同値の場合はリスト内のインデックスで順位を決定
    rank = sorted_comp.index(composite) + 1

    fc_str = str(field_count)
    fg = _field_group_key(field_count)

    # テーブル参照: by_field_count → by_field_group の2段フォールバック
    fc_data = table.get("by_field_count", {}).get(org, {})
    fg_data = table.get("by_field_group", {}).get(org, {})

    entry = None
    rank_str = str(rank)

    if fc_str in fc_data and rank_str in fc_data[fc_str]:
        entry = fc_data[fc_str][rank_str]
    elif fg in fg_data and rank_str in fg_data[fg]:
        entry = fg_data[fg][rank_str]

    if entry is None:
        return None

    base_win = entry["win"]
    base_top2 = entry["top2"]
    base_top3 = entry["top3"]

    # ---- gap補正（一強・混戦の検出）----
    gap_1_2 = (sorted_comp[0] - sorted_comp[1]) if n >= 2 else 0.0

    if gap_1_2 >= RANK_GAP_THRESHOLD_STRONG:
        # 一強レース: 1位を引き上げ、他を圧縮
        if rank == 1:
            gap_mult = 1.0 + min(RANK_GAP_MULT_MAX, (gap_1_2 - 2.5) * 0.12)
        else:
            gap_mult = 1.0 - min(0.15, (gap_1_2 - 2.5) * 0.03)
        base_win *= gap_mult
        # top2/top3は控えめに補正（勝率ほど極端にしない）
        top2_gap_mult = 1.0 + (gap_mult - 1.0) * 0.5
        top3_gap_mult = 1.0 + (gap_mult - 1.0) * 0.3
        base_top2 *= top2_gap_mult
        base_top3 *= top3_gap_mult
    elif gap_1_2 < 1.0:
        # 混戦レース: 均等化方向へ
        flat_factor = max(0, 1.0 - gap_1_2) * RANK_GAP_FLAT_FACTOR_MAX
        base_win = base_win * (1 - flat_factor) + (1.0 / n) * flat_factor
        base_top2 = base_top2 * (1 - flat_factor) + (2.0 / n) * flat_factor
        base_top3 = base_top3 * (1 - flat_factor) + (3.0 / n) * flat_factor

    # ---- pace/course補正 ----
    if all_pace_scores and len(all_pace_scores) == n:
        try:
            mean_pace = _st.mean(all_pace_scores)
            std_pace = _st.stdev(all_pace_scores) if n >= 3 else 1.0
            if std_pace > 0.5:
                pace_z = (pace_score - mean_pace) / std_pace
                # pace偏差が高い馬は連対率にプラス
                base_top2 += pace_z * 0.02
        except Exception:
            pass

    if all_course_scores and len(all_course_scores) == n:
        try:
            mean_course = _st.mean(all_course_scores)
            std_course = _st.stdev(all_course_scores) if n >= 3 else 1.0
            if std_course > 0.5:
                course_z = (course_score - mean_course) / std_course
                # course偏差が高い馬は複勝率にプラス
                base_top3 += course_z * 0.015
        except Exception:
            pass

    # ---- 整合性制約 ----
    win_prob = max(0.01, min(0.85, base_win))
    top2_prob = max(0.02, min(0.92, base_top2))
    top3_prob = max(0.03, min(0.95, base_top3))

    # 個馬制約: win <= top2 <= top3
    top2_prob = max(top2_prob, win_prob)
    top3_prob = max(top3_prob, top2_prob)

    return win_prob, top2_prob, top3_prob


def _estimate_softmax(
    composite: float,
    all_composites: List[float],
    pace_score: float = 50.0,
    course_score: float = 50.0,
    all_pace_scores: Optional[List[float]] = None,
    all_course_scores: Optional[List[float]] = None,
) -> Tuple[float, float, float]:
    """旧softmax方式（フォールバック用）"""
    import math
    import statistics as _st

    n = len(all_composites)
    if n == 0:
        return 0.0, 0.0, 0.0

    _sorted_comp = sorted(all_composites, reverse=True)
    gap_1_2 = (_sorted_comp[0] - _sorted_comp[1]) if n >= 2 else 0.0
    if gap_1_2 >= 5.0:
        _target_top = 0.55
    elif gap_1_2 >= 1.0:
        _target_top = 0.20 + (gap_1_2 - 1.0) * 0.0875
    else:
        _target_top = max(1.5 / n, 0.12)

    _median_comp = _sorted_comp[n // 2] if n >= 2 else _sorted_comp[0]
    _gap_to_med = _sorted_comp[0] - _median_comp
    _target_ratio = _target_top * n / max(0.01, 1.0 - _target_top)
    if _target_ratio > 1.0 and _gap_to_med > 0.5:
        temp_win = _gap_to_med / math.log(_target_ratio)
    else:
        temp_win = 8.0

    temp_win = max(3.0, min(10.0, temp_win))

    exp_win = [math.exp((c - 50) / temp_win) for c in all_composites]
    total_win = sum(exp_win)
    if total_win == 0:
        return 1 / n, min(1.0, 2 / n), min(1.0, 3 / n)

    own_idx = all_composites.index(composite) if composite in all_composites else 0
    win_prob = exp_win[own_idx] / total_win

    if all_pace_scores and len(all_pace_scores) == n:
        try:
            mean_pace = _st.mean(all_pace_scores)
            top2_eff = [c + (p - mean_pace) * 0.3
                        for c, p in zip(all_composites, all_pace_scores)]
        except Exception:
            top2_eff = list(all_composites)
    else:
        top2_eff = list(all_composites)

    temp_top2 = temp_win + 1.4
    exp_top2 = [math.exp((c - 50) / temp_top2) for c in top2_eff]
    total_top2 = sum(exp_top2)
    place2_prob = (exp_top2[own_idx] / total_top2 * 2.0) if total_top2 > 0 else 2 / n

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

    temp_top3 = temp_win + 2.6
    exp_top3 = [math.exp((c - 50) / temp_top3) for c in top3_eff]
    total_top3 = sum(exp_top3)
    place3_prob = (exp_top3[own_idx] / total_top3 * 3.0) if total_top3 > 0 else 3 / n

    place2_prob = min(0.80, place2_prob)
    place3_prob = min(0.85, place3_prob)

    return win_prob, place2_prob, place3_prob
