"""
競馬解析マスターシステム v3.0 - メインオーケストレーター

計算層と分析層を統合して RaceAnalysis を生成する。
入力: RaceInfo + 全馬 Horse リスト + 各種マスタDB
出力: RaceAnalysis (HTML出力まで)
"""

import os
from typing import Any, Dict, List, Optional, Tuple

from src.log import get_logger

logger = get_logger(__name__)

# MLモデルはプロセス起動後の初回のみロード（毎レース再ロードを防ぐキャッシュ）
_CACHE_ML_PREDICTOR:       object = None   # Last3FPredictor
_CACHE_POSITION_PREDICTOR: object = None   # PositionPredictor
_CACHE_PROB_PREDICTOR:     object = None   # ProbabilityPredictor
_CACHE_PACE_ML_PREDICTOR:  object = None   # PacePredictorML (前半3F予測)
_CACHE_TORCH_PREDICTOR:    object = None   # TorchPredictor (PyTorch三連率)
_CACHE_LGBM_PREDICTOR:     object = None   # LGBMPredictor (Step3 SHAP用)
_CACHE_LGBM_RANKER:        object = None   # LGBMRanker (LambdaRank 補完)
_CACHE_ML_LOADED:       bool = False
_CACHE_POS_LOADED:      bool = False
_CACHE_PROB_LOADED:     bool = False
_CACHE_PACE_ML_LOADED:  bool = False
_CACHE_TORCH_LOADED:    bool = False
_CACHE_LGBM_LOADED:     bool = False
_CACHE_RANKER_LOADED:   bool = False

from config.settings import STAKE_DEFAULT
from src.calculator.ability import (
    StandardTimeCalculator,
    TrackCorrector,
    calc_ability_deviation,
    detect_long_break,
)
from src.calculator.betting import (
    allocate_stakes,
    calc_predicted_odds,
    generate_formation_tickets,
    generate_tickets,
    _calc_confidence_score,
    judge_confidence,
    should_buy_race,
)
from src.calculator.predicted_odds import (
    assign_divergence_to_evaluations,
    calc_predicted_sanrenpuku,
    calc_predicted_umaren,
    detect_value_bets,
)
from src.calculator.calibration import (
    diagnose_deviations,
    filter_post_renovation_runs,
    generate_pace_comment,
    get_base_weight,
)
from src.calculator.jockey_trainer import (
    JockeyChangeEvaluator,
    TrainingEvaluator,
    calc_ana_score,
    calc_kiken_score,
    calc_shobu_score,
    calc_tokusen_score,
    estimate_three_win_rates,
)
from src.calculator.pace_analysis import (
    calc_lineup,
    estimate_pace_times_from_runs,
)
from src.calculator.pace_course import (
    CourseAptitudeCalculator,
    Last3FEvaluator,
    PaceDeviationCalculator,
    PacePredictor,
    StyleClassifier,
    calc_gate_bias,
    calc_style_bias_for_course,
)
from src.models import (
    ConfidenceLevel,
    CourseMaster,
    Horse,
    HorseEvaluation,
    JockeyStats,
    PaceType,
    PastRun,
    RaceAnalysis,
    RaceInfo,
    RunningStyle,
    TrainerStats,
)
from src.calculator.popularity_blend import blend_probabilities, load_popularity_stats
from src.output.formatter import HTMLFormatter, assign_marks
from src.scraper.improvement_dbs import (
    build_bloodline_db,
    build_jockey_horse_combo_db,
    build_pace_stats_db,
    calc_odds_consistency_score,
    get_days_since_last_run,
)


# ── target_date 単位のDBキャッシュ（バッチ高速化） ──
_DB_CACHE_DATE = None
_DB_CACHE_PACE = None
_DB_CACHE_L3F_SIGMA = None
_DB_CACHE_GATE_BIAS = None


class RaceAnalysisEngine:
    """
    全計算層を統合するオーケストレーター
    """

    def __init__(
        self,
        course_db: Dict[str, List[PastRun]],
        all_courses: Dict[str, CourseMaster],
        jockey_db: Dict[str, JockeyStats],
        trainer_db: Dict[str, TrainerStats],
        trainer_baseline_db: Dict[str, Dict],
        pace_last3f_db: Dict[str, Dict[str, list]],
        course_style_stats_db: Dict[str, Dict[str, float]],
        gate_bias_db: Optional[Dict[str, Dict]] = None,
        position_sec_per_rank_db: Optional[Dict[str, float]] = None,
        is_jra: bool = True,
        target_date: Optional[str] = None,
    ) -> None:
        self.std_calc = StandardTimeCalculator(course_db)
        self.track_corr = TrackCorrector()
        self.pace_predictor = PacePredictor()
        self.style_classifier = StyleClassifier()

        # コース別ペース傾向DB（案A）- DBから自動ロード（日付単位キャッシュ）
        global _DB_CACHE_DATE, _DB_CACHE_PACE, _DB_CACHE_L3F_SIGMA, _DB_CACHE_GATE_BIAS
        try:
            if _DB_CACHE_DATE == target_date and _DB_CACHE_PACE is not None:
                self.course_pace_tendency = _DB_CACHE_PACE
            else:
                from src.database import get_course_pace_tendency
                self.course_pace_tendency = get_course_pace_tendency(target_date=target_date)
                _DB_CACHE_PACE = self.course_pace_tendency
                _DB_CACHE_DATE = target_date
                logger.info("コース別ペース傾向DB: %d件ロード (target_date=%s)", len(self.course_pace_tendency), target_date)
        except Exception as e:
            logger.warning("コース別ペース傾向DBロード失敗: %s", e)
            self.course_pace_tendency = {}

        # ML上がり3F予測モデル: あれば自動ロード、なければルールベース
        ml_predictor = self._load_ml_predictor()
        self.last3f_evaluator = Last3FEvaluator(pace_last3f_db, ml_predictor=ml_predictor)

        # ML三連率予測モデル (win/top2/top3)
        self._prob_predictor = self._load_prob_predictor()

        # ML前半3F予測モデル (PacePredictorML)
        self._pace_ml_predictor = self._load_pace_ml_predictor()

        # PyTorch 三連率予測モデル (アンサンブル用)
        self._torch_predictor = self._load_torch_predictor()

        # LightGBM 複勝予測モデル (Step3 SHAP寄与度計算用)
        self._lgbm_predictor = self._load_lgbm_predictor()

        # LightGBM LambdaRank 補完モデル (三連複精度向上用・任意)
        self._lgbm_ranker = self._load_lgbm_ranker()

        # 人気別実績統計テーブル（確率ブレンド用）
        self._pop_stats = load_popularity_stats()

        # ML位置取り予測モデル: あれば自動ロード、なければルールベース
        position_predictor = self._load_position_predictor()

        # コース別上がり3F sigma DB（案F-4）（日付単位キャッシュ）
        try:
            if _DB_CACHE_DATE == target_date and _DB_CACHE_L3F_SIGMA is not None:
                _sigma_db = _DB_CACHE_L3F_SIGMA
            else:
                from src.database import get_course_last3f_sigma
                _sigma_db = get_course_last3f_sigma(target_date=target_date)
                _DB_CACHE_L3F_SIGMA = _sigma_db
                logger.info("コース別last3f σDB: %d件ロード (target_date=%s)", len(_sigma_db), target_date)
        except Exception:
            _sigma_db = {}

        # race_log ベースの枠順バイアスDB（案F-5）（日付単位キャッシュ）
        try:
            if _DB_CACHE_DATE == target_date and _DB_CACHE_GATE_BIAS is not None:
                _racelog_gate_bias = _DB_CACHE_GATE_BIAS
            else:
                from src.database import get_gate_bias_from_race_log
                _racelog_gate_bias = get_gate_bias_from_race_log(target_date=target_date)
                _DB_CACHE_GATE_BIAS = _racelog_gate_bias
                logger.info("race_log枠順バイアス: %d競馬場補完", len(_racelog_gate_bias) if _racelog_gate_bias else 0)
            if _racelog_gate_bias:
                for k, v in _racelog_gate_bias.items():
                    if k not in (gate_bias_db or {}):
                        if gate_bias_db is None:
                            gate_bias_db = {}
                        gate_bias_db[k] = v
        except Exception as e:
            logger.debug("race_log枠順バイアスロード失敗: %s", e)

        self.pace_dev_calc = PaceDeviationCalculator(
            position_sec_per_rank_db=position_sec_per_rank_db or {},
            position_predictor=position_predictor,
            last3f_sigma_db=_sigma_db,  # 案F-4追加
        )
        self.course_apt_calc = CourseAptitudeCalculator()
        self.jockey_change_eval = JockeyChangeEvaluator()
        self.training_eval = TrainingEvaluator()
        self.all_courses = all_courses
        self.jockey_db = jockey_db
        self.trainer_db = trainer_db
        self.trainer_baseline_db = trainer_baseline_db
        self.course_style_stats_db = course_style_stats_db
        self.gate_bias_db = gate_bias_db or {}
        self.is_jra = is_jra
        self.formatter = HTMLFormatter(std_calc=self.std_calc)

    @staticmethod
    def _load_ml_predictor():
        """ML上がり3F予測モデルをロード（プロセス内キャッシュ: 2回目以降即返却）"""
        global _CACHE_ML_PREDICTOR, _CACHE_ML_LOADED
        if _CACHE_ML_LOADED:
            return _CACHE_ML_PREDICTOR
        _CACHE_ML_LOADED = True
        try:
            from src.ml.last3f_model import Last3FPredictor

            predictor = Last3FPredictor()
            if predictor.load():
                logger.info("ML上がり3F予測モデルをロードしました")
                _CACHE_ML_PREDICTOR = predictor
        except Exception:
            logger.debug("ML上がり3F予測モデルのロードをスキップ", exc_info=True)
        return _CACHE_ML_PREDICTOR

    @staticmethod
    def _load_position_predictor():
        """ML位置取り予測モデルをロード（プロセス内キャッシュ: 2回目以降即返却）"""
        global _CACHE_POSITION_PREDICTOR, _CACHE_POS_LOADED
        if _CACHE_POS_LOADED:
            return _CACHE_POSITION_PREDICTOR
        _CACHE_POS_LOADED = True
        try:
            from src.ml.position_model import PositionPredictor

            predictor = PositionPredictor()
            if predictor.load():
                logger.info("ML位置取り予測モデルをロードしました")
                _CACHE_POSITION_PREDICTOR = predictor
        except Exception:
            logger.debug("ML位置取り予測モデルのロードをスキップ", exc_info=True)
        return _CACHE_POSITION_PREDICTOR

    @staticmethod
    def _load_prob_predictor():
        """ML三連率予測モデル(win/top2/top3)をロード（プロセス内キャッシュ）"""
        global _CACHE_PROB_PREDICTOR, _CACHE_PROB_LOADED
        if _CACHE_PROB_LOADED:
            return _CACHE_PROB_PREDICTOR
        _CACHE_PROB_LOADED = True
        try:
            from src.ml.probability_model import ProbabilityPredictor

            predictor = ProbabilityPredictor()
            if predictor.load():
                logger.info("ML三連率予測モデルをロードしました")
                _CACHE_PROB_PREDICTOR = predictor
        except Exception:
            logger.debug("ML三連率予測モデルのロードをスキップ", exc_info=True)
        return _CACHE_PROB_PREDICTOR

    @staticmethod
    def _load_pace_ml_predictor():
        """前半3F予測 ML モデルをロード（PacePredictorML）"""
        global _CACHE_PACE_ML_PREDICTOR, _CACHE_PACE_ML_LOADED
        if _CACHE_PACE_ML_LOADED:
            return _CACHE_PACE_ML_PREDICTOR
        _CACHE_PACE_ML_LOADED = True
        try:
            from src.ml.pace_model import PacePredictorML

            predictor = PacePredictorML()
            if predictor.load():
                logger.info("ML前半3F予測モデルをロードしました")
                _CACHE_PACE_ML_PREDICTOR = predictor
        except Exception:
            logger.debug("ML前半3F予測モデルのロードをスキップ", exc_info=True)
        return _CACHE_PACE_ML_PREDICTOR

    @staticmethod
    def _load_torch_predictor():
        """PyTorch 三連率予測モデルをロード（TorchPredictor）"""
        global _CACHE_TORCH_PREDICTOR, _CACHE_TORCH_LOADED
        if _CACHE_TORCH_LOADED:
            return _CACHE_TORCH_PREDICTOR
        _CACHE_TORCH_LOADED = True
        try:
            from src.ml.torch_model import TorchPredictor

            predictor = TorchPredictor()
            if predictor.load():
                logger.info("PyTorch 三連率予測モデルをロードしました")
                _CACHE_TORCH_PREDICTOR = predictor
        except Exception:
            logger.debug("PyTorch モデルのロードをスキップ", exc_info=True)
        return _CACHE_TORCH_PREDICTOR

    @staticmethod
    def _load_lgbm_predictor():
        """LightGBM 複勝モデルをロード（Step3 SHAP寄与度計算用）"""
        global _CACHE_LGBM_PREDICTOR, _CACHE_LGBM_LOADED
        if _CACHE_LGBM_LOADED:
            return _CACHE_LGBM_PREDICTOR
        _CACHE_LGBM_LOADED = True
        try:
            from src.ml.lgbm_model import LGBMPredictor

            predictor = LGBMPredictor()
            if predictor.load():
                logger.info("LightGBM 複勝モデルをロードしました (SHAP用)")
                _CACHE_LGBM_PREDICTOR = predictor
        except Exception:
            logger.debug("LightGBM 複勝モデルのロードをスキップ", exc_info=True)
        return _CACHE_LGBM_PREDICTOR

    @staticmethod
    def _load_lgbm_ranker():
        """LambdaRank モデルをロード（三連複精度向上用・なくても動作）"""
        global _CACHE_LGBM_RANKER, _CACHE_RANKER_LOADED
        if _CACHE_RANKER_LOADED:
            return _CACHE_LGBM_RANKER
        _CACHE_RANKER_LOADED = True
        try:
            from src.ml.lgbm_ranker import LGBMRanker

            ranker = LGBMRanker()
            if ranker.load():
                logger.info("LGBMRanker (LambdaRank) をロードしました")
                _CACHE_LGBM_RANKER = ranker
        except Exception:
            logger.debug("LambdaRank モデルのロードをスキップ", exc_info=True)
        return _CACHE_LGBM_RANKER

    # ------------------------------------------------------------------ #
    # ML-2: 動的ブレンド比率                                              #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _calc_blend_ratio(level: int = 2) -> tuple:
        """
        LGBMサブモデルの使用レベルに応じてルール:ML比率を動的決定する。

        Level 4 (競馬場専用モデル): データ量豊富で高精度 → ML を最も信頼
        Level 3 (JRA馬場×SMILE): 中程度の粒度
        Level 2 (JRA全体/NAR): 現行と同等
        Level 1 (馬場全体): フォールバック発動 → ルール寄り
        Level 0 (globalモデル): 最低精度 → ルール最重視

        Returns:
            (rule_weight, ml_weight) — 合計 1.0
        """
        _BLEND_BY_LEVEL = {
            4: (0.45, 0.55),   # 競馬場専用 (高精度) → ML 55%
            3: (0.55, 0.45),   # JRA馬場×SMILE → ML 45%
            2: (0.60, 0.40),   # JRA全体/NAR (現行維持) → ML 40%
            1: (0.65, 0.35),   # 馬場全体 → ML 35%
            0: (0.70, 0.30),   # globalモデル (最低精度) → ML 30%
        }
        return _BLEND_BY_LEVEL.get(level, (0.60, 0.40))

    @staticmethod
    def _calc_ranker_blend(level: int = 2) -> float:
        """
        LGBMサブモデルのレベルに応じて LambdaRank ブレンド率を動的決定する。

        Returns:
            ranker_weight (float) — LambdaRank スコアの混合率 (0.0〜1.0)
        """
        _RANKER_BLEND_BY_LEVEL = {
            4: 0.12,   # 競馬場専用モデル時: ランカー信頼度を少し上げる
            3: 0.10,   # 通常
            2: 0.10,
            1: 0.08,
            0: 0.06,   # globalモデル時: ランカーも信頼しすぎない
        }
        return _RANKER_BLEND_BY_LEVEL.get(level, 0.10)

    def analyze(
        self,
        race: RaceInfo,
        horses: List[Horse],
        custom_stake: Optional[int] = None,
        netkeiba_client: Optional[Any] = None,
    ) -> RaceAnalysis:
        """
        レース分析を実行して RaceAnalysis を返す

        Steps:
        1. ペース予測 (F-1)
        2. 各馬の能力偏差値 (A-E章)
        3. 各馬の展開偏差値 (F章)
        4. 各馬のコース適性偏差値 (G章)
        5. 騎手・厩舎評価 (H-J章)
        6. 三連率推定・穴馬・危険馬検知 (I章)
        7. 総合偏差値集計
        8. 印付け (I-3)
        9. 買い目生成 (第5章)
        10. 自信度・資金配分
        """

        # ---- 少頭数ガード ----
        if len(horses) == 0:
            from src.calculator.tickets import ConfidenceLevel

            return RaceAnalysis(
                race=race,
                evaluations=[],
                pace_type_predicted=PaceType.MM,
                pace_reliability=ConfidenceLevel.D,
                leading_horses=[],
                front_horses=[],
                mid_horses=[],
                rear_horses=[],
                overall_confidence=ConfidenceLevel.D,
                tickets=[],
                total_budget=0,
                pace_comment="出走馬なし",
                favorable_gate="",
                favorable_style="",
                favorable_style_reason="",
            )
        is_small_field = len(horses) <= 5

        course_db_ref = self.std_calc.course_db

        # ---- 性齢定量: base_weight_kg を補完 ----
        for horse in horses:
            if not horse.base_weight_kg or horse.base_weight_kg == 55.0:
                horse.base_weight_kg = get_base_weight(horse.sex, horse.age, race.race_date)

        # ---- 改修後コースのみ使うようにcourse_dbをフィルタ ----
        for cid in list(course_db_ref.keys()):
            vc = cid.split("_")[0]
            if isinstance(course_db_ref[cid], list):
                course_db_ref[cid] = filter_post_renovation_runs(
                    course_db_ref[cid], vc, race.race_date
                )

        # 騎手×馬コンビ・血統×距離×馬場・ペース別DBを構築 (改善案)
        combo_db = build_jockey_horse_combo_db(horses)
        from config.settings import BLOODLINE_DB_PATH

        bloodline_db = build_bloodline_db(
            horses,
            netkeiba_client=netkeiba_client,
            cache_path=BLOODLINE_DB_PATH,
        )
        pace_db = build_pace_stats_db(horses)

        past_runs_map = {h.horse_id: h.past_runs for h in horses}

        # ---- Step 1: ペース予測 ----
        pace_type, pace_score, leaders, front_rate, max_escape_strength = self.pace_predictor.predict_pace(
            horses, past_runs_map, race.course,
            course_pace_tendency=self.course_pace_tendency,
        )
        pace_reliability = self._judge_pace_reliability(pace_score, len(leaders), len(horses))

        # ---- ペース文脈 (ML推定用) ----
        n_front = len(leaders) + sum(
            1 for h in horses
            if self.pace_predictor._classify_style(h.past_runs).value == "先行"
        )
        # 内枠の逃げ馬候補数（案C）
        n_escape_inner = sum(
            1 for h in horses
            if h.horse_no in leaders and getattr(h, "gate_no", 9) <= 4
        )
        pace_context = {
            "n_front": n_front,
            "front_ratio": front_rate,
            "n_escape": len(leaders),         # 逃げ馬数
            "n_escape_inner": n_escape_inner,  # 内枠逃げ馬数（案C）
            "max_escape_strength": max_escape_strength,  # 最強逃げ馬スコア（案C）
        }

        # ---- Step 2-7: 各馬の評価 ----
        # 2パス方式: baselineがDBにない場合、フィールド全馬のest_last3f平均を使う
        _baseline_from_db = self.last3f_evaluator.get_baseline(
            race.course.course_id, pace_type
        )
        _field_baseline = None
        if _baseline_from_db is None:
            # Pass 1: 全馬のest_last3fを事前計算してフィールド平均を求める
            import statistics as _stat_bl
            _est_l3fs = []
            for horse in horses:
                _si = self.style_classifier.classify(horse.past_runs, surface=race.course.surface)
                _l3f_type = _si.get("last3f_type", "安定中位末脚")
                _el = self.last3f_evaluator.estimate_last3f(
                    horse.past_runs, pace_type, race.course.course_id,
                    _l3f_type, horse=horse, race_info=race, pace_context=pace_context,
                )
                _est_l3fs.append(_el)
            if _est_l3fs:
                _field_baseline = _stat_bl.mean(_est_l3fs)

        evaluations: List[HorseEvaluation] = []
        for horse in horses:
            ev = self._evaluate_horse(
                horse,
                race,
                pace_type,
                combo_db,
                bloodline_db,
                pace_db,
                pace_context=pace_context,
                field_baseline_override=_field_baseline,
            )
            ev.venue_name = race.venue
            evaluations.append(ev)

        # オッズ整合性スコア (モデル vs 市場の乖離)
        from config.settings import get_composite_weights
        from src.scraper.improvement_dbs import calc_weight_change_adjustment

        w = get_composite_weights(race.venue)
        all_base = []
        for ev in evaluations:
            jdev = getattr(ev, "_jockey_dev", None)
            tdev = getattr(ev, "_trainer_dev", None)
            bdev = getattr(ev, "_bloodline_dev", None)
            base = (
                ev.ability.total * w["ability"]
                + ev.pace.total * w["pace"]
                + ev.course.total * w["course"]
                + (jdev if jdev is not None else 50.0) * w.get("jockey", 0.10)
                + (tdev if tdev is not None else 50.0) * w.get("trainer", 0.05)
                + (bdev if bdev is not None else 50.0) * w.get("bloodline", 0.05)
                + calc_weight_change_adjustment(ev.horse.weight_change, ev.horse.horse_weight)
            )
            all_base.append(base)
        for ev, base in zip(evaluations, all_base):
            ev.odds_consistency_adj = calc_odds_consistency_score(base, all_base, ev.horse.odds)

        # ---- Step 6: 三連率推定 (案A: pace/course独立補正, 案C: 動的temperature) ----
        all_composites    = [ev.composite    for ev in evaluations]
        all_pace_scores   = [ev.pace.total   for ev in evaluations]
        all_course_scores = [ev.course.total for ev in evaluations]
        for ev in evaluations:
            w, t2, t3 = estimate_three_win_rates(
                ev.composite,
                all_composites,
                pace_score=ev.pace.total,
                course_score=ev.course.total,
                all_pace_scores=all_pace_scores,
                all_course_scores=all_course_scores,
            )
            ev.win_prob = w
            ev.place2_prob = t2
            ev.place3_prob = t3

        # 正規化: 合計を理論値に一致させる
        # 少頭数の場合は連対・複勝の上限を頭数に合わせる
        n = len(evaluations)
        place2_target = min(n, 2) / n if n > 0 else 0
        place3_target = min(n, 3) / n if n > 0 else 0

        total_win = sum(ev.win_prob for ev in evaluations)
        total_place2 = sum(ev.place2_prob for ev in evaluations)
        total_place3 = sum(ev.place3_prob for ev in evaluations)

        if total_win > 0:
            for ev in evaluations:
                ev.win_prob = min(1.0, ev.win_prob / total_win)
        if total_place2 > 0:
            for ev in evaluations:
                ev.place2_prob = min(1.0, (ev.place2_prob / total_place2) * place2_target * n)
        if total_place3 > 0:
            for ev in evaluations:
                ev.place3_prob = min(1.0, (ev.place3_prob / total_place3) * place3_target * n)

        # ---- Step 6: 予測オッズ (オッズ未確定馬に付与) ----
        predicted = calc_predicted_odds(evaluations, self.is_jra)
        for ev in evaluations:
            if ev.horse.odds is None:
                ev.predicted_odds = predicted.get(ev.horse.horse_id)

        # 脚質別グループ分け (レベル1表示用)
        # 各馬の脚質を一度だけ判定してキャッシュ
        _style_map = {}
        for h in horses:
            _style_map[h.horse_no] = self.pace_predictor._classify_style(h.past_runs)
        leading = [no for no, s in _style_map.items() if s.value == "逃げ"]
        front_h = [no for no, s in _style_map.items() if s.value == "先行"]
        mid_h = [no for no, s in _style_map.items() if s.value == "差し"]
        rear_h = [no for no, s in _style_map.items() if s.value == "追込"]

        # フォールバック: 逃げ馬が0頭は現実的にありえない → 最も前に位置する馬を逃げに再分類
        if not leading and horses:
            import statistics as _st
            best_no, best_avg = None, 1.0
            for h in horses:
                rels = [r.relative_position for r in h.past_runs[:5] if r.relative_position is not None]
                avg = _st.mean(rels) if rels else 0.5
                if avg < best_avg:
                    best_avg = avg
                    best_no = h.horse_no
            if best_no is not None:
                leading.append(best_no)
                front_h = [n for n in front_h if n != best_no]
                mid_h = [n for n in mid_h if n != best_no]
                rear_h = [n for n in rear_h if n != best_no]
                _style_map[best_no] = RunningStyle.NIGASHI
                # per-horse running_style も更新
                for ev in evaluations:
                    if ev.horse.horse_no == best_no and ev.pace:
                        ev.pace.running_style = RunningStyle.NIGASHI
                        break

        # ---- 展開コメント・有利枠・有利脚質（並び・前半/後半3F推定を利用） ----
        lineup = calc_lineup(horses)
        all_runs = [r for h in horses for r in h.past_runs]
        front_3f_est, last_3f_est = estimate_pace_times_from_runs(
            race.course.course_id, pace_type, all_runs,
            surface=race.course.surface, distance=race.course.distance,
        )
        # ---- ML前半3F補正（PacePredictorML が利用可能な場合）----
        if self._pace_ml_predictor and self._pace_ml_predictor.is_available:
            try:
                ml_f3f = self._pace_ml_predictor.predict_first3f(race, pace_context)
                if ml_f3f is not None:
                    # ルールベースと ML の加重平均（ML 60% + ルール 40%）
                    front_3f_est = ml_f3f * 0.6 + (front_3f_est or ml_f3f) * 0.4
            except Exception:
                logger.debug("ML前半3F予測スキップ", exc_info=True)

        pace_comment, favorable_gate, favorable_style, favorable_style_reason = (
            generate_pace_comment(
                pace_type,
                leading,
                front_h,
                rear_h,
                race.course,
                evaluations,
                pace_reliability,
                lineup=lineup,
                mid_horses=mid_h,
                front_3f_est=front_3f_est,
                last_3f_est=last_3f_est,
            )
        )

        # ---- 偏差値診断 ----
        diag = diagnose_deviations(evaluations)
        if diag["status"] == "WARNING":
            logger.warning(diag["message"])

        # ---- フィールド内正規化: 展開偏差値・能力偏差値・コース適性偏差値を 50 中心に補正 ----
        # ★ 必ず正規化の後に印付け・買い目を生成すること（composite が変わるため）
        _normalize_field_deviations(evaluations)

        # ---- 騎手/調教師の偏差値を先行算出 [A4] ----
        # composite プロパティが参照される前に _jockey_dev / _trainer_dev をセットする
        _compute_personnel_devs(evaluations)

        # ---- 同一レース内の上3F ランクを run_records に付与 ----
        _enrich_l3f_rank(evaluations, course_db_ref, self.std_calc)

        # ---- ML推論: LightGBM P(3着以内) ----
        if self._prob_predictor:
            try:
                ml_probs = self._prob_predictor.predict_from_engine(race, horses, evaluations=evaluations)
                for ev in evaluations:
                    ev.ml_place_prob = ml_probs.get(ev.horse.horse_id)
            except Exception as e:
                logger.debug("ML prediction skipped: %s", e)

        # ---- ML推論: 三連率予測 (win/top2/top3) ----
        if self._prob_predictor:
            try:
                prob_map = self._prob_predictor.predict_from_engine(race, horses, evaluations=evaluations)
                for ev in evaluations:
                    probs = prob_map.get(ev.horse.horse_id, {})
                    if probs:
                        ev.ml_win_prob = probs.get("win")
                        ev.ml_top2_prob = probs.get("top2")
                        ev.ml_place_prob = probs.get("top3", ev.ml_place_prob)
            except Exception as e:
                logger.debug("Probability model prediction skipped: %s", e)

        # ---- ML推論: PyTorch アンサンブル (改善A/B) LightGBM 60% + PyTorch 40% ----
        # ブレンド比率を動的決定: LGBMサブモデルのレベルに応じてルール寄り/ML寄りを切替
        from src.ml.lgbm_model import _lgbm_tls
        _lgbm_level = getattr(_lgbm_tls, "last_model_level", 2)
        _lgbm_rule_w, _lgbm_ml_w = self._calc_blend_ratio(_lgbm_level)
        if self._torch_predictor:
            try:
                torch_probs = self._torch_predictor.predict_from_engine(race, horses, evaluations=evaluations)
                if torch_probs:
                    # PyTorch ブレンドは LightGBM ML比率をそのまま流用
                    # (lw=LightGBM寄り, tw=PyTorch寄り として 60:40 の内比)
                    lw, tw = 0.6, 0.4
                    for ev in evaluations:
                        hid = ev.horse.horse_id
                        tp = torch_probs.get(hid, {})
                        if not tp:
                            continue
                        if ev.ml_win_prob is not None:
                            ev.ml_win_prob  = ev.ml_win_prob  * lw + tp.get("win",  ev.ml_win_prob)  * tw
                        elif tp.get("win") is not None:
                            ev.ml_win_prob  = tp["win"]
                        if ev.ml_top2_prob is not None:
                            ev.ml_top2_prob = ev.ml_top2_prob * lw + tp.get("top2", ev.ml_top2_prob) * tw
                        elif tp.get("top2") is not None:
                            ev.ml_top2_prob = tp["top2"]
                        if ev.ml_place_prob is not None:
                            ev.ml_place_prob = ev.ml_place_prob * lw + tp.get("top3", ev.ml_place_prob) * tw
                        elif tp.get("top3") is not None:
                            ev.ml_place_prob = tp["top3"]
            except Exception:
                logger.debug("PyTorch アンサンブルをスキップ", exc_info=True)

        # ---- LambdaRank アンサンブル (補完: 三連複精度向上) ----
        # LambdaRank スコアを softmax 正規化して ml_win_prob/ml_place_prob に 10% 加算
        if self._lgbm_ranker:
            try:
                import math
                race_dict_ranker = {
                    "date": race.race_date, "venue": race.venue,
                    "surface": race.course.surface, "distance": race.course.distance,
                    "condition": race.condition, "field_count": len(horses),
                    "is_jra": self.is_jra, "grade": getattr(race, "grade", ""),
                    "venue_code": getattr(race, "venue_code", ""),
                }
                horse_dicts_ranker = [
                    {
                        "horse_id": h.horse_id, "jockey_id": getattr(h, "jockey_id", ""),
                        "trainer_id": getattr(h, "trainer_id", ""),
                        "gate_no": getattr(h, "gate_no", 0),
                        "horse_no": getattr(h, "horse_no", 0),
                        "sex": getattr(h, "sex", ""), "age": getattr(h, "age", 0),
                        "weight_kg": getattr(h, "weight_kg", 55.0),
                        "horse_weight": getattr(h, "horse_weight", None),
                        "weight_change": getattr(h, "weight_change", None),
                    }
                    for h in horses
                ]
                ranker_scores = self._lgbm_ranker.predict_race(race_dict_ranker, horse_dicts_ranker)
                if ranker_scores:
                    # softmax 正規化
                    vals = list(ranker_scores.values())
                    max_v = max(vals)
                    exp_v = {hid: math.exp(v - max_v) for hid, v in ranker_scores.items()}
                    sum_e = sum(exp_v.values())
                    norm_scores = {hid: ev / sum_e for hid, ev in exp_v.items()}
                    # LambdaRank ブレンド率を動的決定 (Level に応じて 6〜12%)
                    _RW = self._calc_ranker_blend(_lgbm_level)
                    for ev in evaluations:
                        rs = norm_scores.get(ev.horse.horse_id)
                        if rs is None:
                            continue
                        if ev.ml_win_prob is not None:
                            ev.ml_win_prob   = ev.ml_win_prob   * (1 - _RW) + rs * _RW
                        else:
                            ev.ml_win_prob   = rs
                        if ev.ml_place_prob is not None:
                            ev.ml_place_prob = ev.ml_place_prob * (1 - _RW) + rs * _RW
                        else:
                            ev.ml_place_prob = rs
            except Exception:
                logger.debug("LambdaRank アンサンブルをスキップ", exc_info=True)

        # ---- 案B: ML + ルールベース ブレンド → win/place2/place3 を最終値に統合 ----
        # ML値がある馬が1頭でも存在すればブレンドを適用
        # ブレンド比率は LGBMサブモデルのレベルに応じて動的決定済み (_lgbm_rule_w / _lgbm_ml_w)
        _RB_W, _ML_W = _lgbm_rule_w, _lgbm_ml_w
        logger.debug(
            "動的ブレンド比率: Rule %.0f%% / ML %.0f%% (LGBMレベル %s)",
            _RB_W * 100, _ML_W * 100,
            _lgbm_level,
        )
        _has_ml = any(ev.ml_win_prob is not None for ev in evaluations)
        if _has_ml:
            for ev in evaluations:
                if ev.ml_win_prob is not None:
                    ev.win_prob    = _RB_W * ev.win_prob    + _ML_W * ev.ml_win_prob
                if ev.ml_top2_prob is not None:
                    ev.place2_prob = _RB_W * ev.place2_prob + _ML_W * ev.ml_top2_prob
                if ev.ml_place_prob is not None:
                    ev.place3_prob = _RB_W * ev.place3_prob + _ML_W * ev.ml_place_prob
            # ブレンド済みなのでML個別値をクリア（重複表示防止）
            for ev in evaluations:
                ev.ml_win_prob   = None
                ev.ml_top2_prob  = None
                ev.ml_place_prob = None

        # ---- 確率の正規化（ML有無に関わらず常に実施）----
        # 勝率: Σwin_prob = 1.0 (100%)
        # 連対率: Σplace2_prob = 2.0 (200%)
        # 複勝率: Σplace3_prob = 3.0 (300%)
        _normalize_probs(evaluations)

        # ---- Step 5.5: LGBM place3_prob → 能力偏差値への軽量フィードバック（案5） ----
        # LGBMが高確率と判断しているが能力偏差値が低い馬を軽くプッシュ
        # 逆に LGBMが低確率で能力偏差値が高い馬は若干下方修正
        # 補正幅は最大 ±3pt（過剰修正を防ぐために小さめ）
        try:
            _place3_probs = [ev.place3_prob for ev in evaluations]
            _n = len(_place3_probs)
            if _n >= 3:
                _avg_p3 = sum(_place3_probs) / _n
                for ev in evaluations:
                    _lgbm_signal = ev.place3_prob - _avg_p3  # レース内での相対乖離
                    # 偏差値スケールに変換（±0.10 prob ≈ ±2pt 程度）
                    _correction = max(-3.0, min(3.0, _lgbm_signal * 20.0))
                    # AbilityDeviation は dataclass (mutable) なので直接更新
                    # norm_adjustment に書き込む（class_adjustment は元のクラス落差補正を保持）
                    ev.ability.norm_adjustment += _correction * 0.5
        except Exception:
            pass  # フォールバック: 何もしない

        # ---- Step3: SHAP寄与度グループ計算 ----
        if self._lgbm_predictor:
            try:
                shap_result = self._lgbm_predictor.compute_shap_groups(
                    race, horses, evaluations=evaluations
                )
                for ev in evaluations:
                    sg = shap_result.get(ev.horse.horse_id)
                    if sg:
                        ev.shap_groups = sg
            except Exception:
                logger.debug("SHAP寄与度計算をスキップ", exc_info=True)

        # ---- Step3b: 血統 surface×SMILE 分解（見える化） ----
        if self._lgbm_predictor:
            try:
                bd_result = self._lgbm_predictor.get_sire_breakdowns(horses)
                for ev in evaluations:
                    bd = bd_result.get(ev.horse.horse_id)
                    if bd:
                        ev.ability.sire_breakdown = bd
            except Exception:
                logger.debug("血統分解計算をスキップ", exc_info=True)

        # ---- 正規化後の odds_consistency 再計算 [A1] ----
        # 正規化 + MLフィードバックにより ability/pace/course が変わったため再計算
        w_post = get_composite_weights(race.venue)
        all_base_post = []
        for ev in evaluations:
            jdev = getattr(ev, "_jockey_dev", None)
            tdev = getattr(ev, "_trainer_dev", None)
            bdev = getattr(ev, "_bloodline_dev", None)
            base_post = (
                ev.ability.total * w_post["ability"]
                + ev.pace.total * w_post["pace"]
                + ev.course.total * w_post["course"]
                + (jdev if jdev is not None else 50.0) * w_post.get("jockey", 0.10)
                + (tdev if tdev is not None else 50.0) * w_post.get("trainer", 0.05)
                + (bdev if bdev is not None else 50.0) * w_post.get("bloodline", 0.05)
                + calc_weight_change_adjustment(ev.horse.weight_change, ev.horse.horse_weight)
            )
            all_base_post.append(base_post)
        for ev, bp in zip(evaluations, all_base_post):
            # 2回目は1回目との加重平均でダンピング（振動抑制）
            new_adj = calc_odds_consistency_score(bp, all_base_post, ev.horse.odds)
            ev.odds_consistency_adj = 0.3 * new_adj + 0.7 * ev.odds_consistency_adj

        # ---- 正規化後の三連率再推定 [A2] ----
        # MLブレンド済み確率を退避し、再推定結果との加重平均で最終値を決定
        # （再推定がMLブレンド結果を完全に上書きしないようにする）
        all_composites_post = [ev.composite for ev in evaluations]
        all_pace_post = [ev.pace.total for ev in evaluations]
        all_course_post = [ev.course.total for ev in evaluations]
        for ev in evaluations:
            _ml_blended_win = ev.win_prob
            _ml_blended_p2  = ev.place2_prob
            _ml_blended_p3  = ev.place3_prob
            _w, _t2, _t3 = estimate_three_win_rates(
                ev.composite,
                all_composites_post,
                pace_score=ev.pace.total,
                course_score=ev.course.total,
                all_pace_scores=all_pace_post,
                all_course_scores=all_course_post,
            )
            # MLブレンド結果60% + compositeベース再推定40% の加重平均
            ev.win_prob    = 0.6 * _ml_blended_win + 0.4 * _w
            ev.place2_prob = 0.6 * _ml_blended_p2  + 0.4 * _t2
            ev.place3_prob = 0.6 * _ml_blended_p3  + 0.4 * _t3

        # 確率正規化
        _normalize_probs(evaluations)

        # ---- Step 5b: 人気別実績統計ブレンド ----
        if self._pop_stats and any(
            getattr(ev.horse, "popularity", None) for ev in evaluations
        ):
            blend_probabilities(
                evaluations, race.venue, self.is_jra,
                len(evaluations), self._pop_stats,
            )
            _normalize_probs(evaluations)

        # ---- Step 6: 穴馬・危険馬スコア（正規化後の composite 使用）[A3] ----
        for ev in evaluations:
            ev.ana_score, ev.ana_type = calc_ana_score(ev, evaluations)
            ev.kiken_score, ev.kiken_type = calc_kiken_score(ev, evaluations)

        # ---- Step 6b: 特選穴馬スコア（印体系とは独立）----
        from config.settings import TOKUSEN_SCORE_THRESHOLD, TOKUSEN_MAX_PER_RACE
        for ev in evaluations:
            ev.tokusen_score = calc_tokusen_score(ev, evaluations)
        # スコア上位N頭のみ is_tokusen=True
        tokusen_candidates = sorted(
            [ev for ev in evaluations if ev.tokusen_score >= TOKUSEN_SCORE_THRESHOLD],
            key=lambda e: e.tokusen_score, reverse=True,
        )
        for ev in tokusen_candidates[:TOKUSEN_MAX_PER_RACE]:
            ev.is_tokusen = True

        # ---- Step 8: 印付け（composite 降順で ◉◎○▲△★ + 穴馬☆/危険馬×）----
        evaluations = assign_marks(evaluations)

        # ---- Step 9: 買い目生成（◎/◉軸 馬連4点+三連複6点 固定10点×100円=1000円）----
        tickets = generate_tickets(evaluations, race)

        # ---- Step 10: 資金配分集計 ----
        # 固定10点の場合は生成されたticketをそのまま使用（allocate_stakesは不要）
        total_budget = sum(t.get("stake", 0) for t in tickets)  # 最大1000円

        # ---- Step 11: 予想オッズ算出（フォーメーション前に実施）----
        assign_divergence_to_evaluations(evaluations, self.is_jra)
        predicted_umaren = calc_predicted_umaren(evaluations, race)
        predicted_sanrenpuku = calc_predicted_sanrenpuku(evaluations, race)

        # K-5: 自信度判定（predicted_umaren/sanrenpuku を渡してML出現率を考慮）
        confidence = judge_confidence(
            evaluations, pace_reliability,
            predicted_umaren=predicted_umaren,
            predicted_sanrenpuku=predicted_sanrenpuku,
        )

        # ---- Step 10b: フォーメーション買い目生成（出現率グレード使用）----
        u_pct = sum(r["prob"] for r in predicted_umaren[:5]) * 100
        s_pct = sum(r["prob"] for r in predicted_sanrenpuku[:5]) * 100
        coverage_total = round(u_pct + s_pct, 2)
        if coverage_total >= 69:
            coverage_grade = "SS"
        elif coverage_total >= 57:
            coverage_grade = "S"
        elif coverage_total >= 44:
            coverage_grade = "A"
        elif coverage_total >= 34:
            coverage_grade = "B"
        else:
            coverage_grade = "C"
        formation = generate_formation_tickets(evaluations, race, coverage_grade)

        has_any_odds = any(ev.horse.odds is not None for ev in evaluations)
        is_pre_day = not has_any_odds

        value_bets = []
        if has_any_odds:
            value_bets = detect_value_bets(
                evaluations, race, predicted_umaren, predicted_sanrenpuku
            )

        # ---- 走破タイム推定 ----
        predicted_race_time = self._estimate_race_time(
            race, pace_type, front_3f_est, last_3f_est
        )

        # ---- 最終隊列予測 ----
        final_formation = self._predict_final_formation(
            evaluations, _style_map, pace_type, leading, front_h, mid_h, rear_h
        )

        analysis = RaceAnalysis(
            race=race,
            evaluations=evaluations,
            pace_type_predicted=pace_type,
            pace_reliability=pace_reliability,
            leading_horses=leading,
            front_horses=front_h,
            mid_horses=mid_h,
            rear_horses=rear_h,
            overall_confidence=confidence,
            confidence_score=_calc_confidence_score(evaluations),
            tickets=tickets,
            total_budget=total_budget,
            pace_comment=pace_comment,
            favorable_gate=favorable_gate,
            favorable_style=favorable_style,
            favorable_style_reason=favorable_style_reason,
            estimated_front_3f=front_3f_est,
            estimated_last_3f=last_3f_est,
            formation=formation,
            predicted_odds_umaren=predicted_umaren,
            predicted_odds_sanrenpuku=predicted_sanrenpuku,
            value_bets=value_bets,
            is_pre_day_mode=is_pre_day,
            predicted_race_time=predicted_race_time,
            final_formation=final_formation,
            pace_reliability_label=pace_reliability.value if pace_reliability else "B",
        )
        return analysis

    # 予想タイム用クラス補正係数（1勝/C1 = 0.0 基準）
    # JRA良馬場1着タイム実データ検証値ベース（ability.py コメント参照）
    # 秒差 = (sample_avg_factor - race_factor) × dist_coeff
    # 正値 = 基準より速いクラス
    _GRADE_TIME_FACTOR: Dict[str, float] = {
        # JRA (実データ検証値: 1勝=0基準)
        "G1": 2.0, "G2": 1.6, "G3": 1.2,
        "OP": 0.8, "L": 0.8,
        "3勝": 0.6, "1600万": 0.6,
        "2勝": 0.4, "1000万": 0.4,
        "1勝": 0.0, "500万": 0.0,
        "未勝利": -0.9,
        "新馬": -1.3,
        # NAR (JRA相当値で推定)
        "A1": 0.8, "A2": 0.6,
        "B1": 0.4, "B2": 0.3, "B3": 0.1,
        "C1": 0.0, "C2": -0.5, "C3": -0.9,
        "重賞": 1.0, "交流重賞": 1.2, "特別": 0.3,
        "未格付": -0.9,
    }

    def _estimate_race_time(
        self, race: RaceInfo, pace_type: PaceType,
        front_3f_est: Optional[float], last_3f_est: Optional[float],
    ) -> Optional[float]:
        """コース平均走破タイムをペース＋クラスで補正して予想走破タイムを算出"""
        try:
            course_id = race.course.course_id if race.course else None
            if not course_id:
                return None

            course_db_ref = self.std_calc.course_db
            records = course_db_ref.get(course_id, [])

            # course_dbの走破タイム平均（上位3着以内）+ サンプルのクラス情報
            times = []
            sample_factors = []
            for r in records:
                t = getattr(r, "finish_time_sec", None) if hasattr(r, "finish_time_sec") else (r.get("finish_time_sec") if isinstance(r, dict) else None)
                fp = getattr(r, "finish_pos", None) if hasattr(r, "finish_pos") else (r.get("finish_pos") if isinstance(r, dict) else None)
                if t and isinstance(fp, int) and 1 <= fp <= 3:
                    times.append(t)
                    g = getattr(r, "grade", None) if hasattr(r, "grade") else (r.get("grade") if isinstance(r, dict) else None)
                    sample_factors.append(self._GRADE_TIME_FACTOR.get(g, 0.0) if g else 0.0)

            if times:
                import statistics as _st
                avg_time = _st.mean(times)
                # ペース補正: HH→速い(-1.5秒), SS→遅い(+1.5秒)
                pace_corr = {
                    PaceType.HH: -1.5, PaceType.HM: -0.7,
                    PaceType.MM: 0.0, PaceType.MS: 0.7, PaceType.SS: 1.5,
                }.get(pace_type, 0.0)
                # クラス補正: course_dbサンプル平均クラスとの差を秒数に変換
                grade_corr = 0.0
                race_factor = self._GRADE_TIME_FACTOR.get(race.grade, None)
                if race_factor is not None and sample_factors:
                    avg_sample_factor = _st.mean(sample_factors)
                    dist_coeff = self.std_calc.calc_distance_coefficient(
                        race.course.distance
                    )
                    # サンプル平均より低クラス → 正値(遅い), 高クラス → 負値(速い)
                    grade_corr = (avg_sample_factor - race_factor) * dist_coeff
                return round(avg_time + pace_corr + grade_corr, 1)

            # フォールバック: 前半3F + 後半3F + 中間区間推定
            if front_3f_est and last_3f_est and race.course:
                dist = race.course.distance
                mid_dist = dist - 600  # 前半3F(300m) + 後半3F(300m) = 600m ... ではなく3F=600m
                # 3ハロン = 600m なので front_3f + last_3f は前半600m + 後半600mで合計1200m分
                remaining = dist - 1200
                if remaining > 0:
                    per_200m = (front_3f_est + last_3f_est) / 6  # 200mあたりの平均秒数
                    mid_time = remaining / 200 * per_200m
                    return round(front_3f_est + last_3f_est + mid_time, 1)
                else:
                    # 1200m以下の場合
                    ratio = dist / 1200
                    return round((front_3f_est + last_3f_est) * ratio, 1)
        except Exception:
            logger.debug("走破タイム推定をスキップ", exc_info=True)
        return None

    def _predict_final_formation(
        self, evaluations, style_map, pace_type,
        leading, front_h, mid_h, rear_h,
    ) -> Dict[str, List[int]]:
        """初角隊列 + 総合偏差値 + ペースから最終隊列を予測"""
        try:
            # composite順位を取得
            sorted_by_comp = sorted(evaluations, key=lambda e: e.composite, reverse=True)
            n = len(sorted_by_comp)
            comp_rank = {}
            for i, ev in enumerate(sorted_by_comp):
                comp_rank[ev.horse.horse_no] = i + 1

            # ペースタイプによる前後シフト量
            pv = pace_type.value if pace_type else "MM"
            # HH: 前崩れ（逃げ→後方、先行→中団）, SS: 前残り（差し→後方に下がりがち）
            shift = {"HH": 2, "HM": 1, "MM": 0, "MS": -1, "SS": -2}.get(pv, 0)

            # 各馬の最終位置スコアを算出
            # 初角位置: 逃げ=1, 先行=2, 差し=3, 追込=4
            initial_pos = {}
            for no in leading:
                initial_pos[no] = 1
            for no in front_h:
                initial_pos[no] = 2
            for no in mid_h:
                initial_pos[no] = 3
            for no in rear_h:
                initial_pos[no] = 4

            # 最終位置スコア = 初角位置 + ペースシフト - 能力補正
            final_scores = {}
            for ev in evaluations:
                no = ev.horse.horse_no
                ip = initial_pos.get(no, 3)
                rank = comp_rank.get(no, n // 2)
                # 上位馬ほど前に出る (-1〜+1)
                ability_adj = 0
                if rank <= n * 0.25:
                    ability_adj = -1  # 上位25%は前に出る
                elif rank >= n * 0.75:
                    ability_adj = 1   # 下位25%は後ろに下がる

                # ペースシフトは前にいる馬ほど影響大
                pace_adj = 0
                if ip <= 2:  # 逃げ・先行
                    pace_adj = shift * 0.5  # HHなら+1, SSなら-1
                elif ip >= 3:  # 差し・追込
                    pace_adj = -shift * 0.3  # HHなら差し有利で-0.6

                score = ip + pace_adj + ability_adj
                final_scores[no] = score

            # スコアでソートして4グループに分割
            sorted_horses = sorted(final_scores.items(), key=lambda x: x[1])
            total = len(sorted_horses)
            if total == 0:
                return {"先頭": [], "好位": [], "中団": [], "後方": []}

            # 分割: 先頭15%, 好位25%, 中団35%, 後方25%
            cut1 = max(1, round(total * 0.15))
            cut2 = max(cut1 + 1, round(total * 0.40))
            cut3 = max(cut2 + 1, round(total * 0.75))

            result = {
                "先頭": [h[0] for h in sorted_horses[:cut1]],
                "好位": [h[0] for h in sorted_horses[cut1:cut2]],
                "中団": [h[0] for h in sorted_horses[cut2:cut3]],
                "後方": [h[0] for h in sorted_horses[cut3:]],
            }
            return result
        except Exception:
            logger.debug("最終隊列予測をスキップ", exc_info=True)
            return {"先頭": list(leading), "好位": list(front_h), "中団": list(mid_h), "後方": list(rear_h)}

    def render_html(self, analysis: RaceAnalysis, output_path: str) -> str:
        """HTML出力を生成してファイルに保存"""
        html = self.formatter.render(analysis)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        return html

    # ----------------------------------------------------------
    # 各馬評価の内部メソッド
    # ----------------------------------------------------------

    def _evaluate_horse(
        self,
        horse: Horse,
        race: RaceInfo,
        pace_type: PaceType,
        combo_db: Optional[Dict] = None,
        bloodline_db: Optional[Dict] = None,
        pace_db: Optional[Dict] = None,
        pace_context: Optional[Dict] = None,
        field_baseline_override: Optional[float] = None,
    ) -> HorseEvaluation:
        ev = HorseEvaluation(horse=horse)

        # A-E章: 能力偏差値
        ev.ability = calc_ability_deviation(
            horse=horse,
            race_date=race.race_date,
            race_surface=race.course.surface,
            course_id=race.course.course_id,
            std_calc=self.std_calc,
            track_corr=self.track_corr,
            current_condition=race.track_condition_turf or race.track_condition_dirt or "良",
            current_cv=race.cv_value,
            current_moisture=race.moisture_turf or race.moisture_dirt,
            is_jra=self.is_jra,
            race_grade=race.grade,
            race_distance=race.course.distance,
            bloodline_db=bloodline_db or {},
            pace_db=pace_db or {},
            pace_type=pace_type,
        )

        # 騎手・厩舎データ取得
        jockey_stats = self.jockey_db.get(horse.jockey_id)
        trainer_stats = self.trainer_db.get(horse.trainer_id)
        ev.jockey_stats = jockey_stats
        ev.trainer_stats = trainer_stats

        # H-3: 乗り替わり評価 + 騎手×馬コンビ成績
        if jockey_stats:
            # 前走騎手を取得（H-3 C/D/E判定用）
            prev_jid = horse.past_runs[0].jockey_id if horse.past_runs else None
            prev_jockey_stats = self.jockey_db.get(prev_jid) if prev_jid else None
            pat, jockey_pace_score, shobu_contrib = self.jockey_change_eval.evaluate(
                horse, jockey_stats, race.grade,
                combo_db=combo_db or {},
                prev_jockey_stats=prev_jockey_stats,
            )
            ev.jockey_change_pattern = pat
            ev.jockey_change_score = jockey_pace_score
        else:
            jockey_pace_score = 0.0
            shobu_contrib = 0.0

        # G-2: 枠順バイアス（枠番があれば枠番別DBを使用）
        gate_bias = calc_gate_bias(
            horse.horse_no,
            race.field_count,
            race.course,
            self.gate_bias_db,
            gate_no=horse.gate_no or None,
        )

        # F章: 展開偏差値
        style_info = self.style_classifier.classify(horse.past_runs, surface=race.course.surface)
        ev.pace = self.pace_dev_calc.calc(
            horse=horse,
            style_info=style_info,
            pace_type=pace_type,
            last3f_evaluator=self.last3f_evaluator,
            course=race.course,
            gate_bias=gate_bias,
            jockey_pace_score=jockey_pace_score,
            field_count=race.field_count,
            race_info=race,
            pace_context=pace_context,
            field_baseline_override=field_baseline_override,
        )

        # G章: コース適性偏差値
        ev.course = self.course_apt_calc.calc(
            horse=horse,
            course=race.course,
            jockey_stats=jockey_stats,
            all_courses=self.all_courses,
        )

        # J-4: 調教評価
        baseline = self.trainer_baseline_db.get(horse.trainer_id, {})
        # PastRunから直近調教を取得 (実際はスクレイパーから)
        training_recs = getattr(horse, "training_records", []) or []
        ev.training_records = self.training_eval.evaluate(training_recs, baseline)

        # J-2: 勝負気配スコア (休み明け精密判定込み)
        is_long_break, _break_days = detect_long_break(horse.past_runs, race.race_date)
        last_grade = horse.past_runs[0].grade if horse.past_runs else race.grade
        days_since = get_days_since_last_run(horse.past_runs, race.race_date)
        tr = trainer_stats or TrainerStats("", "", "", "")
        ev.shobu_score = (
            calc_shobu_score(
                horse,
                tr,
                jockey_stats or JockeyStats("", ""),
                ev.jockey_change_pattern,
                is_long_break,
                race.grade,
                last_grade,
                days_since_last_run=days_since,
            )
            + shobu_contrib
        )

        return ev

    def _judge_pace_reliability(
        self, pace_score: float, leader_count: int, total: int
    ) -> ConfidenceLevel:
        """展開信頼度の判定"""
        # 逃げ馬が明確に1頭 → 大頭数ほど展開が確定しやすい
        if leader_count == 1 and total >= 10:
            return ConfidenceLevel.S   # 大頭数で逃げ1頭=展開ほぼ確定
        if leader_count == 1 and total >= 8:
            return ConfidenceLevel.A
        if leader_count == 1:
            return ConfidenceLevel.B
        if leader_count == 0:
            return ConfidenceLevel.C  # 逃げ馬不在=読みにくい
        if leader_count >= 3:
            return ConfidenceLevel.B  # ハイペース想定だが確定ではない
        return ConfidenceLevel.B


# ============================================================
# 確率正規化（ML blend後: win=1.0, place2=2.0, place3=3.0）
# ============================================================


def _normalize_probs(evaluations: List[HorseEvaluation]) -> None:
    """
    ML blend後の勝率・連対率・複勝率を理論値に正規化する。
      勝率合計  = 1.0  (100%: レース内で1頭だけ1着)
      連対率合計 = 2.0  (200%: 2頭が2着以内)
      複勝率合計 = 3.0  (300%: 3頭が3着以内)
    """
    tw = sum(ev.win_prob    or 0.0 for ev in evaluations)
    t2 = sum(ev.place2_prob or 0.0 for ev in evaluations)
    t3 = sum(ev.place3_prob or 0.0 for ev in evaluations)

    for ev in evaluations:
        if tw > 0 and ev.win_prob is not None:
            ev.win_prob    = ev.win_prob    / tw          # Σ = 1.0
        if t2 > 0 and ev.place2_prob is not None:
            ev.place2_prob = min(0.95, ev.place2_prob / t2 * 2.0)   # Σ ≈ 2.0
        if t3 > 0 and ev.place3_prob is not None:
            ev.place3_prob = min(0.95, ev.place3_prob / t3 * 3.0)   # Σ ≈ 3.0


# ============================================================
# フィールド内偏差値正規化
# ============================================================


def _normalize_field_deviations(evaluations: List[HorseEvaluation]) -> None:
    """
    展開偏差値・能力偏差値・コース適性偏差値をフィールド内で 50 中心に正規化する。
    同一レースの馬同士は相対比較が本質なので、全馬の平均を 50 に調整。
    σ が小さすぎる場合（全馬が僅差）は最小 σ を保証して差をつける。

    補正は norm_adjustment フィールドに書き込む（元の ai_adjustment / class_adjustment は保持）。
    """
    import statistics as _stat

    # ─── 展開偏差値の正規化 ───
    pace_totals = [ev.pace.total for ev in evaluations]
    if len(pace_totals) >= 2:
        mu = _stat.mean(pace_totals)
        sigma = _stat.pstdev(pace_totals) or 1.0
        # 最小 σ=5 を確保（差が無意味にならないよう）、拡大倍率は最大1.5倍
        target_sigma = max(sigma, 5.0)
        expansion = min(1.5, target_sigma / sigma)
        for ev in evaluations:
            raw = ev.pace.total
            normalized = 50.0 + (raw - mu) * expansion
            ev.pace.norm_adjustment = normalized - raw

    # ─── 能力偏差値の正規化 ───
    ability_totals = [ev.ability.total for ev in evaluations]
    if len(ability_totals) >= 2:
        mu = _stat.mean(ability_totals)
        sigma = _stat.pstdev(ability_totals) or 1.0
        # σ下限4.0、上限8.0、拡大倍率は最大1.5倍
        target_sigma = max(4.0, min(8.0, sigma))
        expansion = min(1.5, target_sigma / sigma)
        for ev in evaluations:
            raw = ev.ability.total
            normalized = 50.0 + (raw - mu) * expansion
            ev.ability.norm_adjustment = normalized - raw

    # ─── コース適性偏差値の正規化 ───
    course_totals = [ev.course.total for ev in evaluations]
    if len(course_totals) >= 2:
        mu = _stat.mean(course_totals)
        sigma = _stat.pstdev(course_totals) or 1.0
        target_sigma = max(4.0, min(8.0, sigma))
        expansion = min(1.5, target_sigma / sigma)
        for ev in evaluations:
            raw = ev.course.total
            normalized = 50.0 + (raw - mu) * expansion
            ev.course.norm_adjustment = normalized - raw


# ============================================================
# 騎手/調教師の偏差値を先行算出（composite 参照前に必要）
# ============================================================


def _compute_personnel_devs(evaluations: List[HorseEvaluation]) -> None:
    """
    騎手・調教師の偏差値を先行算出して各 HorseEvaluation にセットする [A4]。
    composite プロパティが参照される前に呼ぶ必要がある。
    _compute_detail_grades() で詳細グレードは後から算出するが、
    dev 値だけは先にセットして composite の補正項に使えるようにする。
    """
    for ev in evaluations:
        # デフォルト値を先にセット（jockey_stats/trainer_stats が無い場合に備える）
        if not hasattr(ev, "_jockey_dev"):
            ev._jockey_dev = None
        if not hasattr(ev, "_trainer_dev"):
            ev._trainer_dev = None
        if not hasattr(ev, "_bloodline_dev"):
            ev._bloodline_dev = None

        # 騎手偏差値
        if ev.jockey_stats and ev._jockey_dev is None:
            pop = getattr(ev.horse, "popularity", None)
            is_upper = pop is not None and pop <= 3
            jdev = ev.jockey_stats.get_deviation(is_upper)
            # 4象限すべて50.0はデフォルト（データ未取得） → None
            _all_default = (
                ev.jockey_stats.upper_long_dev == 50.0
                and ev.jockey_stats.upper_short_dev == 50.0
                and ev.jockey_stats.lower_long_dev == 50.0
                and ev.jockey_stats.lower_short_dev == 50.0
            )
            ev._jockey_dev = None if _all_default else round(max(30.0, min(70.0, jdev)), 1)

        # 調教師偏差値
        if ev.trainer_stats and ev._trainer_dev is None:
            dev = getattr(ev.trainer_stats, "deviation", None)
            if dev is not None:
                ev._trainer_dev = None if dev == 50.0 else round(max(30.0, min(70.0, dev)), 1)

        # bloodline_dev は _compute_detail_grades() で算出
        # （bloodline_db のロードが必要なため、ここでは省略）


# ============================================================
# 同一レース内の上3F ランク付与
# ============================================================


def _enrich_l3f_rank(
    evaluations: List[HorseEvaluation],
    course_db: Dict[str, List[PastRun]],
    std_calc=None,
) -> None:
    """
    各馬の run_records に対して、course_db から同一レースの全馬の上3Fを取得し、
    ランク（1=最速）を付与して run_records を (PastRun, dev, std_time, l3f_rank) に拡張する。
    同一レースの特定: course_id + race_date が一致し、finish_time_sec が 7秒以内のクラスタ。
    また全 past_run に対して race_level_dev を推定する。
    """
    from collections import defaultdict

    # course_id × race_date でインデックス化（上3F ランク用）
    idx: Dict[tuple, list] = defaultdict(list)
    if course_db:
        for cid, runs in course_db.items():
            if not isinstance(runs, list):
                continue
            for r in runs:
                idx[(cid, r.race_date)].append(r)

    for ev in evaluations:
        new_records = []
        for entry in ev.ability.run_records:
            if len(entry) >= 4:  # 既にランク付き
                new_records.append(entry)
                continue
            past_run, dev, std_time = entry[0], entry[1], entry[2]
            rank = _get_l3f_rank(past_run, idx)
            new_records.append((past_run, dev, std_time, rank))
        ev.ability.run_records = new_records

    # ---- 全PastRunにレースレベル偏差値を付与 ----
    _compute_race_level_devs(evaluations, idx, std_calc)


def _get_l3f_rank(run: PastRun, idx: Dict[Tuple[str, str], List[PastRun]]) -> int | None:
    """course_db インデックスから同一レースの 上3F 順位を算出"""
    if not run.last_3f_sec or run.last_3f_sec <= 0:
        return None

    candidates = idx.get((run.course_id, run.race_date), [])
    if not candidates:
        return None

    # 同一レースを finish_time_sec で特定（7秒以内のクラスタ）
    our_t = run.finish_time_sec
    race_group = [r for r in candidates if abs(r.finish_time_sec - our_t) <= 7.0]
    if not race_group:
        return None

    # 上3F が有効な値の一覧
    valid = [r.last_3f_sec for r in race_group if r.last_3f_sec and 28.0 <= r.last_3f_sec <= 50.0]
    if not valid:
        return None

    valid_sorted = sorted(valid)  # 昇順（小=速い=1位）
    our_l3f = run.last_3f_sec
    # 自分より速い馬の数 + 1 = 順位（同タイムは同順位）
    rank = sum(1 for t in valid_sorted if t < our_l3f) + 1
    return rank


def _compute_race_level_devs(
    evaluations: List[HorseEvaluation],
    idx: Dict[Tuple[str, str], List[PastRun]],
    std_calc=None,
) -> None:
    """
    各PastRunに race_level_dev（レースレベル偏差値）を設定する。

    算出方式:
      1. margin_ahead（前着差・秒換算）から勝ち馬タイムを逆算:
           winner_t = finish_time_sec - margin_ahead  (1着は margin_ahead=0)
      2. 基準タイムは run_records 内の std_time を優先。なければ std_calc で算出。
           course_db の日付インデックスは上3F ランク用途のみ（フォールバック）。
      3. race_level_dev = calc_run_deviation(winner_t, std_time, distance)
         → 値が大きいほど「勝ち馬が標準より速い = ハイレベル戦」
    """
    from src.calculator.ability import calc_run_deviation

    # ev.ability.run_records から race_date → std_time キャッシュを構築
    # (同一コース・馬場条件で同一距離の標準タイムを再利用)
    std_time_by_date: Dict[str, float] = {}
    for ev in evaluations:
        for entry in ev.ability.run_records:
            pr: PastRun = entry[0]
            st: Optional[float] = entry[2] if len(entry) >= 3 else None
            if st is not None and pr.race_date not in std_time_by_date:
                std_time_by_date[pr.race_date] = st

    def _get_std_time(run: PastRun) -> Optional[float]:
        """基準タイムを取得: run_records キャッシュ → std_calc の順で試みる"""
        # ① run_records からの直接キャッシュ
        st = std_time_by_date.get(run.race_date)
        if st is not None:
            return st
        # ② std_calc がある場合はその場で算出
        if std_calc is not None:
            try:
                st, _ = std_calc.calc_standard_time(
                    run.course_id,
                    run.grade or "",
                    run.condition or "良",
                    run.distance,
                )
                return st
            except Exception:
                pass
        # ③ course_db の日付インデックス（旧フォールバック）
        from config.settings import CONVERSION_CONSTANT, DISTANCE_BASE
        import statistics as _stats

        candidates = idx.get((run.course_id, run.race_date), [])
        if not candidates:
            return None
        top3_times = [r.finish_time_sec for r in candidates if r.finish_pos <= 3]
        if len(top3_times) < 3:
            return None
        return _stats.mean(top3_times)

    # race_key → race_level_dev キャッシュ（同一レース内で共通値を保証）
    race_cache: Dict[Tuple[str, str, int], Optional[float]] = {}

    for ev in evaluations:
        for run in ev.horse.past_runs:
            if run.finish_time_sec <= 0:
                run.race_level_dev = None
                continue

            rk = (run.course_id, run.race_date, round(run.finish_time_sec / 7.0))
            if rk in race_cache:
                run.race_level_dev = race_cache[rk]
                continue

            std_time = _get_std_time(run)
            if std_time is None:
                race_cache[rk] = None
                run.race_level_dev = None
                continue

            # 勝ち馬タイムを逆算: finish_time - margin_ahead
            # margin_ahead: 前着差(秒換算)。1着は 0、取得できない場合は 0 とみなす
            margin = run.margin_ahead if run.margin_ahead is not None else 0.0
            margin = max(0.0, min(margin, 10.0))  # 異常値クランプ
            winner_t = run.finish_time_sec - margin

            lvl = calc_run_deviation(winner_t, std_time, run.distance)
            lvl = max(20.0, min(80.0, lvl))

            race_cache[rk] = lvl
            run.race_level_dev = lvl


# ============================================================
# コース適性偏差値 (G-3 追加呼び出し)
# ============================================================


def enrich_course_aptitude_with_style_bias(
    engine: RaceAnalysisEngine,
    analysis: RaceAnalysis,
) -> RaceAnalysis:
    """
    G-3: 脚質バイアスをコース適性偏差値に追加反映
    (オーケストレーター後処理として呼ぶ)
    """
    for ev in analysis.evaluations:
        style_bias = calc_style_bias_for_course(
            ev.horse, analysis.race.course, engine.course_style_stats_db
        )
        # G-3はF-4に一本化(二重計上防止) → course_style_biasに格納するのはF-4
        # ここではコース適性の補足情報として _g3_style_bias に保持（compositeに影響しない）
        ev._g3_style_bias = style_bias

    # ── 全頭診断用グレード算出（非致命的 — 失敗しても分析結果は返す） ──
    try:
        _compute_detail_grades(engine, analysis)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("detail grades failed: %s", e)

    # _compute_detail_grades が _jockey_dev/_trainer_dev/_bloodline_dev を
    # 更新するため composite が変わる → 印を再割当て（印と composite の整合性を保証）
    from src.output.formatter import assign_marks as _assign_marks
    analysis.evaluations[:] = _assign_marks(analysis.evaluations)

    return analysis


def _compute_detail_grades(engine: RaceAnalysisEngine, analysis: RaceAnalysis):
    """全頭診断用の詳細グレードを算出して各 HorseEvaluation に付与する"""
    from src.calculator.grades import (
        compute_profile_grades,
        compute_jockey_detail_grades,
        compute_trainer_detail_grades,
        compute_bloodline_detail_grades,
        compute_course_detail_grades,
        compute_pace_extras,
        compute_ability_extras,
        dev_to_grade,
    )

    race_info = analysis.race
    all_courses = engine.all_courses

    # bloodline_db: キャッシュファイルをベースとし、過去走データで補完
    bloodline_db = {"sire": {}, "bms": {}}
    try:
        import json
        from config.settings import BLOODLINE_DB_PATH
        if BLOODLINE_DB_PATH and os.path.exists(BLOODLINE_DB_PATH):
            with open(BLOODLINE_DB_PATH, "r", encoding="utf-8") as f:
                bloodline_db = json.load(f)
    except Exception:
        pass
    # 過去走データから再構築し、キャッシュにないエントリを追加
    try:
        from src.scraper.improvement_dbs import build_bloodline_db as _build_bl
        _horses_for_bl = [ev.horse for ev in analysis.evaluations]
        if _horses_for_bl:
            _built = _build_bl(_horses_for_bl, netkeiba_client=None, cache_path=None)
            if _built:
                for cat in ("sire", "bms"):
                    for k, v in _built.get(cat, {}).items():
                        if k not in bloodline_db.get(cat, {}):
                            bloodline_db.setdefault(cat, {})[k] = v
    except Exception:
        pass

    # sire_name→sire_id マッピングをロード（sire_idが空の馬の血統ルックアップ用）
    _sire_name_map: Dict[str, str] = {}
    _bms_name_map: Dict[str, str] = {}
    try:
        _name_map_path = os.path.join(os.path.dirname(BLOODLINE_DB_PATH), "bloodline", "sire_name_to_id.json")
        if not os.path.exists(_name_map_path):
            _name_map_path = os.path.join(os.path.dirname(BLOODLINE_DB_PATH), "sire_name_to_id.json")
        if not os.path.exists(_name_map_path):
            from config.settings import DATA_DIR
            _name_map_path = os.path.join(DATA_DIR, "bloodline", "sire_name_to_id.json")
        if os.path.exists(_name_map_path):
            with open(_name_map_path, "r", encoding="utf-8") as f:
                _nm = json.load(f)
            _sire_name_map = _nm.get("sire", {})
            _bms_name_map = _nm.get("bms", {})
    except Exception:
        pass

    # 実力人気順: predicted_tansho_oddsでソートして順位付け
    pred_odds_list = []
    for ev in analysis.evaluations:
        po = getattr(ev, "predicted_tansho_odds", None)
        pred_odds_list.append((ev.horse.horse_no, po if po else 999.9))
    pred_odds_list.sort(key=lambda x: x[1])
    pred_rank_map = {hno: rank + 1 for rank, (hno, _) in enumerate(pred_odds_list)}

    for ev in analysis.evaluations:
        h = ev.horse
        pop = h.popularity

        # sire_id/mgs_id が空の場合、名前マッピングから解決
        _h_sire_id = getattr(h, "sire_id", None) or ""
        _h_mgs_id = getattr(h, "maternal_grandsire_id", None) or ""
        _h_sire_name = getattr(h, "sire", None) or ""
        _h_mgs_name = getattr(h, "maternal_grandsire", None) or ""
        if not _h_sire_id and _h_sire_name:
            import re as _re
            _clean_sire = _re.sub(r'[A-Za-z].*$', '', _h_sire_name.split('(')[0].split('（')[0].strip()).strip()
            _h_sire_id = _sire_name_map.get(_clean_sire) or _sire_name_map.get(_h_sire_name) or ""
        if not _h_mgs_id and _h_mgs_name:
            import re as _re
            _clean_mgs = _re.sub(r'[A-Za-z].*$', '', _h_mgs_name.split('(')[0].split('（')[0].strip()).strip()
            _h_mgs_id = _bms_name_map.get(_clean_mgs) or _bms_name_map.get(_h_mgs_name) or ""

        # プロフィールグレード
        profile = compute_profile_grades(
            ev.jockey_stats, ev.trainer_stats, ev.ability,
            horse_popularity=pop,
            bloodline_db=bloodline_db,
            sire_id=_h_sire_id or None,
            mgs_id=_h_mgs_id or None,
            sire_name=_h_sire_name or None,
            mgs_name=_h_mgs_name or None,
        )
        ev._jockey_grade = profile["jockey_grade"]
        ev._trainer_grade = profile["trainer_grade"]
        ev._sire_grade = profile["sire_grade"]
        ev._mgs_grade = profile["mgs_grade"]
        # 数値（偏差値） — 30-70クランプ
        def _clamp_dev(v):
            return round(max(30.0, min(70.0, v)), 1) if v is not None else None
        ev._jockey_dev = _clamp_dev(profile.get("jockey_dev"))
        ev._trainer_dev = _clamp_dev(profile.get("trainer_dev"))
        ev._sire_dev = _clamp_dev(profile.get("sire_dev"))
        ev._mgs_dev = _clamp_dev(profile.get("mgs_dev"))
        ev._bloodline_dev = _clamp_dev(profile.get("bloodline_dev"))

        # 実力人気順
        ev._predicted_rank = pred_rank_map.get(h.horse_no)

        # 能力追加
        ability_extras = compute_ability_extras(
            h,
            training_records=ev.training_records,
            ability_trend=ev.ability.trend.value if ev.ability.trend else None,
        )
        ev._popularity_trend = ability_extras["popularity_trend"]
        ev._condition_signal = ability_extras["condition_signal"]

        # 展開追加
        pace_extras = compute_pace_extras(
            analysis.evaluations,
            h.horse_no,
            getattr(h, "gate_no", 0) or 0,
        )
        ev._gate_neighbors = pace_extras["gate_neighbors"]
        # 相対位置(0-1) → 実番手に変換（field_count=頭数）
        _fc = len(analysis.evaluations)
        ev._estimated_pos_1c = None
        if ev.pace.estimated_position_4c is not None and _fc > 0:
            ev._estimated_pos_1c = round(ev.pace.estimated_position_4c * 0.85 * _fc + 1, 1)
        ev._estimated_last3f_rank = pace_extras["estimated_last3f_rank"]
        ev._last3f_grade = pace_extras["last3f_grade"]

        # コース適性詳細グレード
        ev._course_detail_grades = compute_course_detail_grades(
            ev.course, race_info, all_courses,
            past_runs=getattr(h, "past_runs", None),
        )

        # 騎手詳細グレード
        ev._jockey_detail_grades = compute_jockey_detail_grades(
            ev.jockey_stats, race_info, all_courses,
            horse_popularity=pop,
            trainer_stats=ev.trainer_stats,
            running_style=getattr(ev.pace, "running_style", None),
            gate_no=getattr(h, "gate_no", 0) or 0,
            field_count=len(analysis.evaluations),
        )

        # 調教師詳細グレード
        ev._trainer_detail_grades = compute_trainer_detail_grades(
            ev.trainer_stats, race_info, all_courses,
            jockey_id=getattr(h, "jockey_id", None),
        )

        # 血統詳細グレード
        _jockey_dev = None
        if ev.jockey_stats:
            _is_upper = pop is not None and pop <= 3
            _raw_jdev = ev.jockey_stats.get_deviation(_is_upper)
            _jockey_dev = max(30.0, min(70.0, _raw_jdev)) if _raw_jdev is not None else None
        ev._bloodline_detail_grades = compute_bloodline_detail_grades(
            bloodline_db,
            _h_sire_id or None,
            _h_mgs_id or None,
            race_info,
            all_courses=all_courses,
            jockey_dev=_jockey_dev,
            sire_name=_h_sire_name or None,
            mgs_name=_h_mgs_name or None,
        )
