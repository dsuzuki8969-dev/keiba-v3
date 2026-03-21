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
    calc_shobu_score,
    calc_tokusen_kiken_score,
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
    classify_field_styles,
    normalize_field_positions,
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
from data.masters.venue_master import is_banei


# ── target_date 単位のDBキャッシュ（バッチ高速化） ──
_DB_CACHE_DATE = None
_DB_CACHE_PACE = None
_DB_CACHE_L3F_SIGMA = None
_DB_CACHE_GATE_BIAS = None

# ── NAR公式ID → netkeiba ID マッピング（プロセス内キャッシュ） ──
_CACHE_NAR_ID_MAP: Optional[dict] = None
_CACHE_NAR_ID_MAP_LOADED: bool = False


def _calc_banei_aptitude(horse, race) -> dict:
    """
    ばんえい適性スコアを計算する。
    PaceDeviationの各フィールド範囲に合わせたスコアを返す。
    - burden: 斤量負担率スコア (-8〜+8)
    - moisture: 馬場水分適性スコア (-8〜+8)
    - time_cv: タイム安定性スコア (-5〜+5)
    - weight_trend: 斤量トレンドスコア (-4〜+4)
    """
    import numpy as np
    from data.masters.venue_master import is_banei as _is_banei_check

    result = {"burden": 0.0, "moisture": 0.0, "time_cv": 0.0, "weight_trend": 0.0}
    past_runs = horse.past_runs or []
    # ばんえいの過去走のみ抽出（帯広のみ）
    banei_runs = [r for r in past_runs if _is_banei_check(r.course_id[:2] if r.course_id else "")]
    if not banei_runs:
        return result

    # ---- ① 斤量負担率スコア (-8〜+8) ----
    # weight_kg / horse_weight が低いほど有利
    wk = horse.weight_kg
    hw = horse.horse_weight
    if wk and hw and hw > 0:
        ratio = wk / hw
        # ばんえい典型: 0.55〜0.95。0.70が中央値。低い=有利
        # 偏差: 0.60 → +6, 0.70 → 0, 0.80 → -6, 0.90 → -12 → clamp
        result["burden"] = max(-8.0, min(8.0, (0.70 - ratio) * 60.0))

    # ---- ② 馬場水分適性スコア (-8〜+8) ----
    # 当日の水分量と過去走の水分量帯別成績の相性
    moisture = race.moisture_dirt
    if moisture is not None and len(banei_runs) >= 2:
        # 過去走を水分量帯で分類し、近い帯での成績を評価
        # 帯分類: 乾燥(≤1.5), 標準(1.5-2.5), 重め(2.5-3.5), 泥深め(>3.5)
        def _water_band(cond: str) -> int:
            # ばんえいでは condition が水分量情報を持たないため、
            # 良=乾燥寄り、稍重=標準、重=重め、不良=泥深めとマッピング
            return {"良": 0, "稍重": 1, "重": 2, "不良": 3}.get(cond, 1)

        if moisture <= 1.5:
            today_band = 0
        elif moisture <= 2.5:
            today_band = 1
        elif moisture <= 3.5:
            today_band = 2
        else:
            today_band = 3

        # 同じ帯での好走率
        same_band = [r for r in banei_runs if _water_band(r.condition) == today_band]
        diff_band = [r for r in banei_runs if _water_band(r.condition) != today_band]
        if same_band:
            same_rate = sum(1 for r in same_band if r.finish_pos <= 3) / len(same_band)
            diff_rate = sum(1 for r in diff_band if r.finish_pos <= 3) / len(diff_band) if diff_band else 0.3
            # 同帯の好走率が全体より高ければプラス
            result["moisture"] = max(-8.0, min(8.0, (same_rate - diff_rate) * 20.0))

    # ---- ③ タイム安定性スコア (-5〜+5) ----
    # 走破タイムのCV（変動係数）が低い = 安定 = プラス
    times = [r.finish_time_sec for r in banei_runs if r.finish_time_sec and r.finish_time_sec > 0]
    if len(times) >= 3:
        mean_t = np.mean(times)
        std_t = np.std(times)
        cv = std_t / mean_t if mean_t > 0 else 0
        # CV典型: 0.05(安定) 〜 0.15(不安定)。0.10が基準
        result["time_cv"] = max(-5.0, min(5.0, (0.10 - cv) * 50.0))

    # ---- ④ 斤量トレンドスコア (-4〜+4) ----
    # 直近3走の斤量変化。上がっている=クラスアップ=厳しい=マイナス
    recent_weights = [r.weight_kg for r in banei_runs[:3] if r.weight_kg]
    if len(recent_weights) >= 2:
        # recent_weights[0]が最新走
        trend = recent_weights[0] - recent_weights[-1]
        # +20kg → -4（大幅増量=厳しい）、-20kg → +4（大幅減量=有利）
        result["weight_trend"] = max(-4.0, min(4.0, -trend * 0.2))

    return result


def _load_nar_id_map() -> dict:
    """NAR公式ID→netkeiba IDマッピングをロード（初回のみ）"""
    global _CACHE_NAR_ID_MAP, _CACHE_NAR_ID_MAP_LOADED
    if _CACHE_NAR_ID_MAP_LOADED:
        return _CACHE_NAR_ID_MAP or {}
    _CACHE_NAR_ID_MAP_LOADED = True
    try:
        import json as _json
        _map_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "nar_id_map.json")
        if os.path.exists(_map_path):
            with open(_map_path, "r", encoding="utf-8") as f:
                _CACHE_NAR_ID_MAP = _json.load(f)
            _j = len(_CACHE_NAR_ID_MAP.get("jockey", {}))
            _t = len(_CACHE_NAR_ID_MAP.get("trainer", {}))
            logger.info("NAR IDマッピングロード: 騎手 %d件, 調教師 %d件", _j, _t)
        else:
            _CACHE_NAR_ID_MAP = {}
            logger.debug("nar_id_map.json 未作成 → マッピングなし")
    except Exception as e:
        logger.warning("NAR IDマッピングロード失敗: %s", e)
        _CACHE_NAR_ID_MAP = {}
    return _CACHE_NAR_ID_MAP


# ── NAR騎手race_logベース偏差値キャッシュ ──
_CACHE_RL_JOCKEY_DEV: Dict[str, Optional[float]] = {}


def _race_log_jockey_dev(jockey_id: str, jockey_name: str) -> Optional[float]:
    """race_logから騎手の勝率ベース偏差値を計算（NAR騎手フォールバック用）

    NAR全体の平均勝率/σとの比較で偏差値化する。
    最低出走数: 10走。キャッシュ済み。
    """
    _cache_key = f"{jockey_id}_{jockey_name}"
    if _cache_key in _CACHE_RL_JOCKEY_DEV:
        return _CACHE_RL_JOCKEY_DEV[_cache_key]

    result = None
    try:
        import sqlite3 as _sql3
        _conn = _sql3.connect("data/keiba.db")
        # 騎手名でrace_logを検索（IDが異なる場合もあるため名前ベース）
        _rows = _conn.execute(
            "SELECT finish_pos, field_count FROM race_log "
            "WHERE jockey_name = ? AND venue_code IN "
            "('30','35','36','42','43','44','45','46','47','48','50','51','54','55')",
            (jockey_name,)
        ).fetchall()
        _conn.close()

        if len(_rows) >= 10:
            _wins = sum(1 for pos, _ in _rows if pos == 1)
            _places = sum(1 for pos, fc in _rows if pos <= max(3, fc // 3 + 1))
            _wr = _wins / len(_rows)
            _pr = _places / len(_rows)
            # NAR全体の平均値で偏差値化
            _mean = 0.27  # JOCKEY_BASE_PARAMS_NAR["mean"] = 複勝率平均
            _sigma = 0.16
            result = round(50.0 + (_pr - _mean) / _sigma * 10.0, 1)
            result = max(30.0, min(70.0, result))
    except Exception:
        pass

    _CACHE_RL_JOCKEY_DEV[_cache_key] = result
    return result


# ── NAR調教師race_logベース偏差値キャッシュ ──
_CACHE_RL_TRAINER_DEV: Dict[str, Optional[float]] = {}


def _race_log_trainer_dev(trainer_id: str, trainer_name: str) -> Optional[float]:
    """race_logから調教師の勝率ベース偏差値を計算（NAR調教師フォールバック用）"""
    _cache_key = f"{trainer_id}_{trainer_name}"
    if _cache_key in _CACHE_RL_TRAINER_DEV:
        return _CACHE_RL_TRAINER_DEV[_cache_key]

    result = None
    try:
        import sqlite3 as _sql3
        _conn = _sql3.connect("data/keiba.db")
        _rows = _conn.execute(
            "SELECT finish_pos, field_count FROM race_log "
            "WHERE trainer_name = ? AND venue_code IN "
            "('30','35','36','42','43','44','45','46','47','48','50','51','54','55')",
            (trainer_name,)
        ).fetchall()
        _conn.close()

        if len(_rows) >= 10:
            _places = sum(1 for pos, fc in _rows if pos <= max(3, fc // 3 + 1))
            _pr = _places / len(_rows)
            # TRAINER_BASE_PARAMS_NAR と同じ基準
            _mean = 0.27
            _sigma = 0.16
            result = round(50.0 + (_pr - _mean) / _sigma * 10.0, 1)
            result = max(30.0, min(70.0, result))
    except Exception:
        pass

    _CACHE_RL_TRAINER_DEV[_cache_key] = result
    return result


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
            4: (0.35, 0.65),   # 競馬場専用 (高精度) → ML 65%（H8: 55→65）
            3: (0.42, 0.58),   # JRA馬場×SMILE → ML 58%（H8: 45→58）
            2: (0.55, 0.45),   # JRA全体/NAR → ML 45%（H8: 40→45）
            1: (0.65, 0.35),   # 馬場全体 → ML 35%（据え置き）
            0: (0.75, 0.25),   # globalモデル (最低精度) → ML 25%（H8: 30→25 控えめに）
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
        _is_banei = is_banei(race.course.venue_code if race.course else "")

        course_db_ref = self.std_calc.course_db

        # ---- 性齢定量: base_weight_kg を補完 ----
        if not _is_banei:
            # ばんえいの斤量は600-1000kgで通常競馬の55kg基準と無関係
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

        # ---- Step 0: フィールド脚質分布を制約付きで一括分類 ----
        if _is_banei:
            # ばんえい: 全馬先行扱い（200m直線・脚質概念なし）
            field_styles = {h.horse_no: (RunningStyle.SENKOU, 0.0) for h in horses}
        else:
            _jockey_pos_cache = None
            _pos_pred = getattr(self.pace_dev_calc, 'position_predictor', None)
            if _pos_pred:
                _jockey_pos_cache = getattr(_pos_pred, '_jockey_cache', None)
            field_styles = classify_field_styles(
                horses, past_runs_map, race.course,
                jockey_cache=_jockey_pos_cache,
            )

        # ---- Step 1: ペース予測 ----
        if _is_banei:
            # ばんえい: ペース予測スキップ（一定ペース）
            pace_type = PaceType.MM
            pace_score = 50.0
            leaders = []
            front_rate = 0.5
            max_escape_strength = 0.0
        else:
            pace_type, pace_score, leaders, front_rate, max_escape_strength = self.pace_predictor.predict_pace(
                horses, past_runs_map, race.course,
                course_pace_tendency=self.course_pace_tendency,
                field_styles=field_styles,
            )
        pace_reliability = self._judge_pace_reliability(pace_score, len(leaders), len(horses))

        # ---- ペース文脈 (ML推定用) ----
        # field_styles から逃げ・先行の数を取得（_classify_styleの重複呼び出しを解消）
        n_front = sum(1 for _, (s, _) in field_styles.items()
                      if s.value in ("逃げ", "先行"))
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
        if _is_banei:
            # ばんえい: 上がり3F/コーナー分析スキップ
            _baseline_from_db = 50.0  # 基準偏差値
            _field_baseline = 50.0
            # 位置取りは馬番順の均等配置
            _normalized_positions = {
                h.horse_no: (i + 1) / (len(horses) + 1)
                for i, h in enumerate(horses)
            }
        else:
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

            # ---- 2パス位置取り正規化: 全馬のest_positionを事前計算 ----
            _pos_predictor = getattr(self.pace_dev_calc, 'position_predictor', None)
            _raw_positions: Dict[int, float] = {}
            for horse in horses:
                _est = None
                if _pos_predictor and _pos_predictor.is_available:
                    _est = _pos_predictor.predict(horse, race, pace_context)
                if _est is None:
                    # field_stylesから脚質を取得してルールベース推定
                    _fs = field_styles.get(horse.horse_no)
                    _style = _fs[0] if _fs else RunningStyle.SENKOU
                    _est = self.pace_dev_calc._estimate_position(_style, pace_type)
                _raw_positions[horse.horse_no] = _est
            # フィールド内正規化
            _normalized_positions = normalize_field_positions(_raw_positions, len(horses))

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
                override_position=_normalized_positions.get(horse.horse_no),
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
                field_count=len(evaluations),
                is_jra=self.is_jra,
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

        # 脚質別グループ分け (レベル1表示用) — 正規化後の位置から導出

        if _is_banei:
            # ばんえい: 脚質グループ・ペースコメント・3F推定を全面スキップ
            leading = []
            front_h = []
            mid_h = []
            rear_h = []
            _style_map: Dict[int, RunningStyle] = {}
            for ev in evaluations:
                ev._normalized_position = 0.5
                if ev.pace:
                    ev.pace.running_style = None  # 脚質なし
            front_3f_est = None
            last_3f_est = None
            from src.calculator.calibration import generate_banei_comment
            pace_comment = generate_banei_comment(race, evaluations)
            favorable_gate = ""
            favorable_style = ""
            favorable_style_reason = ""
        else:
            # 正規化後の位置から脚質を再導出（順位ベース: ML予測と整合させる）
            _sorted_by_pos = sorted(_normalized_positions.items(), key=lambda x: x[1])
            _n = len(_sorted_by_pos)
            _final_style_map: Dict[int, RunningStyle] = {}
            for rank, (hno, _) in enumerate(_sorted_by_pos):
                r = rank / _n if _n > 1 else 0.0  # 0.0=先頭
                if rank == 0:
                    _final_style_map[hno] = RunningStyle.NIGASHI
                elif r <= 0.25:
                    _final_style_map[hno] = RunningStyle.SENKOU
                elif r <= 0.42:
                    _final_style_map[hno] = RunningStyle.KOUI
                elif r <= 0.58:
                    _final_style_map[hno] = RunningStyle.CHUUDAN
                elif r <= 0.78:
                    _final_style_map[hno] = RunningStyle.SASHIKOMI
                else:
                    _final_style_map[hno] = RunningStyle.OIKOMI
            _style_map = _final_style_map
            leading = [no for no, s in _style_map.items() if s == RunningStyle.NIGASHI]
            front_h = [no for no, s in _style_map.items() if s in (RunningStyle.SENKOU, RunningStyle.KOUI)]
            mid_h = [no for no, s in _style_map.items() if s in (RunningStyle.CHUUDAN, RunningStyle.SASHIKOMI)]
            rear_h = [no for no, s in _style_map.items() if s == RunningStyle.OIKOMI]

            for ev in evaluations:
                # 初角位置スコアを保存（展開ビジュアル表示用）
                ev._normalized_position = _normalized_positions.get(ev.horse.horse_no)
                if ev.horse.horse_no in _style_map and ev.pace:
                    _rs = _style_map[ev.horse.horse_no]
                    # 表示用は4分類に正規化（好位→先行、中団→差し）
                    if _rs == RunningStyle.KOUI:
                        _rs = RunningStyle.SENKOU
                    elif _rs == RunningStyle.CHUUDAN:
                        _rs = RunningStyle.SASHIKOMI
                    ev.pace.running_style = _rs

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

        # ---- 騎手/調教師/血統の偏差値を先行算出 [A4 + Phase 12 ハイブリッド] ----
        # composite プロパティが参照される前に _jockey_dev / _trainer_dev / _bloodline_dev をセットする
        # rolling tracker が利用可能ならファクター加重平均で算出（ハイブリッド方式）
        _rolling_tracker = None
        _sire_rolling_tracker = None
        if self._lgbm_predictor:
            _rolling_tracker = self._lgbm_predictor.tracker
            _sire_rolling_tracker = self._lgbm_predictor.sire_tracker
        _compute_personnel_devs(evaluations, race, _rolling_tracker, _sire_rolling_tracker)

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

        # ---- 診断用: LightGBM生値を退避 ----
        for ev in evaluations:
            ev._raw_lgbm_prob = ev.ml_win_prob

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

        # ---- 診断用: アンサンブル後のML値を退避 ----
        for ev in evaluations:
            ev._ensemble_prob = ev.ml_win_prob

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

        # ---- 診断用: ML+Ruleブレンド後の値を退避 ----
        for ev in evaluations:
            ev._ml_rule_prob = ev.win_prob

        # ---- 確率の正規化（ML有無に関わらず常に実施）----
        # 勝率: Σwin_prob = 1.0 (100%)
        # 連対率: Σplace2_prob = 2.0 (200%)
        # 複勝率: Σplace3_prob = 3.0 (300%)
        _normalize_probs(evaluations)

        # ---- Step 5.5: 削除済み（能力偏差値は絶対評価、LGBMフィードバック不要） ----

        # ---- Step 5.6: ML予測確率 → composite 直接反映 ----
        # win_prob（MLブレンド済み）を偏差値スケールに変換し、composite に加算
        # これにより印（composite順）がML予測を反映するようになる
        try:
            _win_probs = [ev.win_prob for ev in evaluations]
            _n_wp = len(_win_probs)
            if _n_wp >= 3:
                _avg_wp = sum(_win_probs) / _n_wp
                _std_wp = (sum((p - _avg_wp) ** 2 for p in _win_probs) / _n_wp) ** 0.5
                if _std_wp > 0.001:
                    for ev in evaluations:
                        # Z変換: (win_prob - 平均) / 標準偏差 → 偏差値スケール
                        _z = (ev.win_prob - _avg_wp) / _std_wp
                        # ±2.5pt にクランプ（穴馬寄り傾向を抑制: 旧±6pt→±3.5pt→±2.5pt）
                        _raw_adj = max(-2.5, min(2.5, _z * 1.0))
                        # 高オッズ馬のML補正をダンピング（穴馬のcomposite逆転を抑制）
                        _odds = getattr(ev.horse, "odds", None) or getattr(ev.horse, "tansho_odds", None)
                        if _odds is not None and _odds >= 30.0 and _raw_adj > 0:
                            _raw_adj *= 0.3  # 30倍超は70%減
                        elif _odds is not None and _odds >= 15.0 and _raw_adj > 0:
                            _raw_adj *= 0.5  # 15倍超は50%減
                        ev.ml_composite_adj = _raw_adj
        except Exception:
            pass

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
            # 高オッズ馬のodds_consistency正補正を抑制（穴馬のcomposite逆転防止）
            _ev_odds = ev.horse.odds
            if _ev_odds is not None and _ev_odds >= 30.0 and new_adj > 0:
                new_adj *= 0.3
            elif _ev_odds is not None and _ev_odds >= 15.0 and new_adj > 0:
                new_adj *= 0.5
            ev.odds_consistency_adj = 0.3 * new_adj + 0.7 * ev.odds_consistency_adj

        # ---- 市場アンカー: オッズから市場偏差値を推定し、compositeを微補正 ----
        # 穴馬がcomposite上位に躍り出るのを抑制しつつ、モデルの独自評価を尊重
        import math as _math_anchor
        _fc = race.field_count or len(evaluations) or 1
        _fair_prob = 1.0 / _fc  # 均等確率
        for ev in evaluations:
            _o = ev.horse.odds
            if _o is not None and _o > 1.0:
                _mp = 1.0 / _o  # 市場確率
                # 市場確率 vs 均等確率のlog比を偏差値スケールに変換
                _log_ratio = _math_anchor.log(max(_mp, 0.005) / _fair_prob)
                # ±3pt にクランプ（人気馬: +, 穴馬: -）
                ev.market_anchor_adj = max(-3.0, min(3.0, _log_ratio * 1.5))

        # ---- 正規化後の三連率再推定 [A2] ----
        # MLブレンド済み確率を退避し、再推定結果との加重平均で最終値を決定
        # （再推定がMLブレンド結果を完全に上書きしないようにする）
        all_composites_post = [ev.composite for ev in evaluations]
        all_pace_post = [ev.pace.total for ev in evaluations]
        all_course_post = [ev.course.total for ev in evaluations]

        # Phase 2-1: model_level依存の再推定比率（高精度モデルほどML情報を保持）
        from config.settings import PIPELINE_V2_ENABLED, REEST_RATIO_BY_LEVEL, REEST_RATIO_DEFAULT
        if PIPELINE_V2_ENABLED:
            _reest_ratio = REEST_RATIO_BY_LEVEL.get(_lgbm_level, REEST_RATIO_DEFAULT)
        else:
            _reest_ratio = REEST_RATIO_DEFAULT
        _ml_keep = 1.0 - _reest_ratio

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
                field_count=len(evaluations),
                is_jra=self.is_jra,
            )
            # Phase 2-1: model_level依存の再推定比率
            ev.win_prob    = _ml_keep * _ml_blended_win + _reest_ratio * _w
            ev.place2_prob = _ml_keep * _ml_blended_p2  + _reest_ratio * _t2
            ev.place3_prob = _ml_keep * _ml_blended_p3  + _reest_ratio * _t3

        # 確率正規化（Phase 2-3: 再推定比率が小さい場合はスキップして情報損失を削減）
        if not PIPELINE_V2_ENABLED or _reest_ratio >= 0.10:
            _normalize_probs(evaluations)

        # ---- 診断用: 人気統計ブレンド前の値を退避 + モデルレベル保存 ----
        for ev in evaluations:
            ev._pre_pop_prob = ev.win_prob
            ev._model_level = _lgbm_level

        # ---- Step 5b: 人気別実績統計ブレンド ----
        if self._pop_stats and any(
            getattr(ev.horse, "popularity", None) for ev in evaluations
        ):
            blend_probabilities(
                evaluations, race.venue, self.is_jra,
                len(evaluations), self._pop_stats,
                model_level=_lgbm_level,
            )
            _normalize_probs(evaluations)

        # ---- Step 6: 穴馬スコア（正規化後の composite 使用）[A3] ----
        for ev in evaluations:
            ev.ana_score, ev.ana_type = calc_ana_score(ev, evaluations)

        # ---- Step 6b: 特選穴馬スコア（composite上位5頭を除外 → ☆候補のみ）----
        from config.settings import TOKUSEN_SCORE_THRESHOLD, TOKUSEN_MAX_PER_RACE
        for ev in evaluations:
            ev.tokusen_score = calc_tokusen_score(ev, evaluations)
        # composite上位5頭を除外（既に◉◎○▲△★が付くため☆不要）
        top5_ids = {ev.horse.horse_id for ev in sorted(
            evaluations, key=lambda e: e.composite, reverse=True
        )[:5]}
        tokusen_candidates = sorted(
            [ev for ev in evaluations
             if ev.tokusen_score >= TOKUSEN_SCORE_THRESHOLD
             and ev.horse.horse_id not in top5_ids],
            key=lambda e: e.tokusen_score, reverse=True,
        )
        for ev in tokusen_candidates[:TOKUSEN_MAX_PER_RACE]:
            ev.is_tokusen = True

        # ---- Step 6c: 特選危険馬スコア（印体系とは独立）----
        from config.settings import TOKUSEN_KIKEN_SCORE_THRESHOLD, TOKUSEN_KIKEN_MAX_PER_RACE
        for ev in evaluations:
            ev.tokusen_kiken_score = calc_tokusen_kiken_score(ev, evaluations, is_jra=self.is_jra)
        tokusen_kiken_candidates = sorted(
            [ev for ev in evaluations if ev.tokusen_kiken_score >= TOKUSEN_KIKEN_SCORE_THRESHOLD],
            key=lambda e: e.tokusen_kiken_score, reverse=True,
        )
        for ev in tokusen_kiken_candidates[:TOKUSEN_KIKEN_MAX_PER_RACE]:
            ev.is_tokusen_kiken = True

        # ---- Step 8: 印付け（composite 降順で ◉◎○▲△★ + 穴馬☆/危険馬×）----
        evaluations = assign_marks(evaluations, is_jra=self.is_jra)

        # ---- Step 9: 買い目（Step 10bでフォーメーション生成後にallocate）----
        tickets = []

        # ---- Step 11: 予想オッズ算出（フォーメーション前に実施）----
        assign_divergence_to_evaluations(evaluations, self.is_jra)
        predicted_umaren = calc_predicted_umaren(evaluations, race)
        predicted_sanrenpuku = calc_predicted_sanrenpuku(evaluations, race)

        # K-5: 自信度判定（predicted_umaren/sanrenpuku を渡してML出現率を考慮）
        confidence = judge_confidence(
            evaluations, pace_reliability,
            predicted_umaren=predicted_umaren,
            predicted_sanrenpuku=predicted_sanrenpuku,
            is_jra=self.is_jra,
            is_banei=_is_banei,
        )

        # ---- Step 10b: フォーメーション買い目生成（表示廃止、formation構造のみ保持） ----
        formation = generate_formation_tickets(evaluations, race, confidence.value)
        tickets = []
        total_budget = 0

        has_any_odds = any(ev.horse.odds is not None for ev in evaluations)
        is_pre_day = not has_any_odds

        value_bets = []
        if has_any_odds:
            value_bets = detect_value_bets(
                evaluations, race, predicted_umaren, predicted_sanrenpuku
            )

        # ---- 走破タイム推定 ----
        if _is_banei:
            predicted_race_time = None  # ばんえいは走破タイム推定不可
        else:
            predicted_race_time = self._estimate_race_time(
                race, pace_type, front_3f_est, last_3f_est
            )

            # 1200m: 前半3F(600m) + 後半3F(600m) = 全走破タイム の整合性保証
            if (front_3f_est and last_3f_est and race.course
                    and race.course.distance == 1200):
                predicted_race_time = round(front_3f_est + last_3f_est, 1)

        # ---- 最終隊列予測 ----
        if _is_banei:
            final_formation = None
        else:
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
            confidence_score=_calc_confidence_score(evaluations, is_jra=self.is_jra, is_banei=_is_banei),
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
        override_position: Optional[float] = None,
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
        _is_banei_race = is_banei(race.course.venue_code if race.course else "")
        if _is_banei_race:
            # ばんえい: 通常のペース分析をスキップし、ばんえい固有の適性スコアで代替
            from src.models import PaceDeviation
            style_info = {"running_style": RunningStyle.SENKOU, "last3f_type": "安定中位末脚"}
            banei_scores = _calc_banei_aptitude(horse, race)
            ev.pace = PaceDeviation(
                base_score=50.0,
                last3f_eval=banei_scores["burden"],        # 斤量負担率スコア
                position_balance=banei_scores["moisture"],  # 馬場水分適性スコア
                course_style_bias=banei_scores["time_cv"],  # タイム安定性スコア
                jockey_pace=banei_scores["weight_trend"],   # 斤量トレンドスコア
                running_style=RunningStyle.SENKOU,
                gate_bias=gate_bias,
            )
        else:
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
                override_position=override_position,
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

    # ─── 能力偏差値は絶対評価（正規化しない） ───
    # 走破偏差値ベースの実力値をそのまま表示する

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


def _compute_personnel_devs(
    evaluations: List[HorseEvaluation],
    race: Optional["RaceInfo"] = None,
    rolling_tracker=None,
    sire_rolling_tracker=None,
) -> None:
    """
    騎手・調教師・血統の偏差値を先行算出して各 HorseEvaluation にセットする [A4 + Phase 12]。
    composite プロパティが参照される前に呼ぶ必要がある。

    rolling_tracker が利用可能な場合は全ファクターの加重平均で偏差値を算出（ハイブリッド方式）。
    利用不可の場合は personnel_db のフォールバック値を使用。
    """
    from src.calculator.grades import compute_category_deviation
    from config.settings import (
        JOCKEY_FACTOR_WEIGHTS, TRAINER_FACTOR_WEIGHTS,
        SIRE_FACTOR_WEIGHTS, BMS_FACTOR_WEIGHTS,
        JOCKEY_BASE_PARAMS_JRA, JOCKEY_BASE_PARAMS_NAR,
        TRAINER_BASE_PARAMS_JRA, TRAINER_BASE_PARAMS_NAR,
        SIRE_BASE_PARAMS, BMS_BASE_PARAMS,
    )

    # レース情報からコンテキスト取得
    venue = ""
    surface = ""
    distance = 0
    condition = "良"
    is_jra = True
    if race:
        venue = race.venue or ""
        surface = race.course.surface if race.course else ""
        distance = race.course.distance if race.course else 0
        if surface == "芝" and race.track_condition_turf:
            condition = race.track_condition_turf
        elif race.track_condition_dirt:
            condition = race.track_condition_dirt
        is_jra = race.is_jra

    # SMILE区分
    def _smile_cat(dist):
        if dist <= 1000: return "ss"
        if dist <= 1400: return "s"
        if dist <= 1800: return "m"
        if dist <= 2200: return "i"
        if dist <= 2600: return "l"
        return "e"
    smile = _smile_cat(distance) if distance else ""

    # 距離帯（_EntityStats の dist_cat dict キーと一致させる）
    def _dist_cat(dist):
        if dist <= 1400: return "sprint"
        if dist <= 1800: return "mile"
        if dist <= 2200: return "middle"
        return "long"
    dc = _dist_cat(distance) if distance else ""

    # JRA/NAR 判定ヘルパー（騎手・調教師の所属ベース: 00xxx/01xxx=JRA, それ以外=NAR）
    def _is_jra_person(pid: str) -> bool:
        return pid.startswith("00") or pid.startswith("01")

    # NAR公式ID → tracker(netkeiba)ID 変換マップを構築
    # NAR公式スクレイパーのIDとMLトレーニングデータ(netkeiba)のIDが異なるため
    _jockey_name_to_tracker_id: Dict[str, str] = {}
    _trainer_name_to_tracker_id: Dict[str, str] = {}
    if rolling_tracker:
        try:
            import sqlite3 as _sql3
            _conn = _sql3.connect("data/keiba.db")
            # 騎手: jockey_name → netkeiba ID（最多出走のIDを採用）
            _jrows = _conn.execute(
                "SELECT jockey_name, jockey_id, COUNT(*) as cnt "
                "FROM race_log WHERE jockey_id != '' AND jockey_name != '' "
                "GROUP BY jockey_name, jockey_id ORDER BY cnt DESC"
            ).fetchall()
            for _jn, _jid, _ in _jrows:
                import re as _re
                _jn_clean = _re.sub(r"[（(].+?[）)]", "", _jn).replace("\u3000", "").strip()
                if _jn_clean not in _jockey_name_to_tracker_id:
                    _jockey_name_to_tracker_id[_jn_clean] = _jid
            # 調教師: 同様
            _trows = _conn.execute(
                "SELECT trainer_name, trainer_id, COUNT(*) as cnt "
                "FROM race_log WHERE trainer_id != '' AND trainer_name != '' "
                "GROUP BY trainer_name, trainer_id ORDER BY cnt DESC"
            ).fetchall()
            for _tn, _tid, _ in _trows:
                import re as _re
                _tn_clean = _re.sub(r"[（(].+?[）)]", "", _tn).replace("\u3000", "").strip()
                if _tn_clean not in _trainer_name_to_tracker_id:
                    _trainer_name_to_tracker_id[_tn_clean] = _tid
            _conn.close()
        except Exception:
            pass

    for ev in evaluations:
        # デフォルト値を先にセット
        if not hasattr(ev, "_jockey_dev"):
            ev._jockey_dev = None
        if not hasattr(ev, "_trainer_dev"):
            ev._trainer_dev = None
        if not hasattr(ev, "_bloodline_dev"):
            ev._bloodline_dev = None

        h = ev.horse
        jid = getattr(h, "jockey_id", "") or ""
        tid = getattr(h, "trainer_id", "") or ""
        hid = getattr(h, "horse_id", "") or ""
        _date_str = race.race_date if race else ""

        # NAR公式ID → netkeiba ID 変換（rolling tracker はnetkeiba IDでキー管理）
        _nar_map = _load_nar_id_map()
        if _nar_map and jid:
            _j_entry = _nar_map.get("jockey", {}).get(jid)
            if _j_entry:
                jid = _j_entry.get("nk_id", jid)
        if _nar_map and tid:
            _t_entry = _nar_map.get("trainer", {}).get(tid)
            if _t_entry:
                tid = _t_entry.get("nk_id", tid)

        # ヘルパー: _EntityStats の dict から runs 数を取得
        def _dict_runs(entity, attr_name, key):
            d = getattr(entity, attr_name, None) or {}
            v = d.get(key)
            return v[2] if v and len(v) >= 3 else 0

        # === 騎手偏差値 ===
        if ev._jockey_dev is None:
            j_entity = rolling_tracker.jockeys.get(jid) if rolling_tracker else None
            # NAR公式IDでヒットしない場合、名前ベースでnetkeiba IDを逆引き
            _resolved_jid = jid
            if not j_entity and rolling_tracker and _jockey_name_to_tracker_id:
                import re as _re
                _jname = getattr(h, "jockey", "") or ""
                _jname_clean = _re.sub(r"[（(].+?[）)]", "", _jname).replace("\u3000", "").strip()
                _alt_jid = _jockey_name_to_tracker_id.get(_jname_clean)
                if _alt_jid and _alt_jid != jid:
                    j_entity = rolling_tracker.jockeys.get(_alt_jid)
                    if j_entity:
                        _resolved_jid = _alt_jid
            if j_entity and j_entity.runs >= 5:
                # ローリング統計からファクター値を取得
                factor_rates = {
                    "overall": j_entity.place_rate,
                    "pr_2y": j_entity.rate_recent_2y(_date_str) if _date_str else None,
                    "venue": j_entity.venue_pr(venue) if venue else None,
                    "sim_venue": j_entity.sim_venue_rate(venue, surface)[1] if venue and surface else None,
                    "distance": j_entity.dist_cat_pr(dc) if dc else None,
                    "smile": j_entity.smile_pr(smile) if smile else None,
                    "condition": j_entity.cond_pr(condition) if condition else None,
                    "pace": None,  # 推論前はペース情報なし
                    "style": None,
                    "gate": None,
                    "horse": j_entity.horse_combo_pr(hid) if hid else None,
                }
                factor_runs = {
                    "overall": j_entity.runs,
                    "pr_2y": j_entity.runs,
                    "venue": _dict_runs(j_entity, 'venue', venue),
                    "sim_venue": j_entity.runs,
                    "distance": _dict_runs(j_entity, 'dist_cat', dc),
                    "smile": _dict_runs(j_entity, 'smile', smile),
                    "condition": _dict_runs(j_entity, 'cond', condition),
                    "pace": 0,
                    "style": 0,
                    "gate": 0,
                    "horse": _dict_runs(j_entity, 'horse_combo', hid),
                }
                _jp = JOCKEY_BASE_PARAMS_JRA if _is_jra_person(_resolved_jid) else JOCKEY_BASE_PARAMS_NAR
                jdev = compute_category_deviation(
                    factor_rates, factor_runs, JOCKEY_FACTOR_WEIGHTS,
                    base_mean=_jp["mean"], base_sigma=_jp["sigma"],
                )
                if jdev is not None:
                    ev._jockey_dev = round(jdev, 1)
            elif ev.jockey_stats:
                # フォールバック: NAR騎手はrace_logから直接計算（personnel_dbは不正確な場合がある）
                _fb_dev = None
                if not _is_jra_person(jid):
                    _fb_dev = _race_log_jockey_dev(jid, getattr(h, "jockey", ""))
                if _fb_dev is not None:
                    ev._jockey_dev = round(max(30.0, min(70.0, _fb_dev)), 1)
                else:
                    # JRA騎手 or race_log取得失敗: 従来通りpersonnel_db使用
                    jdev = ev.jockey_stats.lower_long_dev
                    _all_default = (
                        ev.jockey_stats.upper_long_dev == 50.0
                        and ev.jockey_stats.upper_short_dev == 50.0
                        and ev.jockey_stats.lower_long_dev == 50.0
                        and ev.jockey_stats.lower_short_dev == 50.0
                    )
                    ev._jockey_dev = None if _all_default else round(max(30.0, min(70.0, jdev)), 1)

        # === 調教師偏差値 ===
        if ev._trainer_dev is None:
            t_entity = rolling_tracker.trainers.get(tid) if rolling_tracker else None
            _resolved_tid = tid
            # NAR公式ID → tracker(netkeiba)ID 名前ベースフォールバック
            if not t_entity and rolling_tracker and _trainer_name_to_tracker_id:
                import re as _re
                _tname = getattr(h, "trainer", "") or ""
                _tname_clean = _re.sub(r"[（(].+?[）)]", "", _tname).replace("\u3000", "").strip()
                _alt_tid = _trainer_name_to_tracker_id.get(_tname_clean)
                if _alt_tid and _alt_tid != tid:
                    t_entity = rolling_tracker.trainers.get(_alt_tid)
                    if t_entity:
                        _resolved_tid = _alt_tid
            if t_entity and t_entity.runs >= 5:
                factor_rates = {
                    "overall": t_entity.place_rate,
                    "pr_2y": t_entity.rate_recent_2y(_date_str) if _date_str else None,
                    "venue": t_entity.venue_pr(venue) if venue else None,
                    "sim_venue": t_entity.sim_venue_rate(venue, surface)[1] if venue and surface else None,
                    "distance": t_entity.dist_cat_pr(dc) if dc else None,
                    "smile": t_entity.smile_pr(smile) if smile else None,
                    "condition": t_entity.cond_pr(condition) if condition else None,
                    "pace": None,
                    "style": None,
                    "gate": None,
                    "horse": t_entity.horse_combo_pr(hid) if hid else None,
                }
                factor_runs = {
                    "overall": t_entity.runs,
                    "pr_2y": t_entity.runs,
                    "venue": _dict_runs(t_entity, 'venue', venue),
                    "sim_venue": t_entity.runs,
                    "distance": _dict_runs(t_entity, 'dist_cat', dc),
                    "smile": _dict_runs(t_entity, 'smile', smile),
                    "condition": _dict_runs(t_entity, 'cond', condition),
                    "pace": 0,
                    "style": 0,
                    "gate": 0,
                    "horse": _dict_runs(t_entity, 'horse_combo', hid),
                }
                _tp = TRAINER_BASE_PARAMS_JRA if _is_jra_person(_resolved_tid) else TRAINER_BASE_PARAMS_NAR
                tdev = compute_category_deviation(
                    factor_rates, factor_runs, TRAINER_FACTOR_WEIGHTS,
                    base_mean=_tp["mean"], base_sigma=_tp["sigma"],
                )
                if tdev is not None:
                    ev._trainer_dev = round(tdev, 1)
            elif ev.trainer_stats:
                # フォールバック: NAR調教師はrace_logから直接計算
                _fb_tdev = None
                if not _is_jra_person(tid):
                    _fb_tdev = _race_log_trainer_dev(tid, getattr(h, "trainer", ""))
                if _fb_tdev is not None:
                    ev._trainer_dev = round(max(30.0, min(70.0, _fb_tdev)), 1)
                else:
                    # JRA調教師 or race_log取得失敗: 従来通りpersonnel_db使用
                    dev = getattr(ev.trainer_stats, "deviation", None)
                    if dev is not None:
                        ev._trainer_dev = None if dev == 50.0 else round(max(30.0, min(70.0, dev)), 1)

        # === 血統偏差値 (Phase 12 ハイブリッド) ===
        if ev._bloodline_dev is None and sire_rolling_tracker:
            sire_id = getattr(h, "sire_id", "") or ""
            bms_id = getattr(h, "maternal_grandsire_id", "") or ""
            if sire_id or bms_id:
                # 父偏差値
                sire_dev = None
                if sire_id:
                    s_rates = {
                        "overall": None, "smile": None,
                        "condition": None, "venue": None,
                        "pace": None, "style": None, "gate": None,
                        "jockey": None, "trainer": None,
                    }
                    s_runs = {k: 0 for k in s_rates}
                    # ヘルパー: getattr で安全にアクセス
                    def _sire_rate(attr, key, min_r=5):
                        v = getattr(sire_rolling_tracker, attr, {}).get(key)
                        if v and len(v) >= 3 and v[2] >= min_r:
                            return v[1] / v[2], v[2]
                        return None, 0
                    # 全体
                    _s = getattr(sire_rolling_tracker, '_sire', {}).get(sire_id)
                    if _s and len(_s) >= 3 and _s[2] >= 5:
                        s_rates["overall"] = _s[1] / _s[2]
                        s_runs["overall"] = _s[2]
                    # SMILE別（面×距離帯のプロキシ）
                    r, n = _sire_rate('_sire_smile', (sire_id, smile))
                    if r is not None:
                        s_rates["smile"] = r; s_runs["smile"] = n
                    # 馬場状態別
                    r, n = _sire_rate('_sire_cond', (sire_id, condition))
                    if r is not None:
                        s_rates["condition"] = r; s_runs["condition"] = n
                    # 競馬場別
                    r, n = _sire_rate('_sire_venue', (sire_id, venue))
                    if r is not None:
                        s_rates["venue"] = r; s_runs["venue"] = n
                    sire_dev = compute_category_deviation(
                        s_rates, s_runs, SIRE_FACTOR_WEIGHTS,
                        base_mean=SIRE_BASE_PARAMS["mean"], base_sigma=SIRE_BASE_PARAMS["sigma"],
                    )

                # 母父偏差値
                bms_dev = None
                if bms_id:
                    b_rates = {
                        "overall": None, "smile": None,
                        "condition": None, "venue": None,
                        "pace": None, "style": None, "gate": None,
                        "jockey": None, "trainer": None,
                    }
                    b_runs = {k: 0 for k in b_rates}
                    def _bms_rate(attr, key, min_r=5):
                        v = getattr(sire_rolling_tracker, attr, {}).get(key)
                        if v and len(v) >= 3 and v[2] >= min_r:
                            return v[1] / v[2], v[2]
                        return None, 0
                    _b = getattr(sire_rolling_tracker, '_bms', {}).get(bms_id)
                    if _b and len(_b) >= 3 and _b[2] >= 5:
                        b_rates["overall"] = _b[1] / _b[2]
                        b_runs["overall"] = _b[2]
                    r, n = _bms_rate('_bms_smile', (bms_id, smile))
                    if r is not None:
                        b_rates["smile"] = r; b_runs["smile"] = n
                    r, n = _bms_rate('_bms_cond', (bms_id, condition))
                    if r is not None:
                        b_rates["condition"] = r; b_runs["condition"] = n
                    r, n = _bms_rate('_bms_venue', (bms_id, venue))
                    if r is not None:
                        b_rates["venue"] = r; b_runs["venue"] = n
                    bms_dev = compute_category_deviation(
                        b_rates, b_runs, BMS_FACTOR_WEIGHTS,
                        base_mean=BMS_BASE_PARAMS["mean"], base_sigma=BMS_BASE_PARAMS["sigma"],
                    )

                # 父・母父の平均
                if sire_dev is not None and bms_dev is not None:
                    ev._bloodline_dev = round((sire_dev * 0.6 + bms_dev * 0.4), 1)
                elif sire_dev is not None:
                    ev._bloodline_dev = round(sire_dev, 1)
                elif bms_dev is not None:
                    ev._bloodline_dev = round(bms_dev, 1)


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
    analysis.evaluations[:] = _assign_marks(analysis.evaluations, is_jra=engine.is_jra)

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
        # 数値（偏差値） — 30-70クランプ
        # _compute_personnel_devs() で rolling tracker から既に算出済みの場合は上書きしない
        def _clamp_dev(v):
            return round(max(30.0, min(70.0, v)), 1) if v is not None else None
        if getattr(ev, "_jockey_dev", None) is None:
            ev._jockey_dev = _clamp_dev(profile.get("jockey_dev"))
        if getattr(ev, "_trainer_dev", None) is None:
            ev._trainer_dev = _clamp_dev(profile.get("trainer_dev"))
        ev._sire_dev = _clamp_dev(profile.get("sire_dev"))
        ev._mgs_dev = _clamp_dev(profile.get("mgs_dev"))
        if getattr(ev, "_bloodline_dev", None) is None:
            ev._bloodline_dev = _clamp_dev(profile.get("bloodline_dev"))
        # グレードは偏差値から導出（rolling tracker で算出済みの場合、profile の旧グレードを上書き）
        ev._jockey_grade = dev_to_grade(ev._jockey_dev) if ev._jockey_dev is not None else profile["jockey_grade"]
        ev._trainer_grade = dev_to_grade(ev._trainer_dev) if ev._trainer_dev is not None else profile["trainer_grade"]
        ev._sire_grade = profile["sire_grade"]
        ev._mgs_grade = profile["mgs_grade"]

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
