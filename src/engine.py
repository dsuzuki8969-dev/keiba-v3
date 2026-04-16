"""
競馬解析マスターシステム v3.0 - メインオーケストレーター

計算層と分析層を統合して RaceAnalysis を生成する。
入力: RaceInfo + 全馬 Horse リスト + 各種マスタDB
出力: RaceAnalysis (HTML出力まで)
"""

import os
import threading
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
_CACHE_1C_PREDICTOR:       object = None   # First1CPredictor (初角位置予測)
_CACHE_LGBM_RANKER:        object = None   # LGBMRanker (LambdaRank 補完)
_CACHE_ML_LOADED:       bool = False
_CACHE_POS_LOADED:      bool = False
_CACHE_PROB_LOADED:     bool = False
_CACHE_PACE_ML_LOADED:  bool = False
_CACHE_TORCH_LOADED:    bool = False
_CACHE_LGBM_LOADED:     bool = False
_CACHE_RANKER_LOADED:   bool = False
_CACHE_1C_LOADED:       bool = False

from data.masters.venue_master import is_banei
from src.calculator.ability import (
    StandardTimeCalculator,
    TrackCorrector,
    calc_ability_deviation,
    detect_long_break,
)
from src.calculator.betting import (
    _calc_confidence_score,
    calc_predicted_odds,
    generate_formation_tickets,
    judge_confidence,
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
from src.calculator.popularity_blend import blend_probabilities, load_popularity_stats
from src.calculator.predicted_odds import (
    assign_divergence_to_evaluations,
    calc_predicted_sanrenpuku,
    calc_predicted_umaren,
    detect_value_bets,
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

# ── NAR公式ID → netkeiba ID マッピング（プロセス内キャッシュ） ──
_CACHE_NAR_ID_MAP: Optional[dict] = None
_CACHE_NAR_ID_MAP_LOADED: bool = False


def reset_engine_caches():
    """全グローバルキャッシュをリセットする。初回実行時のキャッシュ不整合対策。
    MLモデルキャッシュはリセットしない（pkl互換性問題はロード時に検知すべき）。"""
    global _DB_CACHE_DATE, _DB_CACHE_PACE, _DB_CACHE_L3F_SIGMA, _DB_CACHE_GATE_BIAS
    global _CACHE_NAR_ID_MAP, _CACHE_NAR_ID_MAP_LOADED
    global _CACHE_RL_JOCKEY_DEV, _CACHE_RL_TRAINER_DEV
    global _CACHE_TRAINING_EXTRACTOR, _CACHE_TRAINING_LOADED
    global _rl_jockey_lock, _rl_trainer_lock
    _DB_CACHE_DATE = None
    _DB_CACHE_PACE = None
    _DB_CACHE_L3F_SIGMA = None
    _DB_CACHE_GATE_BIAS = None
    _CACHE_NAR_ID_MAP = None
    _CACHE_NAR_ID_MAP_LOADED = False
    with _rl_jockey_lock:
        _CACHE_RL_JOCKEY_DEV = {}
    with _rl_trainer_lock:
        _CACHE_RL_TRAINER_DEV = {}
    _CACHE_TRAINING_EXTRACTOR = None
    _CACHE_TRAINING_LOADED = False
    logger.info("エンジンキャッシュをリセットしました")


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
_rl_jockey_lock = threading.Lock()


def _race_log_jockey_dev(jockey_id: str, jockey_name: str) -> Optional[float]:
    """race_logから騎手の複勝率ベース偏差値を計算（NAR騎手フォールバック用）

    NAR全体の平均複勝率/σとの比較で偏差値化する。
    最低出走数: 30走（少数サンプルでの跳ね上がり防止）。
    settings.pyのパラメータを参照。キャッシュ済み。
    """
    from config.settings import JOCKEY_BASE_PARAMS_NAR

    _cache_key = f"{jockey_id}_{jockey_name}"
    # ダブルチェックロッキング: 読み取りはロック不要、書き込みのみ保護
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

        # 最低30走（少数サンプルでの偏差値跳ね上がり防止）
        if len(_rows) >= 30:
            _wins = sum(1 for pos, _ in _rows if pos == 1)
            _places = sum(1 for pos, fc in _rows if pos <= 3)  # 常に3着以内
            _wr = _wins / len(_rows)
            _pr = _places / len(_rows)
            # settings.pyのパラメータを参照
            _mean = JOCKEY_BASE_PARAMS_NAR.get("mean", 0.23)
            _sigma = JOCKEY_BASE_PARAMS_NAR.get("sigma", 0.16)
            # サンプル数による縮小推定（30走→全体平均寄り、100走→信頼度高い）
            _shrink = min(1.0, len(_rows) / 100.0)
            _pr_adj = _pr * _shrink + _mean * (1.0 - _shrink)
            result = round(50.0 + (_pr_adj - _mean) / _sigma * 10.0, 1)
            result = max(20.0, min(100.0, result))
    except Exception:
        pass

    with _rl_jockey_lock:
        _CACHE_RL_JOCKEY_DEV[_cache_key] = result
    return result


# ── NAR調教師race_logベース偏差値キャッシュ ──
_CACHE_RL_TRAINER_DEV: Dict[str, Optional[float]] = {}
_rl_trainer_lock = threading.Lock()


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
            result = max(20.0, min(100.0, result))
    except Exception:
        pass

    with _rl_trainer_lock:
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
            if target_date == _DB_CACHE_DATE and _DB_CACHE_PACE is not None:
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

        # ML初角位置取り予測モデル
        first1c_predictor = self._load_first1c_predictor()

        # コース別上がり3F sigma DB（案F-4）（日付単位キャッシュ）
        try:
            if target_date == _DB_CACHE_DATE and _DB_CACHE_L3F_SIGMA is not None:
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
            if target_date == _DB_CACHE_DATE and _DB_CACHE_GATE_BIAS is not None:
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

        # 事後キャリブレータ（Isotonic Regression）
        self._post_calibrator = self._load_post_calibrator()

    @staticmethod
    def _load_post_calibrator():
        """事後キャリブレータ（Isotonic Regression）をロード"""
        from config.settings import USE_POST_CALIBRATOR
        if not USE_POST_CALIBRATOR:
            return None
        try:
            from src.ml.calibrator import PostCalibrator
            cal = PostCalibrator()
            if cal.load():
                return cal
        except Exception as e:
            logger.debug("事後キャリブレータロード失敗: %s", e)
        return None

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

    @staticmethod
    def _load_first1c_predictor():
        """ML初角位置取り予測モデルをロード（プロセス内キャッシュ）"""
        global _CACHE_1C_PREDICTOR, _CACHE_1C_LOADED
        if _CACHE_1C_LOADED:
            return _CACHE_1C_PREDICTOR
        _CACHE_1C_LOADED = True
        try:
            from src.ml.first1c_model import First1CPredictor

            predictor = First1CPredictor()
            if predictor.ensure_loaded():
                logger.info("ML初角位置取り予測モデルをロードしました")
                _CACHE_1C_PREDICTOR = predictor
        except Exception:
            logger.debug("ML初角位置取り予測モデルのロードをスキップ", exc_info=True)
        return _CACHE_1C_PREDICTOR

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
            4: (0.30, 0.70),   # 競馬場専用 (高精度) → ML 70%（Phase9D': 65→70）
            3: (0.35, 0.65),   # JRA馬場×SMILE → ML 65%（Phase9D': 58→65）
            2: (0.45, 0.55),   # JRA全体/NAR → ML 55%（Phase9D': 45→55）
            1: (0.55, 0.45),   # 馬場全体 → ML 45%（Phase9D': 35→45）
            0: (0.70, 0.30),   # globalモデル (最低精度) → ML 30%（Phase9D': 25→30）
        }
        return _BLEND_BY_LEVEL.get(level, (0.50, 0.50))

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
                pace_type_predicted=PaceType.M,
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

        # ---- past_runs フォールバック: race_logから補完 ----
        for horse in horses:
            if not horse.past_runs and horse.horse_id:
                try:
                    from src.scraper.horse_db_builder import get_past_runs_from_race_log
                    horse.past_runs = get_past_runs_from_race_log(horse.horse_id)
                except Exception:
                    pass

        # ---- 通過順・着差・race_id補完: race_logの修正済みデータで不足分を埋める ----
        _enrich_count = 0
        _enrich_skip = 0
        _enrich_found = 0
        _enrich_updated = 0
        try:
            import json as _js2
            import sqlite3 as _sq3

            from config.settings import DATABASE_PATH
            from data.masters.venue_master import VENUE_NAME_TO_CODE
            _rl2 = _sq3.connect(DATABASE_PATH)
            _rl2.row_factory = _sq3.Row
            for horse in horses:
                _hid = getattr(horse, "horse_id", "") or ""
                for pr in (horse.past_runs or []):
                    _enrich_count += 1
                    # 検索1: race_id + horse_no（最も正確）
                    _rw = None
                    if pr.race_id:
                        _rw = _rl2.execute(
                            "SELECT race_id, positions_corners, position_4c, "
                            "       margin_ahead, margin_behind, jockey_name "
                            "FROM race_log "
                            "WHERE race_id=? AND horse_no=? LIMIT 1",
                            (pr.race_id, pr.horse_no)
                        ).fetchone()
                    # 検索2: horse_id + race_id（馬番変更対応）
                    if not _rw and pr.race_id and _hid:
                        _rw = _rl2.execute(
                            "SELECT race_id, positions_corners, position_4c, "
                            "       margin_ahead, margin_behind, jockey_name "
                            "FROM race_log "
                            "WHERE race_id=? AND horse_id=? LIMIT 1",
                            (pr.race_id, _hid)
                        ).fetchone()
                    # 検索3: horse_id + race_date + venue + distance（race_idなし時の正確なフォールバック）
                    if not _rw and _hid:
                        _vc = VENUE_NAME_TO_CODE.get(pr.venue, "")
                        _vc_alt = {"49": "50", "50": "49"}.get(_vc, "")
                        if _vc:
                            _rw = _rl2.execute(
                                "SELECT race_id, positions_corners, position_4c, "
                                "       margin_ahead, margin_behind, jockey_name "
                                "FROM race_log "
                                "WHERE horse_id=? AND race_date=? AND venue_code IN (?,?) "
                                "AND distance=? LIMIT 1",
                                (_hid, pr.race_date, _vc, _vc_alt or _vc,
                                 pr.distance)
                            ).fetchone()
                    # 検索3b: horse_no + race_date + venue + distance + finish_pos（horse_idなし時）
                    if not _rw:
                        _vc = VENUE_NAME_TO_CODE.get(pr.venue, "")
                        _vc_alt = {"49": "50", "50": "49"}.get(_vc, "")
                        if _vc and pr.horse_no > 0 and _hid:
                            # horse_idも一致する条件を追加（同日同場の別レース誤マッチ防止）
                            _rw = _rl2.execute(
                                "SELECT race_id, positions_corners, position_4c, "
                                "       margin_ahead, margin_behind, jockey_name "
                                "FROM race_log "
                                "WHERE horse_no=? AND race_date=? AND venue_code IN (?,?) "
                                "AND distance=? AND finish_pos=? AND horse_id=? LIMIT 1",
                                (pr.horse_no, pr.race_date, _vc, _vc_alt or _vc,
                                 pr.distance, pr.finish_pos, _hid)
                            ).fetchone()
                    # 検索4: horse_id + race_date + venue（馬番0の場合）
                    if not _rw and _hid:
                        _vc = VENUE_NAME_TO_CODE.get(pr.venue, "")
                        if _vc:
                            _rw = _rl2.execute(
                                "SELECT race_id, positions_corners, position_4c, "
                                "       margin_ahead, margin_behind, jockey_name "
                                "FROM race_log "
                                "WHERE horse_id=? AND race_date=? AND venue_code=? "
                                "AND distance=? LIMIT 1",
                                (_hid, pr.race_date, _vc, pr.distance)
                            ).fetchone()
                    # 検索5: horse_id + venue + distance + finish_pos + 年（JRA日付不正対応）
                    # JRAのrace_logは日付がYYYY-01-0Xのダミーになっている場合がある
                    if not _rw and _hid:
                        _vc = VENUE_NAME_TO_CODE.get(pr.venue, "")
                        _year = pr.race_date[:4] if pr.race_date else ""
                        if _vc and _year:
                            _rw = _rl2.execute(
                                "SELECT race_id, positions_corners, position_4c, "
                                "       margin_ahead, margin_behind, jockey_name "
                                "FROM race_log "
                                "WHERE horse_id=? AND venue_code=? "
                                "AND distance=? AND finish_pos=? "
                                "AND race_id LIKE ? LIMIT 1",
                                (_hid, _vc, pr.distance, pr.finish_pos,
                                 f"{_year}%")
                            ).fetchone()
                    # 検索6: horse_id + race_date（venue不明でも拾う）
                    if not _rw and _hid and pr.race_date:
                        _rw = _rl2.execute(
                            "SELECT race_id, positions_corners, position_4c, "
                            "       margin_ahead, margin_behind, jockey_name "
                            "FROM race_log "
                            "WHERE horse_id=? AND race_date=? LIMIT 1",
                            (_hid, pr.race_date)
                        ).fetchone()
                    # 検索7: horse_id + venue + 年（最も緩い条件、distance不一致でも拾う）
                    if not _rw and _hid:
                        _vc = VENUE_NAME_TO_CODE.get(pr.venue, "")
                        _year = pr.race_date[:4] if pr.race_date else ""
                        if _vc and _year and pr.horse_no > 0:
                            _rw = _rl2.execute(
                                "SELECT race_id, positions_corners, position_4c, "
                                "       margin_ahead, margin_behind, jockey_name "
                                "FROM race_log "
                                "WHERE horse_id=? AND horse_no=? AND venue_code=? "
                                "AND finish_pos=? AND race_id LIKE ? LIMIT 1",
                                (_hid, pr.horse_no, _vc, pr.finish_pos,
                                 f"{_year}%")
                            ).fetchone()
                    if not _rw:
                        _enrich_skip += 1
                        continue
                    _enrich_found += 1
                    # race_id補完（レースリンク用）
                    if _rw["race_id"] and not pr.race_id:
                        pr.race_id = _rw["race_id"]
                    # race_no補完（前走リンク生成用: race_id 11-12桁目）
                    if (not pr.race_no or pr.race_no <= 0) and pr.race_id and len(pr.race_id) >= 12:
                        try:
                            pr.race_no = int(pr.race_id[10:12])
                        except ValueError:
                            pass
                    # 通過順補完（バリデーション付き）
                    if _rw["positions_corners"]:
                        try:
                            _parsed = _js2.loads(_rw["positions_corners"]) if isinstance(_rw["positions_corners"], str) else _rw["positions_corners"]
                            if isinstance(_parsed, list) and len(_parsed) >= 1:
                                # 0を除外した有効コーナーリスト
                                _valid = [v for v in _parsed if isinstance(v, int) and v > 0]
                                if len(_valid) >= 2:
                                    # 2コーナー以上: フル通過順として上書き
                                    existing = pr.positions_corners or []
                                    if len(_valid) > len(existing) or any(v == 0 for v in existing) or len(existing) < 2:
                                        pr.positions_corners = _valid
                                        pr.position_4c = _valid[-1]
                                        _enrich_updated += 1
                                elif len(_valid) == 1 and not pr.positions_corners:
                                    # 1コーナーのみ（短距離等）: position_4cだけ補完
                                    pr.position_4c = _valid[0]
                        except Exception:
                            pass
                    # position_4c フォールバック
                    if not pr.positions_corners and _rw["position_4c"] and (not pr.position_4c or pr.position_4c <= 0):
                        pr.position_4c = _rw["position_4c"]
                    # 騎手名補完（race_logの方が正確: 公式結果由来）
                    if _rw["jockey_name"] and len(_rw["jockey_name"]) > len(pr.jockey):
                        pr.jockey = _rw["jockey_name"]
                    # 着差補完（margin_ahead / margin_behind）
                    if _rw["margin_ahead"] and _rw["margin_ahead"] > 0:
                        if pr.margin_ahead == 0.0:
                            pr.margin_ahead = _rw["margin_ahead"]
                    if _rw["margin_behind"] and _rw["margin_behind"] > 0:
                        if pr.margin_behind == 0.0:
                            pr.margin_behind = _rw["margin_behind"]
            _rl2.close()
            logger.info("race_log通過順補完: 対象=%d, DB該当=%d, 更新=%d, 未該当=%d",
                       _enrich_count, _enrich_found, _enrich_updated, _enrich_skip)
        except Exception as _rl_err:
            logger.warning("race_log通過順補完エラー: %s (対象=%d)", _rl_err, _enrich_count, exc_info=True)

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
            pace_type = PaceType.M
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
        # フィールド強度特徴量（Phase8: est_last3f MLモデル用）
        # 全馬の過去走上がり3F平均 → フィールドの強さを反映
        _field_l3f_vals = []
        _field_fin_vals = []
        for h in horses:
            if h.past_runs:
                _h_l3fs = [r.last_3f_sec for r in h.past_runs[:3]
                           if r.last_3f_sec and 28.0 <= r.last_3f_sec <= 48.0]
                if _h_l3fs:
                    _field_l3f_vals.append(sum(_h_l3fs) / len(_h_l3fs))
                _h_fins = [r.finish_pos for r in h.past_runs[:3] if r.finish_pos]
                if _h_fins:
                    _field_fin_vals.append(sum(_h_fins) / len(_h_fins))
        _field_hist_l3f_mean = (sum(_field_l3f_vals) / len(_field_l3f_vals)) if _field_l3f_vals else None
        _field_hist_fin_mean = (sum(_field_fin_vals) / len(_field_fin_vals)) if _field_fin_vals else None

        # 改善2: 脚質分布カウント（❹コース脚質バイアス相対化用）
        # 好位→先行、中団→差し、マクリ→追込にマッピングして全馬カバー
        _STYLE_MAP = {"逃げ": "逃げ", "先行": "先行", "好位": "先行",
                      "中団": "差し", "差し": "差し", "追込": "追込", "マクリ": "追込"}
        _style_counts = {"逃げ": 0, "先行": 0, "差し": 0, "追込": 0}
        for _, (s, _) in field_styles.items():
            _mapped = _STYLE_MAP.get(s.value)
            if _mapped:
                _style_counts[_mapped] += 1
        _field_style_distribution = _style_counts

        pace_context = {
            "n_front": n_front,
            "front_ratio": front_rate,
            "n_escape": len(leaders),         # 逃げ馬数
            "n_escape_inner": n_escape_inner,  # 内枠逃げ馬数（案C）
            "max_escape_strength": max_escape_strength,  # 最強逃げ馬スコア（案C）
            "field_hist_l3f_mean": _field_hist_l3f_mean,  # Phase8: フィールド平均l3f
            "field_hist_finish_mean": _field_hist_fin_mean,  # Phase8: フィールド平均着順
            "field_style_distribution": _field_style_distribution,  # 改善2: 脚質分布
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
            # 2パス方式: 過去走ありの馬のest_last3f平均を常に計算
            # 初出走馬にはこの平均値をフォールバックとして使用
            import statistics as _stat_bl
            _est_l3fs = []
            for horse in horses:
                if not horse.past_runs:
                    continue  # 初出走馬はPass 1から除外
                _si = self.style_classifier.classify(horse.past_runs, surface=race.course.surface)
                _l3f_type = _si.get("last3f_type", "安定中位末脚")
                _el = self.last3f_evaluator.estimate_last3f(
                    horse.past_runs, pace_type, race.course.course_id,
                    _l3f_type, horse=horse, race_info=race, pace_context=pace_context,
                )
                _est_l3fs.append(_el)
            _field_baseline = _stat_bl.mean(_est_l3fs) if _est_l3fs else None

            # ---- 2パス位置取り正規化: 全馬のraw_position_scoreを直接使用 ----
            # _calc_raw_position_scoreの値（前走通過順ベース）を使うことで
            # 脚質分類の離散化による情報損失を防ぐ
            _raw_positions: Dict[int, float] = {}
            _pos_predictor = getattr(self.pace_dev_calc, 'position_predictor', None)
            for horse in horses:
                _est_ml = None
                _est_rule = None
                # ML予測
                if _pos_predictor and _pos_predictor.is_available:
                    _est_ml = _pos_predictor.predict(horse, race, pace_context)
                # ルールベース予測（前走通過順ベース）
                _fs = field_styles.get(horse.horse_no)
                _est_rule = _fs[1] if _fs else 0.45

                if _est_ml is not None and _est_rule is not None:
                    # 乖離チェック: MLとルールが大きくずれている場合はルールを尊重
                    _gap = abs(_est_ml - _est_rule)
                    if _gap > 0.20:
                        # 前走実績と大きく乖離 → ルールベース80% + ML20%
                        _est = _est_rule * 0.80 + _est_ml * 0.20
                    else:
                        # 正常範囲 → ML60% + ルール40%
                        _est = _est_ml * 0.60 + _est_rule * 0.40
                elif _est_ml is not None:
                    _est = _est_ml
                else:
                    _est = _est_rule
                _raw_positions[horse.horse_no] = _est
            # フィールド内正規化
            _normalized_positions = normalize_field_positions(_raw_positions, len(horses))

        # ---- 1角位置予測（First1CPredictor）----
        _1c_predictor = self._load_first1c_predictor()
        _1c_raw_all: Dict[int, float] = {}
        if _1c_predictor and _1c_predictor.is_available:
            for horse in horses:
                _pred = _1c_predictor.predict(horse, race)
                if _pred is not None:
                    _1c_raw_all[horse.horse_no] = _pred
        # 改善3: MLモデルで予測できなかった馬の1角位置フォールバック改善
        # 従来: 4角位置*0.85（順序同一→正規化後差分ゼロ→❻軌跡スコア死亡）
        # 改善: 過去走コーナー通過データから1角-4角の位置変動平均を算出
        from config.settings import PACE_TRAJECTORY_FIX_ENABLED
        for horse in horses:
            if horse.horse_no not in _1c_raw_all:
                _raw_4c = _raw_positions.get(horse.horse_no)
                if _raw_4c is not None:
                    if PACE_TRAJECTORY_FIX_ENABLED:
                        _deltas = []
                        for r in (horse.past_runs or [])[:5]:
                            corners = getattr(r, 'positions_corners', None) or []
                            if len(corners) >= 2 and corners[0] > 0 and corners[-1] > 0:
                                _fc = getattr(r, 'field_count', 16) or 16
                                _1c_rel = corners[0] / max(1, _fc)
                                _4c_rel = corners[-1] / max(1, _fc)
                                _deltas.append(_4c_rel - _1c_rel)
                        if _deltas:
                            avg_delta = sum(_deltas) / len(_deltas)
                            _1c_raw_all[horse.horse_no] = max(0.01, min(1.0, _raw_4c - avg_delta))
                        else:
                            _1c_raw_all[horse.horse_no] = _raw_4c * 0.85  # 従来フォールバック
                    else:
                        _1c_raw_all[horse.horse_no] = _raw_4c * 0.85
        # フィールド内正規化
        _1c_normalized = normalize_field_positions(_1c_raw_all, len(horses)) if _1c_raw_all else {}

        evaluations: List[HorseEvaluation] = []
        for horse in horses:
            # 展開チャートと展開偏差値で同じ位置推定値を使用
            # _normalized_positions = 前走通過順ベースの正規化済み位置
            _horse_pos = _normalized_positions.get(horse.horse_no)
            _horse_pos_1c = _1c_normalized.get(horse.horse_no)
            ev = self._evaluate_horse(
                horse,
                race,
                pace_type,
                combo_db,
                bloodline_db,
                pace_db,
                pace_context=pace_context,
                field_baseline_override=_field_baseline,
                override_position=_horse_pos,
                override_pos_1c=_horse_pos_1c,
            )
            ev.venue_name = race.venue
            # 改善1: レース条件をセット（composite で条件別ウェイト動的調整に使用）
            ev._race_surface = race.course.surface if race.course else None
            ev._race_field_size = race.field_count
            ev._race_distance = race.course.distance if race.course else None
            evaluations.append(ev)

        # Phase 7c: est_last3f + base_score 能力整合補正
        # 問題: est_last3fのMLモデルは相手関係を考慮しないため、
        #        能力が低い馬にフィールド最速の上がり3Fを付与することがある
        # 解決: ability.totalのフィールド内Z-scoreでest_last3fを直接補正し、
        #        base_scoreも連動して調整（表示値と内部スコアの整合性保証）
        # 統計根拠: ダート53万件で1-3着l3f=39.18秒 vs 7着以下=40.49秒 (1.31秒/2σ→0.65秒/σ)
        #           位置取り効果混入を85%に抑え 0.55秒/σ（応急措置、Phase 8で根本解決予定）
        if len(evaluations) >= 3 and not _is_banei:
            import statistics as _stat_ab
            _ab_totals = [ev.ability.total for ev in evaluations]
            _ab_avg = _stat_ab.mean(_ab_totals)
            _ab_std = _stat_ab.stdev(_ab_totals) if len(_ab_totals) >= 3 else 1.0
            _ab_std = max(1.0, _ab_std)  # ゼロ除算防止

            for ev in evaluations:
                if ev.pace is None or ev.pace.base_score is None:
                    continue
                if ev.pace.estimated_last3f is None:
                    continue

                # 能力偏差値のフィールド内Z-score
                _ab_z = (ev.ability.total - _ab_avg) / _ab_std

                # est_last3f 補正: 能力高い→速く(負), 能力低い→遅く(正)
                # 統計: ダート53万件 1-3着l3f=39.18 vs 7着以下=40.49 (1.31秒/2σ→0.65秒/σ)
                # 0.30では弱すぎてest_l3f最速=ability下位半分が17%残存
                # 0.50に引き上げ（印・三連率・展開図の整合性を優先）
                _l3f_adj = -_ab_z * 0.50
                _l3f_adj = max(-1.20, min(1.20, _l3f_adj))  # クランプ±1.20秒
                ev.pace.estimated_last3f += _l3f_adj

                # base_score 連動補正: est_last3f変化 → goal_diff変化 → base_score変化
                # goal_diff = (baseline - est_last3f) - position_sec
                # est_last3f が +Δ → goal_diff が -Δ → base_score が -Δ * base_coeff
                # 平均base_coeff ≈ 7.5 (range 5.0-10.5) → 0.55 * 7.5 = 4.125/σ
                _bs_adj = -_l3f_adj * 7.5
                ev.pace.base_score += _bs_adj

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

        # 正規化: 比率保持方式
        # estimate_three_win_rates()のwin/place2/place3比率を保持しながら合計を理論値に一致
        # 旧方式（独立正規化）は勝率≒連対率の矛盾を引き起こしていた
        n = len(evaluations)
        place2_target = min(n, 2) / n if n > 0 else 0
        place3_target = min(n, 3) / n if n > 0 else 0

        # Step 1: 各馬のwin/place2/place3比率を保存（正規化前）
        _ratios_p2 = []  # place2/win比率
        _ratios_p3 = []  # place3/win比率
        for ev in evaluations:
            if ev.win_prob > 0.001:
                _ratios_p2.append(ev.place2_prob / ev.win_prob)
                _ratios_p3.append(ev.place3_prob / ev.win_prob)
            else:
                _ratios_p2.append(2.0)
                _ratios_p3.append(3.0)

        # Step 2: win_probを合計1.0に正規化
        total_win = sum(ev.win_prob for ev in evaluations)
        if total_win > 0:
            for ev in evaluations:
                ev.win_prob = min(1.0, ev.win_prob / total_win)

        # Step 3: 比率ベースでplace2/place3を算出
        for i, ev in enumerate(evaluations):
            ev.place2_prob = ev.win_prob * _ratios_p2[i]
            ev.place3_prob = ev.win_prob * _ratios_p3[i]

        # Step 4: place2/place3の合計を理論値に調整（比率は均等スケーリングで保持）
        _p2_sum = sum(ev.place2_prob for ev in evaluations)
        _p3_sum = sum(ev.place3_prob for ev in evaluations)
        _p2_target = place2_target * n  # = min(n, 2)
        _p3_target = place3_target * n  # = min(n, 3)
        if _p2_sum > 0:
            _adj2 = _p2_target / _p2_sum
            for ev in evaluations:
                ev.place2_prob = min(1.0, ev.place2_prob * _adj2)
        if _p3_sum > 0:
            _adj3 = _p3_target / _p3_sum
            for ev in evaluations:
                ev.place3_prob = min(1.0, ev.place3_prob * _adj3)

        # Step 5: 最低比率保証（勝率33%の馬が連対率33%にならないよう）
        # place2 >= win * 1.3, place3 >= place2 * 1.1 を保証
        # 再正規化→制約→再正規化を反復して収束させる（最大5回）
        for _iter in range(5):
            _needs_readjust = False
            for ev in evaluations:
                _min_p2 = ev.win_prob * 1.3
                if ev.place2_prob < _min_p2:
                    ev.place2_prob = _min_p2
                    _needs_readjust = True
                _min_p3 = max(ev.place2_prob * 1.1, ev.win_prob * 1.5)
                if ev.place3_prob < _min_p3:
                    ev.place3_prob = _min_p3
                    _needs_readjust = True
            if not _needs_readjust:
                break
            # 合計を再調整（制約下限を維持しながら他の馬を縮小）
            if n >= 2:
                _p2_sum2 = sum(ev.place2_prob for ev in evaluations)
                _p3_sum2 = sum(ev.place3_prob for ev in evaluations)
                if _p2_sum2 > _p2_target and _p2_sum2 > 0:
                    # 合計超過→全馬を縮小するが、下限制約を維持
                    _adj2b = _p2_target / _p2_sum2
                    for ev in evaluations:
                        _new_p2 = ev.place2_prob * _adj2b
                        ev.place2_prob = max(_new_p2, ev.win_prob * 1.2)
                if _p3_sum2 > _p3_target and _p3_sum2 > 0:
                    _adj3b = _p3_target / _p3_sum2
                    for ev in evaluations:
                        _new_p3 = ev.place3_prob * _adj3b
                        ev.place3_prob = max(_new_p3, ev.place2_prob)

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
            # 改善7: 逃げ馬判定精度向上（スコアリングv2）
            from config.settings import PACE_ESCAPE_SCORING_V2
            _nige_threshold = 0.45
            _1c_sorted = sorted(_1c_normalized.items(), key=lambda x: x[1]) if _1c_normalized else []
            _1c_top_horses = {hno for hno, _ in _1c_sorted[:3]} if _1c_sorted else set()

            # 改善7: 逃げ候補の多角スコアリング
            _nige_set: set = set()
            if PACE_ESCAPE_SCORING_V2 and _1c_sorted:
                _escape_candidates = []
                for hno, pos_1c in _1c_sorted:
                    h = next((h for h in horses if h.horse_no == hno), None)
                    if h is None:
                        continue
                    # 過去5走の逃げ率（1角1番手 or コーナー通過1番手）
                    _nige_runs = 0
                    _valid_runs = 0
                    for r in (h.past_runs or [])[:5]:
                        corners = getattr(r, 'positions_corners', None) or []
                        if corners:
                            _valid_runs += 1
                            if corners[0] <= 2:  # 1角2番手以内
                                _nige_runs += 1
                    _nige_rate = _nige_runs / max(1, _valid_runs) if _valid_runs > 0 else 0.0
                    # 内枠ボーナス（内枠ほど逃げやすい）
                    _gate = getattr(h, 'gate_no', 8) or 8
                    _gate_bonus = max(0.0, (5 - _gate) * 0.05)
                    # 総合逃げスコア: 1角位置(反転) + 過去逃げ率 + 内枠ボーナス
                    _escape_score = (1.0 - pos_1c) * 0.50 + _nige_rate * 0.35 + _gate_bonus * 0.15
                    _escape_candidates.append((hno, _escape_score, pos_1c))

                _escape_candidates.sort(key=lambda x: -x[1])  # スコア降順
                if _escape_candidates:
                    top_score = _escape_candidates[0][1]
                    for hno, sc, p1c in _escape_candidates:
                        if sc >= top_score * 0.75 and p1c < 0.30:
                            _nige_set.add(hno)
                        else:
                            break
                    # 最大3頭まで
                    if len(_nige_set) > 3:
                        _nige_set = {hno for hno, _, _ in _escape_candidates[:3]}

            for rank, (hno, pos_val) in enumerate(_sorted_by_pos):
                r = rank / _n if _n > 1 else 0.0  # 0.0=先頭
                _raw_pos = _raw_positions.get(hno, 0.5)
                _1c_pos = _1c_normalized.get(hno, 0.5) if _1c_normalized else _raw_pos

                # 改善7: 逃げ馬判定
                _is_nige = False
                if PACE_ESCAPE_SCORING_V2:
                    _is_nige = hno in _nige_set
                else:
                    _is_1c_leader = hno in _1c_top_horses and _1c_pos < 0.20
                    _is_raw_leader = _raw_pos < _nige_threshold and rank <= 2
                    _is_nige = _is_1c_leader or _is_raw_leader

                if _is_nige:
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
                # 4角位置スコア（展開図4角タブ用）
                ev._normalized_position = _normalized_positions.get(ev.horse.horse_no)
                # Phase 9A: 1角位置スコア（展開図初角タブ用）
                ev._position_1c = _1c_normalized.get(ev.horse.horse_no)
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

        # ---- 調教偏差値 (7因子目) ----
        _compute_training_devs(evaluations, race)

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

        # ---- gap連動ブレンド調整: composite gapが大きい場合はRule重視 ----
        # MLが均等予測を出す場合にRule（gap補正済み）の情報を保護する
        # バックテスト38,019レース: gap 3-15pt帯で予測が8pt過小評価→gap連動で改善
        _all_composites = sorted([ev.composite for ev in evaluations], reverse=True)
        _gap_1_2 = (_all_composites[0] - _all_composites[1]) if len(_all_composites) >= 2 else 0
        if _gap_1_2 >= 2.0:
            import math as _m
            # gap 2.0ptから緩やかにRule重みを増加（対数的飽和）
            _gap_boost = min(0.25, _m.log1p((_gap_1_2 - 2.0) * 0.25) * 0.15)
            # gap 10pt超で減衰（gap補正との二重効果による過大評価を防止）
            if _gap_1_2 >= 15.0:
                _gap_boost *= 0.40  # 超大gap: 実勝率が低下する帯
            elif _gap_1_2 >= 10.0:
                _gap_boost *= 0.75  # 大gap: 過大評価を抑制
            _RB_W = min(0.80, _RB_W + _gap_boost)
            _ML_W = 1.0 - _RB_W

        logger.debug(
            "動的ブレンド比率: Rule %.0f%% / ML %.0f%% (LGBMレベル %s, gap=%.1f)",
            _RB_W * 100, _ML_W * 100,
            _lgbm_level, _gap_1_2,
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
        # 中間正規化: 合計のみ調整（市場ブレンドは最終のみ適用）
        _normalize_sums_only(evaluations)

        # ---- Step 5.5: 削除済み（能力偏差値は絶対評価、LGBMフィードバック不要） ----

        # ---- Step 5.6: ML予測確率 → composite 直接反映 ----
        # win_prob（MLブレンド済み）を偏差値スケールに変換し、composite に加算
        # これにより印（composite順）がML予測を反映するようになる
        #
        # 改善: ±2.5pt → ±5.0pt に拡大 + 順位乖離ペナルティ
        # composite順位とwin_prob順位が大きく離れている場合、追加ペナルティで矯正
        try:
            _win_probs = [ev.win_prob for ev in evaluations]
            _n_wp = len(_win_probs)
            if _n_wp >= 3:
                _avg_wp = sum(_win_probs) / _n_wp
                _std_wp = (sum((p - _avg_wp) ** 2 for p in _win_probs) / _n_wp) ** 0.5
                if _std_wp > 0.001:
                    # 順位乖離ペナルティ用: 暫定composite順位とwin_prob順位を算出
                    _comp_vals = [(ev.composite, i) for i, ev in enumerate(evaluations)]
                    _comp_vals.sort(key=lambda x: -x[0])
                    _comp_ranks = [0] * _n_wp  # index → 順位(1-based)
                    for rank, (_, idx) in enumerate(_comp_vals):
                        _comp_ranks[idx] = rank + 1

                    _wp_vals = [(ev.win_prob, i) for i, ev in enumerate(evaluations)]
                    _wp_vals.sort(key=lambda x: -x[0])
                    _wp_ranks = [0] * _n_wp
                    for rank, (_, idx) in enumerate(_wp_vals):
                        _wp_ranks[idx] = rank + 1

                    for i, ev in enumerate(evaluations):
                        # Z変換: (win_prob - 平均) / 標準偏差 → 偏差値スケール
                        _z = (ev.win_prob - _avg_wp) / _std_wp
                        # ±5.0pt にクランプ（旧±2.5pt → 拡大でMLの影響力を強化）
                        _raw_adj = max(-5.0, min(5.0, _z * 1.5))

                        # 順位乖離ペナルティ: composite順位よりwin_prob順位が3位以上低い場合
                        # （ML低評価をcompositeが過大評価しているケースのみ）
                        _rank_gap = _wp_ranks[i] - _comp_ranks[i]  # 正=MLの方が低評価
                        if _rank_gap >= 3:
                            # 3位差で-0.5pt, 6位差で-2.0pt, 最大-3.0pt
                            _rank_penalty = min(3.0, (_rank_gap - 2) * 0.5)
                            _raw_adj -= _rank_penalty

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

        # 中間正規化: 合計のみ（市場ブレンドは最終のみ適用）
        if not PIPELINE_V2_ENABLED or _reest_ratio >= 0.10:
            _normalize_sums_only(evaluations)

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

        # ---- メリハリ拡大: PROB_SHARPNESSべき乗変換（win_probのみ）----
        # place2/place3はキャリブレータ後の比率キャップで制約
        from config.settings import PROB_SHARPNESS
        if PROB_SHARPNESS != 1.0:
            # win_probのみシャープ化
            for ev in evaluations:
                if ev.win_prob is not None and ev.win_prob > 0:
                    ev.win_prob = ev.win_prob ** PROB_SHARPNESS
            # win正規化（Σ=1.0）
            _tw = sum(ev.win_prob or 0 for ev in evaluations)
            if _tw > 0:
                for ev in evaluations:
                    if ev.win_prob is not None:
                        ev.win_prob = ev.win_prob / _tw
            # 整合性制約: win <= place2 <= place3
            for ev in evaluations:
                w = ev.win_prob or 0.0
                ev.place2_prob = max(ev.place2_prob or 0.0, w)
                ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)

        # ---- 事後キャリブレーション (Isotonic Regression) ----
        if self._post_calibrator and self._post_calibrator.is_available:
            self._post_calibrator.apply(evaluations)
            # キャリブレータが確率を書き換えるため、Σ=1.0/2.0/3.0に再正規化
            _cal_tw = sum(ev.win_prob or 0 for ev in evaluations)
            _cal_t2 = sum(ev.place2_prob or 0 for ev in evaluations)
            _cal_t3 = sum(ev.place3_prob or 0 for ev in evaluations)
            if _cal_tw > 0:
                for ev in evaluations:
                    if ev.win_prob is not None:
                        ev.win_prob /= _cal_tw
            if _cal_t2 > 0:
                for ev in evaluations:
                    if ev.place2_prob is not None:
                        ev.place2_prob = ev.place2_prob / _cal_t2 * 2.0
            if _cal_t3 > 0:
                for ev in evaluations:
                    if ev.place3_prob is not None:
                        ev.place3_prob = ev.place3_prob / _cal_t3 * 3.0

        # ---- 極端な確率逆転の是正 ----
        # 「1着はないが2,3着はありそう」は正常なパターン。
        # ただし勝率0.4%の馬が連対率56%（1位）のような数学的矛盾は修正する。
        # 制約: 各馬の連対率・複勝率は合理的な上限を超えない
        #   連対率上限 = フィールド平均(2/N) × 3.5
        #   複勝率上限 = フィールド平均(3/N) × 3.5
        _n_horses = len(evaluations)
        if _n_horses >= 2:
            _p2_cap = min(0.85, 2.0 / _n_horses * 3.5)  # 14頭: 50%, 8頭: 87.5%→85%
            _p3_cap = min(0.92, 3.0 / _n_horses * 3.5)  # 14頭: 75%, 8頭: 92%
            _capped = False
            for ev in evaluations:
                if (ev.place2_prob or 0.0) > _p2_cap:
                    ev.place2_prob = _p2_cap
                    _capped = True
                if (ev.place3_prob or 0.0) > _p3_cap:
                    ev.place3_prob = _p3_cap
                    _capped = True
            # キャップ適用後に再正規化
            if _capped:
                _t2_fix = sum(ev.place2_prob or 0.0 for ev in evaluations)
                _t3_fix = sum(ev.place3_prob or 0.0 for ev in evaluations)
                if _t2_fix > 0:
                    for ev in evaluations:
                        if ev.place2_prob is not None:
                            ev.place2_prob = min(1.0, ev.place2_prob / _t2_fix * 2.0)
                if _t3_fix > 0:
                    for ev in evaluations:
                        if ev.place3_prob is not None:
                            ev.place3_prob = min(1.0, ev.place3_prob / _t3_fix * 3.0)
            # 個馬制約: win <= place2 <= place3
            for ev in evaluations:
                w = ev.win_prob or 0.0
                ev.place2_prob = max(ev.place2_prob or 0.0, w)
                ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)

            # 勝率対比の上限制約（全馬対象・反復収束）
            # 実オッズ準拠: 勝率に応じた可変キャップ
            #   win>=20%: r3<=3.5, win>=10%: r3<=5.0, win>=5%: r3<=7.0,
            #   win>=2%: r3<=8.0, win<2%: r3<=10.0
            _n_h = len(evaluations)
            _target2 = min(_n_h, 2)
            _target3 = min(_n_h, 3)

            def _get_ratio_cap(w):
                """勝率に応じた複勝比率上限（実オッズベース）"""
                if w >= 0.20:
                    return 2.5, 3.5
                elif w >= 0.10:
                    return 3.5, 5.0
                elif w >= 0.05:
                    return 5.0, 7.0
                elif w >= 0.02:
                    return 6.0, 8.0
                else:
                    return 7.0, 10.0

            for _rc_iter in range(10):
                _ratio_capped = False
                for ev in evaluations:
                    w = ev.win_prob or 0.0
                    if w > 0:
                        _r2_max, _r3_max = _get_ratio_cap(w)
                        _p2_cap = w * _r2_max
                        _p3_cap = w * _r3_max
                        if (ev.place2_prob or 0.0) > _p2_cap:
                            ev.place2_prob = _p2_cap
                            _ratio_capped = True
                        if (ev.place3_prob or 0.0) > _p3_cap:
                            ev.place3_prob = _p3_cap
                            _ratio_capped = True
                if not _ratio_capped:
                    break
                # 再正規化（place2=2.0, place3=3.0）
                _t2r = sum(ev.place2_prob or 0.0 for ev in evaluations)
                _t3r = sum(ev.place3_prob or 0.0 for ev in evaluations)
                if _t2r > 0:
                    for ev in evaluations:
                        if ev.place2_prob is not None:
                            ev.place2_prob = ev.place2_prob / _t2r * _target2
                if _t3r > 0:
                    for ev in evaluations:
                        if ev.place3_prob is not None:
                            ev.place3_prob = ev.place3_prob / _t3r * _target3
                # 整合性制約: win <= place2 <= place3
                for ev in evaluations:
                    w = ev.win_prob or 0.0
                    ev.place2_prob = max(ev.place2_prob or 0.0, w)
                    ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)

        # ---- Step 6: 穴馬スコア（正規化後の composite 使用）[A3] ----
        for ev in evaluations:
            ev.ana_score, ev.ana_type = calc_ana_score(ev, evaluations)

        # ---- Step 6b: 特選穴馬スコア（composite上位5頭を除外 → ☆候補のみ）----
        from config.settings import TOKUSEN_MAX_PER_RACE, TOKUSEN_SCORE_THRESHOLD
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
        from config.settings import TOKUSEN_KIKEN_MAX_PER_RACE, TOKUSEN_KIKEN_SCORE_THRESHOLD
        for ev in evaluations:
            ev.tokusen_kiken_score = calc_tokusen_kiken_score(ev, evaluations, is_jra=self.is_jra)
        tokusen_kiken_candidates = sorted(
            [ev for ev in evaluations if ev.tokusen_kiken_score >= TOKUSEN_KIKEN_SCORE_THRESHOLD],
            key=lambda e: e.tokusen_kiken_score, reverse=True,
        )
        for ev in tokusen_kiken_candidates[:TOKUSEN_KIKEN_MAX_PER_RACE]:
            ev.is_tokusen_kiken = True

        # Phase 9E（旧オッズキャップ）は撤去済み。
        # 絶対上限キャップ（80%/85%/90%）は _normalize_probs 内で適用。

        # ---- 最終正規化保証: Σwin=1.0, Σp2=2.0, Σp3=3.0 ----
        # 上流の各処理（キャリブレータ、比率キャップ、整合性制約等）で
        # 合計がずれる可能性があるため、最終ステップで強制正規化する。
        _final_tw = sum(ev.win_prob or 0 for ev in evaluations)
        _final_t2 = sum(ev.place2_prob or 0 for ev in evaluations)
        _final_t3 = sum(ev.place3_prob or 0 for ev in evaluations)
        if _final_tw > 0:
            for ev in evaluations:
                if ev.win_prob is not None:
                    ev.win_prob /= _final_tw
        if _final_t2 > 0:
            _s2 = 2.0 / _final_t2
            for ev in evaluations:
                if ev.place2_prob is not None:
                    ev.place2_prob *= _s2
        if _final_t3 > 0:
            _s3 = 3.0 / _final_t3
            for ev in evaluations:
                if ev.place3_prob is not None:
                    ev.place3_prob *= _s3
        # 整合性: win <= place2 <= place3
        for ev in evaluations:
            w = ev.win_prob or 0.0
            ev.place2_prob = max(ev.place2_prob or 0.0, w)
            ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)

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

        # 1角正規化値をanalysisに格納（_compute_detail_gradesで参照）
        analysis._1c_normalized = _1c_normalized

        # ---- Phase 14: 道中タイム算出 ----
        if front_3f_est and last_3f_est and predicted_race_time:
            _mid = round(predicted_race_time - front_3f_est - last_3f_est, 1)
            analysis.estimated_mid_time = max(0, _mid)
        else:
            analysis.estimated_mid_time = None

        # ---- 各馬の個別セクションタイム予測 ----
        # ev.pace が _evaluate_horse() で確定した後に実行
        # （_evaluate_horse内でPaceDeviationCalculator.calc()がev.paceを新オブジェクトに置換するため）
        if front_3f_est and not _is_banei:
            from src.calculator.pace_course import PaceDeviationCalculator
            _is_dirt_course = "ダート" in (race.course.course_id if race.course else "")
            _sec_table = PaceDeviationCalculator.POSITION_SEC_BY_PACE_DIRT if _is_dirt_course else PaceDeviationCalculator.POSITION_SEC_BY_PACE_TURF
            _sec_per_rank = _sec_table.get(
                pace_type, PaceDeviationCalculator.POSITION_SEC_PER_RANK
            )
            _fc = max(1, race.field_count or len(evaluations))
            _race_dist = race.course.distance if race.course else 1200
            # ---- ステップ1: 全馬の過去走からレース基準ペース速度を収集 ----
            # 個別馬の速度をそのまま使うと異常値や脚質無視の問題が起きるため
            # 中央値を「レース基準速度」として使い、position_initialでオフセット
            _raw_speeds = []
            for ev in evaluations:
                _mid_same, _mid_all = [], []
                for _run in (ev.horse.past_runs or [])[-10:]:
                    _ft = getattr(_run, 'finish_time_sec', None)
                    _l3f_r = getattr(_run, 'last_3f_sec', None)
                    _dist = getattr(_run, 'distance', 0)
                    if _ft and _l3f_r and _dist >= 1000 and _ft > 0 and 28 <= _l3f_r <= 50 and _ft > _l3f_r:
                        _spd = (_ft - _l3f_r) / (_dist - 600)
                        _mid_all.append(_spd)
                        if abs(_dist - _race_dist) <= 200:
                            _mid_same.append(_spd)
                _mid_speeds = _mid_same if _mid_same else _mid_all
                if _mid_speeds:
                    _raw_speeds.append(sum(_mid_speeds) / len(_mid_speeds))

            # レース基準速度 = 中央値（個別異常値に強い）
            _today_mid_dist = max(0, _race_dist - 1200)
            if _raw_speeds:
                _raw_speeds.sort()
                _base_spd = _raw_speeds[len(_raw_speeds) // 2]
                _base_f3f = _base_spd * 600
                _base_mid = _base_spd * _today_mid_dist
            elif predicted_race_time and predicted_race_time > 0 and front_3f_est:
                _base_f3f = front_3f_est
                _base_mid = max(0, predicted_race_time - front_3f_est - (last_3f_est or 0))
            else:
                _base_f3f = front_3f_est or 35.0
                _base_mid = 0.0

            # ---- ステップ2: 各馬のfront3f = 基準 + position_initialオフセット ----
            # X座標（predicted_corners）と同じ方向に連動するため矛盾が解消
            # offsetをfront3fとmid_secに配分（合計でoffset）して二重カウント防止
            _mid_ratio = _today_mid_dist / 600 if _today_mid_dist > 0 else 0
            _f3f_share = 1.0 / (1.0 + _mid_ratio) if _mid_ratio > 0 else 1.0
            _mid_share = 1.0 - _f3f_share
            for ev in evaluations:
                _pos_init = getattr(ev, "_normalized_position", 0.5)
                _offset = _pos_init * _fc * _sec_per_rank
                ev.pace.estimated_front_3f = round(_base_f3f + _offset * _f3f_share, 2)
                ev.pace.estimated_mid_sec = round(max(0, _base_mid + _offset * _mid_share), 2)

                # 推定走破タイム
                _l3f = ev.pace.estimated_last3f or (last_3f_est or 0)
                _tt = ev.pace.estimated_front_3f + ev.pace.estimated_mid_sec + _l3f
                ev.pace.estimated_total_time = round(_tt, 2)

            # 全馬の差を現実的な範囲に圧縮（最大差キャップ）
            # 実際のレースで最後方が先頭から離される差: 短距離2-3秒、中距離3-4秒、長距離4-5秒
            _max_gap = 2.5 + (_race_dist - 1000) * 0.001  # 1200m→2.7秒, 1600m→3.1秒, 2000m→3.5秒, 2400m→3.9秒
            _max_gap = max(2.0, min(5.0, _max_gap))
            _all_tt = [ev.pace.estimated_total_time for ev in evaluations if ev.pace.estimated_total_time]
            if _all_tt:
                _min_tt = min(_all_tt)
                _raw_max_gap = max(_all_tt) - _min_tt
                if _raw_max_gap > _max_gap and _raw_max_gap > 0:
                    _scale = _max_gap / _raw_max_gap
                    for ev in evaluations:
                        if ev.pace.estimated_total_time:
                            _diff = ev.pace.estimated_total_time - _min_tt
                            _new_diff = _diff * _scale
                            ev.pace.estimated_total_time = round(_min_tt + _new_diff, 2)
                            # front_3f と mid_sec も同じ比率で圧縮
                            _f_min = min(e.pace.estimated_front_3f for e in evaluations if e.pace.estimated_front_3f)
                            _f_diff = (ev.pace.estimated_front_3f or _f_min) - _f_min
                            ev.pace.estimated_front_3f = round(_f_min + _f_diff * _scale, 2)
                            if ev.pace.estimated_mid_sec:
                                _m_min = min((e.pace.estimated_mid_sec or 0) for e in evaluations)
                                _m_diff = ev.pace.estimated_mid_sec - _m_min
                                ev.pace.estimated_mid_sec = round(_m_min + _m_diff * _scale, 2)

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
                    PaceType.H: -1.1, PaceType.M: 0.0, PaceType.S: 1.1,
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
        """脚質グループを尊重した最終隊列予測

        脚質グループ（逃げ/先行/差し/追込）は _style_map の判定を厳守し、
        グループ内での並び順のみ composite で調整する。
        能力による脚質グループ間の移動は行わない（展開図と脚質の整合性を保証）。
        """
        try:
            # composite順位を取得（グループ内ソート用）
            comp_map = {ev.horse.horse_no: ev.composite for ev in evaluations}

            # 各グループ内を composite 降順でソート（強い馬が前方）
            def _sort_by_comp(horse_list):
                return sorted(horse_list, key=lambda no: comp_map.get(no, 0), reverse=True)

            result = {
                "先頭": _sort_by_comp(list(leading)),
                "好位": _sort_by_comp(list(front_h)),
                "中団": _sort_by_comp(list(mid_h)),
                "後方": _sort_by_comp(list(rear_h)),
            }
            return result
        except Exception:
            logger.debug("最終隊列予測をスキップ", exc_info=True)
            return {"先頭": list(leading), "好位": list(front_h), "中団": list(mid_h), "後方": list(rear_h)}

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
        override_pos_1c: Optional[float] = None,
    ) -> HorseEvaluation:
        ev = HorseEvaluation(horse=horse)

        # 芝ダ転換コンテキスト構築（同馬場走がない馬のみ）
        _switch_ctx = None
        _race_surface = race.course.surface if race.course else None
        if _race_surface is None:
            # レース情報不完全 — 転換判定スキップ
            _has_same_surface = True
        else:
            _has_same_surface = any(
                r.surface == _race_surface for r in (horse.past_runs or [])
            )
        if not _has_same_surface and horse.past_runs:
            # 異馬場走のみ → 転換コンテキストを構築
            from config.settings import DATABASE_PATH as _DB_PATH_CTX
            _source_surface = "芝" if _race_surface == "ダート" else "ダート"
            _switch_dir = "turf_to_dirt" if _race_surface == "ダート" else "dirt_to_turf"

            # 種牡馬・母父の馬場別複勝率をrace_logから取得
            _sire_tgt_pr, _sire_src_pr = None, None
            _bms_tgt_pr, _bms_src_pr = None, None
            try:
                _sire_nm = getattr(horse, 'sire', '') or ''
                _bms_nm = getattr(horse, 'maternal_grandsire', '') or ''
                if _sire_nm or _bms_nm:
                    import sqlite3 as _ctx_sql
                    with _ctx_sql.connect(_DB_PATH_CTX) as _ctx_conn:
                        if _sire_nm:
                            _s_rows = _ctx_conn.execute(
                                "SELECT surface, "
                                "SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)*1.0/COUNT(*) "
                                "FROM race_log WHERE sire_name=? AND is_jra=1 AND finish_pos>0 "
                                "GROUP BY surface HAVING COUNT(*)>=20",
                                (_sire_nm,)
                            ).fetchall()
                            _sire_map = {s: pr for s, pr in _s_rows}
                            _sire_tgt_pr = _sire_map.get(_race_surface)
                            _sire_src_pr = _sire_map.get(_source_surface)
                        if _bms_nm:
                            _b_rows = _ctx_conn.execute(
                                "SELECT surface, "
                                "SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)*1.0/COUNT(*) "
                                "FROM race_log WHERE bms_name=? AND is_jra=1 AND finish_pos>0 "
                                "GROUP BY surface HAVING COUNT(*)>=20",
                                (_bms_nm,)
                            ).fetchall()
                            _bms_map = {s: pr for s, pr in _b_rows}
                            _bms_tgt_pr = _bms_map.get(_race_surface)
                            _bms_src_pr = _bms_map.get(_source_surface)
            except Exception:
                logger.debug("転換コンテキスト: 血統DB参照失敗", exc_info=True)

            # 騎手の馬場別複勝率
            _j_tgt_pr, _j_src_pr = None, None
            try:
                _jid = getattr(horse, 'jockey_id', '') or ''
                if _jid and race.race_date and race.race_date[:4].isdigit():
                    import sqlite3 as _ctx_sql2
                    with _ctx_sql2.connect(_DB_PATH_CTX) as _ctx_conn2:
                        _j_rows = _ctx_conn2.execute(
                            "SELECT surface, "
                            "SUM(CASE WHEN finish_pos<=3 THEN 1 ELSE 0 END)*1.0/COUNT(*) "
                            "FROM race_log WHERE jockey_id=? AND is_jra=1 AND finish_pos>0 "
                            "AND race_date>=? "
                            "GROUP BY surface HAVING COUNT(*)>=10",
                            (_jid, str(int(race.race_date[:4]) - 2) + race.race_date[4:])
                        ).fetchall()
                        _j_map = {s: pr for s, pr in _j_rows}
                        _j_tgt_pr = _j_map.get(_race_surface)
                        _j_src_pr = _j_map.get(_source_surface)
            except Exception:
                logger.debug("転換コンテキスト: 騎手DB参照失敗", exc_info=True)

            # 推定位置取り（override_positionを使用、なければ簡易推定）
            _pred_pos = override_position
            if _pred_pos is None and horse.past_runs:
                # 直近走の位置取りを簡易プロキシとして使用
                _last = horse.past_runs[0]
                _pos4c = getattr(_last, 'position_4c', None)
                _fc = getattr(_last, 'field_count', None) or 12
                if _pos4c and _fc > 0:
                    _pred_pos = _pos4c / _fc  # 0.0=先頭 〜 1.0=最後方

            _switch_ctx = {
                "switch_direction": _switch_dir,
                "predicted_position": _pred_pos,
                "horse_weight": getattr(horse, 'horse_weight', None),
                "age": getattr(horse, 'age', None),
                "sire_target_pr": _sire_tgt_pr,
                "sire_source_pr": _sire_src_pr,
                "jockey_target_pr": _j_tgt_pr,
                "jockey_source_pr": _j_src_pr,
                "gate_no": getattr(horse, 'gate_no', None) or getattr(horse, 'horse_no', None),
                "field_count": race.field_count,
                "bms_target_pr": _bms_tgt_pr,
                "bms_source_pr": _bms_src_pr,
            }

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
            surface_switch_context=_switch_ctx,
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
                override_pos_1c=override_pos_1c,
                field_style_distribution=pace_context.get("field_style_distribution"),  # 改善2
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


def _normalize_sums_only(evaluations: List[HorseEvaluation]) -> None:
    """
    合計正規化のみ（市場ブレンド・オッズキャップなし）。
    パイプライン途中の中間正規化用。市場ブレンドの多重適用を防止する。
    """
    n = len(evaluations)
    tw = sum(ev.win_prob or 0.0 for ev in evaluations)
    t2 = sum(ev.place2_prob or 0.0 for ev in evaluations)
    t3 = sum(ev.place3_prob or 0.0 for ev in evaluations)

    for ev in evaluations:
        if tw > 0 and ev.win_prob is not None:
            ev.win_prob = ev.win_prob / tw
        if t2 > 0 and ev.place2_prob is not None:
            ev.place2_prob = min(0.95, ev.place2_prob / t2 * 2.0)
        if t3 > 0 and ev.place3_prob is not None:
            ev.place3_prob = min(0.95, ev.place3_prob / t3 * 3.0)

    # 整合性: win <= place2 <= place3
    for ev in evaluations:
        w = ev.win_prob or 0.0
        ev.place2_prob = max(ev.place2_prob or 0.0, w)
        ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)


def _normalize_probs(evaluations: List[HorseEvaluation]) -> None:
    """
    ML blend後の勝率・連対率・複勝率を理論値に正規化する。
    市場ブレンド・オッズキャップ含むフル処理。最終正規化でのみ使用。
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

    # 絶対上限キャップ（オッズ非依存: ML異常値のみブロック）
    # 旧: オッズベースキャップ（market_prob * 3.0）→ 大穴馬のML評価を潰していた
    # 新: 純粋な絶対上限のみ。MLが大穴を自由に評価できる
    PROB_CAP_WIN = 0.80    # 勝率上限80%
    PROB_CAP_TOP2 = 0.85   # 連対率上限85%
    PROB_CAP_TOP3 = 0.90   # 複勝率上限90%
    capped = False
    for ev in evaluations:
        if ev.win_prob is not None and ev.win_prob > PROB_CAP_WIN:
            ev.win_prob = PROB_CAP_WIN
            capped = True
        if ev.place2_prob is not None and ev.place2_prob > PROB_CAP_TOP2:
            ev.place2_prob = PROB_CAP_TOP2
        if ev.place3_prob is not None and ev.place3_prob > PROB_CAP_TOP3:
            ev.place3_prob = PROB_CAP_TOP3

    # キャップ後の再正規化
    if capped:
        tw2 = sum(ev.win_prob or 0.0 for ev in evaluations)
        if tw2 > 0:
            for ev in evaluations:
                if ev.win_prob is not None:
                    ev.win_prob = ev.win_prob / tw2

    # 市場確率アンカリング: モデル推定と市場確率をブレンド
    from config.settings import MARKET_BLEND_RATIO
    has_odds = sum(1 for ev in evaluations if ev.horse.odds is not None and ev.horse.odds > 0)
    if MARKET_BLEND_RATIO > 0 and has_odds >= len(evaluations) * 0.5:
        for ev in evaluations:
            odds = ev.horse.odds
            if odds is not None and odds > 0 and ev.win_prob is not None:
                market_prob = min(0.95, 0.80 / odds)
                ev.win_prob = (1 - MARKET_BLEND_RATIO) * ev.win_prob + MARKET_BLEND_RATIO * market_prob
        # ブレンド後の再正規化
        tw3 = sum(ev.win_prob or 0.0 for ev in evaluations)
        if tw3 > 0:
            for ev in evaluations:
                if ev.win_prob is not None:
                    ev.win_prob = ev.win_prob / tw3

    # 勝率基準で連対率・複勝率を数学的に導出
    # P(2着以内) = P(勝ち) + P(勝たない) × P(2着|勝たない)
    n = len(evaluations)
    # フィールド平均の「勝たない場合に2着になる確率」をフォールバック値として計算
    # 理論値: N頭中2頭が2着以内 → 勝馬以外のN-1頭から1頭 → 1/(N-1)
    _default_p2_cond = 1.0 / max(1, n - 1)
    _default_p3_cond = 1.0 / max(1, n - 2)
    for ev in evaluations:
        w = ev.win_prob or 0.0
        p2_raw = ev.place2_prob or 0.0
        p3_raw = ev.place3_prob or 0.0
        if w > 0 and w < 1.0:
            if p2_raw > w:
                p2_given_not_win = (p2_raw - w) / (1.0 - w)
            else:
                # 正規化で逆転した場合: フィールド平均を下限に使う
                p2_given_not_win = _default_p2_cond
            ev.place2_prob = w + (1.0 - w) * p2_given_not_win
            p2_new = ev.place2_prob
            if p3_raw > p2_raw and p2_raw < 1.0:
                p3_given_not_p2 = (p3_raw - p2_raw) / (1.0 - p2_raw)
            else:
                p3_given_not_p2 = _default_p3_cond
            ev.place3_prob = p2_new + (1.0 - p2_new) * p3_given_not_p2
        else:
            ev.place2_prob = max(p2_raw, w)
            ev.place3_prob = max(p3_raw, ev.place2_prob)

    # 合計を2.0/3.0に正規化
    t2_after = sum(ev.place2_prob or 0.0 for ev in evaluations)
    t3_after = sum(ev.place3_prob or 0.0 for ev in evaluations)
    if t2_after > 0:
        scale2 = 2.0 / t2_after
        for ev in evaluations:
            if ev.place2_prob is not None:
                ev.place2_prob *= scale2
    if t3_after > 0:
        scale3 = 3.0 / t3_after
        for ev in evaluations:
            if ev.place3_prob is not None:
                ev.place3_prob *= scale3

    # 最終整合性チェック（正規化後も win <= place2 <= place3 を保証）
    for ev in evaluations:
        w = ev.win_prob or 0.0
        ev.place2_prob = max(ev.place2_prob or 0.0, w)
        ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob)


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
# グレード判定用フィールド内正規化
# ============================================================


def _normalize_for_grading(evaluations: List[HorseEvaluation]) -> None:
    """
    グレード判定用に全ファクターの偏差値をN(52.5, 6.4)に正規化する。
    composite算出に使う生の偏差値(ability.total, pace.total等)は変更しない。
    正規化後の値を _grade_*_dev 属性にセットし、グレード算出時に参照する。
    """
    import statistics as _stat

    _TARGET_MEAN = 52.5
    _TARGET_SIGMA = 7.0

    # (属性名suffix, 値取得関数)
    _factors = [
        ("ability", lambda ev: ev.ability.total),
        ("pace", lambda ev: ev.pace.total),
        ("course", lambda ev: ev.course.total),
        ("jockey", lambda ev: getattr(ev, "_jockey_dev", None)),
        ("trainer", lambda ev: getattr(ev, "_trainer_dev", None)),
        ("bloodline", lambda ev: getattr(ev, "_bloodline_dev", None)),
        ("sire", lambda ev: getattr(ev, "_sire_dev", None)),
        ("mgs", lambda ev: getattr(ev, "_mgs_dev", None)),
        ("composite", lambda ev: ev.composite),
    ]

    for name, getter in _factors:
        vals = [getter(ev) for ev in evaluations if getter(ev) is not None]
        if len(vals) < 2:
            continue
        mu = _stat.mean(vals)
        sigma = _stat.pstdev(vals) or 1.0
        for ev in evaluations:
            raw = getter(ev)
            if raw is None:
                continue
            normalized = _TARGET_MEAN + (raw - mu) / sigma * _TARGET_SIGMA
            # 20-100クランプ
            normalized = max(20.0, min(100.0, normalized))
            setattr(ev, f"_grade_{name}_dev", round(normalized, 1))


# ============================================================
# 調教偏差値（7因子目）
# ============================================================

# モジュールレベルキャッシュ（TrainingFeatureExtractorはロードに10秒かかるため）
_CACHE_TRAINING_EXTRACTOR = None
_CACHE_TRAINING_LOADED = False


def _inject_memory_training(ext, evaluations, race_id: str):
    """
    メモリ上のHorse.training_recordsからTrainingFeatureExtractorの
    _race_trainingにデータを注入する。

    run_analysis_date.py実行時、調教データはメモリに取得されるが
    DBには書き込まれない。TrainingFeatureExtractorはDBからしか読まないため、
    当日レースの調教データが取得できない。
    このフォールバックでメモリ上のデータを注入して解決する。
    """
    import json as _json

    # ハロン表記 → メートル表記の変換テーブル
    _F_TO_M = {"1F": 200, "2F": 400, "3F": 600, "4F": 800, "5F": 1000, "6F": 1200}

    injected = {}
    for ev in evaluations:
        horse = ev.horse
        hname = horse.horse_name
        if not hname or not horse.training_records:
            continue

        records = []
        for tr in horse.training_records:
            # TrainingRecord.splits のキーをメートル単位に変換
            # DB形式: {"600": 39.5, "800": 54.0}
            # メモリ形式: {"3F": 39.5, "4F": 54.0}
            converted_splits = {}
            if tr.splits:
                for k, v in tr.splits.items():
                    if k in _F_TO_M:
                        converted_splits[_F_TO_M[k]] = v
                    else:
                        try:
                            converted_splits[int(k)] = v
                        except (ValueError, TypeError):
                            pass
            splits_json = _json.dumps(converted_splits) if converted_splits else "{}"
            records.append({
                "race_id": race_id,
                "horse_name": hname,
                "date": tr.date or "",
                "course": tr.course or "",
                "splits_json": splits_json,
                "intensity_label": tr.intensity_label or "",
                "sigma_from_mean": tr.sigma_from_mean or 0.0,
                "comment": tr.comment or "",
                "stable_comment": tr.stable_comment or "",
            })

        if records:
            injected[hname] = records

    if injected:
        ext._race_training[race_id] = injected
        logger.debug("調教偏差値: メモリから%d馬の調教データを注入 (race_id=%s)",
                     len(injected), race_id)


def _compute_training_devs(
    evaluations: List[HorseEvaluation],
    race: Optional["RaceInfo"] = None,
):
    """
    レース全馬の調教偏差値を算出し、ev._training_dev にセットする。
    TrainingFeatureExtractorを使って調教特徴量を取得し、
    calc_training_dev()で偏差値化する。
    """
    global _CACHE_TRAINING_EXTRACTOR, _CACHE_TRAINING_LOADED

    # 全馬にデフォルト値をセット
    for ev in evaluations:
        if not hasattr(ev, "_training_dev"):
            ev._training_dev = None

    if not race:
        return

    try:
        # TrainingFeatureExtractorのロード（初回のみ）
        if not _CACHE_TRAINING_LOADED:
            try:
                from src.ml.training_features import TrainingFeatureExtractor
                ext = TrainingFeatureExtractor()
                ext.load_all()
                _CACHE_TRAINING_EXTRACTOR = ext
                logger.info("TrainingFeatureExtractor ロード成功")
            except Exception as e:
                logger.warning("調教偏差値: TrainingFeatureExtractorロード失敗: %s", e)
            _CACHE_TRAINING_LOADED = True

        ext = _CACHE_TRAINING_EXTRACTOR
        if not ext:
            return

        # 調教データがある馬が3頭未満の場合はスキップ
        horses_with_training = sum(
            1 for ev in evaluations
            if getattr(ev.horse, "training_records", None)
        )
        if horses_with_training < 3:
            logger.debug("調教偏差値スキップ: 調教データ%d頭のみ", horses_with_training)
            return

        # 馬名リストとレース情報
        horse_names = [ev.horse.horse_name for ev in evaluations if ev.horse.horse_name]
        if len(horse_names) < 3:
            return

        race_id = race.race_id
        race_date = race.race_date

        # DBにデータがない場合、メモリ上のtraining_recordsから注入
        if race_id not in ext._race_training:
            _inject_memory_training(ext, evaluations, race_id)

        # 調教特徴量を取得
        feat_map = ext.get_race_training_features(race_id, horse_names, race_date)
        if not feat_map:
            return

        # 偏差値を算出
        from src.ml.training_features import calc_training_dev
        dev_map = calc_training_dev(feat_map)

        # 各馬にセット
        for ev in evaluations:
            hname = ev.horse.horse_name
            if hname in dev_map and dev_map[hname] is not None:
                ev._training_dev = round(max(20.0, min(100.0, dev_map[hname])), 1)

    except Exception as e:
        logger.warning("調教偏差値算出エラー: %s", e)


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
    from config.settings import (
        BMS_BASE_PARAMS,
        BMS_FACTOR_WEIGHTS,
        JOCKEY_BASE_PARAMS_JRA,
        JOCKEY_BASE_PARAMS_NAR,
        JOCKEY_FACTOR_WEIGHTS,
        SIRE_BASE_PARAMS,
        SIRE_FACTOR_WEIGHTS,
        TRAINER_BASE_PARAMS_JRA,
        TRAINER_BASE_PARAMS_NAR,
        TRAINER_FACTOR_WEIGHTS,
    )
    from src.calculator.grades import compute_category_deviation

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
    # 31xxx（NAR公式略称ID）は除外し、非31xxx IDのみをマッピングに入れる
    _jockey_name_to_tracker_id: Dict[str, str] = {}
    _trainer_name_to_tracker_id: Dict[str, str] = {}
    # 略称名→正式名の前方一致用: {正式名: netkeiba_id} のリスト
    _jockey_fullnames: Dict[str, str] = {}
    _trainer_fullnames: Dict[str, str] = {}
    if rolling_tracker:
        try:
            import sqlite3 as _sql3
            _conn = _sql3.connect("data/keiba.db")
            # 騎手: jockey_name → netkeiba ID（最多出走のIDを採用）
            # 31xxx（NAR公式略称ID）は除外して正式名のみ収集
            _jrows = _conn.execute(
                "SELECT jockey_name, jockey_id, COUNT(*) as cnt "
                "FROM race_log WHERE jockey_id != '' AND jockey_name != '' "
                "AND jockey_id NOT LIKE '31%' "
                "GROUP BY jockey_name, jockey_id ORDER BY cnt DESC"
            ).fetchall()
            for _jn, _jid, _ in _jrows:
                import re as _re
                _jn_clean = _re.sub(r"[（(].+?[）)]", "", _jn).replace("\u3000", "").strip()
                if _jn_clean not in _jockey_name_to_tracker_id:
                    _jockey_name_to_tracker_id[_jn_clean] = _jid
                    _jockey_fullnames[_jn_clean] = _jid
            # 調教師: 同様（31xxxは調教師にも存在する可能性）
            _trows = _conn.execute(
                "SELECT trainer_name, trainer_id, COUNT(*) as cnt "
                "FROM race_log WHERE trainer_id != '' AND trainer_name != '' "
                "AND trainer_id NOT LIKE '31%' "
                "GROUP BY trainer_name, trainer_id ORDER BY cnt DESC"
            ).fetchall()
            for _tn, _tid, _ in _trows:
                import re as _re
                _tn_clean = _re.sub(r"[（(].+?[）)]", "", _tn).replace("\u3000", "").strip()
                if _tn_clean not in _trainer_name_to_tracker_id:
                    _trainer_name_to_tracker_id[_tn_clean] = _tid
                    _trainer_fullnames[_tn_clean] = _tid
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

        # 脚質キー・枠番キーの初期化（jockey/trainer両方で使用）
        _style_key = None
        _gate_key = None

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
                # 完全一致しない場合、略称名→正式名のsubsequence一致で検索
                # NAR公式出馬表の略称（例: "山本紀"）→ 正式名（例: "山本聡紀"）の解決
                # subsequence: 略称の全文字が正式名に順序通り含まれるかチェック
                if not j_entity and _jname_clean and len(_jname_clean) >= 2 and _jockey_fullnames:
                    # 旧字体→新字体の変換テーブル（NAR公式は旧字体を使うことがある）
                    # 旧字体→新字体の正規化。齋/齊/斎→斉 に統一
                    _KANJI_NORMALIZE = str.maketrans(
                        "渡邊邉齋齊斎斉國嶋髙櫻瀨藤廣濱邨澤塚靑曻龍鷗",
                        "渡辺辺斉斉斉斉国島高桜瀬藤広浜村沢塚青昇竜鷗",
                    )
                    def _norm(s: str) -> str:
                        return s.translate(_KANJI_NORMALIZE)
                    def _is_subseq(short: str, full: str) -> bool:
                        """短い名前の全文字が長い名前に順序通り含まれるか（異体字正規化済み）"""
                        pos = 0
                        for ch in short:
                            found = full.find(ch, pos)
                            if found < 0:
                                return False
                            pos = found + 1
                        return True
                    _jname_norm = _norm(_jname_clean)
                    _candidates = []
                    for _fn, _fid in _jockey_fullnames.items():
                        _fn_norm = _norm(_fn)
                        # 姓一致（先頭2文字、正規化後）+ subsequence一致 + 正式名の方が長い
                        if (_fn_norm[:2] == _jname_norm[:2]
                            and len(_fn) > len(_jname_clean)
                            and _is_subseq(_jname_norm, _fn_norm)
                            and _fid != jid):
                            _candidates.append((_fn, _fid))
                    # 候補が1つなら確定、複数ならrolling_trackerに存在するものを優先
                    if len(_candidates) == 1:
                        _alt_jid2 = _candidates[0][1]
                        j_entity = rolling_tracker.jockeys.get(_alt_jid2)
                        if j_entity:
                            _resolved_jid = _alt_jid2
                    elif len(_candidates) > 1:
                        for _cn, _cid in _candidates:
                            _ce = rolling_tracker.jockeys.get(_cid)
                            if _ce:
                                j_entity = _ce
                                _resolved_jid = _cid
                                break
            if j_entity and j_entity.runs >= 5:
                # 枠番帯キー算出（馬番→g12/g34/g56/g78）
                _hno = getattr(h, "horse_no", 0) or 0
                _fc = getattr(ev, "_field_count", 0) or len(evaluations) or 12
                _gate_key = None
                if _hno > 0:
                    _waku = _hno if _fc <= 8 else min(8, max(1, (_hno - 1) * 8 // _fc + 1))
                    _gate_key = f"g{(_waku - 1) // 2 * 2 + 1}{(_waku - 1) // 2 * 2 + 2}"
                # 脚質キー算出（逃げ/先行→front, 中団/差し→middle, 追込/後方→rear）
                _rs = getattr(ev, "_predicted_style", None) or getattr(getattr(ev, "pace", None), "running_style_jp", None) or ""
                _style_key = None
                if _rs in ("逃げ", "先行", "好位"):
                    _style_key = "front"
                elif _rs in ("中団", "差し"):
                    _style_key = "middle"
                elif _rs in ("追込", "後方"):
                    _style_key = "rear"
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
                    "style": j_entity.style_pr(_style_key) if _style_key else None,
                    "gate": j_entity.gate_pr(_gate_key) if _gate_key else None,
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
                    "style": _dict_runs(j_entity, 'style', _style_key) if _style_key else 0,
                    "gate": _dict_runs(j_entity, 'gate', _gate_key) if _gate_key else 0,
                    "horse": _dict_runs(j_entity, 'horse_combo', hid),
                }
                _jp = JOCKEY_BASE_PARAMS_JRA if _is_jra_person(_resolved_jid) else JOCKEY_BASE_PARAMS_NAR
                jdev = compute_category_deviation(
                    factor_rates, factor_runs, JOCKEY_FACTOR_WEIGHTS,
                    base_mean=_jp["mean"], base_sigma=_jp["sigma"],
                )
                if jdev is not None:
                    # キャリアキャップ: 少数騎乗の騎手は偏差値上限を抑制
                    # 5走→max55.0(B), 20走→max60.0(S), 50走→上限なし
                    _total_runs = j_entity.runs
                    if _total_runs < 50:
                        _career_cap = 55.0 + max(0, (_total_runs - 5)) * (15.0 / 45.0)
                        _career_cap = min(_career_cap, 70.0)
                        jdev = min(jdev, _career_cap)
                    ev._jockey_dev = round(max(20.0, min(100.0, jdev)), 1)
            elif ev.jockey_stats:
                # フォールバック: NAR騎手はrace_logから直接計算（personnel_dbは不正確な場合がある）
                _fb_dev = None
                if not _is_jra_person(jid):
                    _fb_dev = _race_log_jockey_dev(jid, getattr(h, "jockey", ""))
                if _fb_dev is not None:
                    ev._jockey_dev = round(max(20.0, min(100.0, _fb_dev)), 1)
                else:
                    # JRA騎手 or race_log取得失敗: 従来通りpersonnel_db使用
                    jdev = ev.jockey_stats.lower_long_dev
                    _all_default = (
                        ev.jockey_stats.upper_long_dev == 50.0
                        and ev.jockey_stats.upper_short_dev == 50.0
                        and ev.jockey_stats.lower_long_dev == 50.0
                        and ev.jockey_stats.lower_short_dev == 50.0
                    )
                    ev._jockey_dev = None if _all_default else round(max(20.0, min(100.0, jdev)), 1)

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
                # 完全一致しない場合、略称名→正式名のsubsequence一致で検索（異体字対応）
                if not t_entity and _tname_clean and len(_tname_clean) >= 2 and _trainer_fullnames:
                    _KANJI_NORM_T = str.maketrans(
                        "渡邊邉齋齊斎斉國嶋髙櫻瀨藤廣濱邨澤塚靑曻龍鷗",
                        "渡辺辺斉斉斉斉国島高桜瀬藤広浜村沢塚青昇竜鷗",
                    )
                    def _norm_t(s: str) -> str:
                        return s.translate(_KANJI_NORM_T)
                    def _is_subseq_t(short: str, full: str) -> bool:
                        pos = 0
                        for ch in short:
                            found = full.find(ch, pos)
                            if found < 0:
                                return False
                            pos = found + 1
                        return True
                    _tname_norm = _norm_t(_tname_clean)
                    _t_candidates = []
                    for _tfn, _tfid in _trainer_fullnames.items():
                        _tfn_norm = _norm_t(_tfn)
                        if (_tfn_norm[:2] == _tname_norm[:2]
                            and len(_tfn) > len(_tname_clean)
                            and _is_subseq_t(_tname_norm, _tfn_norm)
                            and _tfid != tid):
                            _t_candidates.append((_tfn, _tfid))
                    if len(_t_candidates) == 1:
                        _alt_tid2 = _t_candidates[0][1]
                        t_entity = rolling_tracker.trainers.get(_alt_tid2)
                        if t_entity:
                            _resolved_tid = _alt_tid2
                    elif len(_t_candidates) > 1:
                        for _tcn, _tcid in _t_candidates:
                            _tce = rolling_tracker.trainers.get(_tcid)
                            if _tce:
                                t_entity = _tce
                                _resolved_tid = _tcid
                                break
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
                    "style": t_entity.style_pr(_style_key) if _style_key and hasattr(t_entity, 'style_pr') else None,
                    "gate": t_entity.gate_pr(_gate_key) if _gate_key and hasattr(t_entity, 'gate_pr') else None,
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
                    "style": _dict_runs(t_entity, 'style', _style_key) if _style_key and hasattr(t_entity, 'style') else 0,
                    "gate": _dict_runs(t_entity, 'gate', _gate_key) if _gate_key and hasattr(t_entity, 'gate') else 0,
                    "horse": _dict_runs(t_entity, 'horse_combo', hid),
                }
                _tp = TRAINER_BASE_PARAMS_JRA if _is_jra_person(_resolved_tid) else TRAINER_BASE_PARAMS_NAR
                tdev = compute_category_deviation(
                    factor_rates, factor_runs, TRAINER_FACTOR_WEIGHTS,
                    base_mean=_tp["mean"], base_sigma=_tp["sigma"],
                )
                if tdev is not None:
                    # キャリアキャップ: 少数管理の調教師は偏差値上限を抑制
                    _total_truns = t_entity.runs
                    if _total_truns < 50:
                        _t_cap = 55.0 + max(0, (_total_truns - 5)) * (15.0 / 45.0)
                        _t_cap = min(_t_cap, 70.0)
                        tdev = min(tdev, _t_cap)
                    ev._trainer_dev = round(max(20.0, min(100.0, tdev)), 1)
            elif ev.trainer_stats:
                # フォールバック: NAR調教師はrace_logから直接計算
                _fb_tdev = None
                if not _is_jra_person(tid):
                    _fb_tdev = _race_log_trainer_dev(tid, getattr(h, "trainer", ""))
                if _fb_tdev is not None:
                    ev._trainer_dev = round(max(20.0, min(100.0, _fb_tdev)), 1)
                else:
                    # JRA調教師 or race_log取得失敗: 従来通りpersonnel_db使用
                    dev = getattr(ev.trainer_stats, "deviation", None)
                    if dev is not None:
                        ev._trainer_dev = None if dev == 50.0 else round(max(20.0, min(100.0, dev)), 1)

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
                    ev._bloodline_dev = round(max(20.0, min(100.0, sire_dev * 0.6 + bms_dev * 0.4)), 1)
                elif sire_dev is not None:
                    ev._bloodline_dev = round(max(20.0, min(100.0, sire_dev)), 1)
                elif bms_dev is not None:
                    ev._bloodline_dev = round(max(20.0, min(100.0, bms_dev)), 1)


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
    """race_log DBからそのレースの全馬の上がり3Fを取得して順位を算出。
    DBにデータがない場合はcourse_dbフォールバック。
    順位 = そのレースで自分より速い上がりタイムの馬の数 + 1"""
    if not run.last_3f_sec or run.last_3f_sec <= 0:
        return None

    our_l3f = run.last_3f_sec

    # 1. race_logから同一レースの全馬last_3fを取得（正確・確定データ）
    if run.race_id:
        try:
            import sqlite3 as _sql3
            _rl_conn = getattr(_get_l3f_rank, "_conn", None)
            if _rl_conn is None:
                from config.settings import DATABASE_PATH
                _rl_conn = _sql3.connect(DATABASE_PATH, check_same_thread=False)
                _rl_conn.execute("PRAGMA journal_mode=WAL")
                _get_l3f_rank._conn = _rl_conn
            rows = _rl_conn.execute(
                "SELECT last_3f_sec FROM race_log WHERE race_id=? AND finish_pos < 90 AND last_3f_sec > 0",
                (run.race_id,)
            ).fetchall()
            if rows:
                valid = [r[0] for r in rows if 28.0 <= r[0] <= 50.0]
                if valid:
                    rank = sum(1 for t in valid if t < our_l3f) + 1
                    return rank
        except Exception:
            pass

    # 2. フォールバック: course_dbインデックスから推定（出走馬のみ）
    candidates = idx.get((run.course_id, run.race_date), [])
    if not candidates:
        return None
    our_t = run.finish_time_sec
    race_group = [r for r in candidates if abs(r.finish_time_sec - our_t) <= 7.0]
    if not race_group:
        return None
    valid = [r.last_3f_sec for r in race_group if r.last_3f_sec and 28.0 <= r.last_3f_sec <= 50.0]
    if not valid:
        return None
    rank = sum(1 for t in valid if t < our_l3f) + 1
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
            lvl = max(20.0, min(100.0, lvl))

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
    # Phase 9E（旧オッズキャップ）は撤去済み → 絶対上限は _normalize_probs 内で適用

    # 再assign_marks前に最終正規化保証: Σwin=1.0, Σp2=2.0, Σp3=3.0
    _ft_w = sum(ev.win_prob or 0 for ev in analysis.evaluations)
    _ft_2 = sum(ev.place2_prob or 0 for ev in analysis.evaluations)
    _ft_3 = sum(ev.place3_prob or 0 for ev in analysis.evaluations)
    if _ft_w > 0:
        for ev in analysis.evaluations:
            if ev.win_prob is not None:
                ev.win_prob /= _ft_w
    if _ft_2 > 0:
        _s2 = 2.0 / _ft_2
        for ev in analysis.evaluations:
            if ev.place2_prob is not None:
                ev.place2_prob *= _s2
    if _ft_3 > 0:
        _s3 = 3.0 / _ft_3
        for ev in analysis.evaluations:
            if ev.place3_prob is not None:
                ev.place3_prob *= _s3

    from src.output.formatter import assign_marks as _assign_marks
    analysis.evaluations[:] = _assign_marks(analysis.evaluations, is_jra=engine.is_jra)

    return analysis


def _compute_detail_grades(engine: RaceAnalysisEngine, analysis: RaceAnalysis):
    """全頭診断用の詳細グレードを算出して各 HorseEvaluation に付与する"""
    from src.calculator.grades import (
        compute_ability_extras,
        compute_bloodline_detail_grades,
        compute_course_detail_grades,
        compute_jockey_detail_grades,
        compute_pace_extras,
        compute_profile_grades,
        compute_trainer_detail_grades,
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

    # analyze()で計算済みの1角正規化値を取得
    _1c_normalized = getattr(analysis, '_1c_normalized', {})

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
        # 数値（偏差値） — 20-100クランプ
        # _compute_personnel_devs() で rolling tracker から既に算出済みの場合は上書きしない
        def _clamp_dev(v):
            return round(max(20.0, min(100.0, v)), 1) if v is not None else None
        if getattr(ev, "_jockey_dev", None) is None:
            _j_dev = profile.get("jockey_dev")
            ev._jockey_dev = _clamp_dev(_j_dev) if _j_dev is not None else 50.0
        if getattr(ev, "_trainer_dev", None) is None:
            _t_dev = profile.get("trainer_dev")
            ev._trainer_dev = _clamp_dev(_t_dev) if _t_dev is not None else 50.0
        ev._sire_dev = _clamp_dev(profile.get("sire_dev"))
        ev._mgs_dev = _clamp_dev(profile.get("mgs_dev"))
        if getattr(ev, "_bloodline_dev", None) is None:
            _bl_dev = profile.get("bloodline_dev")
            # 血統データ欠損時は50.0（B中央）をデフォルト設定
            ev._bloodline_dev = _clamp_dev(_bl_dev) if _bl_dev is not None else 50.0
        # グレードは偏差値から導出（ループ後の _normalize_for_grading で正規化済み値に上書きされる）
        ev._jockey_grade = dev_to_grade(ev._jockey_dev) if ev._jockey_dev is not None else profile["jockey_grade"]
        ev._trainer_grade = dev_to_grade(ev._trainer_dev) if ev._trainer_dev is not None else profile["trainer_grade"]
        ev._sire_grade = dev_to_grade(ev._sire_dev) if ev._sire_dev is not None else profile["sire_grade"]
        ev._mgs_grade = dev_to_grade(ev._mgs_dev) if ev._mgs_dev is not None else profile["mgs_grade"]

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
        # 1角位置: 正規化済み値から算出（MLモデル + フォールバック統合）
        _1c_norm = _1c_normalized.get(h.horse_no)
        if _1c_norm is not None and _fc > 0:
            ev._estimated_pos_1c = round(max(1, _1c_norm * _fc + 1), 1)
        else:
            ev._estimated_pos_1c = None
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
            _jockey_dev = max(20.0, min(100.0, _raw_jdev)) if _raw_jdev is not None else None
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

    # ── グレード判定用フィールド内正規化（forループ後に実行） ──
    # 全馬の偏差値が確定した後、N(52.5, 6.4) に正規化して _grade_*_dev にセット。
    # composite算出に使う生の偏差値は変更しない。グレード表示のみに影響する。
    _normalize_for_grading(analysis.evaluations)

    # 正規化済み偏差値でグレードを再適用
    for ev in analysis.evaluations:
        _gj = getattr(ev, "_grade_jockey_dev", None)
        if _gj is not None:
            ev._jockey_grade = dev_to_grade(_gj)
        _gt = getattr(ev, "_grade_trainer_dev", None)
        if _gt is not None:
            ev._trainer_grade = dev_to_grade(_gt)
        _gs = getattr(ev, "_grade_sire_dev", None)
        if _gs is not None:
            ev._sire_grade = dev_to_grade(_gs)
        _gm = getattr(ev, "_grade_mgs_dev", None)
        if _gm is not None:
            ev._mgs_grade = dev_to_grade(_gm)
