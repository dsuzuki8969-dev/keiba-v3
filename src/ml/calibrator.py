"""
事後キャリブレーション: Isotonic Regression (JRA/NAR 分離版)

パイプライン全体の出力（win_prob, place2_prob, place3_prob）に対して
Isotonic Regression を適用し、キャリブレーションを改善する。

- 単調変換のため、馬の順序（AUC）は保存される
- 41万頭のデータで学習するため、過学習リスクは低い
- scripts/build_calibrator.py で学習・保存

Phase 3 (2026-05-24) 改修:
- JRA データ (人気・調教師厳格) と NAR データ (混戦傾向強・配当大) の
  分布差を反映するため、JRA/NAR 別 Isotonic モデルを学習・適用
- 互換性: 新しい _jra.pkl / _nar.pkl が揃っていればそれを優先、
  どちらか欠落時は legacy (calibrator_*.pkl, JRA/NAR 共通) にフォールバック
"""

import os
import pickle

from src.log import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
CALIBRATOR_DIR = os.path.join(_PROJECT_ROOT, "data", "models")

# JRA 専用 (Phase 3)
CALIBRATOR_PATHS_JRA = {
    "win": os.path.join(CALIBRATOR_DIR, "calibrator_win_jra.pkl"),
    "top2": os.path.join(CALIBRATOR_DIR, "calibrator_top2_jra.pkl"),
    "top3": os.path.join(CALIBRATOR_DIR, "calibrator_top3_jra.pkl"),
}

# NAR 専用 (Phase 3)
CALIBRATOR_PATHS_NAR = {
    "win": os.path.join(CALIBRATOR_DIR, "calibrator_win_nar.pkl"),
    "top2": os.path.join(CALIBRATOR_DIR, "calibrator_top2_nar.pkl"),
    "top3": os.path.join(CALIBRATOR_DIR, "calibrator_top3_nar.pkl"),
}

# レガシー (Phase 3 以前・JRA/NAR 共通)
CALIBRATOR_PATHS_LEGACY = {
    "win": os.path.join(CALIBRATOR_DIR, "calibrator_win.pkl"),
    "top2": os.path.join(CALIBRATOR_DIR, "calibrator_top2.pkl"),
    "top3": os.path.join(CALIBRATOR_DIR, "calibrator_top3.pkl"),
}

# 後方互換用エイリアス (既存コード参照対策)
CALIBRATOR_PATHS = CALIBRATOR_PATHS_LEGACY


class PostCalibrator:
    """Isotonic Regression による事後キャリブレーション (JRA/NAR 分離対応)"""

    def __init__(self):
        self._models_jra = {}    # JRA 専用
        self._models_nar = {}    # NAR 専用
        self._models_legacy = {} # フォールバック (JRA/NAR 共通)
        self._loaded = False
        self._mode = "none"      # "split" (jra+nar) / "legacy" / "none"

    @staticmethod
    def _try_load(paths: dict, models: dict) -> bool:
        """指定パス群から pkl をロード。全て揃えば True。"""
        for key, path in paths.items():
            if not os.path.exists(path):
                return False
            try:
                with open(path, "rb") as f:
                    models[key] = pickle.load(f)
            except Exception as e:
                logger.warning(f"キャリブレータ読込エラー: {path}: {e}")
                return False
        return True

    def load(self) -> bool:
        """キャリブレータモデルをロード。

        優先順位:
          1. JRA 専用 + NAR 専用 が両方揃っている → split モード (Phase 3)
          2. legacy (JRA/NAR 共通) のみ → legacy モード (フォールバック)
          3. 何も見つからない → False
        """
        jra_ok = self._try_load(CALIBRATOR_PATHS_JRA, self._models_jra)
        nar_ok = self._try_load(CALIBRATOR_PATHS_NAR, self._models_nar)

        if jra_ok and nar_ok:
            self._loaded = True
            self._mode = "split"
            logger.info("事後キャリブレータ ロード完了 (Phase 3 split: JRA 3 + NAR 3 = 6 モデル)")
            return True

        # フォールバック: legacy
        legacy_ok = self._try_load(CALIBRATOR_PATHS_LEGACY, self._models_legacy)
        if legacy_ok:
            self._loaded = True
            self._mode = "legacy"
            logger.info("事後キャリブレータ ロード完了 (legacy: JRA/NAR 共通 3 モデル)")
            return True

        logger.debug("事後キャリブレータ未検出")
        return False

    @property
    def is_available(self) -> bool:
        return self._loaded and self._mode != "none"

    @property
    def mode(self) -> str:
        """現在のロードモード ("split" / "legacy" / "none")"""
        return self._mode

    def _select_models(self, is_jra: bool) -> dict:
        """is_jra に応じて適用するモデル群を選択"""
        if self._mode == "split":
            return self._models_jra if is_jra else self._models_nar
        elif self._mode == "legacy":
            return self._models_legacy
        return {}

    def apply(self, evaluations: list, is_jra: bool = True) -> None:
        """確率を事後キャリブレーション → ソフト正規化

        Args:
            evaluations: HorseEvaluation のリスト
            is_jra: True なら JRA モデル適用、False なら NAR モデル適用
                    (split モード時のみ有効。legacy モード時は共通モデル適用)

        旧方式: Isotonic変換後に合計1.0に厳密正規化
          → 問題: 15%→28%の補正が正規化で16%に戻される（キャリブ効果消滅）
        新方式: Isotonic変換後の確率をそのまま使用。
          合計が目標の±30%を超えた場合のみソフトスケーリング。
          キャリブレーション精度 > 確率合計の厳密性
        """
        if not self.is_available or not evaluations:
            return

        models = self._select_models(is_jra)
        if not models or len(models) < 3:
            return

        import numpy as np

        # 変換前の確率を取得
        win_probs = np.array([ev.win_prob or 0.0 for ev in evaluations])
        p2_probs = np.array([ev.place2_prob or 0.0 for ev in evaluations])
        p3_probs = np.array([ev.place3_prob or 0.0 for ev in evaluations])

        # Isotonic変換
        cal_win = models["win"].transform(win_probs)
        cal_p2 = models["top2"].transform(p2_probs)
        cal_p3 = models["top3"].transform(p3_probs)

        # クリップ (0.001 ~ 0.999)
        cal_win = np.clip(cal_win, 0.001, 0.999)
        cal_p2 = np.clip(cal_p2, 0.001, 0.999)
        cal_p3 = np.clip(cal_p3, 0.001, 0.999)

        # ソフト正規化: 合計が目標の±30%を超えた場合のみスケーリング
        # win: 合計目標1.0, 許容0.7〜1.3
        # place2: 合計目標min(n,2), 許容±30%
        # place3: 合計目標min(n,3), 許容±30%
        n = len(evaluations)
        cal_win = self._soft_normalize(cal_win, target=1.0, tolerance=0.30)
        cal_p2 = self._soft_normalize(cal_p2, target=min(n, 2), tolerance=0.30)
        cal_p3 = self._soft_normalize(cal_p3, target=min(n, 3), tolerance=0.30)

        # 適用
        for i, ev in enumerate(evaluations):
            ev.win_prob = float(cal_win[i])
            ev.place2_prob = float(cal_p2[i])
            ev.place3_prob = float(cal_p3[i])

        # 整合性保証: win < place2 < place3
        _min_gap = 0.005
        for ev in evaluations:
            w = ev.win_prob or 0.0
            ev.place2_prob = max(ev.place2_prob or 0.0, w + _min_gap)
            ev.place3_prob = max(ev.place3_prob or 0.0, ev.place2_prob + _min_gap)

    def apply_dict(self, horses: list, is_jra: bool = True,
                   win_key: str = "win_prob",
                   p2_key: str = "place2_prob",
                   p3_key: str = "place3_prob") -> None:
        """dict ベースで Isotonic 較正を適用 (batch_wf_fast.py 用)

        Args:
            horses: dict のリスト (各 dict に win_key/p2_key/p3_key を持つ)
            is_jra: True なら JRA モデル、False なら NAR モデル
            win_key/p2_key/p3_key: dict 内の確率キー名 (デフォルト: win_prob/place2_prob/place3_prob)
        """
        if not self.is_available or not horses:
            return

        models = self._select_models(is_jra)
        if not models or len(models) < 3:
            return

        import numpy as np

        win_probs = np.array([h.get(win_key) or 0.0 for h in horses])
        p2_probs = np.array([h.get(p2_key) or 0.0 for h in horses])
        p3_probs = np.array([h.get(p3_key) or 0.0 for h in horses])

        cal_win = np.clip(models["win"].transform(win_probs), 0.001, 0.999)
        cal_p2 = np.clip(models["top2"].transform(p2_probs), 0.001, 0.999)
        cal_p3 = np.clip(models["top3"].transform(p3_probs), 0.001, 0.999)

        n = len(horses)
        cal_win = self._soft_normalize(cal_win, target=1.0, tolerance=0.30)
        cal_p2 = self._soft_normalize(cal_p2, target=min(n, 2), tolerance=0.30)
        cal_p3 = self._soft_normalize(cal_p3, target=min(n, 3), tolerance=0.30)

        # 整合性: win < place2 < place3
        _min_gap = 0.005
        for i, h in enumerate(horses):
            w = float(cal_win[i])
            p2 = max(float(cal_p2[i]), w + _min_gap)
            p3 = max(float(cal_p3[i]), p2 + _min_gap)
            h[win_key] = round(w, 6)
            h[p2_key] = round(p2, 6)
            h[p3_key] = round(p3, 6)

    @staticmethod
    def _soft_normalize(probs, target: float, tolerance: float = 0.30):
        """ソフト正規化: 合計が許容範囲を超えた場合のみスケーリング

        - 合計が target*(1-tolerance) 〜 target*(1+tolerance) 内: そのまま
        - 範囲外: 最寄りの許容限界までスケーリング
        例: target=1.0, tolerance=0.30 → 合計0.7〜1.3は無補正
        """
        total = probs.sum()
        if total <= 0:
            return probs

        lo = target * (1.0 - tolerance)
        hi = target * (1.0 + tolerance)

        if total < lo:
            # 合計が小さすぎる → 下限まで引き上げ
            return probs * (lo / total)
        elif total > hi:
            # 合計が大きすぎる → 上限まで引き下げ
            return probs * (hi / total)
        else:
            # 許容範囲内 → そのまま（キャリブレーション結果を尊重）
            return probs
