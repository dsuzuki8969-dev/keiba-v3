"""
事後キャリブレーション: Isotonic Regression

パイプライン全体の出力（win_prob, place2_prob, place3_prob）に対して
Isotonic Regression を適用し、キャリブレーションを改善する。

- 単調変換のため、馬の順序（AUC）は保存される
- 41万頭のデータで学習するため、過学習リスクは低い
- scripts/build_calibrator.py で学習・保存
"""

import os
import pickle
from typing import List, Optional

from src.log import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
CALIBRATOR_DIR = os.path.join(_PROJECT_ROOT, "data", "models")

CALIBRATOR_PATHS = {
    "win": os.path.join(CALIBRATOR_DIR, "calibrator_win.pkl"),
    "top2": os.path.join(CALIBRATOR_DIR, "calibrator_top2.pkl"),
    "top3": os.path.join(CALIBRATOR_DIR, "calibrator_top3.pkl"),
}


class PostCalibrator:
    """Isotonic Regression による事後キャリブレーション"""

    def __init__(self):
        self._models = {}  # {"win": IsotonicRegression, "top2": ..., "top3": ...}
        self._loaded = False

    def load(self) -> bool:
        """キャリブレータモデルをロード。全3モデルが揃っていればTrue"""
        for key, path in CALIBRATOR_PATHS.items():
            if not os.path.exists(path):
                logger.debug(f"キャリブレータ未検出: {path}")
                return False
            try:
                with open(path, "rb") as f:
                    self._models[key] = pickle.load(f)
            except Exception as e:
                logger.warning(f"キャリブレータ読込エラー: {path}: {e}")
                return False

        self._loaded = True
        logger.info(f"事後キャリブレータ ロード完了 (3モデル)")
        return True

    @property
    def is_available(self) -> bool:
        return self._loaded and len(self._models) == 3

    def apply(self, evaluations: list) -> None:
        """確率を事後キャリブレーション → ソフト正規化

        旧方式: Isotonic変換後に合計1.0に厳密正規化
          → 問題: 15%→28%の補正が正規化で16%に戻される（キャリブ効果消滅）
        新方式: Isotonic変換後の確率をそのまま使用。
          合計が目標の±30%を超えた場合のみソフトスケーリング。
          キャリブレーション精度 > 確率合計の厳密性
        """
        if not self.is_available or not evaluations:
            return

        import numpy as np

        # 変換前の確率を取得
        win_probs = np.array([ev.win_prob or 0.0 for ev in evaluations])
        p2_probs = np.array([ev.place2_prob or 0.0 for ev in evaluations])
        p3_probs = np.array([ev.place3_prob or 0.0 for ev in evaluations])

        # Isotonic変換
        cal_win = self._models["win"].transform(win_probs)
        cal_p2 = self._models["top2"].transform(p2_probs)
        cal_p3 = self._models["top3"].transform(p3_probs)

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

    @staticmethod
    def _soft_normalize(probs, target: float, tolerance: float = 0.30):
        """ソフト正規化: 合計が許容範囲を超えた場合のみスケーリング

        - 合計が target*(1-tolerance) 〜 target*(1+tolerance) 内: そのまま
        - 範囲外: 最寄りの許容限界までスケーリング
        例: target=1.0, tolerance=0.30 → 合計0.7〜1.3は無補正
        """
        import numpy as np
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
