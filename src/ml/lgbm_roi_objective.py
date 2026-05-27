"""M-3 Phase 2b: ROI 期待値最大化 custom objective for LightGBM

定式化:
  - y_true: 1 if 1 着 (head_top3 使用時は 3 着以内), else 0
  - prob: sigmoid(raw_pred) = LightGBM の raw スコアを確率化
  - odds: 当日確定 tansho_odds (倍率, 100 円投票時の払戻倍率)
  - payout: 1 着なら odds × 100 円 (単勝配当), それ以外 0 円
  - ROI 期待値 = prob × payout - 投資額(100 円)

核心アイデア:
  - ROI weighted binary cross-entropy
    loss = -[ y_true × log(prob) × odds_weight + (1 - y_true) × log(1 - prob) ]
  - つまり「odds が高い当たり馬を重要視」する勾配方向
  - odds が高い = 人気薄 = 正解すれば大きなリターン → 積極的に確率を押し上げる
  - 低 odds の当たり馬 (人気馬) は weight が小さく → 勾配が小さい = あまり引き寄せない

代替案:
  - 案 B: sample_weight = odds を Dataset に設定 (シンプル)
  - 案 C: log(odds) を weight に使う (高 odds 馬の過剰 weight 緩和)
  - 案 D: payout function 直接最大化 (gradient ascent)

注意点:
  - LightGBM custom objective は raw_score (logit) で入力される
  - predict() 時は内部で sigmoid が適用されるが、custom objective 使用時は
    num_class=1 の場合でも sigmoid 後の確率が返ることを確認済み
  - 早期終了には feval (custom metric) を使用

Usage:
    from src.ml.lgbm_roi_objective import make_roi_objective, make_roi_metric

    odds_train = X_train[:, odds_col_index]
    objective_fn = make_roi_objective(odds_train)
    metric_fn = make_roi_metric(odds_valid)

    booster = lgb.train(
        params={
            'objective': objective_fn,
            'metric': 'None',
            'verbose': -1,
            ...
        },
        train_set=lgb.Dataset(X_train, y_train),
        feval=metric_fn,
        ...
    )
"""

import numpy as np
from typing import Callable, Tuple
from src.log import get_logger

logger = get_logger(__name__)

# ============================================================
# 内部ユーティリティ
# ============================================================

def _sigmoid(x: np.ndarray) -> np.ndarray:
    """数値安定 sigmoid"""
    return np.where(
        x >= 0,
        1.0 / (1.0 + np.exp(-x)),
        np.exp(x) / (1.0 + np.exp(x)),
    )


def _safe_odds(odds: np.ndarray, cap: float = 100.0) -> np.ndarray:
    """
    odds の安全化:
    - 0 以下 / NaN → 1.0 (最低倍率)
    - cap 超え → cap (過剰 weight 抑制)

    Args:
        odds: 生の odds 配列
        cap: 最大倍率 (デフォルト 100.0)

    Returns:
        安全化された odds 配列
    """
    safe = np.where(np.isnan(odds) | (odds <= 0.0), 1.0, odds)
    safe = np.minimum(safe, cap)
    return safe


# ============================================================
# 案 A: ROI Weighted Binary Cross-Entropy (main)
# ============================================================

def make_roi_objective(
    odds_array: np.ndarray,
    odds_cap: float = 50.0,
    odds_scale: float = 1.0,
) -> Callable:
    """
    ROI weighted binary cross-entropy objective を返す closure。

    gradient 導出:
      loss = -[ y × log(p) × w(odds) + (1-y) × log(1-p) ]
      ∂loss/∂raw = (p - y) × w  (y=1) または (p - y) × 1  (y=0)
      → weight = odds (y=1) / 1.0 (y=0) の非対称 weight

    Args:
        odds_array: 学習データの odds 列 (shape: n_samples)
        odds_cap:   odds 上限 (デフォルト 50.0 = 案 A の適切な cap)
        odds_scale: odds をスケーリングする係数 (デフォルト 1.0)
                    0.1〜0.5 を試す場合は縮小 (勾配の安定化)
    """
    odds = _safe_odds(odds_array.astype(np.float64), cap=odds_cap)
    odds = odds * odds_scale
    logger.info(
        "[ROI objective] 案 A: ROI weighted BCE "
        f"odds_cap={odds_cap} odds_scale={odds_scale} "
        f"odds stats: min={odds.min():.2f} max={odds.max():.2f} mean={odds.mean():.2f}"
    )

    def objective(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Args:
            y_true: 正解ラベル (0 or 1) の float64 配列
            y_pred: raw logit スコア (LightGBM 内部 raw score)

        Returns:
            (gradient, hessian) どちらも shape (n_samples,)
        """
        prob = _sigmoid(y_pred)
        # y=1 (当たり馬) には odds を weight として掛ける
        # y=0 (外れ馬) には weight=1 (通常 BCE と同じ)
        w = np.where(y_true == 1, odds, 1.0)
        gradient = (prob - y_true) * w
        # hessian: 近似 = p(1-p) * w
        # 注: hessian が小さすぎると学習不安定 → clip で下限保護
        hessian = np.clip(prob * (1.0 - prob) * w, 1e-6, None)
        return gradient, hessian

    return objective


def make_roi_objective_log(
    odds_array: np.ndarray,
    odds_cap: float = 100.0,
) -> Callable:
    """
    案 C: log(odds) weight variant (高 odds 馬の過剰 weight 緩和)。

    odds=50 のとき weight ≈ log(50) ≈ 3.9 (案 A の 50 に比べて大幅緩和)。
    extreme odds (100+) の馬でも勾配爆発しにくい。

    Args:
        odds_array: 学習データの odds 列
        odds_cap:   上限 (安全化のみ / log スケール後は自然に緩和)
    """
    odds = _safe_odds(odds_array.astype(np.float64), cap=odds_cap)
    log_odds = np.log1p(odds)  # log(1 + odds): odds=1 → 0.69, odds=50 → 3.93
    logger.info(
        "[ROI objective] 案 C: log(odds) weight "
        f"log_odds stats: min={log_odds.min():.2f} max={log_odds.max():.2f} mean={log_odds.mean():.2f}"
    )

    def objective(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        prob = _sigmoid(y_pred)
        w = np.where(y_true == 1, log_odds, 1.0)
        gradient = (prob - y_true) * w
        hessian = np.clip(prob * (1.0 - prob) * w, 1e-6, None)
        return gradient, hessian

    return objective


# ============================================================
# 案 B: sample_weight を Dataset に渡すシンプル版 (参考)
# ============================================================

def make_sample_weights(
    y_true: np.ndarray,
    odds_array: np.ndarray,
    odds_cap: float = 50.0,
    background_weight: float = 1.0,
) -> np.ndarray:
    """
    案 B: lgb.Dataset(weight=...) に渡す sample_weight 配列を生成。

    - y=1 (当たり馬): weight = min(odds, cap)
    - y=0 (外れ馬):  weight = background_weight (デフォルト 1.0)

    この方式は objective= を変えず、標準 binary_logloss のまま
    dataset レベルで odds weight を掛ける。

    Args:
        y_true: 正解ラベル (0 or 1)
        odds_array: odds 配列
        odds_cap: 最大重み上限
        background_weight: 外れ馬の基準重み

    Returns:
        shape (n_samples,) の sample weight 配列
    """
    odds = _safe_odds(odds_array.astype(np.float64), cap=odds_cap)
    weights = np.where(y_true == 1, odds, background_weight)
    logger.info(
        "[ROI sample_weight] 案 B: dataset weight "
        f"mean(y=1)={weights[y_true == 1].mean():.2f} "
        f"mean(y=0)={weights[y_true == 0].mean():.2f}"
    )
    return weights


# ============================================================
# Custom metric: ROI 期待値 (eval metric)
# ============================================================

def make_roi_metric(
    odds_array: np.ndarray,
    odds_cap: float = 100.0,
) -> Callable:
    """
    LightGBM custom eval metric: ROI 期待値 を返す。

    計算方式:
      - 各レース内で prob が最大の馬を「◎ (本命)」として選ぶ
      - その馬が 1 着かどうか × odds で ROI を計算
      ※ただし eval set 全体でレース分割情報がないため、
        「全サンプルで prob × odds の平均」を代用指標として使用。
        実際の TOP1 ROI とは若干異なるが学習の方向性は一致する。

    Args:
        odds_array: 検証データの odds 列
        odds_cap: 上限

    Returns:
        LightGBM feval 形式の関数: (y_true, y_pred) -> (name, value, higher_is_better)
    """
    odds = _safe_odds(odds_array.astype(np.float64), cap=odds_cap)

    def metric(y_true: np.ndarray, y_pred: np.ndarray):
        """
        Args:
            y_true: 正解ラベル (LightGBM が渡す形式)
            y_pred: raw logit score (LightGBM が渡す形式)

        Returns:
            (metric_name, metric_value, higher_is_better)
        """
        prob = _sigmoid(y_pred)
        # expected value per bet = prob × odds × y_true (当たったときの払戻期待値)
        # シンプル近似: 全サンプルの prob × odds × y_true の平均
        # → 正解馬の確率 × 配当倍率を高くするほど値が上がる
        ev = np.mean(prob * odds * y_true) * 100.0  # %スケール
        return "roi_ev", float(ev), True  # higher_is_better=True

    return metric


# ============================================================
# ファクトリ: variant 名から objective を選択
# ============================================================

def get_objective_and_metric(
    variant: str,
    odds_train: np.ndarray,
    odds_valid: np.ndarray,
) -> Tuple[Callable, Callable, dict]:
    """
    variant 名 (+odds+ROI_loss / +odds+ROI_loss_log / +odds+sample_weight)
    に対応する (objective_fn, metric_fn, extra_params) を返す。

    Args:
        variant: バリアント名
        odds_train: 学習データの odds 列
        odds_valid: 検証データの odds 列

    Returns:
        (objective_fn, metric_fn, extra_params)
        - objective_fn: lgb.train の params['objective'] に渡す関数 (または None = 標準 binary)
        - metric_fn: lgb.train の feval に渡す関数 (または None)
        - extra_params: params dict に追加する設定 (metric='None' など)
    """
    metric_fn = make_roi_metric(odds_valid)

    if "ROI_loss_log" in variant:
        # 案 C: log(odds) weight
        obj_fn = make_roi_objective_log(odds_train)
        extra = {"metric": "None"}  # 標準 metric を無効化
        logger.info(f"[Phase 2b] variant={variant}: 案 C (log odds weight) を使用")
    elif "ROI_loss" in variant:
        # 案 A: ROI weighted BCE
        obj_fn = make_roi_objective(odds_train, odds_cap=50.0)
        extra = {"metric": "None"}
        logger.info(f"[Phase 2b] variant={variant}: 案 A (ROI weighted BCE) を使用")
    elif "sample_weight" in variant:
        # 案 B: sample_weight (objective は標準 binary_logloss)
        obj_fn = None  # 標準 binary のまま
        extra = {}  # metric は auc のまま
        logger.info(f"[Phase 2b] variant={variant}: 案 B (sample_weight) を使用")
    else:
        obj_fn = None
        extra = {}
        logger.info(f"[Phase 2b] variant={variant}: 標準 binary_logloss を使用")

    return obj_fn, metric_fn, extra
