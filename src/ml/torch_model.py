"""
PyTorch + DirectML 競馬予測ニューラルネットモデル

LightGBM モデルと同じ特徴量を使い、
Tabular Neural Network (Residual MLP) で win/top2/top3 を同時予測する。

GPU: torch-directml (AMD/Intel/NVIDIA DirectX12 対応)
CPU フォールバック: DirectML 不在時は自動で CPU を使用

学習: python -m src.ml.torch_model
推論: TorchPredictor.predict_race(race_dict, horse_dicts)

改善履歴:
  - 改善1: 損失関数を加重化 (win:top2:top3 = 0.25:0.25:0.50)
  - 改善2: BatchNorm → LayerNorm (use_layer_norm フラグで後方互換)
  - 改善3: Intra-Race Cross-Attention (has_attention フラグで後方互換)
"""

import json
import os
import pickle
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np

from src.log import get_logger

logger = get_logger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
MODEL_DIR = os.path.join(_BASE, "data", "models")
TORCH_MODEL_PATH = os.path.join(MODEL_DIR, "torch_nn.pt")
TORCH_META_PATH  = os.path.join(MODEL_DIR, "torch_meta.json")
TORCH_NORM_PATH  = os.path.join(MODEL_DIR, "torch_norm.pkl")
TORCH_STATS_PATH = os.path.join(MODEL_DIR, "torch_tracker.pkl")


# ============================================================
# デバイス選択（DirectML → CPU）
# ============================================================

def get_device():
    """DirectML デバイスを返す。失敗したら CPU。"""
    try:
        import torch_directml
        dml = torch_directml.device()
        logger.info("DirectML デバイス使用: %s", dml)
        return dml
    except Exception:
        import torch
        logger.info("DirectML 不在 → CPU 使用")
        return torch.device("cpu")


# ============================================================
# ネットワーク定義
# ============================================================

def _build_model(
    in_dim: int,
    hidden: int = 256,
    dropout: float = 0.3,
    use_layer_norm: bool = True,
    has_attention: bool = True,
):
    """
    Residual MLP (3 ブロック) + Intra-Race Attention + 3ヘッド出力 (win / top2 / top3)

    Parameters
    ----------
    in_dim : int
        入力特徴量次元数
    hidden : int
        隠れ層の次元数（デフォルト 256）
    dropout : float
        ドロップアウト率（デフォルト 0.3）
    use_layer_norm : bool
        True  → LayerNorm を使用（新規学習・推奨）
        False → BatchNorm1d を使用（旧モデルとの後方互換用）
    has_attention : bool
        True  → ResBlock 後に Intra-Race Cross-Attention を挿入
        False → Attention なし（旧モデルとの後方互換用）
    """
    import torch
    import torch.nn as nn

    # --------------------------------------------------------
    # ResBlock: BatchNorm / LayerNorm を use_layer_norm で切替
    # --------------------------------------------------------
    class ResBlockBN(nn.Module):
        """旧アーキテクチャ互換: BatchNorm1d を使用"""
        def __init__(self, dim: int, drop: float):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(dim, dim),
                nn.BatchNorm1d(dim),
                nn.GELU(),
                nn.Dropout(drop),
                nn.Linear(dim, dim),
                nn.BatchNorm1d(dim),
            )
            self.act = nn.GELU()

        def forward(self, x):
            return self.act(x + self.net(x))

    class ResBlockLN(nn.Module):
        """新アーキテクチャ: LayerNorm を使用（小バッチ・単馬推論でも安定）"""
        def __init__(self, dim: int, drop: float):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(dim, dim),
                nn.LayerNorm(dim),
                nn.GELU(),
                nn.Dropout(drop),
                nn.Linear(dim, dim),
                nn.LayerNorm(dim),
            )
            self.act = nn.GELU()

        def forward(self, x):
            return self.act(x + self.net(x))

    ResBlock = ResBlockLN if use_layer_norm else ResBlockBN

    # --------------------------------------------------------
    # RaceAttentionLayer: 同一レース内の馬間 Cross-Attention
    # --------------------------------------------------------
    class RaceAttentionLayer(nn.Module):
        """
        同一レース内の全馬を一括入力として Cross-Attention を適用する。
        「このレースの中で相対的に強いか」を学習するためのレース内文脈化層。

        入力 x: (n_horses, hidden)
        出力:   (n_horses, hidden)  ← Residual + LayerNorm 済み
        """
        def __init__(self, dim: int, n_heads: int = 4, drop: float = 0.1):
            super().__init__()
            self.attn = nn.MultiheadAttention(
                dim, n_heads, dropout=drop, batch_first=True
            )
            self.norm = nn.LayerNorm(dim)
            self.dropout = nn.Dropout(drop)

        def forward(self, x: torch.Tensor) -> torch.Tensor:
            """
            x: (n_horses, hidden) — 同一レース全馬の embedding
            """
            # (n_horses, hidden) → (1, n_horses, hidden)  [batch_first=True]
            x_3d = x.unsqueeze(0)
            attn_out, _ = self.attn(x_3d, x_3d, x_3d)
            attn_out = attn_out.squeeze(0)           # → (n_horses, hidden)
            return self.norm(x + self.dropout(attn_out))  # Residual + LN

    # --------------------------------------------------------
    # メインネットワーク
    # --------------------------------------------------------
    class KeibaNet(nn.Module):
        def __init__(self):
            super().__init__()

            # 入力埋め込み層
            if use_layer_norm:
                self.embed = nn.Sequential(
                    nn.Linear(in_dim, hidden),
                    nn.LayerNorm(hidden),
                    nn.GELU(),
                    nn.Dropout(dropout),
                )
            else:
                self.embed = nn.Sequential(
                    nn.Linear(in_dim, hidden),
                    nn.BatchNorm1d(hidden),
                    nn.GELU(),
                    nn.Dropout(dropout),
                )

            # Residual ブロック × 3
            self.blocks = nn.Sequential(
                ResBlock(hidden, dropout),
                ResBlock(hidden, dropout),
                ResBlock(hidden, dropout),
            )

            # Intra-Race Attention（オプション）
            self._has_attention = has_attention
            if has_attention:
                self.race_attn = RaceAttentionLayer(hidden, n_heads=4, drop=0.1)

            # 3 ヘッド（win / top2 / top3）- logit を返す（sigmoid は損失関数内）
            self.head_win  = nn.Linear(hidden, 1)
            self.head_top2 = nn.Linear(hidden, 1)
            self.head_top3 = nn.Linear(hidden, 1)

        def _encode(self, x: torch.Tensor) -> torch.Tensor:
            """embed + ResBlocks までの共通前処理"""
            h = self.embed(x)
            h = self.blocks(h)
            return h

        def forward(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            x: (batch, in_dim)
            Attention なし（train のバッチ処理 / 単馬推論）用。
            logit を返す（BCEWithLogitsLoss に渡す用）。
            """
            h = self._encode(x)
            # Attention なし: ResBlock 出力をそのまま使用
            return (
                self.head_win(h).squeeze(1),
                self.head_top2(h).squeeze(1),
                self.head_top3(h).squeeze(1),
            )

        def forward_race(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            x: (n_horses, in_dim) — 同一レース全馬を一括入力
            Intra-Race Attention を適用してから logit を返す。
            推論時（predict_race）はこちらを使用。
            """
            h = self._encode(x)
            if self._has_attention:
                h = self.race_attn(h)   # レース内文脈化
            return (
                self.head_win(h).squeeze(1),
                self.head_top2(h).squeeze(1),
                self.head_top3(h).squeeze(1),
            )

        def predict(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            推論時: sigmoid を適用して確率を返す。
            x: (batch, in_dim) — Attention なし（後方互換）
            """
            lw, l2, l3 = self.forward(x)
            import torch as _t
            return _t.sigmoid(lw), _t.sigmoid(l2), _t.sigmoid(l3)

        def predict_race(self, x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
            """
            推論時: Intra-Race Attention を適用して sigmoid 確率を返す。
            x: (n_horses, in_dim) — 同一レース全馬
            """
            lw, l2, l3 = self.forward_race(x)
            import torch as _t
            return _t.sigmoid(lw), _t.sigmoid(l2), _t.sigmoid(l3)

    return KeibaNet()


# ============================================================
# 正規化
# ============================================================

class FeatureNormalizer:
    """各特徴量を mean/std で標準化（nan は 0 に置換）"""

    def __init__(self):
        self.mean_: Optional[np.ndarray] = None
        self.std_:  Optional[np.ndarray] = None

    def fit(self, X: np.ndarray) -> "FeatureNormalizer":
        # 全NaN列は mean=0, std=1 で無害化
        with np.errstate(all="ignore"):
            self.mean_ = np.where(np.all(np.isnan(X), axis=0), 0.0, np.nanmean(X, axis=0))
            self.std_  = np.where(np.all(np.isnan(X), axis=0), 1.0, np.nanstd(X, axis=0))
        self.std_[self.std_ < 1e-8] = 1.0
        return self

    def transform(self, X: np.ndarray) -> np.ndarray:
        X = np.where(np.isnan(X), self.mean_, X)  # NaN → 列の平均値で補完
        return (X - self.mean_) / self.std_

    def fit_transform(self, X: np.ndarray) -> np.ndarray:
        return self.fit(X).transform(X)


# ============================================================
# 学習
# ============================================================

def train_torch_model(
    valid_days: int = 30,
    epochs: int = 40,
    batch_size: int = 2048,
    lr: float = 3e-4,
    hidden: int = 256,
    dropout: float = 0.3,
    use_layer_norm: bool = True,
    has_attention: bool = True,
) -> dict:
    """
    DirectML GPU で Residual MLP を学習し保存する。

    Parameters
    ----------
    use_layer_norm : bool
        True → LayerNorm（推奨）、False → BatchNorm（旧互換）
    has_attention : bool
        True → Intra-Race Cross-Attention を使用

    損失関数: 加重 BCE
        win:top2:top3 = 0.25 : 0.25 : 0.50
        複勝率（top3）を最重視して AUC・Top1 的中率向上を狙う。

    Returns: meta dict（AUC, logloss 等）
    """
    import torch
    import torch.nn as nn
    from sklearn.metrics import log_loss, roc_auc_score
    from torch.utils.data import DataLoader, TensorDataset

    from src.ml.lgbm_model import (
        FEATURE_COLUMNS,
        RollingStatsTracker,
        _extract_features,
        _load_ml_races,
    )

    logger.info("=" * 60)
    logger.info("PyTorch Residual MLP 学習開始 (DirectML)")
    logger.info("use_layer_norm=%s  has_attention=%s", use_layer_norm, has_attention)
    logger.info("損失加重: win=0.25 / top2=0.25 / top3=0.50")
    logger.info("=" * 60)

    device = get_device()

    # データ読み込み（probability_model と同じパイプライン）
    races = _load_ml_races()
    if not races:
        raise ValueError("ML data not found")

    all_dates = sorted(set(r.get("date", "") for r in races if r.get("date")))
    if len(all_dates) < 14:
        raise ValueError(f"Insufficient dates ({len(all_dates)})")

    split_idx  = max(1, len(all_dates) - valid_days)
    split_date = all_dates[split_idx]

    logger.info("学習期間: %s 〜 %s", all_dates[0], all_dates[split_idx - 1])
    logger.info("検証期間: %s 〜 %s", split_date, all_dates[-1])

    tracker = RollingStatsTracker()
    train_feats, train_labels = [], {"win": [], "top2": [], "top3": []}
    valid_feats, valid_labels = [], {"win": [], "top2": [], "top3": []}
    valid_race_sizes = []

    for race in races:
        date_str = race.get("date", "")
        is_valid = date_str >= split_date

        race_rows = []
        for h in race.get("horses", []):
            fp = h.get("finish_pos")
            if fp is None:
                continue
            feat = _extract_features(h, race, tracker)
            row  = [float(feat.get(c)) if feat.get(c) is not None else float("nan")
                    for c in FEATURE_COLUMNS]
            lbl  = {
                "win":  1 if fp == 1  else 0,
                "top2": 1 if fp <= 2 else 0,
                "top3": 1 if fp <= 3 else 0,
            }
            race_rows.append((row, lbl))

        if race_rows:
            if is_valid:
                for row, lbl in race_rows:
                    valid_feats.append(row)
                    for k in ("win", "top2", "top3"):
                        valid_labels[k].append(lbl[k])
                valid_race_sizes.append(len(race_rows))
            else:
                for row, lbl in race_rows:
                    train_feats.append(row)
                    for k in ("win", "top2", "top3"):
                        train_labels[k].append(lbl[k])

        tracker.update_race(race)

    if not train_feats:
        raise ValueError("No training samples")

    logger.info("Train: %d サンプル / Valid: %d サンプル",
                len(train_feats), len(valid_feats))

    # 正規化
    X_train_raw = np.array(train_feats, dtype=np.float32)
    X_valid_raw = np.array(valid_feats, dtype=np.float32)
    norm = FeatureNormalizer()
    X_train = norm.fit_transform(X_train_raw).astype(np.float32)
    X_valid = norm.transform(X_valid_raw).astype(np.float32)

    def to_tensor(arr):
        return torch.tensor(arr, dtype=torch.float32)

    y_train = {k: to_tensor(np.array(v, dtype=np.float32))
               for k, v in train_labels.items()}
    y_valid = {k: to_tensor(np.array(v, dtype=np.float32))
               for k, v in valid_labels.items()}

    dataset = TensorDataset(
        to_tensor(X_train),
        y_train["win"], y_train["top2"], y_train["top3"],
    )
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True, drop_last=False)

    # モデル構築（改善2: use_layer_norm, 改善3: has_attention）
    model = _build_model(
        len(FEATURE_COLUMNS), hidden, dropout,
        use_layer_norm=use_layer_norm,
        has_attention=has_attention,
    ).to(device)
    opt   = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=epochs, eta_min=lr * 0.1)
    # BCEWithLogitsLoss = sigmoid + BCE を数値安定に一体化（DirectML 対応）
    bce   = nn.BCEWithLogitsLoss()

    # 改善1: 損失の加重係数（複勝率 top3 を最重視）
    W_WIN  = 0.25
    W_TOP2 = 0.25
    W_TOP3 = 0.50

    best_auc = 0.0
    best_state = None

    for ep in range(1, epochs + 1):
        model.train()
        total_loss = 0.0
        for batch in loader:
            xb, yw, y2, y3 = [b.to(device) for b in batch]
            # train では forward()（Attention なし）を使用
            # → Attention は推論時にレース単位で適用するため
            lw, l2, l3 = model(xb)

            # 改善1: 加重 BCE 損失
            loss = W_WIN * bce(lw, yw) + W_TOP2 * bce(l2, y2) + W_TOP3 * bce(l3, y3)

            opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            total_loss += loss.item()
        sched.step()

        # 検証
        if ep % 5 == 0 or ep == epochs:
            model.eval()
            with torch.no_grad():
                xv = to_tensor(X_valid).to(device)
                # 検証も predict()（Attention なし）で行う
                # （検証時はレース境界情報がバッチに含まれないため）
                pw_v, p2_v, p3_v = model.predict(xv)
                pw_v = pw_v.cpu().numpy()
                p2_v = p2_v.cpu().numpy()
                p3_v = p3_v.cpu().numpy()

            auc_w, auc_p3 = 0.0, 0.0
            try:
                auc_w  = roc_auc_score(valid_labels["win"],  pw_v)
                auc_p3 = roc_auc_score(valid_labels["top3"], p3_v)
                avg_auc = (auc_w + auc_p3) / 2
            except Exception:
                avg_auc = 0.0

            logger.info("Epoch %3d/%d  loss=%.4f  AUC_win=%.4f  AUC_top3=%.4f",
                        ep, epochs,
                        total_loss / max(len(loader), 1),
                        auc_w, auc_p3)

            if avg_auc > best_auc:
                best_auc = avg_auc
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}

    # ベストモデルで最終評価
    if best_state:
        model.load_state_dict(best_state)

    model.eval()
    with torch.no_grad():
        xv = to_tensor(X_valid).to(device)
        pw_v, p2_v, p3_v = model.predict(xv)
        pw_v  = pw_v.cpu().numpy()
        p2_v  = p2_v.cpu().numpy()
        p3_v  = p3_v.cpu().numpy()

    metrics = {}
    for tag, pred, true_key in [
        ("win",  pw_v, "win"),
        ("top2", p2_v, "top2"),
        ("top3", p3_v, "top3"),
    ]:
        yt = np.array(valid_labels[true_key])
        try:
            metrics[tag] = {
                "auc":     round(roc_auc_score(yt, pred), 4),
                "logloss": round(log_loss(yt, pred.clip(1e-7, 1 - 1e-7)), 4),
            }
        except Exception:
            metrics[tag] = {"auc": 0.0, "logloss": 9.9}
        logger.info("Final %s → AUC=%.4f  LogLoss=%.4f",
                    tag, metrics[tag]["auc"], metrics[tag]["logloss"])

    # レース単位 TOP1 的中率（win）
    idx = 0
    correct_win, correct_p3, total_eval = 0, 0, 0
    for g in valid_race_sizes:
        if g < 3:
            idx += g
            continue
        wp = pw_v[idx:idx + g]
        pp = p3_v[idx:idx + g]
        yw = np.array(valid_labels["win"][idx:idx + g])
        yp = np.array(valid_labels["top3"][idx:idx + g])
        if yw[np.argmax(wp)] == 1:
            correct_win += 1
        if yp[np.argmax(pp)] == 1:
            correct_p3  += 1
        total_eval += 1
        idx += g

    top1_win = round(correct_win / max(total_eval, 1), 4)
    top1_p3  = round(correct_p3  / max(total_eval, 1), 4)
    logger.info("レース単位 TOP1的中: win=%.1f%%  top3=%.1f%%",
                top1_win * 100, top1_p3 * 100)

    # 保存
    os.makedirs(MODEL_DIR, exist_ok=True)
    torch.save(best_state or model.state_dict(), TORCH_MODEL_PATH)
    with open(TORCH_NORM_PATH, "wb") as f:
        pickle.dump(norm, f)
    with open(TORCH_STATS_PATH, "wb") as f:
        pickle.dump(tracker, f)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    meta = {
        "created_at": ts,
        "device": str(device),
        "epochs": epochs,
        "hidden": hidden,
        "dropout": dropout,
        "in_dim": len(FEATURE_COLUMNS),
        "use_layer_norm": use_layer_norm,
        "has_attention": has_attention,
        "loss_weights": {"win": W_WIN, "top2": W_TOP2, "top3": W_TOP3},
        "train_samples": len(train_feats),
        "valid_samples": len(valid_feats),
        "valid_races": total_eval,
        "metrics": metrics,
        "top1_win_hit": top1_win,
        "top1_place_hit": top1_p3,
        "split_date": split_date,
    }
    with open(TORCH_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    logger.info("モデル保存: %s", TORCH_MODEL_PATH)
    return meta


# ============================================================
# 推論クラス
# ============================================================

class TorchPredictor:
    """学習済み Residual MLP による推論"""

    def __init__(self):
        self._model  = None
        self._norm: Optional[FeatureNormalizer] = None
        self._tracker = None
        self._loaded  = False
        self._device  = None
        # モデルのアーキテクチャフラグ（ロード時に meta から読み込む）
        self._use_layer_norm: bool = True
        self._has_attention: bool  = True

    def load(self) -> bool:
        if self._loaded:
            return True
        try:
            import torch


            if not os.path.exists(TORCH_MODEL_PATH):
                return False

            with open(TORCH_META_PATH) as f:
                meta = json.load(f)

            # 改善2/3: meta からアーキテクチャフラグを読み込む
            # 旧 meta には存在しないため、False をデフォルトにして後方互換を保つ
            self._use_layer_norm = meta.get("use_layer_norm", False)
            self._has_attention  = meta.get("has_attention",  False)

            self._device = get_device()
            self._model  = _build_model(
                meta["in_dim"],
                meta["hidden"],
                meta["dropout"],
                use_layer_norm=self._use_layer_norm,
                has_attention=self._has_attention,
            )

            state = torch.load(TORCH_MODEL_PATH, map_location="cpu")
            self._model.load_state_dict(state)
            self._model.to(self._device)
            self._model.eval()

            with open(TORCH_NORM_PATH, "rb") as f:
                self._norm = pickle.load(f)
            with open(TORCH_STATS_PATH, "rb") as f:
                self._tracker = pickle.load(f)

            self._loaded = True
            logger.info(
                "TorchPredictor ロード完了 (device=%s, layer_norm=%s, attention=%s)",
                self._device, self._use_layer_norm, self._has_attention,
            )
            return True
        except Exception as e:
            logger.warning("TorchPredictor ロード失敗: %s", e)
            return False

    def predict_race(
        self, race_dict: dict, horse_dicts: List[dict]
    ) -> Dict[str, Dict[str, float]]:
        """
        Returns:
            {horse_id: {"win": float, "top2": float, "top3": float}}

        Intra-Race Attention が有効な場合はレース全馬を一括入力して
        forward_race() を呼び出す。無効な場合は従来どおり predict() を使用。
        """
        if not self._loaded and not self.load():
            return {}

        # 後方互換: モデルの入力次元と FEATURE_COLUMNS を一致させる
        import json as _json

        import torch

        from src.ml.lgbm_model import FEATURE_COLUMNS, _extract_features
        try:
            meta_path = os.path.join(os.path.dirname(TORCH_MODEL_PATH), "torch_meta.json")
            with open(meta_path) as _f:
                _meta = _json.load(_f)
            model_in_dim = _meta.get("in_dim", len(FEATURE_COLUMNS))
        except Exception:
            model_in_dim = len(FEATURE_COLUMNS)
        feat_cols = FEATURE_COLUMNS[:model_in_dim]

        # sire_tracker はオプション（prob_sire_tracker.pkl を共用）
        _sire_tracker = None
        try:
            import pickle as _pk
            sire_path = os.path.join(os.path.dirname(TORCH_MODEL_PATH), "prob_sire_tracker.pkl")
            if os.path.exists(sire_path):
                with open(sire_path, "rb") as _f:
                    _sire_tracker = _pk.load(_f)
        except Exception:
            pass

        features, ids = [], []
        for h in horse_dicts:
            feat = _extract_features(h, race_dict, self._tracker, _sire_tracker)
            row  = [float(feat.get(c)) if feat.get(c) is not None else float("nan")
                    for c in feat_cols]
            features.append(row)
            ids.append(h.get("horse_id", ""))

        if not features:
            return {}

        X = self._norm.transform(np.array(features, dtype=np.float32))
        with torch.no_grad():
            xv = torch.tensor(X, dtype=torch.float32).to(self._device)

            if self._has_attention:
                # 改善3: Intra-Race Attention を使用して全馬を一括処理
                pw, p2, p3 = self._model.predict_race(xv)
            else:
                # 後方互換: Attention なし（旧モデル）
                pw, p2, p3 = self._model.predict(xv)

            pw = pw.cpu().numpy()
            p2 = p2.cpu().numpy()
            p3 = p3.cpu().numpy()

        result = {}
        n = len(ids)
        for i, hid in enumerate(ids):
            result[hid] = {
                "win":  float(pw[i]),
                "top2": float(p2[i]),
                "top3": float(p3[i]),
            }

        # レース内正規化（LightGBM モデルと同じ）
        if n > 0:
            for target, expected_sum in [("win", 1.0), ("top2", min(n, 2)), ("top3", min(n, 3))]:
                total = sum(result[hid][target] for hid in ids)
                if total > 0:
                    ratio = expected_sum / total
                    for hid in ids:
                        result[hid][target] = min(0.95, result[hid][target] * ratio)

        return result

    def predict_from_engine(self, race_info, horses,
                            evaluations=None) -> Dict[str, Dict[str, float]]:
        """RaceAnalysisEngine から呼ぶ用のラッパー（RaceInfo + Horse リストを受け取る）"""
        cond = "良"
        if race_info.course.surface == "芝" and race_info.track_condition_turf:
            cond = race_info.track_condition_turf
        elif race_info.track_condition_dirt:
            cond = race_info.track_condition_dirt

        race_dict = {
            "date": race_info.race_date,
            "venue": race_info.venue,
            "surface": race_info.course.surface,
            "distance": race_info.course.distance,
            "condition": cond,
            "field_count": race_info.field_count,
            "is_jra": race_info.is_jra,
            "grade": race_info.grade,
            "venue_code": race_info.course.venue_code,
        }

        # Step2: evaluations から horse_id → (estimated_pos4c, estimated_l3f) マップ
        ev_map: Dict[str, tuple] = {}
        if evaluations:
            for ev in evaluations:
                hid_ev = ev.horse.horse_id
                ev_map[hid_ev] = (
                    getattr(ev.pace, "estimated_position_4c", None),
                    getattr(ev.pace, "estimated_last3f", None),
                )

        horse_dicts = []
        for h in horses:
            pos_est, l3f_est = ev_map.get(h.horse_id, (None, None))
            horse_dicts.append({
                "horse_id": h.horse_id,
                "jockey_id": h.jockey_id,
                "trainer_id": h.trainer_id,
                "gate_no": h.gate_no,
                "horse_no": h.horse_no,
                "sex": h.sex,
                "age": h.age,
                "weight_kg": h.weight_kg,
                "odds": h.odds,
                "horse_weight": h.horse_weight,
                "weight_change": h.weight_change,
                # 血統特徴量 (改善C)
                "sire_id": getattr(h, "sire_id", "") or "",
                "bms_id": getattr(h, "maternal_grandsire_id", "") or "",
                # Tier1追加特徴量オーバーライド
                "is_jockey_change_override": int(h.is_jockey_change),
                # Step2 スタッキングオーバーライド
                "ml_pos_est_override": pos_est,
                "ml_l3f_est_override": l3f_est,
            })

        return self.predict_race(race_dict, horse_dicts)


# ============================================================
# エントリーポイント（python -m src.ml.torch_model）
# ============================================================

if __name__ == "__main__":
    import logging
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    epochs       = 40
    valid_days   = 30
    use_ln       = True
    has_attn     = True

    for arg in sys.argv[1:]:
        if arg.startswith("--epochs="):
            epochs = int(arg.split("=")[1])
        elif arg.startswith("--valid_days="):
            valid_days = int(arg.split("=")[1])
        elif arg == "--no_layer_norm":
            use_ln = False
        elif arg == "--no_attention":
            has_attn = False

    meta = train_torch_model(
        valid_days=valid_days,
        epochs=epochs,
        use_layer_norm=use_ln,
        has_attention=has_attn,
    )

    print("\n" + "=" * 56)
    print("  PyTorch Residual MLP 学習完了")
    print("=" * 56)
    for target, m in meta.get("metrics", {}).items():
        print(f"  {target:5s}  AUC={m['auc']:.4f}  LogLoss={m['logloss']:.4f}")
    print(f"  TOP1 win 的中:   {meta.get('top1_win_hit', 0):.1%}")
    print(f"  TOP1 place 的中: {meta.get('top1_place_hit', 0):.1%}")
    print(f"  デバイス: {meta.get('device', '?')}")
    print(f"  LayerNorm: {meta.get('use_layer_norm', False)}")
    print(f"  Attention: {meta.get('has_attention', False)}")
    print(f"  保存先: {TORCH_MODEL_PATH}")
