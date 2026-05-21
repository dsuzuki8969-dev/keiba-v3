#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DB再構築スキップ ML推論高速バッチ

pred.json に保存済みの horse_dict / race_dict 情報と
rolling_stats.pkl / rolling_sire_stats.pkl から特徴量を構築して
LightGBM (P(top3)) + ProbabilityPredictor (win/top2/top3) + PyTorch + LambdaRank の
4モデルアンサンブル → ml_composite_adj → composite更新 → 印・買い目再生成 を高速で行う。

従来 batch_regenerate_fast.py は DB再構築 (course_db, personnel, bloodline) を
毎回行っていたため 1日あたり ~9.5分。本スクリプトは pred.json + rolling_stats のみ
参照するため、1日あたり ~1-3秒 を目標とする。

使い方:
  python scripts/batch_wf_fast.py --start 20240101 --end 20251231
  python scripts/batch_wf_fast.py --year 2024
  python scripts/batch_wf_fast.py --all
  python scripts/batch_wf_fast.py --dates 20240101 20240102
  python scripts/batch_wf_fast.py --dry-run  # 変更を保存しない
"""

import argparse
import glob
import json
import math
import os
import pickle
import sys
import time
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, PROJECT_ROOT)
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import PREDICTIONS_DIR


# ============================================================
# 定数
# ============================================================

# LGBMサブモデルレベルに応じた Rule:ML ブレンド比率 (engine.py _calc_blend_ratio と同一)
BLEND_BY_LEVEL = {
    4: (0.30, 0.70),
    3: (0.35, 0.65),
    2: (0.45, 0.55),
    1: (0.55, 0.45),
    0: (0.70, 0.30),
}

# LambdaRank ブレンド率 (engine.py _calc_ranker_blend と同一)
RANKER_BLEND_BY_LEVEL = {
    4: 0.12,
    3: 0.10,
    2: 0.10,
    1: 0.08,
    0: 0.06,
}


# ============================================================
# sire_map ロード (horse_id → (sire_id, bms_id))
# pred.json に sire_id/bms_id が保存されていないため、
# 事前構築済み horse_sire_map.pkl から参照する
# ============================================================

def load_sire_map() -> dict:
    """horse_sire_map.pkl をロードする"""
    from src.ml.lgbm_model import SIRE_MAP_PATH
    if os.path.exists(SIRE_MAP_PATH):
        try:
            with open(SIRE_MAP_PATH, "rb") as f:
                return pickle.load(f)
        except Exception:
            pass
    # フォールバック: _load_horse_sire_map() を直接呼ぶ
    from src.ml.lgbm_model import _load_horse_sire_map
    return _load_horse_sire_map()


# ============================================================
# モデルロード
# ============================================================

class ModelBundle:
    """4モデルを一括管理するバンドル"""

    def __init__(self):
        self.lgbm_predictor = None       # LGBMPredictor (P(top3))
        self.prob_predictor = None       # ProbabilityPredictor (win/top2/top3)
        self.torch_predictor = None      # TorchPredictor (win/top2/top3)
        self.lgbm_ranker = None          # LGBMRanker (rank score)
        self.sire_map = {}               # horse_id → (sire_id, bms_id)
        self._loaded = False

    def load(self) -> bool:
        """全モデルをロード。約30秒かかる"""
        if self._loaded:
            return True

        t0 = time.time()

        # (1) sire_map
        print("  [1/5] sire_map ロード中...")
        self.sire_map = load_sire_map()
        print(f"         {len(self.sire_map)} horses")

        # (2) LGBMPredictor (rolling_stats.pkl + 階層モデル + calibration)
        print("  [2/5] LGBMPredictor ロード中...")
        from src.ml.lgbm_model import LGBMPredictor
        self.lgbm_predictor = LGBMPredictor()
        if not self.lgbm_predictor.load():
            print("  ERROR: LGBMPredictor ロード失敗")
            return False

        # (3) ProbabilityPredictor (win/top2/top3 個別モデル)
        print("  [3/5] ProbabilityPredictor ロード中...")
        try:
            from src.ml.probability_model import ProbabilityPredictor
            self.prob_predictor = ProbabilityPredictor()
            if not self.prob_predictor.load():
                print("  WARNING: ProbabilityPredictor ロード失敗、LGBMのみで続行")
                self.prob_predictor = None
        except Exception as e:
            print(f"  WARNING: ProbabilityPredictor スキップ: {e}")
            self.prob_predictor = None

        # (4) TorchPredictor (PyTorch Residual MLP)
        print("  [4/5] TorchPredictor ロード中...")
        try:
            from src.ml.torch_model import TorchPredictor
            self.torch_predictor = TorchPredictor()
            if not self.torch_predictor.load():
                print("  WARNING: TorchPredictor ロード失敗、スキップ")
                self.torch_predictor = None
        except Exception as e:
            print(f"  WARNING: TorchPredictor スキップ: {e}")
            self.torch_predictor = None

        # (5) LGBMRanker (LambdaRank)
        print("  [5/5] LGBMRanker ロード中...")
        try:
            from src.ml.lgbm_ranker import LGBMRanker
            self.lgbm_ranker = LGBMRanker()
            if not self.lgbm_ranker.load():
                print("  WARNING: LGBMRanker ロード失敗、スキップ")
                self.lgbm_ranker = None
        except Exception as e:
            print(f"  WARNING: LGBMRanker スキップ: {e}")
            self.lgbm_ranker = None

        self._loaded = True
        print(f"  全モデルロード完了: {time.time() - t0:.1f}秒")
        return True


# ============================================================
# pred.json から race_dict / horse_dicts を構築
# ============================================================

def build_race_dict(race: dict, date_str: str) -> dict:
    """pred.json の race エントリから ML推論用の race_dict を構築"""
    return {
        "race_id": race.get("race_id", ""),
        "date": date_str,  # YYYY-MM-DD 形式
        "venue": race.get("venue", ""),
        "venue_code": str(race.get("venue_code", "") or "").zfill(2),
        "surface": race.get("surface", ""),
        "distance": race.get("distance", 0),
        "condition": race.get("condition", "良"),
        "field_count": race.get("field_count", 0),
        "is_jra": race.get("is_jra", False),
        "grade": race.get("grade", ""),
    }


def build_horse_dicts(horses: list, sire_map: dict) -> list:
    """pred.json の horses リストから ML推論用の horse_dicts を構築"""
    result = []
    for h in horses:
        if h.get("is_scratched"):
            continue

        hid = h.get("horse_id", "")
        sid, bid = sire_map.get(hid, ("", ""))

        result.append({
            "horse_id": hid,
            "horse_name": h.get("horse_name", ""),
            "horse_no": h.get("horse_no", 0),
            "jockey_id": h.get("jockey_id", ""),
            "trainer_id": h.get("trainer_id", ""),
            "gate_no": h.get("gate_no", 0),
            "sex": h.get("sex", ""),
            "age": h.get("age", 0),
            "weight_kg": h.get("weight_kg", 55.0),
            "horse_weight": h.get("horse_weight"),
            "weight_change": h.get("weight_change"),
            "sire_id": sid or h.get("sire_id", ""),
            "bms_id": bid or h.get("bms_id", ""),
            # エンジンオーバーライド値 (pred.jsonに保存済みならML特徴量に反映される)
            "ml_pos_est_override": h.get("pace_estimated_pos4c"),
            "ml_l3f_est_override": h.get("pace_estimated_last3f"),
        })
    return result


# ============================================================
# 4モデルアンサンブル + Rule:ML ブレンド + ml_composite_adj 計算
# engine.py L1590-1805 のロジックをpred.json直接操作で再現
# ============================================================

def run_inference_for_race(
    race: dict,
    date_str: str,
    bundle: ModelBundle,
) -> bool:
    """1レース分の推論と結果書き戻し。変更があれば True を返す"""

    horses = race.get("horses", [])
    active = [h for h in horses if not h.get("is_scratched")]
    if len(active) < 2:
        return False

    race_dict = build_race_dict(race, date_str)
    horse_dicts = build_horse_dicts(horses, bundle.sire_map)

    if len(horse_dicts) < 2:
        return False

    # field_count が未設定なら出走頭数で補完
    if not race_dict["field_count"]:
        race_dict["field_count"] = len(horse_dicts)

    # ================================================================
    # (A) ProbabilityPredictor: win/top2/top3 個別予測 (最も精度が高い)
    # ================================================================
    prob_map = {}  # {horse_id: {"win": float, "top2": float, "top3": float}}
    if bundle.prob_predictor:
        try:
            prob_map = bundle.prob_predictor.predict_race(race_dict, horse_dicts)
        except Exception:
            pass

    # ================================================================
    # (B) LGBMPredictor: P(3着以内) のみ (フォールバック)
    # ================================================================
    lgbm_probs = {}  # {horse_id: float}
    lgbm_level = 2   # デフォルト
    if bundle.lgbm_predictor:
        try:
            lgbm_probs = bundle.lgbm_predictor.predict_race(race_dict, horse_dicts)
            from src.ml.lgbm_model import _lgbm_tls
            lgbm_level = getattr(_lgbm_tls, "last_model_level", 2)
        except Exception:
            pass

    # ProbabilityPredictor が使える場合はそちらを主にする
    # ProbabilityPredictor が失敗した場合、LGBMPredictor の P(top3) から近似分配
    ml_win = {}   # {horse_id: float}
    ml_top2 = {}
    ml_top3 = {}

    for hd in horse_dicts:
        hid = hd["horse_id"]
        pp = prob_map.get(hid, {})
        lp = lgbm_probs.get(hid)

        if pp:
            # ProbabilityPredictor の結果を使用
            ml_win[hid] = pp.get("win", 0.0)
            ml_top2[hid] = pp.get("top2", 0.0)
            ml_top3[hid] = pp.get("top3", 0.0)
        elif lp is not None:
            # LGBMPredictor のみ: P(top3) から近似分配
            # engine.py では ProbabilityPredictor がフォールバック的に P(top3) を ev.ml_place_prob に設定
            ml_win[hid] = lp * 0.33   # P(top3) の約1/3 が P(win)
            ml_top2[hid] = lp * 0.67
            ml_top3[hid] = lp

    if not ml_win:
        return False

    # ================================================================
    # (C) PyTorch アンサンブル (LightGBM 60% + PyTorch 40%)
    # engine.py L1616-1646
    # ================================================================
    if bundle.torch_predictor:
        try:
            torch_probs = bundle.torch_predictor.predict_race(race_dict, horse_dicts)
            if torch_probs:
                lw, tw = 0.6, 0.4
                for hid in list(ml_win.keys()):
                    tp = torch_probs.get(hid, {})
                    if not tp:
                        continue
                    if ml_win[hid] is not None:
                        ml_win[hid] = ml_win[hid] * lw + tp.get("win", ml_win[hid]) * tw
                    elif tp.get("win") is not None:
                        ml_win[hid] = tp["win"]
                    if ml_top2[hid] is not None:
                        ml_top2[hid] = ml_top2[hid] * lw + tp.get("top2", ml_top2[hid]) * tw
                    elif tp.get("top2") is not None:
                        ml_top2[hid] = tp["top2"]
                    if ml_top3[hid] is not None:
                        ml_top3[hid] = ml_top3[hid] * lw + tp.get("top3", ml_top3[hid]) * tw
                    elif tp.get("top3") is not None:
                        ml_top3[hid] = tp["top3"]
        except Exception:
            pass

    # ================================================================
    # (D) LambdaRank アンサンブル
    # engine.py L1648-1696
    # ================================================================
    if bundle.lgbm_ranker:
        try:
            ranker_scores = bundle.lgbm_ranker.predict_race(race_dict, horse_dicts)
            if ranker_scores:
                # softmax 正規化
                vals = list(ranker_scores.values())
                max_v = max(vals)
                exp_v = {hid: math.exp(v - max_v) for hid, v in ranker_scores.items()}
                sum_e = sum(exp_v.values())
                norm_scores = {hid: ev / sum_e for hid, ev in exp_v.items()}

                _RW = RANKER_BLEND_BY_LEVEL.get(lgbm_level, 0.10)
                for hid in list(ml_win.keys()):
                    rs = norm_scores.get(hid)
                    if rs is None:
                        continue
                    ml_win[hid] = ml_win[hid] * (1 - _RW) + rs * _RW
                    ml_top3[hid] = ml_top3[hid] * (1 - _RW) + rs * _RW
        except Exception:
            pass

    # 診断値を保存
    for h in active:
        hid = h.get("horse_id", "")
        lp = lgbm_probs.get(hid)
        if lp is not None:
            h["raw_lgbm_prob"] = round(lp, 6)
        ep = ml_win.get(hid)
        if ep is not None:
            h["ensemble_prob"] = round(ep, 6)

    # ================================================================
    # (E) Rule:ML ブレンド
    # engine.py L1702-1753
    #
    # Rule-based win_prob は既存の pred.json から復元:
    # 旧 ml_composite_adj を除去した純 composite から softmax
    # ================================================================

    _RB_W, _ML_W = BLEND_BY_LEVEL.get(lgbm_level, (0.45, 0.55))

    # gap連動ブレンド調整 (engine.py L1707-1722)
    all_composites = sorted([h.get("composite", 50.0) or 50.0 for h in active], reverse=True)
    gap_1_2 = (all_composites[0] - all_composites[1]) if len(all_composites) >= 2 else 0
    if gap_1_2 >= 2.0:
        gap_boost = min(0.25, math.log1p((gap_1_2 - 2.0) * 0.25) * 0.15)
        if gap_1_2 >= 15.0:
            gap_boost *= 0.40
        elif gap_1_2 >= 10.0:
            gap_boost *= 0.75
        _RB_W = min(0.80, _RB_W + gap_boost)
        _ML_W = 1.0 - _RB_W

    # Rule-based 確率を復元: 純 composite (旧adj除去) から softmax
    pure_composites = []
    for h in active:
        old_adj = h.get("ml_composite_adj", 0.0) or 0.0
        pc = (h.get("composite", 50.0) or 50.0) - old_adj
        pure_composites.append(pc)

    # softmax (temperature=10)
    max_c = max(pure_composites) if pure_composites else 0
    exp_c = [math.exp((c - max_c) / 10.0) for c in pure_composites]
    sum_ec = sum(exp_c) or 1.0
    rule_win_probs = [e / sum_ec for e in exp_c]

    # rule_place2 / rule_place3 は既存 pred.json のブレンド前値を使う
    # (正確には composite 順位ベースだが、簡易的に rule_win_prob を比率ベースで拡張)

    # ML+Rule ブレンド → 最終 win/place2/place3
    n_active = len(active)
    win_probs = []
    place2_probs = []
    place3_probs = []

    for i, h in enumerate(active):
        hid = h.get("horse_id", "")
        r_wp = rule_win_probs[i]

        m_wp = ml_win.get(hid)
        m_p2 = ml_top2.get(hid)
        m_p3 = ml_top3.get(hid)

        if m_wp is not None:
            wp = _RB_W * r_wp + _ML_W * m_wp
        else:
            wp = r_wp

        # place2/place3: rule-based は win_prob の比率拡張で近似
        r_p2 = r_wp * min(n_active, 2)
        r_p3 = r_wp * min(n_active, 3)

        if m_p2 is not None:
            p2 = _RB_W * r_p2 + _ML_W * m_p2
        else:
            p2 = r_p2

        if m_p3 is not None:
            p3 = _RB_W * r_p3 + _ML_W * m_p3
        else:
            p3 = r_p3

        win_probs.append(wp)
        place2_probs.append(p2)
        place3_probs.append(p3)

    # ================================================================
    # (F) 正規化 (engine.py _normalize_sums_only 相当)
    # ================================================================
    tw = sum(win_probs) or 1.0
    t2 = sum(place2_probs) or 1.0
    t3 = sum(place3_probs) or 1.0

    win_probs = [p / tw for p in win_probs]
    place2_probs = [min(0.95, p / t2 * min(n_active, 2)) for p in place2_probs]
    place3_probs = [min(0.95, p / t3 * min(n_active, 3)) for p in place3_probs]

    # 整合性: win <= place2 <= place3
    for i in range(n_active):
        place2_probs[i] = max(place2_probs[i], win_probs[i])
        place3_probs[i] = max(place3_probs[i], place2_probs[i])

    # ================================================================
    # (G) ml_composite_adj 計算 (engine.py L1757-1805)
    # ================================================================
    avg_wp = sum(win_probs) / n_active if n_active else 0
    std_wp = (sum((p - avg_wp) ** 2 for p in win_probs) / n_active) ** 0.5 if n_active >= 3 else 0

    # composite 順位と win_prob 順位を算出
    comp_vals = [(pure_composites[i], i) for i in range(n_active)]
    comp_vals.sort(key=lambda x: -x[0])
    comp_ranks = [0] * n_active
    for rank, (_, idx) in enumerate(comp_vals):
        comp_ranks[idx] = rank + 1

    wp_vals = [(win_probs[i], i) for i in range(n_active)]
    wp_vals.sort(key=lambda x: -x[0])
    wp_ranks = [0] * n_active
    for rank, (_, idx) in enumerate(wp_vals):
        wp_ranks[idx] = rank + 1

    for i, h in enumerate(active):
        old_adj = h.get("ml_composite_adj", 0.0) or 0.0
        old_composite = (h.get("composite", 50.0) or 50.0)
        base_composite = old_composite - old_adj  # 純 composite

        if std_wp > 0.001:
            z = (win_probs[i] - avg_wp) / std_wp
            raw_adj = max(-5.0, min(5.0, z * 1.5))

            # 順位乖離ペナルティ (engine.py L1789-1795)
            rank_gap = wp_ranks[i] - comp_ranks[i]  # 正=MLの方が低評価
            if rank_gap >= 3:
                rank_penalty = min(3.0, (rank_gap - 2) * 0.5)
                raw_adj -= rank_penalty

            # 高オッズ馬のML補正をダンピング (engine.py L1797-1802)
            odds = h.get("odds") or h.get("predicted_tansho_odds") or 0
            if odds and odds >= 30.0 and raw_adj > 0:
                raw_adj *= 0.3
            elif odds and odds >= 15.0 and raw_adj > 0:
                raw_adj *= 0.5

            h["ml_composite_adj"] = round(raw_adj, 4)
        else:
            h["ml_composite_adj"] = 0.0

        # composite 更新 (旧adj除去 + 新adj付与)
        h["composite"] = round(
            max(20.0, min(100.0, base_composite + h["ml_composite_adj"])), 2
        )

        # 確率更新
        h["win_prob"] = round(win_probs[i], 6)
        h["place2_prob"] = round(place2_probs[i], 6)
        h["place3_prob"] = round(place3_probs[i], 6)

        # ML個別値クリア (engine.py L1739-1742: ブレンド済みなので重複表示防止)
        h["ml_win_prob"] = None
        h["ml_top2_prob"] = None
        h["ml_place_prob"] = None

        # モデルレベル保存
        h["model_level"] = lgbm_level

    return True


# ============================================================
# 印・買い目再生成 (regen_strategy.py のロジックを呼び出す)
# ============================================================

def regen_marks_and_tickets(race: dict):
    """印と買い目を再生成する (regen_strategy.py の関数を呼ぶ)"""
    from scripts.regen_strategy import _regen_marks_for_race, _regen_tickets_for_race

    horses = race.get("horses", [])
    is_jra = race.get("is_jra", False)

    _regen_marks_for_race(horses, is_jra=is_jra)
    _regen_tickets_for_race(race)


# ============================================================
# 1日分処理
# ============================================================

def process_one_day(pred_path: str, bundle: ModelBundle, dry_run: bool = False) -> dict:
    """1日分の pred.json をML推論して更新する

    Returns: {"date": str, "races": int, "horses": int, "predicted": int, "elapsed": float}
    """
    t0 = time.time()

    with open(pred_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # ファイル名から日付を抽出 (YYYYMMDD_pred.json)
    fname = Path(pred_path).name
    dt = fname.replace("_pred.json", "")
    # tracker は YYYY-MM-DD 形式を期待する
    date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}"

    total_horses = 0
    predicted_races = 0

    for race in payload.get("races", []):
        # ばんえい（帯広）除外
        vc = str(race.get("venue_code", "") or "").zfill(2)
        if vc in ("52", "65"):
            continue

        horses = race.get("horses", [])
        active = [h for h in horses if not h.get("is_scratched")]
        total_horses += len(active)

        # ML推論
        ok = run_inference_for_race(race, date_str, bundle)

        if ok:
            # 印・買い目再生成
            regen_marks_and_tickets(race)
            predicted_races += 1

    elapsed = time.time() - t0

    if not dry_run and predicted_races > 0:
        with open(pred_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    return {
        "date": dt,
        "races": len(payload.get("races", [])),
        "horses": total_horses,
        "predicted": predicted_races,
        "elapsed": elapsed,
    }


# ============================================================
# 対象ファイル収集
# ============================================================

def collect_pred_files(args) -> list:
    """引数に応じて対象 pred.json ファイルのリストを返す"""
    pred_files = []

    if args.dates:
        for d in args.dates:
            p = os.path.join(PREDICTIONS_DIR, f"{d}_pred.json")
            if os.path.exists(p):
                pred_files.append(p)
            else:
                print(f"  WARNING: 見つからない: {p}")
    elif args.start and args.end:
        # --start / --end 範囲指定
        all_files = sorted(glob.glob(os.path.join(PREDICTIONS_DIR, "*_pred.json")))
        for f in all_files:
            fname = os.path.basename(f)
            if "_prev" in fname or ".bak" in fname:
                continue
            date_part = fname[:8]
            if date_part.isdigit() and args.start <= date_part <= args.end:
                pred_files.append(f)
    elif args.year:
        pattern = os.path.join(PREDICTIONS_DIR, f"{args.year}*_pred.json")
        pred_files = sorted(glob.glob(pattern))
        pred_files = [f for f in pred_files if "_prev" not in f and ".bak" not in f]
    elif args.all:
        all_files = sorted([
            os.path.join(PREDICTIONS_DIR, f)
            for f in os.listdir(PREDICTIONS_DIR)
            if f.endswith("_pred.json") and "_prev" not in f and ".bak" not in f
        ])
        pred_files = all_files
    else:
        print("ERROR: --year, --dates, --start/--end, または --all を指定してください")
        sys.exit(1)

    return sorted(pred_files)


# ============================================================
# メイン
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="DB再構築スキップ ML推論高速バッチ (pred.json + rolling_stats のみ)"
    )
    parser.add_argument("--start", type=str, help="開始日 (YYYYMMDD)")
    parser.add_argument("--end", type=str, help="終了日 (YYYYMMDD)")
    parser.add_argument("--year", type=str, help="対象年 (例: 2024)")
    parser.add_argument("--dates", nargs="+", help="個別日付 (例: 20240101 20240102)")
    parser.add_argument("--all", action="store_true", help="全期間")
    parser.add_argument("--dry-run", action="store_true", help="変更を保存しない")
    args = parser.parse_args()

    # 引数バリデーション
    has_range = args.start and args.end
    has_any = has_range or args.year or args.dates or args.all
    if not has_any:
        parser.error("--year, --dates, --start/--end, または --all を指定してください")

    # 対象ファイル収集
    pred_files = collect_pred_files(args)
    if not pred_files:
        print("対象ファイルが見つかりません")
        return

    total = len(pred_files)
    dry_label = " [DRY-RUN]" if args.dry_run else ""

    print(f"{'=' * 70}")
    print(f"  DB再構築スキップ ML推論高速バッチ{dry_label}")
    print(f"  対象: {total} 日分")
    print(f"{'=' * 70}")
    print()

    # モデルロード (1回のみ)
    print("[Step 1] モデルロード...")
    bundle = ModelBundle()
    if not bundle.load():
        print("ERROR: モデルロード失敗")
        sys.exit(1)
    print()

    # 推論
    print(f"[Step 2] ML推論 ({total} 日分)...")
    total_elapsed = 0
    total_horses = 0
    total_predicted = 0
    errors = []

    for i, fpath in enumerate(pred_files):
        try:
            result = process_one_day(fpath, bundle, dry_run=args.dry_run)
            total_elapsed += result["elapsed"]
            total_horses += result["horses"]
            total_predicted += result["predicted"]

            pct = (i + 1) / total * 100
            bar_len = 40
            filled = int(bar_len * (i + 1) / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            print(
                f"  [{bar}] {pct:5.1f}% {result['date']} "
                f"{result['races']}R {result['horses']}頭 "
                f"ML={result['predicted']}R "
                f"{result['elapsed']:.2f}秒"
            )
        except Exception as e:
            errors.append((fpath, str(e)))
            print(f"  ERROR: {Path(fpath).name}: {e}")

    # サマリ
    print()
    print(f"{'=' * 70}")
    avg_sec = total_elapsed / total if total > 0 else 0
    print(f"  完了: {total} 日 / {total_horses} 頭 / ML推論 {total_predicted} レース")
    print(f"  所要時間: {total_elapsed:.1f} 秒 ({total_elapsed / 60:.1f} 分)")
    print(f"  平均: {avg_sec:.2f} 秒/日")
    if errors:
        print(f"  エラー: {len(errors)} 件")
        for fpath, err in errors[:5]:
            print(f"    {Path(fpath).name}: {err}")
    if args.dry_run:
        print("  (dry-run: 変更は保存されていません)")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
