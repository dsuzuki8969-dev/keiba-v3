"""LightGBM 特徴量重要度 (feature_importance) 分析 — 印選定構造課題の原因究明

マスター指示 (2026-05-28): 「全ての特徴量の比重からの予想がおかしい」
→ 現状の ML モデルがどの特徴量を重視しているか確認し、人気馬偏重の構造を可視化する。

対象: data/models/wf_2026/ の lgbm_*.txt (place 系 + win_global)
出力:
  1. 各モデルの Top 15 特徴量 (gain importance)
  2. 全モデル平均 Top 30 (横並び比較)
  3. ROI 観点で疑わしい特徴量 (popularity/odds系) の重要度集計
"""

import os
import sys
from collections import defaultdict
from glob import glob

import lightgbm as lgb

sys.stdout.reconfigure(encoding="utf-8")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODEL_DIR = os.path.join(ROOT, "data", "models", "wf_2026")


def load_importance(model_path: str) -> dict:
    """LightGBM モデルから {feature_name: gain_importance} を取得"""
    booster = lgb.Booster(model_file=model_path)
    importances = booster.feature_importance(importance_type="gain")
    feat_names = booster.feature_name()
    return dict(zip(feat_names, importances))


def main():
    print("=" * 110)
    print("LightGBM 特徴量重要度 分析 — wf_2026 全モデル")
    print("=" * 110)

    # 全 lgbm_*.txt を取得
    model_files = sorted(glob(os.path.join(MODEL_DIR, "lgbm_*.txt")))
    print(f"対象モデル: {len(model_files)} 個")

    all_importance = defaultdict(float)
    per_model = {}
    for mf in model_files:
        name = os.path.basename(mf).replace("lgbm_", "").replace(".txt", "")
        try:
            imp = load_importance(mf)
            per_model[name] = imp
            for feat, val in imp.items():
                all_importance[feat] += val
        except Exception as e:
            print(f"  ⚠ {name}: {e}")

    # 全モデル合算 Top 30
    print()
    print("=" * 110)
    print(f"【全モデル合算】 Top 30 特徴量 (gain importance 合計)")
    print("=" * 110)
    sorted_all = sorted(all_importance.items(), key=lambda x: -x[1])
    print(f"{'順位':>3} {'特徴量':<40} {'合計gain':>15} {'相対%':>8}")
    print("-" * 110)
    total = sum(all_importance.values())
    for i, (feat, gain) in enumerate(sorted_all[:30], 1):
        pct = gain / total * 100 if total else 0
        print(f"{i:>3}. {feat:<40} {gain:>15,.0f} {pct:>7.2f}%")

    # ROI 観点で疑わしい特徴量カテゴリ
    print()
    print("=" * 110)
    print("【ROI 観点 疑わしい特徴量 カテゴリ別】")
    print("=" * 110)
    categories = {
        "人気・オッズ系 (popularity/odds)": ["popularity", "odds", "tansho", "market", "ninki"],
        "過去成績 (past_runs/dev)": ["past", "dev_run", "relative_dev", "history"],
        "騎手・厩舎 (jockey/trainer)": ["jockey", "trainer", "stable"],
        "ペース・展開 (pace/sec)": ["pace", "sec_per", "agari", "early"],
        "馬体・血統 (weight/blood)": ["weight", "bloodline", "sire", "horse_weight"],
        "ML 出力 (composite/win_prob)": ["composite", "win_prob", "ml_", "head_"],
        "コース適性 (course/dist)": ["course", "distance", "surface", "venue"],
        "斤量・性齢 (sex/age/burden)": ["sex", "age", "burden", "carried"],
    }
    cat_totals = {}
    matched_feats = set()
    for cat, keywords in categories.items():
        total_gain = 0
        feats = []
        for feat, gain in all_importance.items():
            if any(kw in feat.lower() for kw in keywords):
                total_gain += gain
                feats.append((feat, gain))
                matched_feats.add(feat)
        cat_totals[cat] = (total_gain, feats)
    # その他
    other_gain = sum(gain for feat, gain in all_importance.items() if feat not in matched_feats)
    cat_totals["その他"] = (other_gain, [(f, g) for f, g in all_importance.items() if f not in matched_feats])

    print(f"{'カテゴリ':<40} {'合計gain':>15} {'相対%':>8} {'特徴量数':>8}")
    print("-" * 110)
    sorted_cats = sorted(cat_totals.items(), key=lambda x: -x[1][0])
    for cat, (g, feats) in sorted_cats:
        pct = g / total * 100 if total else 0
        print(f"{cat:<40} {g:>15,.0f} {pct:>7.2f}% {len(feats):>7,}")

    # 人気・オッズ系の Top 10
    print()
    print("=" * 110)
    print("【人気・オッズ系 特徴量 Top 10】 (ROI を押し下げている疑い)")
    print("=" * 110)
    pop_feats = cat_totals["人気・オッズ系 (popularity/odds)"][1]
    pop_feats.sort(key=lambda x: -x[1])
    print(f"{'特徴量':<40} {'合計gain':>15} {'相対%':>8}")
    print("-" * 110)
    for feat, g in pop_feats[:10]:
        pct = g / total * 100 if total else 0
        print(f"{feat:<40} {g:>15,.0f} {pct:>7.2f}%")

    # 重要モデル個別 Top 10
    print()
    print("=" * 110)
    print("【主要モデル個別 Top 10】 (place / win_global / jra_turf)")
    print("=" * 110)
    for target in ["place", "win_global", "place_jra_turf"]:
        if target in per_model:
            print(f"\n--- {target} ---")
            sorted_m = sorted(per_model[target].items(), key=lambda x: -x[1])[:10]
            for feat, g in sorted_m:
                print(f"  {feat:<40} {g:>12,.0f}")

    print()
    print("=" * 110)
    print("【分析の含意】")
    print("=" * 110)
    pop_pct = cat_totals["人気・オッズ系 (popularity/odds)"][0] / total * 100 if total else 0
    past_pct = cat_totals["過去成績 (past_runs/dev)"][0] / total * 100 if total else 0
    ml_pct = cat_totals["ML 出力 (composite/win_prob)"][0] / total * 100 if total else 0
    print(f"  人気・オッズ系 比重: {pop_pct:.1f}% {'← 高い場合 ROI 押し下げ要因' if pop_pct > 15 else ''}")
    print(f"  過去成績 比重: {past_pct:.1f}%")
    print(f"  ML 内部出力 比重: {ml_pct:.1f}%")
    print()
    print("  → ROI 観点で再選定すべき特徴量: 人気・オッズ系 (人気馬偏重の温床)")
    print("  → 強化すべき特徴量: 隠れた実力指標 (前走 dev / 騎手乗替り / ペース適性)")


if __name__ == "__main__":
    main()
