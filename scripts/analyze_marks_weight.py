"""
analyze_marks_weight.py
========================
軸馬度・穴馬度の各構成要素の重みをデータで最適化する分析スクリプト。

【軸馬度】
  正解 = 複勝 (finish_pos <= 3)
  候補要素: composite / place3_prob / レース内実力順位(composite降順)

【穴馬度】
  正解 = 人気薄の好走 (popularity >= 5 かつ finish_pos <= 3)
  候補要素: composite / place3_prob / ana_score / odds_divergence

分析手法:
  (a) 単変量: 各要素を5〜10分位に区切り、分位別の正解率を出す
  (b) 多変量: 標準化 z-score → ロジスティック回帰の標準化係数 → 推奨重み%

本番ファイル非改変・git commit不要。
"""

import sys
import io
import json
import sqlite3
import math
from collections import defaultdict
from typing import Optional

# Windows cp932対策: stdout を UTF-8 に再設定
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

DB_PATH = "data/keiba.db"

# ────────────────────────────────────────────────
# DB 読み込みユーティリティ
# ────────────────────────────────────────────────

def build_finish_map(conn: sqlite3.Connection) -> dict:
    """race_id -> {horse_no: finish_pos} のマップを構築"""
    cur = conn.cursor()
    cur.execute(
        "SELECT race_id, horse_no, finish_pos FROM race_log WHERE finish_pos IS NOT NULL"
    )
    result: dict[str, dict[int, int]] = defaultdict(dict)
    for race_id, horse_no, finish_pos in cur.fetchall():
        result[race_id][int(horse_no)] = int(finish_pos)
    return result


# ────────────────────────────────────────────────
# データ収集
# ────────────────────────────────────────────────

def load_samples(conn: sqlite3.Connection, finish_map: dict) -> tuple[list, list]:
    """
    軸馬度サンプル・穴馬度サンプルをそれぞれ収集。

    Returns:
        axis_rows: list of dict (軸馬度分析用)
        ana_rows:  list of dict (穴馬度分析用)
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT race_id, horses_json
        FROM predictions
        WHERE race_id != '' AND horses_json != '[]' AND horses_json IS NOT NULL
        ORDER BY race_id
        """
    )
    rows = cur.fetchall()
    print(f"  predictions ロード: {len(rows):,} レース")

    axis_rows: list[dict] = []
    ana_rows:  list[dict] = []
    skipped = 0
    matched = 0

    for race_id, horses_json in rows:
        if race_id not in finish_map:
            skipped += 1
            continue

        try:
            horses = json.loads(horses_json)
        except json.JSONDecodeError:
            skipped += 1
            continue

        if not isinstance(horses, list) or len(horses) < 3:
            skipped += 1
            continue

        # composite が有効な馬のみ
        valid = [
            h for h in horses
            if isinstance(h.get("composite"), (int, float))
            and h.get("composite") is not None
        ]
        if len(valid) < 3:
            skipped += 1
            continue

        matched += 1
        fmap = finish_map[race_id]

        # composite 降順でレース内順位を割り当て (1=最高)
        sorted_by_comp = sorted(valid, key=lambda x: x["composite"], reverse=True)
        comp_rank: dict[int, int] = {}
        for rank, h in enumerate(sorted_by_comp, start=1):
            comp_rank[int(h["horse_no"])] = rank

        for h in valid:
            horse_no = int(h["horse_no"])
            fpos = fmap.get(horse_no)
            if fpos is None:
                continue

            composite    = float(h["composite"])
            place3_prob  = h.get("place3_prob")
            ana_score    = h.get("ana_score")
            odds_diverg  = h.get("odds_divergence")
            popularity   = h.get("popularity")
            rank         = comp_rank.get(horse_no)

            # place3_prob が None の場合はスキップ（軸馬度）
            if place3_prob is None or rank is None:
                continue

            is_place = int(fpos <= 3)

            axis_rows.append({
                "composite":   composite,
                "place3_prob": float(place3_prob),
                "comp_rank":   float(rank),
                "is_place":    is_place,
                "race_id":     race_id,
            })

            # 穴馬度: popularity >= 5 の馬のみ対象
            if (
                popularity is not None
                and int(popularity) >= 5
                and ana_score is not None
                and odds_diverg is not None
            ):
                ana_rows.append({
                    "composite":      composite,
                    "place3_prob":    float(place3_prob),
                    "ana_score":      float(ana_score),
                    "odds_divergence": float(odds_diverg),
                    "is_upset":       is_place,  # 人気薄が3着以内
                    "race_id":        race_id,
                })

    print(f"  突合成功: {matched:,} レース / スキップ: {skipped:,}")
    print(f"  軸馬度サンプル: {len(axis_rows):,} 頭")
    print(f"  穴馬度サンプル: {len(ana_rows):,} 頭 (popularity>=5)")
    return axis_rows, ana_rows


# ────────────────────────────────────────────────
# 単変量分析: 分位別正解率
# ────────────────────────────────────────────────

def univariate_quantile(values: list[float], labels: list[int], n_quantiles: int = 10,
                         col_name: str = "feature") -> None:
    """
    values を n_quantiles に分けて、各分位の正解率を表示。
    """
    arr = np.array(values, dtype=float)
    lab = np.array(labels, dtype=int)

    # 分位境界 (重複を除く)
    quantiles = np.linspace(0, 100, n_quantiles + 1)
    edges = np.unique(np.percentile(arr, quantiles))

    print(f"\n  【{col_name}】 分位別正解率 ({n_quantiles}分位)")
    print(f"  {'分位':>4}  {'下限':>9}  {'上限':>9}  {'N':>6}  {'正解率':>8}")
    print("  " + "-" * 48)

    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        if i == len(edges) - 2:
            mask = (arr >= lo)
        else:
            mask = (arr >= lo) & (arr < hi)
        n = mask.sum()
        if n == 0:
            continue
        rate = lab[mask].mean() * 100
        print(f"  {i+1:>4}  {lo:>9.3f}  {hi:>9.3f}  {n:>6,}  {rate:>7.1f}%")


# ────────────────────────────────────────────────
# 多変量分析: ロジスティック回帰
# ────────────────────────────────────────────────

def logistic_weights(X: np.ndarray, y: np.ndarray, feature_names: list[str]) -> np.ndarray:
    """
    標準化 z-score → ロジスティック回帰 → 標準化係数を返す。
    abs を正規化して推奨重み%を返す。
    """
    scaler = StandardScaler()
    X_z = scaler.fit_transform(X)

    lr = LogisticRegression(max_iter=1000, solver="lbfgs", C=1.0, random_state=42)
    lr.fit(X_z, y)

    coef = lr.coef_[0]  # shape: (n_features,)
    abs_coef = np.abs(coef)
    weight_pct = abs_coef / abs_coef.sum() * 100

    print(f"\n  【ロジスティック回帰 標準化係数】")
    print(f"  {'要素':>18}  {'係数':>8}  {'|係数|':>8}  {'推奨重み':>8}")
    print("  " + "-" * 52)
    for name, c, ac, wp in zip(feature_names, coef, abs_coef, weight_pct):
        direction = "+" if c >= 0 else "-"
        print(f"  {name:>18}  {c:>+8.4f}  {ac:>8.4f}  {wp:>7.1f}%")

    return weight_pct


def print_separator(title: str) -> None:
    print(f"\n{'='*65}")
    print(f"  {title}")
    print(f"{'='*65}")


# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  analyze_marks_weight.py")
    print("  軸馬度・穴馬度 構成要素 重み最適化分析")
    print("=" * 65)

    conn = sqlite3.connect(DB_PATH)

    print("\n[1/3] DBロード中...")
    finish_map = build_finish_map(conn)
    print(f"  race_log: {len(finish_map):,} レース")

    print("\n[2/3] サンプル収集中...")
    axis_rows, ana_rows = load_samples(conn, finish_map)
    conn.close()

    # ════════════════════════════════════════════
    # ① 軸馬度分析
    # ════════════════════════════════════════════
    print_separator("① 軸馬度（本命信頼度）分析")
    print(f"  正解定義: finish_pos <= 3 (複勝)")
    print(f"  候補要素: composite / place3_prob / comp_rank（composite降順の順位）")

    axis_composite  = [r["composite"]   for r in axis_rows]
    axis_place3     = [r["place3_prob"] for r in axis_rows]
    axis_rank       = [r["comp_rank"]   for r in axis_rows]
    axis_label      = [r["is_place"]    for r in axis_rows]

    # --- 単変量 ---
    print("\n── 単変量分析（分位別正解率） ──")
    univariate_quantile(axis_composite, axis_label, n_quantiles=10, col_name="composite")
    univariate_quantile(axis_place3,    axis_label, n_quantiles=10, col_name="place3_prob")
    # comp_rank は降順(1=最高)なので逆にして確認
    univariate_quantile(axis_rank,      axis_label, n_quantiles=10,
                        col_name="comp_rank（1=最高）")

    # --- 多変量 ---
    print("\n── 多変量分析（ロジスティック回帰） ──")
    X_axis = np.column_stack([axis_composite, axis_place3, axis_rank])
    y_axis = np.array(axis_label)
    # comp_rank は「低いほど良い」ので符号反転 (-1 を掛けて「高いほど良い」に揃える)
    X_axis_sign = X_axis.copy()
    X_axis_sign[:, 2] = -X_axis_sign[:, 2]
    feature_names_axis = ["composite", "place3_prob", "comp_rank(↑好)"]
    axis_weights = logistic_weights(X_axis_sign, y_axis, feature_names_axis)

    # 正解率の全体値も表示
    overall_place = np.mean(axis_label) * 100
    print(f"\n  [参考] 全体複勝率: {overall_place:.1f}%  サンプル数: {len(axis_rows):,}")

    # ════════════════════════════════════════════
    # ② 穴馬度分析
    # ════════════════════════════════════════════
    print_separator("② 穴馬度（過小評価妙味）分析")
    print(f"  正解定義: popularity >= 5 かつ finish_pos <= 3（人気薄の好走）")
    print(f"  候補要素: composite / place3_prob / ana_score / odds_divergence")

    ana_composite  = [r["composite"]       for r in ana_rows]
    ana_place3     = [r["place3_prob"]     for r in ana_rows]
    ana_score      = [r["ana_score"]       for r in ana_rows]
    ana_oddsDiv    = [r["odds_divergence"] for r in ana_rows]
    ana_label      = [r["is_upset"]        for r in ana_rows]

    # --- 単変量 ---
    print("\n── 単変量分析（分位別正解率） ──")
    univariate_quantile(ana_composite, ana_label, n_quantiles=10, col_name="composite")
    univariate_quantile(ana_place3,    ana_label, n_quantiles=10, col_name="place3_prob")
    univariate_quantile(ana_score,     ana_label, n_quantiles=10, col_name="ana_score")
    univariate_quantile(ana_oddsDiv,   ana_label, n_quantiles=10, col_name="odds_divergence")

    # --- 多変量 ---
    print("\n── 多変量分析（ロジスティック回帰） ──")
    X_ana = np.column_stack([ana_composite, ana_place3, ana_score, ana_oddsDiv])
    y_ana = np.array(ana_label)
    feature_names_ana = ["composite", "place3_prob", "ana_score", "odds_divergence"]
    ana_weights = logistic_weights(X_ana, y_ana, feature_names_ana)

    overall_upset = np.mean(ana_label) * 100
    print(f"\n  [参考] 人気薄複勝率(全体): {overall_upset:.1f}%  サンプル数: {len(ana_rows):,}")

    # ════════════════════════════════════════════
    # ③ 推奨重み まとめ
    # ════════════════════════════════════════════
    print_separator("③ 推奨重み まとめ")

    print("\n▼ 軸馬度（複勝予測）推奨重み配分:")
    for name, wp in zip(feature_names_axis, axis_weights):
        bar = "█" * int(round(wp / 5))
        print(f"    {name:>18}: {wp:5.1f}%  {bar}")

    print("\n▼ 穴馬度（人気薄好走予測）推奨重み配分:")
    for name, wp in zip(feature_names_ana, ana_weights):
        bar = "█" * int(round(wp / 5))
        print(f"    {name:>18}: {wp:5.1f}%  {bar}")

    # ════════════════════════════════════════════
    # ④ 相関係数サマリ（クロスチェック用）
    # ════════════════════════════════════════════
    print_separator("④ 相関係数サマリ（単変量予測力クロスチェック）")

    print("\n▼ 軸馬度 各要素 vs 複勝ラベル のピアソン相関:")
    for name, vals in zip(
        ["composite", "place3_prob", "comp_rank(neg=好)"],
        [axis_composite, axis_place3, [-r for r in axis_rank]]
    ):
        r = np.corrcoef(vals, axis_label)[0, 1]
        print(f"    {name:>22}: r = {r:+.4f}")

    print("\n▼ 穴馬度 各要素 vs 人気薄複勝ラベル のピアソン相関:")
    for name, vals in zip(
        ["composite", "place3_prob", "ana_score", "odds_divergence"],
        [ana_composite, ana_place3, ana_score, ana_oddsDiv]
    ):
        r = np.corrcoef(vals, ana_label)[0, 1]
        print(f"    {name:>22}: r = {r:+.4f}")

    print("\n" + "=" * 65)
    print("  分析完了")
    print("=" * 65)


if __name__ == "__main__":
    main()
