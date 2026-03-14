"""
特選穴馬 特徴量分析スクリプト
確定オッズ15倍以上で3着以内に入った馬の特徴量を統計分析する。

使い方:
  python scripts/analyze_ana_features.py
"""

import io
import json
import os
import sys
import warnings
from pathlib import Path

# Windows stdout エンコーディング修正
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from sklearn.metrics import roc_auc_score

warnings.filterwarnings("ignore", category=RuntimeWarning)

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from config.settings import PREDICTIONS_DIR, RESULTS_DIR, DATA_DIR

try:
    from rich.console import Console
    from rich.table import Table
    console = Console()
    P = console.print
except ImportError:
    P = print

# ============================================================
# 定数
# ============================================================
ODDS_THRESHOLD = 15.0
PLACE_THRESHOLD = 3
OUTPUT_DIR = os.path.join(DATA_DIR, "analysis")

NUMERIC_FEATURES = [
    "composite", "ability_total", "ability_max", "ability_wa",
    "ability_alpha", "ability_class_adj", "ability_bloodline_adj",
    "pace_total", "pace_base", "pace_last3f_eval", "pace_position_balance",
    "pace_gate_bias", "pace_course_style_bias", "pace_jockey",
    "pace_estimated_pos4c", "pace_estimated_last3f",
    "course_total", "course_record", "course_venue_apt", "course_jockey",
    "win_prob", "place2_prob", "place3_prob",
    "popularity", "odds", "predicted_tansho_odds",
    "odds_divergence", "odds_consistency_adj",
    "ana_score", "shobu_score", "jockey_change_score", "kiken_score",
    "horse_weight", "weight_change", "gate_no", "weight_kg",
    "jockey_dev", "trainer_dev", "bloodline_dev",
    "confidence_score",
]

CATEGORICAL_FEATURES = [
    "running_style", "ability_trend", "ability_reliability",
    "ability_chakusa_pattern", "divergence_signal",
    "jockey_grade", "trainer_grade", "sire_grade", "mgs_grade",
    "mark", "ana_type",
]

RACE_FEATURES = [
    "surface", "distance", "field_count", "is_jra",
    "condition", "venue", "grade", "pace_predicted",
]


# ============================================================
# Phase 1: データ収集・結合
# ============================================================

def load_all_data() -> pd.DataFrame:
    """全日付の予想/結果JSONを結合してDataFrameを返す"""
    pred_files = sorted(Path(PREDICTIONS_DIR).glob("*_pred.json"))
    res_dir = Path(RESULTS_DIR)

    rows = []
    loaded = 0
    skipped = 0

    for pf in pred_files:
        date_key = pf.stem.replace("_pred", "")
        rf = res_dir / f"{date_key}_results.json"
        if not rf.exists():
            skipped += 1
            continue

        try:
            with open(pf, encoding="utf-8") as f:
                pred = json.load(f)
            with open(rf, encoding="utf-8") as f:
                res = json.load(f)
        except Exception:
            skipped += 1
            continue

        for race in pred.get("races", []):
            race_id = race.get("race_id", "")
            if race_id not in res:
                continue

            result_data = res[race_id]
            order = result_data.get("order", [])
            if not order:
                continue

            finish_map = {o["horse_no"]: o["finish"] for o in order if "horse_no" in o}
            actual_odds_map = {o["horse_no"]: o.get("odds") for o in order if "horse_no" in o}

            for horse in race.get("horses", []):
                hno = horse.get("horse_no")
                if hno is None or hno not in finish_map:
                    continue

                row = {
                    "date": date_key,
                    "race_id": race_id,
                    "horse_no": hno,
                    "horse_name": horse.get("horse_name", ""),
                    "finish": finish_map[hno],
                    "actual_odds": actual_odds_map.get(hno),
                }

                # レースレベル
                for rf_key in RACE_FEATURES:
                    row[rf_key] = race.get(rf_key)
                row["confidence_score"] = race.get("confidence_score", 0)

                # 馬レベル数値特徴量
                for nf in NUMERIC_FEATURES:
                    if nf in ("confidence_score",):
                        continue  # 既にレースレベルで取得済み
                    row[nf] = horse.get(nf)

                # カテゴリ特徴量
                for cf in CATEGORICAL_FEATURES:
                    row[cf] = horse.get(cf)

                # training_intensity → training_sigma
                ti = horse.get("training_intensity")
                if isinstance(ti, dict):
                    row["training_sigma"] = ti.get("sigma")
                else:
                    row["training_sigma"] = None

                rows.append(row)

        loaded += 1
        if loaded % 100 == 0:
            P(f"  ... {loaded}日読込完了")

    P(f"  読込完了: {loaded}日 (スキップ: {skipped}日)")
    return pd.DataFrame(rows)


# ============================================================
# Phase 2: ラベリング
# ============================================================

def label_ana_horses(df: pd.DataFrame) -> pd.DataFrame:
    """actual_odds >= ODDS_THRESHOLD の馬を抽出し hit/miss ラベル付与"""
    ana = df[df["actual_odds"] >= ODDS_THRESHOLD].copy()
    ana["hit"] = (ana["finish"] <= PLACE_THRESHOLD).astype(int)
    return ana


# ============================================================
# Phase 3A: 数値特徴量分析
# ============================================================

def analyze_numeric_features(ana_df: pd.DataFrame) -> pd.DataFrame:
    """数値特徴量のhit/miss比較 + 判別力ランキング"""
    hit = ana_df[ana_df["hit"] == 1]
    miss = ana_df[ana_df["hit"] == 0]

    all_features = NUMERIC_FEATURES + ["training_sigma"]
    records = []

    for feat in all_features:
        h_vals = hit[feat].dropna()
        m_vals = miss[feat].dropna()

        if len(h_vals) < 10 or len(m_vals) < 10:
            continue

        h_mean = h_vals.mean()
        m_mean = m_vals.mean()

        # Cohen's d
        pooled_std = np.sqrt((h_vals.std()**2 + m_vals.std()**2) / 2)
        cohens_d = (h_mean - m_mean) / pooled_std if pooled_std > 0 else 0

        # Mann-Whitney U検定
        try:
            u_stat, p_val = sp_stats.mannwhitneyu(h_vals, m_vals, alternative="two-sided")
        except Exception:
            p_val = 1.0

        # ROC-AUC
        try:
            combined = pd.concat([h_vals, m_vals])
            labels = np.array([1]*len(h_vals) + [0]*len(m_vals))
            auc = roc_auc_score(labels, combined.values)
            # AUCが0.5未満の場合は反転（逆方向の判別力）
            auc_adj = max(auc, 1 - auc)
        except Exception:
            auc_adj = 0.5

        records.append({
            "feature": feat,
            "hit_mean": round(h_mean, 4),
            "hit_median": round(h_vals.median(), 4),
            "miss_mean": round(m_mean, 4),
            "miss_median": round(m_vals.median(), 4),
            "hit_std": round(h_vals.std(), 4),
            "miss_std": round(m_vals.std(), 4),
            "hit_q25": round(h_vals.quantile(0.25), 4),
            "hit_q75": round(h_vals.quantile(0.75), 4),
            "cohens_d": round(cohens_d, 4),
            "p_value": round(p_val, 6),
            "auc": round(auc_adj, 4),
            "hit_n": len(h_vals),
            "miss_n": len(m_vals),
            "direction": "hit高" if h_mean > m_mean else "miss高",
        })

    result = pd.DataFrame(records).sort_values("auc", ascending=False)
    return result


# ============================================================
# Phase 3B: カテゴリ特徴量分析
# ============================================================

def analyze_categorical_features(ana_df: pd.DataFrame) -> pd.DataFrame:
    """カテゴリ特徴量の的中率クロス集計"""
    overall_hit_rate = ana_df["hit"].mean()
    records = []

    for feat in CATEGORICAL_FEATURES:
        vals = ana_df[feat].fillna("(N/A)")
        for val in sorted(vals.unique()):
            subset = ana_df[vals == val]
            n = len(subset)
            if n < 5:
                continue
            n_hit = subset["hit"].sum()
            hit_rate = n_hit / n
            lift = hit_rate / overall_hit_rate if overall_hit_rate > 0 else 0

            records.append({
                "feature": feat,
                "value": str(val),
                "count": n,
                "hit": n_hit,
                "hit_rate": round(hit_rate, 4),
                "lift": round(lift, 2),
            })

    return pd.DataFrame(records).sort_values(["feature", "lift"], ascending=[True, False])


# ============================================================
# Phase 4: パターン分析
# ============================================================

def _distance_band(d):
    if d is None:
        return "不明"
    if d <= 1400:
        return "短距離"
    if d <= 2000:
        return "中距離"
    return "長距離"

def _field_band(n):
    if n is None:
        return "不明"
    if n <= 8:
        return "少頭数"
    if n <= 14:
        return "中頭数"
    return "多頭数"

def analyze_condition_patterns(ana_df: pd.DataFrame) -> pd.DataFrame:
    """条件交差分析"""
    df = ana_df.copy()
    df["distance_band"] = df["distance"].apply(_distance_band)
    df["field_band"] = df["field_count"].apply(_field_band)
    df["org"] = df["is_jra"].apply(lambda x: "JRA" if x else "NAR")

    overall_hit_rate = df["hit"].mean()
    records = []

    # 条件交差パターン
    cross_keys = [
        ("surface", "distance_band"),
        ("org", "surface"),
        ("org", "distance_band"),
        ("field_band", "pace_predicted"),
        ("running_style", "pace_predicted"),
        ("surface", "condition"),
        ("org", "field_band"),
    ]

    for k1, k2 in cross_keys:
        for v1 in sorted(df[k1].dropna().unique()):
            for v2 in sorted(df[k2].dropna().unique()):
                subset = df[(df[k1] == v1) & (df[k2] == v2)]
                n = len(subset)
                if n < 10:
                    continue
                n_hit = subset["hit"].sum()
                hit_rate = n_hit / n
                lift = hit_rate / overall_hit_rate if overall_hit_rate > 0 else 0
                records.append({
                    "key1": k1, "val1": str(v1),
                    "key2": k2, "val2": str(v2),
                    "count": n, "hit": n_hit,
                    "hit_rate": round(hit_rate, 4),
                    "lift": round(lift, 2),
                })

    return pd.DataFrame(records).sort_values("lift", ascending=False)


def analyze_existing_ana_score(ana_df: pd.DataFrame) -> pd.DataFrame:
    """既存ana_scoreとの照合分析"""
    records = []

    # ana_scoreの帯別分析
    bins = [(-0.1, 0), (0, 3), (3, 5), (5, 7), (7, 10), (10, 100)]
    labels = ["0 (判定外)", "0-3 (低)", "3-5 (中)", "5-7 (穴B域)", "7-10 (穴A域)", "10+ (超高)"]

    for (lo, hi), label in zip(bins, labels):
        subset = ana_df[(ana_df["ana_score"] > lo) & (ana_df["ana_score"] <= hi)]
        n = len(subset)
        if n < 5:
            continue
        n_hit = subset["hit"].sum()
        hit_rate = n_hit / n if n > 0 else 0

        # 的中馬の平均特徴量
        hit_subset = subset[subset["hit"] == 1]
        avg_composite = hit_subset["composite"].mean() if len(hit_subset) > 0 else None
        avg_ability = hit_subset["ability_total"].mean() if len(hit_subset) > 0 else None
        avg_pace = hit_subset["pace_total"].mean() if len(hit_subset) > 0 else None

        records.append({
            "ana_score_band": label,
            "count": n,
            "hit": n_hit,
            "hit_rate": round(hit_rate, 4),
            "hit_avg_composite": round(avg_composite, 2) if avg_composite else None,
            "hit_avg_ability": round(avg_ability, 2) if avg_ability else None,
            "hit_avg_pace": round(avg_pace, 2) if avg_pace else None,
        })

    # ana_score=0 で的中した馬 vs ana_score>0で的中した馬の比較
    hit_df = ana_df[ana_df["hit"] == 1]
    picked = hit_df[hit_df["ana_score"] > 0]
    missed = hit_df[hit_df["ana_score"] == 0]
    records.append({
        "ana_score_band": "--- 的中馬のみ ---",
        "count": len(hit_df),
        "hit": len(hit_df),
        "hit_rate": 1.0,
        "hit_avg_composite": None,
        "hit_avg_ability": None,
        "hit_avg_pace": None,
    })
    records.append({
        "ana_score_band": "既存穴馬判定で拾えた的中馬",
        "count": len(picked),
        "hit": len(picked),
        "hit_rate": round(len(picked) / max(len(hit_df), 1), 4),
        "hit_avg_composite": round(picked["composite"].mean(), 2) if len(picked) > 0 else None,
        "hit_avg_ability": round(picked["ability_total"].mean(), 2) if len(picked) > 0 else None,
        "hit_avg_pace": round(picked["pace_total"].mean(), 2) if len(picked) > 0 else None,
    })
    records.append({
        "ana_score_band": "既存判定で拾えなかった的中馬(拾い漏れ)",
        "count": len(missed),
        "hit": len(missed),
        "hit_rate": round(len(missed) / max(len(hit_df), 1), 4),
        "hit_avg_composite": round(missed["composite"].mean(), 2) if len(missed) > 0 else None,
        "hit_avg_ability": round(missed["ability_total"].mean(), 2) if len(missed) > 0 else None,
        "hit_avg_pace": round(missed["pace_total"].mean(), 2) if len(missed) > 0 else None,
    })

    return pd.DataFrame(records)


# ============================================================
# Phase 5: 出力
# ============================================================

def print_summary(ana_df: pd.DataFrame, total_df: pd.DataFrame):
    """基本統計サマリー"""
    n_total = len(total_df)
    n_ana = len(ana_df)
    n_hit = ana_df["hit"].sum()
    n_miss = n_ana - n_hit
    hit_rate = n_hit / n_ana * 100 if n_ana > 0 else 0

    P(f"\n[bold]═══ 基本統計 ═══[/bold]")
    P(f"  全馬数: {n_total:,}")
    P(f"  オッズ>={ODDS_THRESHOLD}倍: {n_ana:,} ({n_ana/n_total*100:.1f}%)")
    P(f"  うち3着以内: {n_hit:,} (的中率: {hit_rate:.2f}%)")
    P(f"  ハズレ: {n_miss:,}")

    # JRA/NAR別
    for org, label in [(True, "JRA"), (False, "NAR")]:
        sub = ana_df[ana_df["is_jra"] == org]
        if len(sub) == 0:
            continue
        h = sub["hit"].sum()
        P(f"  {label}: {len(sub):,}頭 → 的中{h:,} ({h/len(sub)*100:.2f}%)")


def print_feature_ranking(feat_df: pd.DataFrame, top_n: int = 20):
    """判別力ランキング表示"""
    P(f"\n[bold]═══ 特徴量判別力 TOP{top_n} (AUC順) ═══[/bold]")
    try:
        table = Table(show_header=True)
        table.add_column("#", style="dim", width=3)
        table.add_column("特徴量", width=24)
        table.add_column("AUC", justify="right", width=6)
        table.add_column("Cohen's d", justify="right", width=9)
        table.add_column("方向", width=6)
        table.add_column("hit平均", justify="right", width=8)
        table.add_column("miss平均", justify="right", width=8)
        table.add_column("p値", justify="right", width=10)

        for i, (_, row) in enumerate(feat_df.head(top_n).iterrows()):
            star = "***" if row["p_value"] < 0.001 else ("**" if row["p_value"] < 0.01 else ("*" if row["p_value"] < 0.05 else ""))
            table.add_row(
                str(i+1),
                row["feature"],
                f"{row['auc']:.3f}",
                f"{row['cohens_d']:+.3f}",
                row["direction"],
                f"{row['hit_mean']:.3f}",
                f"{row['miss_mean']:.3f}",
                f"{row['p_value']:.1e}{star}",
            )
        console.print(table)
    except Exception:
        # Richなしフォールバック
        for i, (_, row) in enumerate(feat_df.head(top_n).iterrows()):
            P(f"  {i+1:2d}. {row['feature']:24s} AUC={row['auc']:.3f} d={row['cohens_d']:+.3f} {row['direction']} p={row['p_value']:.1e}")


def print_categorical_top(cat_df: pd.DataFrame, min_count: int = 20):
    """カテゴリ別的中率のハイライト"""
    P(f"\n[bold]═══ カテゴリ別的中率 (リフト値高順, N>={min_count}) ═══[/bold]")
    filtered = cat_df[cat_df["count"] >= min_count].sort_values("lift", ascending=False)
    try:
        table = Table(show_header=True)
        table.add_column("特徴量", width=22)
        table.add_column("値", width=20)
        table.add_column("N", justify="right", width=6)
        table.add_column("的中", justify="right", width=5)
        table.add_column("的中率", justify="right", width=7)
        table.add_column("リフト", justify="right", width=6)

        for _, row in filtered.head(25).iterrows():
            table.add_row(
                row["feature"], str(row["value"]),
                str(row["count"]), str(row["hit"]),
                f"{row['hit_rate']*100:.1f}%",
                f"{row['lift']:.2f}",
            )
        console.print(table)
    except Exception:
        for _, row in filtered.head(25).iterrows():
            P(f"  {row['feature']:22s} {str(row['value']):20s} N={row['count']:5d} 的中={row['hit']:4d} ({row['hit_rate']*100:.1f}%) lift={row['lift']:.2f}")


def print_ana_audit(audit_df: pd.DataFrame):
    """既存ana_score照合結果"""
    P(f"\n[bold]═══ 既存穴馬スコア照合 ═══[/bold]")
    try:
        table = Table(show_header=True)
        table.add_column("ana_scoreレンジ", width=34)
        table.add_column("N", justify="right", width=6)
        table.add_column("的中", justify="right", width=5)
        table.add_column("的中率", justify="right", width=7)
        table.add_column("的中馬composite", justify="right", width=14)

        for _, row in audit_df.iterrows():
            comp_str = f"{row['hit_avg_composite']:.1f}" if row['hit_avg_composite'] else "—"
            table.add_row(
                row["ana_score_band"],
                str(row["count"]),
                str(row["hit"]),
                f"{row['hit_rate']*100:.1f}%",
                comp_str,
            )
        console.print(table)
    except Exception:
        for _, row in audit_df.iterrows():
            P(f"  {row['ana_score_band']:34s} N={row['count']:5d} 的中={row['hit']:4d} ({row['hit_rate']*100:.1f}%)")


def print_pattern_top(pat_df: pd.DataFrame, top_n: int = 15):
    """条件パターン上位"""
    P(f"\n[bold]═══ 高的中率パターン TOP{top_n} ═══[/bold]")
    filtered = pat_df[pat_df["count"] >= 20].head(top_n)
    try:
        table = Table(show_header=True)
        table.add_column("条件1", width=14)
        table.add_column("値1", width=10)
        table.add_column("条件2", width=14)
        table.add_column("値2", width=10)
        table.add_column("N", justify="right", width=6)
        table.add_column("的中", justify="right", width=5)
        table.add_column("的中率", justify="right", width=7)
        table.add_column("リフト", justify="right", width=6)

        for _, row in filtered.iterrows():
            table.add_row(
                row["key1"], str(row["val1"]),
                row["key2"], str(row["val2"]),
                str(row["count"]), str(row["hit"]),
                f"{row['hit_rate']*100:.1f}%",
                f"{row['lift']:.2f}",
            )
        console.print(table)
    except Exception:
        for _, row in filtered.iterrows():
            P(f"  {row['key1']}={row['val1']} × {row['key2']}={row['val2']}  N={row['count']} 的中={row['hit']} ({row['hit_rate']*100:.1f}%) lift={row['lift']:.2f}")


def save_csvs(feat_df, cat_df, pat_df, audit_df, ana_df):
    """CSV出力"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    feat_df.to_csv(os.path.join(OUTPUT_DIR, "feature_importance.csv"), index=False, encoding="utf-8-sig")
    cat_df.to_csv(os.path.join(OUTPUT_DIR, "categorical_crosstab.csv"), index=False, encoding="utf-8-sig")
    pat_df.to_csv(os.path.join(OUTPUT_DIR, "condition_patterns.csv"), index=False, encoding="utf-8-sig")
    audit_df.to_csv(os.path.join(OUTPUT_DIR, "ana_score_audit.csv"), index=False, encoding="utf-8-sig")

    # 的中馬の全特徴量一覧
    hit_horses = ana_df[ana_df["hit"] == 1].sort_values("actual_odds", ascending=False)
    cols_out = ["date", "race_id", "venue", "horse_no", "horse_name",
                "actual_odds", "finish", "surface", "distance",
                "composite", "ability_total", "pace_total", "course_total",
                "win_prob", "place3_prob", "ana_score", "ana_type",
                "running_style", "ability_trend", "shobu_score",
                "jockey_grade", "trainer_grade", "sire_grade",
                "jockey_dev", "trainer_dev", "bloodline_dev"]
    existing = [c for c in cols_out if c in hit_horses.columns]
    hit_horses[existing].to_csv(
        os.path.join(OUTPUT_DIR, "hit_horses_detail.csv"),
        index=False, encoding="utf-8-sig",
    )

    P(f"\n  CSV出力先: {OUTPUT_DIR}/")
    P(f"    feature_importance.csv")
    P(f"    categorical_crosstab.csv")
    P(f"    condition_patterns.csv")
    P(f"    ana_score_audit.csv")
    P(f"    hit_horses_detail.csv ({len(hit_horses)}頭)")


# ============================================================
# main
# ============================================================

def main():
    P("[bold white on #0d2b5e]  特選穴馬 特徴量分析  [/]")
    P(f"  閾値: 確定オッズ>={ODDS_THRESHOLD}倍, 3着以内\n")

    # Phase 1
    P("[bold cyan]\\[1/5][/] データ収集・結合...")
    df = load_all_data()
    P(f"  全馬数: {len(df):,}  日数: {df['date'].nunique()}")

    # Phase 2
    P("[bold cyan]\\[2/5][/] 穴馬ラベリング...")
    ana_df = label_ana_horses(df)
    print_summary(ana_df, df)

    # Phase 3A
    P("[bold cyan]\\[3/5][/] 特徴量分析...")
    feat_df = analyze_numeric_features(ana_df)
    print_feature_ranking(feat_df)

    # Phase 3B
    cat_df = analyze_categorical_features(ana_df)
    print_categorical_top(cat_df)

    # Phase 4
    P("[bold cyan]\\[4/5][/] パターン分析...")
    pat_df = analyze_condition_patterns(ana_df)
    print_pattern_top(pat_df)

    audit_df = analyze_existing_ana_score(ana_df)
    print_ana_audit(audit_df)

    # Phase 5
    P("[bold cyan]\\[5/5][/] CSV出力...")
    save_csvs(feat_df, cat_df, pat_df, audit_df, ana_df)

    P("\n[bold green]完了[/]")


if __name__ == "__main__":
    main()
