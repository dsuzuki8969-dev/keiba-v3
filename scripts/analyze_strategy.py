# -*- coding: utf-8 -*-
"""戦略分析レポート — eval_all.csv から6種の分析を一括実行

① 弱点分析 (条件別ROI)
② 印精度検証
③ 自信度(confidence)校正
④ オッズ帯 × 印 期待値マップ
⑤ 月次収支推移
⑥ 戦略パラメータサーチ

使い方:
  python scripts/analyze_strategy.py
  python scripts/analyze_strategy.py --csv data/csv/eval_2026.csv
  python scripts/analyze_strategy.py --year 2026
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd
import numpy as np

# ── ユーティリティ ──

def _print_title(num, title):
    print()
    print(f"{'=' * 70}")
    print(f"  {num}  {title}")
    print(f"{'=' * 70}")


def _roi_stats(group):
    """単勝100円ベースのROI計算"""
    n = len(group)
    wins = (group["finish_pos"] == 1).sum()
    top2 = (group["finish_pos"] <= 2).sum()
    top3 = (group["finish_pos"] <= 3).sum()
    # 単勝回収: 勝った馬のresult_odds × 100
    payout = group.loc[group["finish_pos"] == 1, "result_odds"].sum() * 100
    stake = n * 100
    roi = payout / stake * 100 if stake > 0 else 0
    return pd.Series({
        "頭数": n,
        "勝率%": wins / n * 100 if n > 0 else 0,
        "連対率%": top2 / n * 100 if n > 0 else 0,
        "複勝率%": top3 / n * 100 if n > 0 else 0,
        "単勝ROI%": roi,
        "勝ち数": wins,
        "投資額": stake,
        "回収額": payout,
    })


# ── ① 弱点分析 ──

def analyze_weakpoints(df):
    _print_title("①", "弱点分析 — どこで負けてるか")

    # 対象: 印付き馬 (◎○▲△★☆)
    marked = df[df["mark"].isin(["◎", "○", "▲", "△", "★", "☆"])]

    # (a) JRA vs NAR
    print("\n【JRA / NAR】")
    grp = marked.groupby("is_jra").apply(_roi_stats, include_groups=False).round(1)
    grp.index = grp.index.map({True: "JRA", False: "NAR"})
    print(grp.to_string())

    # (b) 芝 vs ダート
    print("\n【芝 / ダート】")
    grp = marked.groupby("surface").apply(_roi_stats, include_groups=False).round(1)
    print(grp.to_string())

    # (c) 距離帯
    print("\n【距離帯】")
    bins = [0, 1200, 1600, 2000, 2400, 9999]
    labels = ["~1200", "1201-1600", "1601-2000", "2001-2400", "2401~"]
    marked = marked.copy()
    marked["距離帯"] = pd.cut(marked["distance"], bins=bins, labels=labels, right=True)
    grp = marked.groupby("距離帯", observed=True).apply(_roi_stats, include_groups=False).round(1)
    print(grp.to_string())

    # (d) グレード別
    print("\n【グレード別】(頭数50以上)")
    grp = marked.groupby("grade").apply(_roi_stats, include_groups=False).round(1)
    grp = grp[grp["頭数"] >= 50].sort_values("単勝ROI%", ascending=False)
    print(grp.to_string())

    # (e) 会場別 (上位/下位)
    print("\n【会場別 — 単勝ROI 上位10】")
    grp = marked.groupby("venue").apply(_roi_stats, include_groups=False).round(1)
    grp = grp[grp["頭数"] >= 30]
    print(grp.sort_values("単勝ROI%", ascending=False).head(10).to_string())

    print("\n【会場別 — 単勝ROI 下位10】")
    print(grp.sort_values("単勝ROI%", ascending=True).head(10).to_string())


# ── ② 印精度検証 ──

def analyze_mark_accuracy(df):
    _print_title("②", "印精度検証 — 印ごとの的中率・回収率")

    mark_order = ["◎", "○", "▲", "△", "★", "☆", "×", "-"]
    marks_present = [m for m in mark_order if m in df["mark"].values]

    grp = df.groupby("mark").apply(_roi_stats, include_groups=False).round(1)
    grp = grp.reindex([m for m in marks_present if m in grp.index])
    print(grp.to_string())

    # ◎の年別推移
    print("\n【◎ の年別推移】")
    honmei = df[df["mark"] == "◎"].copy()
    honmei["year"] = honmei["date"].str[:4]
    grp = honmei.groupby("year").apply(_roi_stats, include_groups=False).round(1)
    print(grp.to_string())


# ── ③ 自信度校正 ──

def analyze_confidence(df):
    _print_title("③", "自信度(confidence)校正 — SS/S/A/B/C/D の精度")

    # 全馬ベース
    print("\n【全馬ベース — 自信度別】")
    conf_order = ["SS", "S", "A", "B", "C", "D"]
    grp = df.groupby("confidence").apply(_roi_stats, include_groups=False).round(1)
    ordered = [c for c in conf_order if c in grp.index]
    grp = grp.reindex(ordered)
    print(grp.to_string())

    # ◎のみ × 自信度
    print("\n【◎限定 — 自信度別】")
    honmei = df[df["mark"] == "◎"]
    grp = honmei.groupby("confidence").apply(_roi_stats, include_groups=False).round(1)
    ordered = [c for c in conf_order if c in grp.index]
    grp = grp.reindex(ordered)
    print(grp.to_string())


# ── ④ オッズ帯 × 印 期待値マップ ──

def analyze_odds_map(df):
    _print_title("④", "オッズ帯 × 印 — 期待値マップ")

    target = df[df["mark"].isin(["◎", "○", "▲", "△", "★", "☆"])].copy()
    bins = [0, 3, 5, 10, 20, 30, 50, 100, 9999]
    labels = ["~3.0", "3.1-5.0", "5.1-10", "10.1-20", "20.1-30", "30.1-50", "50.1-100", "100~"]
    target["オッズ帯"] = pd.cut(target["odds"], bins=bins, labels=labels, right=True)

    # ピボット: 単勝ROI
    print("\n【単勝ROI% — オッズ帯 × 印】")
    pivot = target.groupby(["オッズ帯", "mark"]).apply(
        lambda g: round(g.loc[g["finish_pos"] == 1, "result_odds"].sum() * 100 / (len(g) * 100) * 100, 1) if len(g) > 0 else 0,
        include_groups=False,
    ).unstack(fill_value=0)
    mark_cols = [m for m in ["◎", "○", "▲", "△", "★", "☆"] if m in pivot.columns]
    pivot = pivot[mark_cols]
    print(pivot.to_string())

    # ピボット: 頭数
    print("\n【頭数 — オッズ帯 × 印】")
    pivot_n = target.groupby(["オッズ帯", "mark"]).size().unstack(fill_value=0)
    pivot_n = pivot_n[[m for m in mark_cols if m in pivot_n.columns]]
    print(pivot_n.to_string())

    # 美味しいゾーン
    print("\n【美味しいゾーン (ROI 100%超 & 頭数30以上)】")
    for odds_band in labels:
        for mark in mark_cols:
            try:
                roi = pivot.loc[odds_band, mark]
                n = pivot_n.loc[odds_band, mark]
                if roi > 100 and n >= 30:
                    print(f"  {mark} × {odds_band}倍: ROI {roi:.1f}% ({n}頭)")
            except (KeyError, TypeError):
                pass


# ── ⑤ 月次収支推移 ──

def analyze_monthly_trend(df):
    _print_title("⑤", "月次収支推移 — モデル劣化・改善の監視")

    marked = df[df["mark"].isin(["◎", "○", "▲", "△", "★", "☆"])].copy()
    marked["year_month"] = marked["date"].str[:7]  # YYYY-MM or YYYYMM

    # date形式が YYYYMMDD の場合
    if marked["year_month"].str.contains("-").sum() == 0:
        marked["year_month"] = marked["date"].str[:4] + "-" + marked["date"].str[4:6]

    grp = marked.groupby("year_month").apply(_roi_stats, include_groups=False).round(1)
    grp["累計収支"] = (grp["回収額"] - grp["投資額"]).cumsum()

    print("\n【月次 — 印付き馬の単勝成績】")
    display_cols = ["頭数", "勝ち数", "勝率%", "単勝ROI%", "投資額", "回収額", "累計収支"]
    print(grp[display_cols].to_string())

    # ◎だけの月次
    print("\n【月次 — ◎のみ】")
    honmei = df[df["mark"] == "◎"].copy()
    honmei["year_month"] = honmei["date"].str[:4] + "-" + honmei["date"].str[4:6] \
        if honmei["date"].str.contains("-").sum() == 0 \
        else honmei["date"].str[:7]
    grp_h = honmei.groupby("year_month").apply(_roi_stats, include_groups=False).round(1)
    grp_h["累計収支"] = (grp_h["回収額"] - grp_h["投資額"]).cumsum()
    print(grp_h[display_cols].to_string())


# ── ⑥ 戦略パラメータサーチ ──

def analyze_parameter_search(df):
    _print_title("⑥", "戦略パラメータサーチ — 閾値変更の影響シミュレーション")

    has_result = df[df["finish_pos"].notna() & (df["finish_pos"] > 0)].copy()

    # (a) composite 閾値別: 「この値以上の馬だけ◎にしたら」
    print("\n【composite 閾値別 — 単勝ROI (この値以上の馬を買った場合)】")
    thresholds = [50, 55, 58, 60, 62, 65, 68, 70, 75, 80]
    rows = []
    for th in thresholds:
        subset = has_result[has_result["composite"] >= th]
        if len(subset) == 0:
            continue
        wins = (subset["finish_pos"] == 1).sum()
        payout = subset.loc[subset["finish_pos"] == 1, "result_odds"].sum() * 100
        stake = len(subset) * 100
        rows.append({
            "composite≧": th,
            "頭数": len(subset),
            "勝ち数": wins,
            "勝率%": round(wins / len(subset) * 100, 1),
            "単勝ROI%": round(payout / stake * 100, 1) if stake > 0 else 0,
        })
    print(pd.DataFrame(rows).set_index("composite≧").to_string())

    # (b) win_prob 閾値別
    print("\n【win_prob 閾値別 — 単勝ROI】")
    wp_thresholds = [0.03, 0.05, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25, 0.30]
    rows = []
    for th in wp_thresholds:
        subset = has_result[has_result["win_prob"] >= th]
        if len(subset) == 0:
            continue
        wins = (subset["finish_pos"] == 1).sum()
        payout = subset.loc[subset["finish_pos"] == 1, "result_odds"].sum() * 100
        stake = len(subset) * 100
        rows.append({
            "win_prob≧": th,
            "頭数": len(subset),
            "勝ち数": wins,
            "勝率%": round(wins / len(subset) * 100, 1),
            "単勝ROI%": round(payout / stake * 100, 1) if stake > 0 else 0,
        })
    print(pd.DataFrame(rows).set_index("win_prob≧").to_string())

    # (c) composite × odds 組み合わせ最適化
    print("\n【composite × オッズ上限 — 最適組み合わせ TOP15】")
    combos = []
    for comp_th in [55, 58, 60, 62, 65, 68, 70]:
        for odds_max in [5, 10, 15, 20, 30, 50, 999]:
            subset = has_result[
                (has_result["composite"] >= comp_th) &
                (has_result["odds"] <= odds_max)
            ]
            if len(subset) < 50:  # 最低50頭
                continue
            wins = (subset["finish_pos"] == 1).sum()
            payout = subset.loc[subset["finish_pos"] == 1, "result_odds"].sum() * 100
            stake = len(subset) * 100
            roi = payout / stake * 100 if stake > 0 else 0
            combos.append({
                "composite≧": comp_th,
                "odds≦": odds_max,
                "頭数": len(subset),
                "勝率%": round(wins / len(subset) * 100, 1),
                "単勝ROI%": round(roi, 1),
                "収支": int(payout - stake),
            })
    combos_df = pd.DataFrame(combos).sort_values("単勝ROI%", ascending=False).head(15)
    print(combos_df.to_string(index=False))

    # (d) EV (期待値) 閾値別
    print("\n【EV(期待値) 閾値別 — 単勝ROI】")
    ev_thresholds = [0.5, 0.8, 1.0, 1.2, 1.5, 2.0, 2.5, 3.0]
    rows = []
    ev_valid = has_result[has_result["ev"].notna() & (has_result["ev"] > 0)]
    for th in ev_thresholds:
        subset = ev_valid[ev_valid["ev"] >= th]
        if len(subset) < 20:
            continue
        wins = (subset["finish_pos"] == 1).sum()
        payout = subset.loc[subset["finish_pos"] == 1, "result_odds"].sum() * 100
        stake = len(subset) * 100
        rows.append({
            "EV≧": th,
            "頭数": len(subset),
            "勝ち数": wins,
            "勝率%": round(wins / len(subset) * 100, 1),
            "単勝ROI%": round(payout / stake * 100, 1) if stake > 0 else 0,
        })
    if rows:
        print(pd.DataFrame(rows).set_index("EV≧").to_string())
    else:
        print("  (EVデータが不十分)")


# ── メイン ──

def main():
    parser = argparse.ArgumentParser(description="戦略分析レポート")
    parser.add_argument("--csv", type=str, default="data/csv/eval_all.csv", help="入力CSV")
    parser.add_argument("--year", type=str, help="年フィルタ (例: 2026)")
    parser.add_argument("--output", type=str, help="結果をテキストファイルに保存")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"CSVが見つかりません: {csv_path}")
        print("先に python scripts/export_eval_csv.py --all を実行してください")
        return

    print(f"読み込み中: {csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig", low_memory=False)
    print(f"  {len(df):,}行 × {len(df.columns)}列")

    # 年フィルタ
    if args.year:
        df["date"] = df["date"].astype(str)
        df = df[df["date"].str.startswith(args.year)]
        print(f"  → {args.year}年フィルタ: {len(df):,}行")

    # 型変換
    numeric_cols = [
        "finish_pos", "composite", "ability_total", "pace_total", "course_total",
        "race_relative_dev", "hybrid_total", "win_prob", "place2_prob", "place3_prob",
        "ml_win_prob", "odds", "result_odds", "ev", "distance", "field_count",
        "jockey_dev", "trainer_dev", "bloodline_dev", "training_dev",
        "payout_tansho", "payout_fukusho", "payout_sanrenpuku", "payout_sanrentan",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 取消馬を除外
    df = df[df["is_scratched"] != True]
    df = df[df["is_scratched"] != "True"]

    # 着順ありデータのサマリ
    has_result = df[df["finish_pos"].notna() & (df["finish_pos"] > 0)]
    no_result = df[df["finish_pos"].isna() | (df["finish_pos"] == 0)]
    print(f"  着順あり: {len(has_result):,}行 / 着順なし: {len(no_result):,}行")

    # 着順ありのみで分析
    df_valid = has_result.copy()

    # date を文字列に統一
    df_valid["date"] = df_valid["date"].astype(str)

    # mark の欠損を "-" に
    df_valid["mark"] = df_valid["mark"].fillna("-")

    print(f"\n分析対象: {len(df_valid):,}行")

    # ── 6分析実行 ──
    analyze_weakpoints(df_valid)
    analyze_mark_accuracy(df_valid)
    analyze_confidence(df_valid)
    analyze_odds_map(df_valid)
    analyze_monthly_trend(df_valid)
    analyze_parameter_search(df_valid)

    # サマリ
    print()
    print(f"{'=' * 70}")
    print(f"  分析完了: {len(df_valid):,}行 (全{len(df):,}行中)")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
