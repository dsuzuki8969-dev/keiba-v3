#!/usr/bin/env python3
"""
展開予想（通過順位予測）の精度検証スクリプト

予測値(data/predictions/*.json) と 実績値(data/ml/*.json) を突き合わせ、
通過順位予測の精度を多角的に評価する。

指標:
  1. 逃げ予想一致率（leading_horses vs 初角1番手）
  2. 4角通過順位精度（pace_estimated_pos4c vs positions_corners[-1]）
  3. 脚質グループ一致率（leading/front/mid/rear の4分類）
  4. 1角通過順位精度（estimated_pos_1c vs positions_corners[0]）

分析軸:
  - JRA/NAR別 / 芝/ダート別 / 距離帯別 / 頭数帯別
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Windows環境でのUTF-8出力対応
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif os.name == "nt":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# プロジェクトルート
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console(width=120)

PRED_DIR = ROOT / "data" / "predictions"
ML_DIR = ROOT / "data" / "ml"


# ============================================================
# ユーティリティ
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="展開予想（通過順位）精度検証")
    parser.add_argument("--start", default="2026-01-01", help="開始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-03-22", help="終了日 (YYYY-MM-DD)")
    return parser.parse_args()


def date_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return dates


def load_json(path):
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def distance_band(dist):
    if dist <= 1400:
        return "sprint"
    elif dist <= 1600:
        return "mile"
    elif dist <= 2200:
        return "middle"
    else:
        return "long"


def field_band(fc):
    if fc <= 8:
        return "少頭数(8以下)"
    elif fc <= 14:
        return "中頭数(9-14)"
    else:
        return "多頭数(15以上)"


def spearman_corr(x, y):
    """スピアマン順位相関係数"""
    n = len(x)
    if n < 3:
        return None

    def ranks(vals):
        indexed = sorted(enumerate(vals), key=lambda t: t[1])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                r[indexed[k][0]] = avg_rank
            i = j + 1
        return r

    rx = ranks(x)
    ry = ranks(y)
    d_sq = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return round(1 - 6 * d_sq / (n * (n * n - 1)), 4)


def classify_actual_group(pos, field_count):
    """実績通過順位 -> 脚質グループ分類"""
    ratio = pos / max(field_count, 1)
    if ratio <= 0.15:
        return "leading"
    elif ratio <= 0.40:
        return "front"
    elif ratio <= 0.75:
        return "mid"
    else:
        return "rear"


def pct_str(n, total):
    """パーセント文字列"""
    if total == 0:
        return "-"
    return f"{n/total*100:.1f}%"


def color_pct(val, thresholds=(30, 50, 70)):
    """パーセント値に色付け"""
    low, mid, high = thresholds
    if val >= high:
        return "bold green"
    elif val >= mid:
        return "green"
    elif val >= low:
        return "yellow"
    else:
        return "red"


def color_corr(val):
    """相関係数に色付け"""
    if val >= 0.8:
        return "bold green"
    elif val >= 0.6:
        return "green"
    elif val >= 0.4:
        return "yellow"
    else:
        return "red"


def make_bar(pct, width=30):
    """テキストバー生成"""
    filled = int(pct / 100 * width)
    return "█" * filled + "░" * (width - filled)


# ============================================================
# データ収集
# ============================================================
def collect_data(dates):
    """予測と実績のペアデータを収集"""
    horse_records = []
    race_records = []

    for date_str in dates:
        pred_path = PRED_DIR / f"{date_str}_pred.json"
        ml_path = ML_DIR / f"{date_str}.json"

        pred_data = load_json(pred_path)
        ml_data = load_json(ml_path)
        if not pred_data or not ml_data:
            continue

        ml_races = {r["race_id"]: r for r in ml_data.get("races", [])}

        for pred_race in pred_data.get("races", []):
            race_id = pred_race.get("race_id", "")
            ml_race = ml_races.get(race_id)
            if not ml_race:
                continue

            is_jra = ml_race.get("is_jra", False)
            surface = ml_race.get("surface", "")
            distance = ml_race.get("distance", 0)
            field_count = ml_race.get("field_count", 0)

            # 実績馬（取消・除外馬は除外）
            ml_horses = {}
            for h in ml_race.get("horses", []):
                fp = h.get("finish_pos")
                if fp is None or (isinstance(fp, int) and fp >= 90):
                    continue
                corners = h.get("positions_corners", [])
                if not corners:
                    continue
                ml_horses[h["horse_no"]] = h

            if not ml_horses:
                continue

            # 予測グループ
            leading = set(pred_race.get("leading_horses", []))
            front = set(pred_race.get("front_horses", []))
            mid_set = set(pred_race.get("mid_horses", []))
            rear = set(pred_race.get("rear_horses", []))

            def pred_group(hno):
                if hno in leading:
                    return "leading"
                elif hno in front:
                    return "front"
                elif hno in mid_set:
                    return "mid"
                elif hno in rear:
                    return "rear"
                return None

            # 実績corners長判定
            corner_count = pred_race.get("corner_count", 0)
            sample_corners_len = max(
                (len(mh["positions_corners"]) for mh in ml_horses.values()), default=0
            )

            # 逃げ判定
            actual_leader_hnos = []
            if sample_corners_len >= 2:
                for hno, mh in ml_horses.items():
                    c = mh["positions_corners"]
                    if len(c) >= 2 and c[0] == 1:
                        actual_leader_hnos.append(hno)
            else:
                for hno, mh in ml_horses.items():
                    c = mh["positions_corners"]
                    if c and c[0] == 1:
                        actual_leader_hnos.append(hno)

            # 実際の逃げ馬が予測でどのグループだったか
            actual_leader_pred_groups = []
            for hno in actual_leader_hnos:
                pg = pred_group(hno)
                actual_leader_pred_groups.append(pg)

            race_rec = {
                "race_id": race_id,
                "is_jra": is_jra,
                "surface": surface,
                "distance": distance,
                "dist_band": distance_band(distance),
                "field_count": field_count,
                "field_band": field_band(field_count),
                "corner_count": corner_count,
                "corners_len": sample_corners_len,
                "leading_predicted": list(leading),
                "front_predicted": list(front),
                "actual_leader_hnos": actual_leader_hnos,
                "actual_leader_pred_groups": actual_leader_pred_groups,
                "has_first_corner": True,
            }
            race_records.append(race_rec)

            # 馬単位
            pred_horses = {h["horse_no"]: h for h in pred_race.get("horses", [])}

            # 実績上がり3F順位を計算
            actual_l3f_valid = {
                h["horse_no"]: h["last_3f_sec"]
                for h in ml_race.get("horses", [])
                if h.get("last_3f_sec") and h["last_3f_sec"] > 0
                and h.get("finish_pos") is not None
                and not (isinstance(h.get("finish_pos"), int) and h["finish_pos"] >= 90)
            }
            actual_l3f_sorted = sorted(actual_l3f_valid.items(), key=lambda x: x[1])
            actual_l3f_rank = {hno: rank + 1 for rank, (hno, _) in enumerate(actual_l3f_sorted)}

            for hno, mh in ml_horses.items():
                ph = pred_horses.get(hno)
                if not ph:
                    continue
                corners = mh["positions_corners"]
                hrec = {
                    "race_id": race_id,
                    "horse_no": hno,
                    "is_jra": is_jra,
                    "surface": surface,
                    "distance": distance,
                    "dist_band": distance_band(distance),
                    "field_count": field_count,
                    "field_band": field_band(field_count),
                    "corner_count": corner_count,
                    "corners_len": sample_corners_len,
                    "est_pos4c": ph.get("pace_estimated_pos4c"),
                    "est_pos1c": ph.get("estimated_pos_1c"),
                    "pred_group": pred_group(hno),
                    "actual_last_corner": corners[-1],
                    "actual_first_corner": corners[0] if len(corners) >= 2 else None,
                    "actual_group": classify_actual_group(corners[-1], field_count),
                    "finish_pos": mh.get("finish_pos"),
                    "running_style": ph.get("running_style"),
                    # 上がり3F
                    "est_last3f_rank": ph.get("estimated_last3f_rank"),
                    "est_last3f_sec": ph.get("pace_estimated_last3f"),
                    "actual_last3f_sec": mh.get("last_3f_sec"),
                    "actual_last3f_rank": actual_l3f_rank.get(hno),
                }
                horse_records.append(hrec)

    return horse_records, race_records


# ============================================================
# 分析軸の定義
# ============================================================
AXES = [
    ("JRA/NAR", lambda r: "JRA" if r["is_jra"] else "NAR"),
    ("芝/ダート", lambda r: r["surface"]),
    ("距離帯", lambda r: r["dist_band"]),
    ("頭数帯", lambda r: r["field_band"]),
]


# ============================================================
# 指標1: 逃げ予想一致率
# ============================================================
def analyze_leading(race_records):
    valid = [r for r in race_records if r["has_first_corner"] and r["leading_predicted"]]
    if not valid:
        console.print("[red]逃げ予想データなし[/red]")
        return

    total = len(valid)
    exact = sum(1 for r in valid if set(r["leading_predicted"]) & set(r["actual_leader_hnos"]))
    # 逃げor先行に含まれていた（広義一致）
    broad = sum(
        1 for r in valid
        if set(r["leading_predicted"] + r["front_predicted"]) & set(r["actual_leader_hnos"])
    )

    # ヘッダーパネル
    console.print()
    console.rule("[bold cyan]【1】逃げ予想一致率[/bold cyan]", style="cyan")
    console.print(f"  対象: [bold]{total}[/bold] レース\n")

    # メイン指標
    t1 = Table(title="逃げ予想の的中", show_header=True, header_style="bold", padding=(0, 2))
    t1.add_column("指標", style="bold")
    t1.add_column("的中", justify="right")
    t1.add_column("対象", justify="right")
    t1.add_column("的中率", justify="right")
    t1.add_column("", min_width=30)

    exact_pct = exact / total * 100
    broad_pct = broad / total * 100
    t1.add_row(
        "逃げ馬的中 (初角1番手)",
        str(exact), str(total),
        Text(f"{exact_pct:.1f}%", style=color_pct(exact_pct, (40, 50, 60))),
        make_bar(exact_pct),
    )
    t1.add_row(
        "逃げor先行に含有",
        str(broad), str(total),
        Text(f"{broad_pct:.1f}%", style=color_pct(broad_pct, (50, 65, 80))),
        make_bar(broad_pct),
    )
    console.print(t1)

    # 実際の逃げ馬が予測でどのグループにいたか
    group_dist = defaultdict(int)
    for r in valid:
        for pg in r["actual_leader_pred_groups"]:
            group_dist[pg or "分類外"] += 1
    total_leaders = sum(group_dist.values())

    if total_leaders > 0:
        console.print()
        t_dist = Table(title="実際の逃げ馬が予測ではどのグループだったか", show_header=True, header_style="bold", padding=(0, 2))
        t_dist.add_column("予測グループ", style="bold")
        t_dist.add_column("件数", justify="right")
        t_dist.add_column("割合", justify="right")
        t_dist.add_column("", min_width=30)

        group_ja = {"leading": "逃げ(的中)", "front": "先行", "mid": "中団", "rear": "後方", "分類外": "分類外"}
        for g in ["leading", "front", "mid", "rear", "分類外"]:
            cnt = group_dist.get(g, 0)
            if cnt == 0:
                continue
            p = cnt / total_leaders * 100
            style = "bold green" if g == "leading" else ("yellow" if g == "front" else "red")
            t_dist.add_row(group_ja.get(g, g), str(cnt), Text(f"{p:.1f}%", style=style), make_bar(p))
        console.print(t_dist)

    # 分析軸
    console.print()
    t_axis = Table(title="逃げ予想一致率 - 分析軸別", show_header=True, header_style="bold", padding=(0, 1))
    t_axis.add_column("分析軸", style="bold")
    t_axis.add_column("カテゴリ", style="bold")
    t_axis.add_column("的中", justify="right")
    t_axis.add_column("対象", justify="right")
    t_axis.add_column("的中率", justify="right")
    t_axis.add_column("", min_width=25)

    for axis_name, axis_fn in AXES:
        groups = defaultdict(list)
        for r in valid:
            groups[axis_fn(r)].append(r)
        for g in sorted(groups.keys()):
            recs = groups[g]
            m = sum(1 for r in recs if set(r["leading_predicted"]) & set(r["actual_leader_hnos"]))
            t = len(recs)
            p = m / t * 100
            t_axis.add_row(
                axis_name, g, str(m), str(t),
                Text(f"{p:.1f}%", style=color_pct(p, (40, 50, 60))),
                make_bar(p),
            )
        t_axis.add_section()

    console.print(t_axis)


# ============================================================
# 指標2: 4角通過順位の精度
# ============================================================
def analyze_pos4c(horse_records):
    valid = [r for r in horse_records if r["est_pos4c"] is not None and r["est_pos4c"] > 0]
    if not valid:
        console.print("[red]4角通過順位データなし[/red]")
        return

    console.print()
    console.rule("[bold cyan]【2】4角通過順位予測精度[/bold cyan]", style="cyan")
    console.print(f"  対象: [bold]{len(valid)}[/bold] 頭\n")

    _print_pos_detail(valid, "est_pos4c", "actual_last_corner", "4角通過")


def analyze_pos1c(horse_records):
    valid = [r for r in horse_records
             if r["est_pos1c"] is not None and r["est_pos1c"] > 0
             and r["actual_first_corner"] is not None
             and r["corners_len"] >= 2]
    if not valid:
        console.print("[red]1角通過順位データなし[/red]")
        return

    console.print()
    console.rule("[bold cyan]【4】1角通過順位予測精度[/bold cyan]", style="cyan")
    console.print(f"  対象: [bold]{len(valid)}[/bold] 頭 (コーナー2以上のレースのみ)\n")

    _print_pos_detail(valid, "est_pos1c", "actual_first_corner", "1角通過")


def _print_pos_detail(valid, pred_key, actual_key, label):
    """通過順位の詳細分析（共通処理）"""
    errors = [r[pred_key] - r[actual_key] for r in valid]
    abs_errors = [abs(e) for e in errors]
    n = len(valid)

    mae = sum(abs_errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    mean_err = sum(errors) / n
    median_err = sorted(abs_errors)[n // 2]

    exact = sum(1 for e in abs_errors if e < 0.5) / n * 100
    w1 = sum(1 for e in abs_errors if e <= 1.0) / n * 100
    w2 = sum(1 for e in abs_errors if e <= 2.0) / n * 100
    w3 = sum(1 for e in abs_errors if e <= 3.0) / n * 100

    # メイン指標テーブル
    t1 = Table(title=f"{label} - 基本指標", show_header=True, header_style="bold", padding=(0, 2))
    t1.add_column("指標", style="bold")
    t1.add_column("値", justify="right")
    t1.add_column("評価")

    bias = "予測が後方寄り" if mean_err > 0.3 else ("予測が前方寄り" if mean_err < -0.3 else "ほぼ中立")
    t1.add_row("MAE (平均絶対誤差)", f"{mae:.2f} 番手", "小さいほど良い")
    t1.add_row("RMSE", f"{rmse:.2f} 番手", "外れ値に敏感")
    t1.add_row("中央絶対誤差", f"{median_err:.1f} 番手", "外れ値に頑健")
    t1.add_row("平均誤差 (バイアス)", f"{mean_err:+.2f} 番手", bias)
    console.print(t1)

    # 一致率テーブル
    console.print()
    t2 = Table(title=f"{label} - 許容誤差別の一致率", show_header=True, header_style="bold", padding=(0, 2))
    t2.add_column("許容誤差", style="bold")
    t2.add_column("一致率", justify="right")
    t2.add_column("件数", justify="right")
    t2.add_column("", min_width=35)

    for lbl, pct, cnt in [
        ("完全一致 (+-0.5)", exact, sum(1 for e in abs_errors if e < 0.5)),
        ("+-1 番手以内", w1, sum(1 for e in abs_errors if e <= 1.0)),
        ("+-2 番手以内", w2, sum(1 for e in abs_errors if e <= 2.0)),
        ("+-3 番手以内", w3, sum(1 for e in abs_errors if e <= 3.0)),
    ]:
        t2.add_row(
            lbl, Text(f"{pct:.1f}%", style=color_pct(pct, (25, 45, 65))),
            str(cnt), make_bar(pct),
        )
    console.print(t2)

    # 誤差分布ヒストグラム
    console.print()
    bins_def = [
        ("0 (完全一致)", 0, 0.5),
        ("1 番手差", 0.5, 1.5),
        ("2 番手差", 1.5, 2.5),
        ("3 番手差", 2.5, 3.5),
        ("4 番手差", 3.5, 4.5),
        ("5+ 番手差", 4.5, 999),
    ]
    t_hist = Table(title=f"{label} - 誤差分布", show_header=True, header_style="bold", padding=(0, 1))
    t_hist.add_column("誤差", style="bold", min_width=14)
    t_hist.add_column("件数", justify="right")
    t_hist.add_column("割合", justify="right")
    t_hist.add_column("累積", justify="right")
    t_hist.add_column("分布", min_width=35)

    cum = 0
    for lbl, lo, hi in bins_def:
        cnt = sum(1 for e in abs_errors if lo <= e < hi)
        pct = cnt / n * 100
        cum += pct
        bar = "█" * int(pct / 2) + "░" * max(0, 20 - int(pct / 2))
        t_hist.add_row(lbl, str(cnt), f"{pct:.1f}%", f"{cum:.1f}%", bar + f" {pct:.1f}%")
    console.print(t_hist)

    # スピアマン相関
    race_corrs = defaultdict(lambda: {"pred": [], "actual": []})
    for r in valid:
        race_corrs[r["race_id"]]["pred"].append(r[pred_key])
        race_corrs[r["race_id"]]["actual"].append(r[actual_key])

    corrs = []
    for rid, data in race_corrs.items():
        c = spearman_corr(data["pred"], data["actual"])
        if c is not None:
            corrs.append(c)

    if corrs:
        avg_corr = sum(corrs) / len(corrs)
        med_corr = sorted(corrs)[len(corrs) // 2]

        console.print()
        t_sp = Table(title=f"{label} - レース内順序相関 (スピアマン)", show_header=True, header_style="bold", padding=(0, 1))
        t_sp.add_column("指標", style="bold")
        t_sp.add_column("値", justify="right")

        t_sp.add_row("平均相関係数", Text(f"{avg_corr:.3f}", style=color_corr(avg_corr)))
        t_sp.add_row("中央値", Text(f"{med_corr:.3f}", style=color_corr(med_corr)))
        t_sp.add_row("対象レース数", f"{len(corrs)} R")
        console.print(t_sp)

        # 相関分布
        console.print()
        bins_corr = [
            ("<0.0 (逆相関)", -1, 0),
            ("0.0-0.3 (弱)", 0, 0.3),
            ("0.3-0.5 (やや弱)", 0.3, 0.5),
            ("0.5-0.7 (中)", 0.5, 0.7),
            ("0.7-0.9 (強)", 0.7, 0.9),
            ("0.9-1.0 (非常に強)", 0.9, 1.01),
        ]
        t_cd = Table(title="相関係数の分布", show_header=True, header_style="bold", padding=(0, 1))
        t_cd.add_column("レンジ", style="bold", min_width=20)
        t_cd.add_column("件数", justify="right")
        t_cd.add_column("割合", justify="right")
        t_cd.add_column("分布", min_width=35)

        for lbl, lo, hi in bins_corr:
            cnt = sum(1 for c in corrs if lo <= c < hi)
            pct = cnt / len(corrs) * 100
            bar = "█" * int(pct / 2) + "░" * max(0, 20 - int(pct / 2))
            style = "green" if lo >= 0.5 else ("yellow" if lo >= 0.3 else "red")
            t_cd.add_row(lbl, str(cnt), Text(f"{pct:.1f}%", style=style), bar)
        console.print(t_cd)

    # 分析軸テーブル
    console.print()
    t_axis = Table(
        title=f"{label} - 分析軸別の精度",
        show_header=True, header_style="bold", padding=(0, 1),
    )
    t_axis.add_column("分析軸", style="bold")
    t_axis.add_column("カテゴリ", style="bold")
    t_axis.add_column("n", justify="right")
    t_axis.add_column("MAE", justify="right")
    t_axis.add_column("RMSE", justify="right")
    t_axis.add_column("+-1", justify="right")
    t_axis.add_column("+-2", justify="right")
    t_axis.add_column("+-3", justify="right")
    t_axis.add_column("相関", justify="right")
    t_axis.add_column("バイアス", justify="right")

    for axis_name, axis_fn in AXES:
        groups = defaultdict(list)
        for r in valid:
            groups[axis_fn(r)].append(r)

        for g in sorted(groups.keys()):
            recs = groups[g]
            errs = [r[pred_key] - r[actual_key] for r in recs]
            ae = [abs(e) for e in errs]
            t = len(ae)
            m = sum(ae) / t
            rm = math.sqrt(sum(e * e for e in errs) / t)
            wi1 = sum(1 for e in ae if e <= 1.0) / t * 100
            wi2 = sum(1 for e in ae if e <= 2.0) / t * 100
            wi3 = sum(1 for e in ae if e <= 3.0) / t * 100
            me = sum(errs) / t

            # スピアマン
            rc = defaultdict(lambda: {"p": [], "a": []})
            for r in recs:
                rc[r["race_id"]]["p"].append(r[pred_key])
                rc[r["race_id"]]["a"].append(r[actual_key])
            cs = [c for rid, d in rc.items() if (c := spearman_corr(d["p"], d["a"])) is not None]
            sp_val = sum(cs) / len(cs) if cs else None
            sp_text = Text(f"{sp_val:.3f}", style=color_corr(sp_val)) if sp_val else Text("N/A")

            t_axis.add_row(
                axis_name, g, str(t),
                f"{m:.2f}", f"{rm:.2f}",
                Text(f"{wi1:.1f}%", style=color_pct(wi1, (25, 35, 50))),
                Text(f"{wi2:.1f}%", style=color_pct(wi2, (40, 55, 70))),
                Text(f"{wi3:.1f}%", style=color_pct(wi3, (55, 70, 85))),
                sp_text,
                f"{me:+.2f}",
            )
        t_axis.add_section()

    console.print(t_axis)


# ============================================================
# 指標3: 脚質グループ一致率
# ============================================================
GROUP_JA = {"leading": "逃げ", "front": "先行", "mid": "中団", "rear": "後方"}
GROUP_ORDER = ["leading", "front", "mid", "rear"]


def to_half(g):
    return "前半" if g in ("leading", "front") else "後半"


def analyze_group(horse_records):
    valid = [r for r in horse_records if r["pred_group"] is not None]
    if not valid:
        console.print("[red]脚質グループデータなし[/red]")
        return

    n = len(valid)
    m4 = sum(1 for r in valid if r["pred_group"] == r["actual_group"])
    m2 = sum(1 for r in valid if to_half(r["pred_group"]) == to_half(r["actual_group"]))

    console.print()
    console.rule("[bold cyan]【3】脚質グループ一致率[/bold cyan]", style="cyan")
    console.print(f"  対象: [bold]{n}[/bold] 頭")
    console.print(f"  分類基準(実績): 先頭=上位15% / 好位=~40% / 中団=~75% / 後方=残り\n")

    # メイン指標
    t1 = Table(title="グループ一致率", show_header=True, header_style="bold", padding=(0, 2))
    t1.add_column("分類", style="bold")
    t1.add_column("一致率", justify="right")
    t1.add_column("件数", justify="right")
    t1.add_column("", min_width=35)

    p4 = m4 / n * 100
    p2 = m2 / n * 100
    t1.add_row(
        "4グループ (逃げ/先行/中団/後方)",
        Text(f"{p4:.1f}%", style=color_pct(p4, (40, 50, 60))),
        f"{m4}/{n}", make_bar(p4),
    )
    t1.add_row(
        "2分類 (前半/後半)",
        Text(f"{p2:.1f}%", style=color_pct(p2, (60, 70, 80))),
        f"{m2}/{n}", make_bar(p2),
    )
    console.print(t1)

    # 混同行列
    console.print()
    matrix = defaultdict(lambda: defaultdict(int))
    for r in valid:
        matrix[r["pred_group"]][r["actual_group"]] += 1

    t_cm = Table(title="混同行列 (行=予測, 列=実績)", show_header=True, header_style="bold", padding=(0, 1))
    t_cm.add_column("予測 \\ 実績", style="bold", min_width=10)
    for lbl in GROUP_ORDER:
        t_cm.add_column(GROUP_JA[lbl], justify="right", min_width=7)
    t_cm.add_column("合計", justify="right", style="dim")
    t_cm.add_column("精度", justify="right")

    for pred_lbl in GROUP_ORDER:
        row_total = sum(matrix[pred_lbl][a] for a in GROUP_ORDER)
        correct = matrix[pred_lbl][pred_lbl]
        acc = correct / row_total * 100 if row_total > 0 else 0

        cells = []
        for act_lbl in GROUP_ORDER:
            val = matrix[pred_lbl][act_lbl]
            if act_lbl == pred_lbl:
                cells.append(Text(str(val), style="bold green"))
            elif val > 0:
                cells.append(str(val))
            else:
                cells.append(Text("0", style="dim"))

        t_cm.add_row(
            GROUP_JA[pred_lbl], *cells,
            str(row_total),
            Text(f"{acc:.0f}%", style=color_pct(acc, (40, 55, 70))),
        )

    # リコール行
    t_cm.add_section()
    recall_cells = []
    for act_lbl in GROUP_ORDER:
        col_total = sum(matrix[p][act_lbl] for p in GROUP_ORDER)
        correct = matrix[act_lbl][act_lbl]
        recall = correct / col_total * 100 if col_total > 0 else 0
        recall_cells.append(Text(f"{recall:.0f}%", style=color_pct(recall, (40, 55, 70))))
    t_cm.add_row("再現率", *recall_cells, "", "")

    console.print(t_cm)

    # グループ別の詳細
    console.print()
    t_detail = Table(title="グループ別の精度詳細", show_header=True, header_style="bold", padding=(0, 1))
    t_detail.add_column("グループ", style="bold")
    t_detail.add_column("予測数", justify="right")
    t_detail.add_column("的中数", justify="right")
    t_detail.add_column("適合率", justify="right")
    t_detail.add_column("実績数", justify="right")
    t_detail.add_column("再現率", justify="right")
    t_detail.add_column("隣接一致", justify="right")
    t_detail.add_column("", min_width=20)

    for g in GROUP_ORDER:
        pred_count = sum(matrix[g][a] for a in GROUP_ORDER)
        correct = matrix[g][g]
        precision = correct / pred_count * 100 if pred_count else 0
        actual_count = sum(matrix[p][g] for p in GROUP_ORDER)
        recall = correct / actual_count * 100 if actual_count else 0

        # 隣接一致（1グループずれまでOK）
        idx = GROUP_ORDER.index(g)
        adjacent = set()
        adjacent.add(g)
        if idx > 0:
            adjacent.add(GROUP_ORDER[idx - 1])
        if idx < len(GROUP_ORDER) - 1:
            adjacent.add(GROUP_ORDER[idx + 1])
        adj_match = sum(matrix[g][a] for a in adjacent)
        adj_pct = adj_match / pred_count * 100 if pred_count else 0

        t_detail.add_row(
            GROUP_JA[g], str(pred_count), str(correct),
            Text(f"{precision:.1f}%", style=color_pct(precision, (40, 55, 70))),
            str(actual_count),
            Text(f"{recall:.1f}%", style=color_pct(recall, (40, 55, 70))),
            Text(f"{adj_pct:.1f}%", style=color_pct(adj_pct, (60, 75, 85))),
            make_bar(precision, 20),
        )
    console.print(t_detail)

    # 分析軸テーブル
    console.print()
    t_axis = Table(title="脚質グループ一致率 - 分析軸別", show_header=True, header_style="bold", padding=(0, 1))
    t_axis.add_column("分析軸", style="bold")
    t_axis.add_column("カテゴリ", style="bold")
    t_axis.add_column("n", justify="right")
    t_axis.add_column("4分類", justify="right")
    t_axis.add_column("2分類", justify="right")
    t_axis.add_column("逃げ適合", justify="right")
    t_axis.add_column("後方適合", justify="right")

    for axis_name, axis_fn in AXES:
        groups = defaultdict(list)
        for r in valid:
            groups[axis_fn(r)].append(r)

        for g in sorted(groups.keys()):
            recs = groups[g]
            t = len(recs)
            a4 = sum(1 for r in recs if r["pred_group"] == r["actual_group"]) / t * 100
            a2 = sum(1 for r in recs if to_half(r["pred_group"]) == to_half(r["actual_group"])) / t * 100

            # 逃げ適合率
            lead_pred = [r for r in recs if r["pred_group"] == "leading"]
            lead_ok = sum(1 for r in lead_pred if r["actual_group"] == "leading")
            lead_pct = lead_ok / len(lead_pred) * 100 if lead_pred else 0

            # 後方適合率
            rear_pred = [r for r in recs if r["pred_group"] == "rear"]
            rear_ok = sum(1 for r in rear_pred if r["actual_group"] == "rear")
            rear_pct = rear_ok / len(rear_pred) * 100 if rear_pred else 0

            t_axis.add_row(
                axis_name, g, str(t),
                Text(f"{a4:.1f}%", style=color_pct(a4, (40, 50, 60))),
                Text(f"{a2:.1f}%", style=color_pct(a2, (60, 70, 80))),
                Text(f"{lead_pct:.0f}%", style=color_pct(lead_pct, (40, 55, 70))) if lead_pred else Text("-"),
                Text(f"{rear_pct:.0f}%", style=color_pct(rear_pct, (40, 55, 70))) if rear_pred else Text("-"),
            )
        t_axis.add_section()

    console.print(t_axis)


# ============================================================
# 指標5: 上がり3F順位の精度
# ============================================================
def analyze_last3f_rank(horse_records):
    valid = [r for r in horse_records
             if r.get("est_last3f_rank") is not None
             and r.get("actual_last3f_rank") is not None]
    if not valid:
        console.print("[red]上がり3F順位データなし[/red]")
        return

    console.print()
    console.rule("[bold cyan]【5】上がり3F順位予測精度[/bold cyan]", style="cyan")
    console.print(f"  対象: [bold]{len(valid)}[/bold] 頭\n")

    _print_pos_detail(valid, "est_last3f_rank", "actual_last3f_rank", "上がり3F順位")

    # 上がり3位以内の的中精度（実用的な指標）
    console.print()
    t_top = Table(title="上がり上位の的中精度", show_header=True, header_style="bold", padding=(0, 2))
    t_top.add_column("指標", style="bold")
    t_top.add_column("的中", justify="right")
    t_top.add_column("対象", justify="right")
    t_top.add_column("的中率", justify="right")
    t_top.add_column("", min_width=30)

    for top_n, label in [(1, "上がり最速(1位)的中"), (3, "上がり3位以内的中"), (5, "上がり5位以内的中")]:
        # 予測でtop_n以内 → 実際もtop_n以内
        pred_in = [r for r in valid if r["est_last3f_rank"] <= top_n]
        if pred_in:
            hit = sum(1 for r in pred_in if r["actual_last3f_rank"] <= top_n)
            pct = hit / len(pred_in) * 100
            t_top.add_row(
                label, str(hit), str(len(pred_in)),
                Text(f"{pct:.1f}%", style=color_pct(pct, (30, 50, 70))),
                make_bar(pct),
            )
    console.print(t_top)


# ============================================================
# サマリー
# ============================================================
def print_summary(horse_records, race_records):
    console.print()
    console.rule("[bold white on blue] 総合サマリー [/bold white on blue]", style="blue")
    console.print()

    t = Table(show_header=True, header_style="bold", padding=(0, 2), title="展開予想精度 一覧")
    t.add_column("指標", style="bold", min_width=24)
    t.add_column("主要値", justify="right", min_width=12)
    t.add_column("詳細", min_width=35)
    t.add_column("評価")

    # 逃げ予想
    valid_lead = [r for r in race_records if r["has_first_corner"] and r["leading_predicted"]]
    if valid_lead:
        m = sum(1 for r in valid_lead if set(r["leading_predicted"]) & set(r["actual_leader_hnos"]))
        p = m / len(valid_lead) * 100
        t.add_row(
            "逃げ予想一致率",
            Text(f"{p:.1f}%", style=color_pct(p, (40, 50, 60))),
            f"{m}/{len(valid_lead)} レース",
            "初角1番手の的中",
        )

    # 4角
    v4 = [r for r in horse_records if r["est_pos4c"] is not None and r["est_pos4c"] > 0]
    if v4:
        errs = [abs(r["est_pos4c"] - r["actual_last_corner"]) for r in v4]
        mae = sum(errs) / len(errs)
        w2 = sum(1 for e in errs if e <= 2.0) / len(errs) * 100
        # スピアマン平均
        rc = defaultdict(lambda: {"p": [], "a": []})
        for r in v4:
            rc[r["race_id"]]["p"].append(r["est_pos4c"])
            rc[r["race_id"]]["a"].append(r["actual_last_corner"])
        cs = [c for _, d in rc.items() if (c := spearman_corr(d["p"], d["a"])) is not None]
        sp = sum(cs) / len(cs) if cs else 0

        t.add_row(
            "4角通過順位 MAE",
            Text(f"{mae:.2f} 番手", style="bold"),
            f"+-2以内={w2:.1f}%  n={len(v4)}",
            f"順序相関={sp:.3f}",
        )

    # 脚質グループ
    vg = [r for r in horse_records if r["pred_group"] is not None]
    if vg:
        m4 = sum(1 for r in vg if r["pred_group"] == r["actual_group"]) / len(vg) * 100
        m2 = sum(1 for r in vg if to_half(r["pred_group"]) == to_half(r["actual_group"])) / len(vg) * 100
        t.add_row(
            "脚質グループ一致",
            Text(f"{m4:.1f}%", style=color_pct(m4, (40, 50, 60))),
            f"2分類={m2:.1f}%  n={len(vg)}",
            "4グループ(逃先中後)",
        )

    # 1角
    v1 = [r for r in horse_records
          if r["est_pos1c"] is not None and r["est_pos1c"] > 0
          and r["actual_first_corner"] is not None and r["corners_len"] >= 2]
    if v1:
        errs = [abs(r["est_pos1c"] - r["actual_first_corner"]) for r in v1]
        mae = sum(errs) / len(errs)
        w2 = sum(1 for e in errs if e <= 2.0) / len(errs) * 100
        t.add_row(
            "1角通過順位 MAE",
            Text(f"{mae:.2f} 番手", style="bold"),
            f"+-2以内={w2:.1f}%  n={len(v1)}",
            "JRAのみ有効",
        )

    # 上がり3F順位
    vl = [r for r in horse_records
          if r.get("est_last3f_rank") is not None and r.get("actual_last3f_rank") is not None]
    if vl:
        errs = [abs(r["est_last3f_rank"] - r["actual_last3f_rank"]) for r in vl]
        mae = sum(errs) / len(errs)
        w2 = sum(1 for e in errs if e <= 2.0) / len(errs) * 100
        # 上がり3位以内の的中率
        pred_top3 = [r for r in vl if r["est_last3f_rank"] <= 3]
        top3_hit = sum(1 for r in pred_top3 if r["actual_last3f_rank"] <= 3) / len(pred_top3) * 100 if pred_top3 else 0
        rc = defaultdict(lambda: {"p": [], "a": []})
        for r in vl:
            rc[r["race_id"]]["p"].append(r["est_last3f_rank"])
            rc[r["race_id"]]["a"].append(r["actual_last3f_rank"])
        cs = [c for _, d in rc.items() if (c := spearman_corr(d["p"], d["a"])) is not None]
        sp = sum(cs) / len(cs) if cs else 0
        t.add_row(
            "上がり3F順位 MAE",
            Text(f"{mae:.2f} 位", style="bold"),
            f"+-2以内={w2:.1f}%  3位内的中={top3_hit:.1f}%",
            f"順序相関={sp:.3f}",
        )

    console.print(t)


# ============================================================
# メイン
# ============================================================
def main():
    args = parse_args()
    dates = date_range(args.start, args.end)

    console.print(Panel(
        f"[bold]展開予想精度検証[/bold]\n期間: {args.start} ~ {args.end} ({len(dates)}日)",
        style="bold blue",
    ))

    horse_records, race_records = collect_data(dates)

    # 概要
    jra_r = sum(1 for r in race_records if r["is_jra"])
    nar_r = len(race_records) - jra_r

    t_overview = Table(show_header=False, padding=(0, 2))
    t_overview.add_column("", style="bold")
    t_overview.add_column("")
    t_overview.add_row("対象レース数", f"{len(race_records)} R (JRA: {jra_r} / NAR: {nar_r})")
    t_overview.add_row("対象馬数", f"{len(horse_records)} 頭")
    console.print(t_overview)

    if not race_records:
        console.print("[red]データなし。終了。[/red]")
        return

    analyze_leading(race_records)
    analyze_pos4c(horse_records)
    analyze_group(horse_records)
    analyze_pos1c(horse_records)
    analyze_last3f_rank(horse_records)
    print_summary(horse_records, race_records)


if __name__ == "__main__":
    main()
