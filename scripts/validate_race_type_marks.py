"""
断層ベースのレースタイプ分類 x 印別 的中率検証スクリプト

レースタイプ:
  - 独走型: 1位-2位間gap >= 5pt
  - 2強型: 1位-2位間gap < 2 かつ 2位-3位間gap >= 4pt
  - 3強型: 1位-2位間gap < 2 かつ 2位-3位間gap < 2 かつ 3位-4位間gap >= 3pt
  - 全混戦: max_gap < 3pt
  - 上位拮抗: それ以外

Usage:
  REPORT_OUTPUT=report.txt python scripts/validate_race_type_marks.py
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

# Windows環境でのエンコーディング対策
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

# 出力ファイル（指定時はファイルにも出力）
_OUTPUT_FILE = os.environ.get("REPORT_OUTPUT")
_OUT_FH = None
if _OUTPUT_FILE:
    _OUT_FH = open(_OUTPUT_FILE, "w", encoding="utf-8")

def P(*args, **kwargs):
    """printの代わり。flush=True付き。REPORT_OUTPUT指定時はファイルにも出力"""
    kwargs.pop("flush", None)
    print(*args, **kwargs, flush=True)
    if _OUT_FH:
        print(*args, **kwargs, file=_OUT_FH, flush=True)

# プロジェクトルート
PROJECT_ROOT = Path(__file__).resolve().parent.parent
PREDICTIONS_DIR = PROJECT_ROOT / "data" / "predictions"
RESULTS_DIR = PROJECT_ROOT / "data" / "results"

# 今日の日付（除外対象）
EXCLUDE_DATE = "20260412"


def classify_race_type(horses: list) -> str:
    """composite値のソート済みリストから断層ベースでレースタイプを分類"""
    if len(horses) < 2:
        return "不明"

    # composite降順ソート
    scores = sorted([h.get("composite", 0) or 0 for h in horses], reverse=True)

    if len(scores) < 2:
        return "不明"

    gap_1_2 = scores[0] - scores[1]
    gap_2_3 = (scores[1] - scores[2]) if len(scores) >= 3 else 0
    gap_3_4 = (scores[2] - scores[3]) if len(scores) >= 4 else 0

    # 全gapの最大値
    gaps = [scores[i] - scores[i + 1] for i in range(len(scores) - 1)]
    max_gap = max(gaps) if gaps else 0

    # 分類ロジック
    if gap_1_2 >= 5:
        return "独走型"
    elif gap_1_2 < 2 and gap_2_3 >= 4:
        return "2強型"
    elif gap_1_2 < 2 and gap_2_3 < 2 and gap_3_4 >= 3:
        return "3強型"
    elif max_gap < 3:
        return "全混戦"
    else:
        return "上位拮抗"


def load_and_merge(date_str: str):
    """pred.jsonとresults.jsonを読み込んでマージ"""
    pred_path = PREDICTIONS_DIR / f"{date_str}_pred.json"
    result_path = RESULTS_DIR / f"{date_str}_results.json"

    if not pred_path.exists() or not result_path.exists():
        return []

    try:
        with open(pred_path, "r", encoding="utf-8") as f:
            pred_data = json.load(f)
        with open(result_path, "r", encoding="utf-8") as f:
            result_data = json.load(f)
    except (json.JSONDecodeError, UnicodeDecodeError):
        # 壊れたJSONはスキップ
        return []

    merged = []
    for race in pred_data.get("races", []):
        race_id = race.get("race_id", "")
        horses = race.get("horses", [])
        if not horses or race_id not in result_data:
            continue

        result_info = result_data[race_id]
        order = result_info.get("order", [])
        if not order:
            continue

        # horse_no -> finish のマッピング
        finish_map = {}
        for o in order:
            hno = o.get("horse_no")
            fin = o.get("finish")
            if hno is not None and fin is not None:
                finish_map[hno] = fin

        # レースタイプ分類
        race_type = classify_race_type(horses)

        # 馬ごとに結果を付与
        for h in horses:
            hno = h.get("horse_no")
            mark = h.get("mark", "")
            composite = h.get("composite", 0) or 0
            finish = finish_map.get(hno)

            if finish is None or finish == 0:
                continue

            merged.append({
                "race_id": race_id,
                "race_type": race_type,
                "horse_no": hno,
                "mark": mark,
                "composite": composite,
                "finish": finish,
                "field_count": len(horses),
            })

    return merged


def main():
    # pred.jsonファイルの日付リストを取得
    dates = []
    for f in sorted(PREDICTIONS_DIR.glob("*_pred.json")):
        date_str = f.stem.replace("_pred", "")
        if date_str != EXCLUDE_DATE and len(date_str) == 8:
            dates.append(date_str)

    P(f"対象日数: {len(dates)} 日")
    P(f"期間: {dates[0] if dates else '?'} ~ {dates[-1] if dates else '?'}")

    # 全データをマージ
    all_records = []
    matched_dates = 0
    skipped = 0
    for i, d in enumerate(dates):
        records = load_and_merge(d)
        if records:
            all_records.extend(records)
            matched_dates += 1
        else:
            skipped += 1
        if (i + 1) % 100 == 0:
            P(f"  読み込み進捗: {i+1}/{len(dates)} 日 ({len(all_records):,} レコード)")

    P(f"結果照合済み日数: {matched_dates} 日 (スキップ: {skipped})")
    P(f"総レコード数: {len(all_records):,}")

    if not all_records:
        P("データなし。終了します。")
        return

    # レースタイプ別x印別に集計
    MARK_LABELS = {"◎": "◎(本命)", "○": "○(対抗)", "▲": "▲(単穴)", "△": "△(連下)",
                   "★": "★(注目)", "×": "×(危険)", "☆": "☆(穴)", "": "無印"}

    RACE_TYPES = ["独走型", "2強型", "3強型", "上位拮抗", "全混戦"]

    # 集計構造: {race_type: {mark: {count, win, place2, place3}}}
    stats = defaultdict(lambda: defaultdict(lambda: {"count": 0, "win": 0, "place2": 0, "place3": 0}))
    race_type_counts = defaultdict(int)  # レース数

    # レースIDごとにレースタイプを特定（レース数カウント用）
    race_types_map = {}
    for rec in all_records:
        rid = rec["race_id"]
        if rid not in race_types_map:
            race_types_map[rid] = rec["race_type"]
            race_type_counts[rec["race_type"]] += 1

    # 印別集計（レースタイプ別 + 全体）
    for rec in all_records:
        rt = rec["race_type"]
        mk = rec["mark"]
        fin = rec["finish"]

        for target_rt in (rt, "全体"):
            s = stats[target_rt][mk]
            s["count"] += 1
            if fin == 1:
                s["win"] += 1
            if fin <= 2:
                s["place2"] += 1
            if fin <= 3:
                s["place3"] += 1

    race_type_counts["全体"] = len(race_types_map)

    # レースIDごとにインデックスを構築（組み合わせ的中率用）
    # mark -> finish だけ保持（メモリ節約）
    race_mark_finish = defaultdict(dict)  # {race_id: {mark: finish}}
    for rec in all_records:
        mk = rec["mark"]
        if mk:
            race_mark_finish[rec["race_id"]][mk] = rec["finish"]

    combo_stats = defaultdict(lambda: {"total": 0, "wide_hit": 0, "trio_hit": 0,
                                        "honmei_taiko_quinella": 0})
    for rid, rtype in race_types_map.items():
        mf = race_mark_finish.get(rid, {})

        combo_stats[rtype]["total"] += 1
        combo_stats["全体"]["total"] += 1

        honmei = mf.get("◎")
        taiko = mf.get("○")
        tanana = mf.get("▲")

        # ◎○ワイド: ◎と○が両方3着以内
        if honmei and taiko and honmei <= 3 and taiko <= 3:
            combo_stats[rtype]["wide_hit"] += 1
            combo_stats["全体"]["wide_hit"] += 1

        # ◎○馬連: ◎と○が1-2着（順不同）
        if honmei and taiko and honmei <= 2 and taiko <= 2:
            combo_stats[rtype]["honmei_taiko_quinella"] += 1
            combo_stats["全体"]["honmei_taiko_quinella"] += 1

        # ◎○▲三連: 3頭とも3着以内
        if honmei and taiko and tanana and honmei <= 3 and taiko <= 3 and tanana <= 3:
            combo_stats[rtype]["trio_hit"] += 1
            combo_stats["全体"]["trio_hit"] += 1

    # composite平均（◎印のみ、レースタイプ別）事前集計
    comp_sums = defaultdict(lambda: {"total": 0.0, "count": 0})
    for rec in all_records:
        if rec["mark"] == "◎":
            cs = comp_sums[rec["race_type"]]
            cs["total"] += rec["composite"]
            cs["count"] += 1

    # --- 出力 ---
    P("")
    P("=" * 90)
    P("断層ベース レースタイプ x 印別 的中率検証レポート")
    P("=" * 90)

    # レースタイプ分布
    P("")
    P("■ レースタイプ分布")
    P(f"{'タイプ':<10} {'レース数':>8} {'割合':>8}")
    P("-" * 30)
    total_races = race_type_counts["全体"]
    for rt in RACE_TYPES:
        cnt = race_type_counts.get(rt, 0)
        pct = cnt / total_races * 100 if total_races else 0
        P(f"{rt:<10} {cnt:>8,} {pct:>7.1f}%")
    P(f"{'合計':<10} {total_races:>8,} {'100.0':>7}%")

    # 印別成績（レースタイプ別）
    for rt in RACE_TYPES + ["全体"]:
        P(f"")
        P(f"■ 【{rt}】 印別成績 (レース数: {race_type_counts.get(rt, 0):,})")
        P(f"{'印':<12} {'頭数':>6} {'勝率':>7} {'連対率':>7} {'複勝率':>7} {'勝数':>5} {'連対':>5} {'複勝':>5}")
        P("-" * 70)

        for mk in ["◎", "○", "▲", "△", "★"]:
            s = stats[rt][mk]
            cnt = s["count"]
            if cnt == 0:
                continue
            win_r = s["win"] / cnt * 100
            p2_r = s["place2"] / cnt * 100
            p3_r = s["place3"] / cnt * 100
            label = MARK_LABELS.get(mk, mk)
            P(f"{label:<12} {cnt:>6,} {win_r:>6.1f}% {p2_r:>6.1f}% {p3_r:>6.1f}% {s['win']:>5} {s['place2']:>5} {s['place3']:>5}")

        # 無印
        s = stats[rt][""]
        if s["count"] > 0:
            cnt = s["count"]
            win_r = s["win"] / cnt * 100
            p2_r = s["place2"] / cnt * 100
            p3_r = s["place3"] / cnt * 100
            P(f"{'無印':<12} {cnt:>6,} {win_r:>6.1f}% {p2_r:>6.1f}% {p3_r:>6.1f}% {s['win']:>5} {s['place2']:>5} {s['place3']:>5}")

    # 組み合わせ的中率
    P("")
    P("■ 組み合わせ的中率（レースタイプ別）")
    P(f"{'タイプ':<10} {'レース数':>8} {'◎○ワイド':>10} {'◎○馬連':>10} {'◎○▲三連':>10}")
    P("-" * 60)
    for rt in RACE_TYPES + ["全体"]:
        cs = combo_stats[rt]
        tot = cs["total"]
        if tot == 0:
            continue
        wide_r = cs["wide_hit"] / tot * 100
        quin_r = cs["honmei_taiko_quinella"] / tot * 100
        trio_r = cs["trio_hit"] / tot * 100
        P(f"{rt:<10} {tot:>8,} {wide_r:>9.1f}% {quin_r:>9.1f}% {trio_r:>9.1f}%")

    # 独走型の◎勝率詳細
    P("")
    P("■ 独走型の詳細分析")
    s = stats["独走型"]["◎"]
    if s["count"] > 0:
        P(f"  ◎本命の勝率: {s['win']}/{s['count']} = {s['win']/s['count']*100:.1f}%")
        P(f"  ◎本命の複勝率: {s['place3']}/{s['count']} = {s['place3']/s['count']*100:.1f}%")
    else:
        P("  ◎本命のデータなし")

    # 全混戦の分析
    P("")
    P("■ 全混戦の詳細分析")
    s = stats["全混戦"]["◎"]
    if s["count"] > 0:
        P(f"  ◎本命の勝率: {s['win']}/{s['count']} = {s['win']/s['count']*100:.1f}%")
        P(f"  ◎本命の複勝率: {s['place3']}/{s['count']} = {s['place3']/s['count']*100:.1f}%")
    else:
        P("  ◎本命のデータなし")

    # composite平均差（レースタイプ別）
    P("")
    P("■ レースタイプ別 composite平均（◎印のみ）")
    for rt in RACE_TYPES:
        cs = comp_sums[rt]
        if cs["count"] > 0:
            P(f"  {rt:<10}: 平均 {cs['total']/cs['count']:.1f}, 頭数 {cs['count']}")

    P("")
    P("=" * 90)
    P("検証完了")

    if _OUT_FH:
        _OUT_FH.close()


if __name__ == "__main__":
    main()
