# -*- coding: utf-8 -*-
"""予測+結果 統合CSV出力 — 全馬のデータを1ファイルに集約

pred.json (予測: 印・偏差値・確率) と results.json (結果: 着順・払戻金) を
race_id × horse_no でマージし、分析用CSVを生成する。

出力: data/csv/eval_YYYY.csv (年別) or eval_all.csv (全期間)

pandasで読めば即座に:
  - 印別的中率・回収率
  - 偏差値帯別着順分布
  - 自信度別収支推移
  - 任意の戦略シミュレーション
が可能。

使い方:
  python scripts/export_eval_csv.py --year 2026
  python scripts/export_eval_csv.py --year 2026 --output data/csv/eval_2026.csv
  python scripts/export_eval_csv.py --all
"""
import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8")

from config.settings import PREDICTIONS_DIR, RESULTS_DIR

# ── 出力CSVのカラム定義 ──

# レースコンテキスト
RACE_COLS = [
    "date", "race_id", "venue", "race_no", "surface", "distance",
    "grade", "field_count", "is_jra", "is_banei", "condition",
    "confidence", "overall_confidence", "pace_predicted",
]

# 馬の予測データ (pred.json から)
PRED_COLS = [
    "horse_no", "horse_name", "horse_id", "sex", "age", "gate_no",
    "weight_kg", "weight_change",
    "jockey", "jockey_id", "trainer", "trainer_id",
    "sire", "dam", "maternal_grandsire",
    "mark", "composite", "ability_total", "pace_total", "course_total",
    "race_relative_dev", "hybrid_total",
    "win_prob", "place2_prob", "place3_prob",
    "ml_win_prob", "ml_top2_prob", "ml_place_prob",
    "raw_lgbm_prob", "ensemble_prob",
    "odds", "popularity", "predicted_tansho_odds",
    "running_style", "position_initial",
    "jockey_dev", "trainer_dev", "bloodline_dev", "training_dev",
    "ev", "ana_score", "ana_type",
    "is_tokusen", "tokusen_score", "is_tokusen_kiken", "kiken_score",
    "odds_consistency_adj", "ml_composite_adj",
    "is_scratched",
]

# 馬の結果データ (results.json から)
RESULT_COLS = [
    "finish_pos", "finish_time_sec", "last_3f", "margin",
    "result_popularity", "result_odds",
]

# レースの払戻データ (results.json から)
PAYOUT_COLS = [
    "payout_tansho", "payout_fukusho",
    "payout_sanrenpuku", "payout_sanrentan",
]

ALL_COLS = RACE_COLS + PRED_COLS + RESULT_COLS + PAYOUT_COLS


def _load_results(date_str: str) -> dict:
    """results.json を読み込み、{race_id: {order, payouts}} を返す"""
    fpath = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
    if not os.path.exists(fpath):
        return {}
    with open(fpath, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_payout(payouts: dict, ticket_type: str) -> int:
    """払戻金リストから最大払戻額を取得"""
    entries = payouts.get(ticket_type, [])
    if not entries:
        return 0
    if isinstance(entries, list):
        return max((e.get("payout", 0) or 0) for e in entries) if entries else 0
    return 0


def _find_horse_result(order: list, horse_no: int) -> dict:
    """着順リストから馬番で検索"""
    for entry in order:
        if entry.get("horse_no") == horse_no:
            return entry
    return {}


def process_one_day(date_str: str) -> list:
    """1日分の pred.json + results.json をマージして行リストを返す"""
    pred_path = os.path.join(PREDICTIONS_DIR, f"{date_str}_pred.json")
    if not os.path.exists(pred_path):
        return []

    with open(pred_path, "r", encoding="utf-8") as f:
        pred = json.load(f)

    results = _load_results(date_str)
    rows = []

    for race in pred.get("races", []):
        race_id = race.get("race_id", "")

        # レースコンテキスト
        race_ctx = {col: race.get(col, "") for col in RACE_COLS}
        race_ctx["date"] = date_str

        # 結果データ
        result_entry = results.get(race_id, {})
        order = result_entry.get("order", [])
        payouts = result_entry.get("payouts", {})

        # レース払戻
        race_payouts = {
            "payout_tansho": _extract_payout(payouts, "tansho"),
            "payout_fukusho": _extract_payout(payouts, "fukusho"),
            "payout_sanrenpuku": _extract_payout(payouts, "sanrenpuku"),
            "payout_sanrentan": _extract_payout(payouts, "sanrentan"),
        }

        for horse in race.get("horses", []):
            row = {**race_ctx}

            # 予測データ
            for col in PRED_COLS:
                val = horse.get(col, "")
                # list/dict はCSVに入れない
                if isinstance(val, (list, dict)):
                    val = ""
                if val is None:
                    val = ""
                row[col] = val

            # 結果データ
            hno = horse.get("horse_no", 0)
            hr = _find_horse_result(order, hno)
            row["finish_pos"] = hr.get("finish", "")
            row["finish_time_sec"] = hr.get("time_sec", "")
            row["last_3f"] = hr.get("last_3f", "")
            row["margin"] = hr.get("margin", "")
            row["result_popularity"] = hr.get("popularity", "")
            row["result_odds"] = hr.get("odds", "")

            # 払戻 (レースレベル — 全馬同じ値)
            row.update(race_payouts)

            rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser(description="予測+結果 統合CSV出力")
    parser.add_argument("--year", type=str, help="対象年 (例: 2026)")
    parser.add_argument("--all", action="store_true", help="全期間")
    parser.add_argument("--output", type=str, help="出力先パス")
    args = parser.parse_args()

    # 対象日付収集
    pred_files = sorted([
        f.replace("_pred.json", "")
        for f in os.listdir(PREDICTIONS_DIR)
        if f.endswith("_pred.json") and "_prev" not in f
    ])

    if args.year:
        pred_files = [d for d in pred_files if d.startswith(args.year)]
    elif not args.all:
        parser.error("--year または --all を指定してください")

    if not pred_files:
        print("対象ファイルが見つかりません")
        return

    # 出力先
    csv_dir = Path("data/csv")
    csv_dir.mkdir(parents=True, exist_ok=True)
    if args.output:
        csv_path = args.output
    elif args.year:
        csv_path = str(csv_dir / f"eval_{args.year}.csv")
    else:
        csv_path = str(csv_dir / "eval_all.csv")

    total = len(pred_files)
    print(f"=== 統合CSV出力 ({total}日分) → {csv_path} ===")

    t0 = time.time()
    all_rows = []
    results_found = 0
    results_missing = 0

    for i, date_str in enumerate(pred_files):
        rows = process_one_day(date_str)
        all_rows.extend(rows)

        # 結果データの有無チェック
        res_path = os.path.join(RESULTS_DIR, f"{date_str}_results.json")
        if os.path.exists(res_path):
            results_found += 1
        else:
            results_missing += 1

        if (i + 1) % 20 == 0 or i == total - 1:
            pct = (i + 1) / total * 100
            print(f"  [{pct:5.1f}%] {i + 1}/{total}日 ({len(all_rows):,}行)")

    # CSV書き出し
    if all_rows:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=ALL_COLS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_rows)

    elapsed = time.time() - t0
    file_size = os.path.getsize(csv_path) / 1024 / 1024 if os.path.exists(csv_path) else 0

    print()
    print(f"{'=' * 60}")
    print(f"完了: {total}日 / {len(all_rows):,}行 / {elapsed:.1f}秒")
    print(f"出力: {csv_path} ({file_size:.1f}MB)")
    print(f"結果データ: あり={results_found}日 / なし={results_missing}日")
    print(f"{'=' * 60}")

    # サンプル統計
    if all_rows:
        marks = {}
        for r in all_rows:
            m = r.get("mark", "-")
            marks[m] = marks.get(m, 0) + 1
        print(f"\n印分布:")
        for m, cnt in sorted(marks.items(), key=lambda x: -x[1]):
            print(f"  {m}: {cnt:,}頭")

        # 着順あり馬の数
        has_finish = sum(1 for r in all_rows if r.get("finish_pos", "") not in ("", None, 0))
        print(f"\n着順あり: {has_finish:,} / {len(all_rows):,}頭 ({has_finish / len(all_rows) * 100:.1f}%)")


if __name__ == "__main__":
    main()
