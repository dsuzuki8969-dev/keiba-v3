"""
WF版較正テーブル生成スクリプト (T-7 Phase1)

目的:
  walk_forward_backtest.py --composite-probe 実行後に生成された
  data/_diag/wf_calib_raw/*.json (raw counts) を合算し、
  WF版(リーク無)の偏差値bin別実率テーブルを生成する。

  本番版 calibration_composite.json を絶対に上書きしない。
  出力は calibration_composite_wf.json / .csv (別名固定)。

出力:
  data/_diag/calibration_composite_wf.json
  data/_diag/calibration_composite_wf.csv

検証出力:
  本番版 vs WF版をbin毎に勝率/複勝率対比・差分・各binのn表示

使い方:
  python scripts/build_calibration_composite_wf.py
  python scripts/build_calibration_composite_wf.py --raw-dir data/_diag/wf_calib_raw
"""

import sys
import argparse
import json
import csv
from pathlib import Path
from collections import defaultdict

# Windows cp932 環境での日本語 print 即死を防ぐ (reconfigure 方式)
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, ...) 置換は元stdoutのGCで
# buffer が閉じ "I/O operation on closed file" を起こすため不可。
# finalize_predictions.py:28-33 / preview_calibration_t7.py と同一の reconfigure 方式に統一。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

# プロジェクトルートをパスに追加 (scripts/ から実行時)
sys.path.insert(0, str(Path(__file__).parent.parent))

# build_calibration_composite から bin定義と rate変換関数を import
from scripts.build_calibration_composite import composite_to_bin, BIN_LABELS, build_rate_table  # noqa: E402

# ================== パス設定 ==================
PROJECT_ROOT = Path(__file__).parent.parent
OUT_DIR = PROJECT_ROOT / "data" / "_diag"

# WF版出力先 (本番版とは別名固定 — 取り違え防止)
OUT_JSON_WF = OUT_DIR / "calibration_composite_wf.json"
OUT_CSV_WF  = OUT_DIR / "calibration_composite_wf.csv"

# 本番版 (比較用読み込み専用・絶対に上書きしない)
OUT_JSON_PROD = OUT_DIR / "calibration_composite.json"


# ================== raw counts 読み込み・合算 ==================

def load_and_merge_raw(raw_dir: Path) -> dict:
    """
    raw_dir 内の *.json (walk_forward_backtest が出力した raw counts) を
    すべて読み込み、カウント単位で合算して返す。

    返り値形式: {org: {bin_label: {n, win, place2, place3}}}
    """
    json_files = sorted(raw_dir.glob("*.json"))
    if not json_files:
        print(f"[ERROR] {raw_dir} に *.json が見つかりません。")
        print("  walk_forward_backtest.py --composite-probe を先に実行してください。")
        sys.exit(1)

    print(f"[1/4] raw counts ファイル読み込み中: {len(json_files)} ファイル")
    for f in json_files:
        print(f"  - {f.name}")

    merged: dict = defaultdict(
        lambda: defaultdict(lambda: {"n": 0, "win": 0, "place2": 0, "place3": 0})
    )

    for fpath in json_files:
        with open(fpath, encoding="utf-8") as fp:
            data = json.load(fp)
        for org, bins in data.items():
            for bin_label, cnt in bins.items():
                rec = merged[org][bin_label]
                rec["n"]      += cnt.get("n", 0)
                rec["win"]    += cnt.get("win", 0)
                rec["place2"] += cnt.get("place2", 0)
                rec["place3"] += cnt.get("place3", 0)

    # defaultdict → 通常dict に変換
    result = {o: {b: dict(c) for b, c in bins.items()} for o, bins in merged.items()}
    total_n = sum(v["n"] for v in result.get("ALL", {}).values())
    print(f"  合算完了: 総馬数(ALL) = {total_n:,}")
    return result


# ================== 出力 ==================

def write_outputs_wf(rate_table: dict) -> None:
    """WF版 JSON と CSV を出力する (本番版パスとは別名・絶対に上書きしない)。"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 安全ガード: 本番版パスへの書き込みを禁止
    assert OUT_JSON_WF != OUT_JSON_PROD, "WF版と本番版のパスが一致しています — 中断"

    # JSON
    with open(OUT_JSON_WF, "w", encoding="utf-8") as f:
        json.dump(rate_table, f, ensure_ascii=False, indent=2)
    print(f"[3/4] JSON 出力: {OUT_JSON_WF}")

    # CSV
    orgs_ordered = ["ALL", "JRA", "NAR"]
    with open(OUT_CSV_WF, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["org", "composite_bin", "n", "win%", "place2%", "place3%"])
        for org in orgs_ordered:
            if org not in rate_table:
                continue
            for label in BIN_LABELS:
                row_data = rate_table[org].get(label)
                if row_data is None:
                    continue
                writer.writerow([
                    org, label,
                    row_data["n"],
                    row_data["win"],
                    row_data["place2"],
                    row_data["place3"],
                ])
    print(f"[3/4] CSV 出力: {OUT_CSV_WF}")


# ================== 検証出力 (本番版 vs WF版 対比) ==================

def print_comparison(wf_table: dict) -> None:
    """
    本番版と WF版を bin 毎に勝率/複勝率・差分・n を対比表示する。
    本番版が存在しない場合は WF版のみ表示。
    """
    print("\n[4/4] 本番版 vs WF版 対比 (ALL)")
    print("  ⚠️  本番版はリーク版(後追い集計)。WF版がリーク無の較正値。")
    print(f"  {'bin':>7}  {'n_wf':>6}  "
          f"{'win%(prod)':>10}  {'win%(wf)':>8}  {'diff':>5}  |  "
          f"{'p3%(prod)':>9}  {'p3%(wf)':>7}  {'diff':>5}")
    print("  " + "-" * 75)

    prod_table: dict = {}
    if OUT_JSON_PROD.exists():
        with open(OUT_JSON_PROD, encoding="utf-8") as f:
            prod_table = json.load(f)
    else:
        print(f"  (本番版 {OUT_JSON_PROD} が存在しないため差分は表示しません)")

    wf_all = wf_table.get("ALL", {})
    prod_all = prod_table.get("ALL", {})

    def _fmt(v: "float | None", width: int = 8) -> str:
        return f"{v:>{width}.1f}" if v is not None else f"{'N/A':>{width}}"

    def _diff(a: "float | None", b: "float | None") -> str:
        if a is None or b is None:
            return "  N/A"
        d = b - a
        return f"{d:>+5.1f}"

    for label in BIN_LABELS:
        wf_r   = wf_all.get(label)
        prod_r = prod_all.get(label)

        if wf_r is None:
            continue

        n_wf = wf_r["n"]

        # WF値
        win_wf = wf_r["win"]    # None or float
        p3_wf  = wf_r["place3"]

        # 本番値
        win_prod = prod_r["win"]    if prod_r else None
        p3_prod  = prod_r["place3"] if prod_r else None

        print(f"  {label:>7}  {n_wf:>6,}  "
              f"{_fmt(win_prod, 10)}  {_fmt(win_wf, 8)}  {_diff(win_prod, win_wf)}  |  "
              f"{_fmt(p3_prod, 9)}  {_fmt(p3_wf, 7)}  {_diff(p3_prod, p3_wf)}")

    print()
    # JRA / NAR 別サマリー
    for org in ("JRA", "NAR"):
        wf_org = wf_table.get(org, {})
        total_n = sum(v.get("n", 0) for v in wf_org.values() if v)
        if total_n:
            print(f"  {org}: 総馬数(WF) = {total_n:,}")


# ================== main ==================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="WF版較正テーブル生成 (T-7 Phase1) — raw counts → 率化 → JSON/CSV 出力"
    )
    parser.add_argument(
        "--raw-dir",
        default="data/_diag/wf_calib_raw",
        help="walk_forward_backtest が出力した raw counts ディレクトリ (default: data/_diag/wf_calib_raw)",
    )
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    print(f"\n{'=' * 62}")
    print(f"  WF版較正テーブル生成 (T-7 Phase1)")
    print(f"  raw-dir: {raw_dir.resolve()}")
    print(f"  出力: {OUT_JSON_WF}")
    print(f"  ⚠️  本番版 ({OUT_JSON_PROD.name}) は絶対に上書きしない")
    print(f"{'=' * 62}\n")

    # [1/4] raw counts 読み込み・合算
    merged_counts = load_and_merge_raw(raw_dir)

    # [2/4] カウント → 率(%) に変換
    print("[2/4] カウント → 率(%) 変換中...")
    rate_table = build_rate_table(merged_counts)
    print(f"  変換完了: {list(rate_table.keys())} org")

    # [3/4] JSON / CSV 出力
    write_outputs_wf(rate_table)

    # [4/4] 本番版 vs WF版 対比表示
    print_comparison(rate_table)

    print(f"{'=' * 62}")
    print(f"  完了！ WF版テーブル: {OUT_JSON_WF}")
    print(f"  次手順: T-7 Phase2 でプレビュースクリプトを --table wf で再実行")
    print(f"{'=' * 62}\n")


if __name__ == "__main__":
    main()
