"""
偏差値別実率テーブル生成スクリプト (本番非改変・読み取り専用)

目的:
  predictions テーブルの composite と race_log の finish_pos を突合し、
  composite 5 刻み bin × 組織(ALL/JRA/NAR) で
  勝率/連対率/複勝率の実測値テーブルを生成する。

出力:
  data/_diag/calibration_composite.json
  data/_diag/calibration_composite.csv

注意:
  - DB への書き込みは一切しない（読み取り専用）
  - 本番ファイルを変更しない
  - git add / git commit は絶対にしない
"""

import sys
import io

# Windows cp932 環境での日本語 print 即死を防ぐ (必須)
sys.stdout = io.TextIOWrapper(
    sys.stdout.buffer, encoding='utf-8', line_buffering=True, errors='replace'
)

import json
import csv
import sqlite3
from pathlib import Path
from collections import defaultdict

# ================== パス設定 ==================
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "keiba.db"
OUT_DIR = PROJECT_ROOT / "data" / "_diag"
OUT_JSON = OUT_DIR / "calibration_composite.json"
OUT_CSV = OUT_DIR / "calibration_composite.csv"

# ================== composite bin 定義 ==================
# 5 刻み。各タプル: (下限, 上限含む, ラベル)
BINS = [
    (0,   29.9, "<30"),
    (30,  34.9, "30-34"),
    (35,  39.9, "35-39"),
    (40,  44.9, "40-44"),
    (45,  49.9, "45-49"),
    (50,  54.9, "50-54"),
    (55,  59.9, "55-59"),
    (60,  64.9, "60-64"),
    (65,  69.9, "65-69"),
    (70,  74.9, "70-74"),
    (75,  999,  "75+"),
]

BIN_LABELS = [b[2] for b in BINS]


def composite_to_bin(c: float) -> str | None:
    """composite 値を bin ラベルに変換。範囲外は None。"""
    if c is None:
        return None
    for lo, hi, label in BINS:
        if lo <= c <= hi:
            return label
    return None


# ================== 組織分類 ==================
# ばんえい venue_code=65 は除外
BANEI_VENUE_CODE = "65"


def get_org(is_jra: int, venue_code: str) -> str | None:
    """is_jra / venue_code から 'JRA' / 'NAR' を返す。ばんえいは None。"""
    if venue_code == BANEI_VENUE_CODE:
        return None
    return "JRA" if is_jra else "NAR"


# ================== 集計 ==================

def fetch_and_aggregate() -> dict:
    """
    DB から突合データを取得し、bin × org で集計する。

    返り値: {org: {bin: {'n': int, 'win': int, 'place2': int, 'place3': int}}}
      ここで win / place2 / place3 はカウント（後で率に変換）。
    """
    # {org: {bin: {n, win, place2, place3}}}
    counts: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"n": 0, "win": 0, "place2": 0, "place3": 0})
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # predictions.horses_json には horse_no・composite がある
    # race_log には race_id・horse_no・finish_pos・is_jra・venue_code がある
    # ばんえいは is_jra=0 かつ venue_code='65'
    #
    # 突合: predictions.race_id = race_log.race_id
    #        AND json_extract の horse_no = race_log.horse_no
    #
    # SQLite の json_each を使って horses_json を展開する。
    # 大規模データ（46,508 レース × 平均約 10 頭 = 465,000 行）なので
    # バッチ処理で進捗を表示する。

    print("[1/4] DB からレース一覧を取得中...")

    # predictions から race_id・horses_json を全件取得
    cur.execute("""
        SELECT
            p.race_id,
            p.horses_json
        FROM predictions p
        ORDER BY p.race_id
    """)
    pred_rows = cur.fetchall()
    print(f"      predictions: {len(pred_rows):,} レース")

    # race_log を race_id でインデックス化 (horse_no → finish_pos / is_jra / venue_code)
    print("[2/4] race_log を読み込みインデックス化中...")
    cur.execute("""
        SELECT race_id, horse_no, finish_pos, is_jra, venue_code
        FROM race_log
        WHERE venue_code != '65'
          AND finish_pos > 0
    """)
    race_log_index: dict[str, dict[int, tuple]] = defaultdict(dict)
    total_rl = 0
    for rl in cur.fetchall():
        race_log_index[rl["race_id"]][rl["horse_no"]] = (
            rl["finish_pos"],
            rl["is_jra"],
            rl["venue_code"],
        )
        total_rl += 1
    print(f"      race_log: {total_rl:,} 行インデックス化完了")

    conn.close()

    # 集計処理
    print("[3/4] 集計中...")
    matched = 0
    skipped_no_log = 0
    skipped_no_composite = 0
    skipped_banei = 0

    for i, pred_row in enumerate(pred_rows):
        if (i + 1) % 5000 == 0 or (i + 1) == len(pred_rows):
            pct = (i + 1) / len(pred_rows) * 100
            print(f"      [{i+1:,}/{len(pred_rows):,}] {pct:.1f}%", flush=True)

        race_id = pred_row["race_id"]
        horses = json.loads(pred_row["horses_json"] or "[]")
        log_for_race = race_log_index.get(race_id, {})

        for horse in horses:
            horse_no = horse.get("horse_no")
            composite = horse.get("composite")

            if horse_no is None or composite is None:
                skipped_no_composite += 1
                continue

            # race_log と突合
            log_entry = log_for_race.get(horse_no)
            if log_entry is None:
                skipped_no_log += 1
                continue

            finish_pos, is_jra, venue_code = log_entry

            # ばんえい除外
            org = get_org(is_jra, venue_code)
            if org is None:
                skipped_banei += 1
                continue

            # bin 分類
            bin_label = composite_to_bin(composite)
            if bin_label is None:
                skipped_no_composite += 1
                continue

            # 集計 (ALL も同時に)
            for target_org in (org, "ALL"):
                c = counts[target_org][bin_label]
                c["n"] += 1
                if finish_pos == 1:
                    c["win"] += 1
                if finish_pos <= 2:
                    c["place2"] += 1
                if finish_pos <= 3:
                    c["place3"] += 1

            matched += 1

    print(f"      突合成功: {matched:,} 頭")
    print(f"      スキップ(race_log未突合): {skipped_no_log:,}")
    print(f"      スキップ(composite/馬番なし): {skipped_no_composite:,}")
    print(f"      スキップ(ばんえい): {skipped_banei:,}")

    return dict(counts)


def build_rate_table(counts: dict) -> dict:
    """
    カウントを率(%) に変換した出力辞書を構築する。
    {org: {bin: {n, win(%), place2(%), place3(%)}}}
    """
    result = {}
    for org, bins in counts.items():
        result[org] = {}
        for label in BIN_LABELS:
            c = bins.get(label, {"n": 0, "win": 0, "place2": 0, "place3": 0})
            n = c["n"]
            if n == 0:
                result[org][label] = {"n": 0, "win": None, "place2": None, "place3": None}
            else:
                result[org][label] = {
                    "n": n,
                    "win": round(c["win"] / n * 100, 1),
                    "place2": round(c["place2"] / n * 100, 1),
                    "place3": round(c["place3"] / n * 100, 1),
                }
    return result


def write_outputs(rate_table: dict) -> None:
    """JSON と CSV を書き出す。"""
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # JSON
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rate_table, f, ensure_ascii=False, indent=2)
    print(f"[4/4] JSON 出力: {OUT_JSON}")

    # CSV
    orgs_ordered = ["ALL", "JRA", "NAR"]
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        header = ["org", "composite_bin", "n", "win%", "place2%", "place3%"]
        writer.writerow(header)
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
    print(f"      CSV 出力: {OUT_CSV}")


def print_verification(rate_table: dict) -> None:
    """
    検証: 別途確認済みの実測値と照合して報告する。

    期待値:
      偏差値<30  → 勝1.2% / 複6.9%
      50-54     → 勝9.1% / 複32.1%
      65-69     → 勝15.9% / 複46.7%
      75+       → 勝33.5% / 複69.3%
    """
    EXPECTED = {
        "<30":   {"win": 1.2,  "place3": 6.9},
        "50-54": {"win": 9.1,  "place3": 32.1},
        "65-69": {"win": 15.9, "place3": 46.7},
        "75+":   {"win": 33.5, "place3": 69.3},
    }

    print()
    print("=" * 70)
    print("【検証】偏差値別実率 vs 期待値 (ALL)")
    print(f"{'bin':<8} {'N':>7}  {'勝%(実)':>8} {'勝%(期)':>8}  {'複%(実)':>8} {'複%(期)':>8}  判定")
    print("-" * 70)

    all_ok = True
    all_data = rate_table.get("ALL", {})
    for label, exp in EXPECTED.items():
        row_data = all_data.get(label)
        if row_data is None or row_data["n"] == 0:
            print(f"{label:<8} {'─':>7}  データなし")
            all_ok = False
            continue
        actual_win = row_data["win"]
        actual_p3 = row_data["place3"]
        n = row_data["n"]
        # 許容差: ±3pt
        ok_win = abs(actual_win - exp["win"]) <= 3.0
        ok_p3 = abs(actual_p3 - exp["place3"]) <= 3.0
        ok_str = "OK" if (ok_win and ok_p3) else "※要確認"
        if not (ok_win and ok_p3):
            all_ok = False
        print(
            f"{label:<8} {n:>7,}  {actual_win:>7.1f}% {exp['win']:>7.1f}%  "
            f"{actual_p3:>7.1f}% {exp['place3']:>7.1f}%  {ok_str}"
        )

    print("-" * 70)
    if all_ok:
        print("→ 全ビン OK (±3pt 以内)")
    else:
        print("→ 要確認あり: 突合キーや組織判定を見直してください")

    # ALL の全 bin 表示
    print()
    print("【全 bin テーブル (ALL)】")
    print(f"{'bin':<8} {'N':>7}  {'勝%':>6}  {'連%':>6}  {'複%':>6}")
    print("-" * 40)
    for label in BIN_LABELS:
        d = all_data.get(label)
        if d is None or d["n"] == 0:
            print(f"{label:<8} {'─':>7}")
            continue
        win_s = f"{d['win']:.1f}" if d['win'] is not None else " ─"
        p2_s  = f"{d['place2']:.1f}" if d['place2'] is not None else " ─"
        p3_s  = f"{d['place3']:.1f}" if d['place3'] is not None else " ─"
        print(f"{label:<8} {d['n']:>7,}  {win_s:>6}  {p2_s:>6}  {p3_s:>6}")


def main() -> None:
    print("=" * 70)
    print("偏差値別実率テーブル生成 (本番非改変・読み取り専用)")
    print(f"DB: {DB_PATH}")
    print("=" * 70)

    counts = fetch_and_aggregate()
    rate_table = build_rate_table(counts)
    write_outputs(rate_table)
    print_verification(rate_table)

    print()
    print("=" * 70)
    print("完了 — DB は一切変更していません")
    print("=" * 70)


if __name__ == "__main__":
    main()
