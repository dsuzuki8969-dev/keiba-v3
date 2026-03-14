#!/usr/bin/env python
"""
VENUE_COMPOSITE_WEIGHTS 自動較正スクリプト（6因子版）

race_log のデータから競馬場別の6因子（能力/展開/適性/騎手/調教師/血統）の
寄与度を推定し、VENUE_COMPOSITE_WEIGHTS の推奨値を出力する。

アルゴリズム:
  - race_log から騎手/調教師/父馬の通算複勝率、脚質、馬場情報を取得
  - 6つのproxy特徴量を構築:
    x1: 騎手通算複勝率            → 「能力」proxy（強い馬に強い騎手が乗る）
    x2: 脚質×ペース圧相互作用      → 「展開」proxy
    x3: (surface, distance, condition) → 「適性」proxy（面×距離×馬場の平均着順）
    x4: 騎手の当競馬場成績偏差      → 「騎手」proxy（騎手の場別の得意/不得意）
    x5: 調教師の当競馬場成績偏差    → 「調教師」proxy
    x6: 父馬の通算複勝率           → 「血統」proxy
  - 競馬場ごとにロジスティック回帰: y = finish_pos <= 3
  - 標準化係数の絶対値から各要素の相対重みを算出
  - 結果を data/models/venue_weights_calibrated.json に保存

Usage:
    python scripts/calibrate_venue_weights.py
    python scripts/calibrate_venue_weights.py --update-settings
"""
import argparse
import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.stdout.reconfigure(encoding="utf-8")

# ------------------------------------------------------------------ パス定数
_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DB_PATH = os.path.join(_BASE, "data", "keiba.db")
SETTINGS_PATH = os.path.join(_BASE, "config", "settings.py")
OUTPUT_JSON = os.path.join(_BASE, "data", "models", "venue_weights_calibrated.json")
LOG_PATH = os.path.join(_BASE, "log", "calibrate_venue.log")

# ------------------------------------------------------------------ 競馬場コード→名前
VENUE_CODE_TO_NAME: dict[str, str] = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
    "30": "門別", "35": "盛岡", "36": "水沢",
    "42": "浦和", "43": "船橋", "44": "大井", "45": "川崎",
    "46": "金沢", "47": "笠松", "48": "名古屋",
    "49": "園田", "50": "園田",
    "51": "姫路", "52": "帯広(ばんえい)", "54": "高知", "55": "佐賀",
    "65": "帯広(ばんえい)",
}

VENUE_CODE_TO_SETTINGS_NAME: dict[str, str] = {
    **VENUE_CODE_TO_NAME,
    "52": "帯広", "65": "帯広",
}

# 較正不可能な競馬場コード（ばんえい）
SKIP_VENUE_CODES: set[str] = {"65", "52"}

# 6因子の名前と最低ウェイト
FACTOR_NAMES = ["ability", "pace", "course", "jockey", "trainer", "bloodline"]
FACTOR_FLOOR = {
    "ability": 0.15, "pace": 0.05, "course": 0.05,
    "jockey": 0.03, "trainer": 0.02, "bloodline": 0.02,
}


# ==================================================================
# データ読み込み
# ==================================================================
def load_race_log_6factor(min_samples: int = 100):
    """
    race_log から6因子较正用データを構築。

    Returns:
        data_by_venue: {vc: {rows: [(...)]}}   各行は (y, x1..x6) のタプル
        stats: デバッグ用統計
    """
    import sqlite3

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    MIN_RIDES = 10

    # ===== グローバル統計を先に計算 =====
    print("  グローバル統計を計算中...")

    # 騎手の全体複勝率
    jockey_pr: dict[str, float] = {}
    for row in conn.execute("""
        SELECT jockey_id, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0 AND jockey_id != ''
        GROUP BY jockey_id HAVING n >= ?
    """, (MIN_RIDES,)):
        jockey_pr[row[0].strip()] = row[2] / row[1]
    print(f"    騎手: {len(jockey_pr)} 名")

    # 調教師の全体複勝率
    trainer_pr: dict[str, float] = {}
    for row in conn.execute("""
        SELECT trainer_id, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0 AND trainer_id != ''
        GROUP BY trainer_id HAVING n >= ?
    """, (MIN_RIDES,)):
        trainer_pr[row[0].strip()] = row[2] / row[1]
    print(f"    調教師: {len(trainer_pr)} 名")

    # 父馬の全体複勝率（sire_name ベース）
    sire_pr: dict[str, float] = {}
    for row in conn.execute("""
        SELECT sire_name, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0 AND sire_name IS NOT NULL AND sire_name != ''
        GROUP BY sire_name HAVING n >= ?
    """, (MIN_RIDES,)):
        sire_pr[row[0].strip()] = row[2] / row[1]
    print(f"    父馬: {len(sire_pr)} 名")

    # 騎手×競馬場の複勝率（騎手proxyとの差分 = 場別得意/不得意）
    jockey_venue_pr: dict[tuple, float] = {}
    for row in conn.execute("""
        SELECT jockey_id, venue_code, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0 AND jockey_id != ''
        GROUP BY jockey_id, venue_code HAVING n >= 5
    """):
        jid = row[0].strip()
        vc = str(row[1]).strip().zfill(2)
        jockey_venue_pr[(jid, vc)] = row[3] / row[2]
    print(f"    騎手×競馬場: {len(jockey_venue_pr)} ペア")

    # 調教師×競馬場の複勝率
    trainer_venue_pr: dict[tuple, float] = {}
    for row in conn.execute("""
        SELECT trainer_id, venue_code, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0 AND trainer_id != ''
        GROUP BY trainer_id, venue_code HAVING n >= 5
    """):
        tid = row[0].strip()
        vc = str(row[1]).strip().zfill(2)
        trainer_venue_pr[(tid, vc)] = row[3] / row[2]
    print(f"    調教師×競馬場: {len(trainer_venue_pr)} ペア")

    # (surface, distance_cat, condition) 別の平均複勝率 → 適性proxy
    # 距離を4カテゴリに区分
    def _dist_cat(d):
        if d <= 1400: return "S"
        if d <= 1800: return "M"
        if d <= 2200: return "I"
        return "L"

    context_pr: dict[tuple, float] = {}
    for row in conn.execute("""
        SELECT surface, distance, condition, COUNT(*) AS n,
               SUM(CASE WHEN finish_pos <= 3 THEN 1 ELSE 0 END) AS p
        FROM race_log WHERE finish_pos > 0
        GROUP BY surface, distance, condition HAVING n >= 10
    """):
        surf = (row[0] or "").strip()
        dist = row[1]
        cond = (row[2] or "").strip()
        dc = _dist_cat(dist)
        key = (surf, dc, cond)
        # 同一キーに複数distance がマッピングされるので累積
        if key not in context_pr:
            context_pr[key] = [0, 0]
        context_pr[key][0] += row[4]  # place3
        context_pr[key][1] += row[3]  # n
    # 最終比率化
    context_pr_final: dict[tuple, float] = {}
    for k, (p, n) in context_pr.items():
        if n >= 10:
            context_pr_final[k] = p / n
    print(f"    面×距離帯×馬場: {len(context_pr_final)} パターン")

    # 脚質エンコード（展開proxy）
    STYLE_MAP = {"逃げ": 0.9, "先行": 0.6, "差し": 0.3, "追込": 0.1}

    # レース別の先行馬比率（ペース圧）→ race_id 別に事前集計
    print("  レース別ペース圧を計算中...")
    race_pace: dict[str, float] = {}
    for row in conn.execute("""
        SELECT race_id, COUNT(*) AS n,
               SUM(CASE WHEN running_style IN ('逃げ','先行') THEN 1 ELSE 0 END) AS front
        FROM race_log WHERE finish_pos > 0 AND running_style IS NOT NULL
        GROUP BY race_id
    """):
        rid = str(row[0]).strip()
        n = row[1]
        race_pace[rid] = row[2] / n if n > 0 else 0.3
    print(f"    レース数: {len(race_pace)}")

    # ===== 行ごとのデータ構築 =====
    print("  行データを構築中...")
    data_by_venue: dict[str, list] = defaultdict(list)
    total_rows = 0
    skipped = 0

    JOCKEY_PR_DEFAULT = 0.25
    TRAINER_PR_DEFAULT = 0.20
    SIRE_PR_DEFAULT = 0.22
    CONTEXT_PR_DEFAULT = 0.25

    query = """
        SELECT race_id, venue_code, finish_pos, jockey_id, trainer_id,
               running_style, surface, distance, condition, sire_name
        FROM race_log
        WHERE finish_pos IS NOT NULL AND finish_pos > 0
    """
    for row in conn.execute(query):
        rid = str(row[0]).strip()
        vc = str(row[1]).strip().zfill(2)
        fp = row[2]
        jid = str(row[3] or "").strip()
        tid = str(row[4] or "").strip()
        style = (row[5] or "").strip()
        surf = (row[6] or "").strip()
        dist = row[7] or 1600
        cond = (row[8] or "").strip()
        sire = (row[9] or "").strip()

        if jid in ("001", "002", "003"):
            skipped += 1
            continue

        y = 1 if fp <= 3 else 0

        # x1: 能力proxy（騎手の通算複勝率）
        x1 = jockey_pr.get(jid, JOCKEY_PR_DEFAULT)

        # x2: 展開proxy（脚質×ペース圧の相互作用）
        style_val = STYLE_MAP.get(style, 0.5)
        pace_val = race_pace.get(rid, 0.3)
        # 追込(0.1)×ハイペース(0.5+) = 高スコア、逃げ(0.9)×ハイペース = 低スコア
        x2 = (1.0 - style_val) * pace_val + style_val * (1.0 - pace_val)

        # x3: 適性proxy（面×距離帯×馬場状態の平均複勝率）
        dc = _dist_cat(dist)
        x3 = context_pr_final.get((surf, dc, cond), CONTEXT_PR_DEFAULT)

        # x4: 騎手proxy（当競馬場の複勝率 − 全体複勝率 → 場別の偏差）
        jv = jockey_venue_pr.get((jid, vc))
        jg = jockey_pr.get(jid, JOCKEY_PR_DEFAULT)
        x4 = (jv - jg) if jv is not None else 0.0

        # x5: 調教師proxy（当競馬場の複勝率 − 全体複勝率）
        tv = trainer_venue_pr.get((tid, vc))
        tg = trainer_pr.get(tid, TRAINER_PR_DEFAULT)
        x5 = (tv - tg) if tv is not None else 0.0

        # x6: 血統proxy（父馬の通算複勝率）
        x6 = sire_pr.get(sire, SIRE_PR_DEFAULT)

        data_by_venue[vc].append((y, x1, x2, x3, x4, x5, x6))
        total_rows += 1

    conn.close()

    print(f"  総行数: {total_rows:,} (スキップ: {skipped:,})")
    print(f"  競馬場数: {len(data_by_venue)}")
    for vc in sorted(data_by_venue.keys()):
        n = len(data_by_venue[vc])
        name = VENUE_CODE_TO_NAME.get(vc, vc)
        print(f"    {name:12s} ({vc}): {n:>6,} 件")

    return data_by_venue


# ==================================================================
# 較正メイン
# ==================================================================
def calibrate_6factor(data_by_venue: dict, min_samples: int = 100) -> dict:
    """
    競馬場別に6因子ロジスティック回帰を実施。

    Returns:
        {vc: {"ability": f, "pace": f, "course": f, "jockey": f, "trainer": f, "bloodline": f, "n_samples": n}}
    """
    import numpy as np
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    results = {}

    for vc in sorted(data_by_venue.keys()):
        rows = data_by_venue[vc]
        n = len(rows)
        name = VENUE_CODE_TO_NAME.get(vc, vc)

        if vc in SKIP_VENUE_CODES:
            print(f"  {name:14s} ({vc}): ばんえい → スキップ")
            continue
        if n < min_samples:
            print(f"  {name:14s} ({vc}): サンプル不足 ({n:,} < {min_samples}) → スキップ")
            continue

        arr = np.array(rows, dtype=np.float32)
        y = arr[:, 0].astype(np.int8)
        X = arr[:, 1:]  # (n, 6)

        n_pos = int(y.sum())
        n_neg = n - n_pos
        if n_pos < 10 or n_neg < 10:
            print(f"  {name:14s} ({vc}): 正例/負例不足 → スキップ")
            continue

        # 分散チェック: 分散ゼロの特徴量はスキップ
        stds = np.std(X, axis=0)
        valid_mask = stds > 1e-6
        if valid_mask.sum() < 2:
            print(f"  {name:14s} ({vc}): 特徴量分散不足 → スキップ")
            continue

        try:
            scaler = StandardScaler()
            X_sc = scaler.fit_transform(X)

            lr = LogisticRegression(max_iter=500, C=1.0, solver="lbfgs")
            lr.fit(X_sc, y)

            raw_coefs = lr.coef_[0]  # (6,)
            abs_coefs = np.abs(raw_coefs)

            # 分散ゼロの特徴量は係数を0にリセット
            for i in range(6):
                if not valid_mask[i]:
                    abs_coefs[i] = 0.0

            total_ab = abs_coefs.sum()
            if total_ab < 1e-9:
                # 全係数ゼロ → デフォルト
                weights = {"ability": 0.40, "pace": 0.25, "course": 0.15,
                           "jockey": 0.10, "trainer": 0.05, "bloodline": 0.05}
            else:
                # 各因子の相対重みを係数絶対値から算出
                raw_weights = {}
                for i, fname in enumerate(FACTOR_NAMES):
                    raw_weights[fname] = float(abs_coefs[i] / total_ab)

                # フロア制約を適用（最低重みを保証）
                weights = {}
                floor_total = sum(FACTOR_FLOOR.values())  # 0.32
                remain = 1.0 - floor_total  # 0.68
                raw_total = sum(raw_weights.values())

                for fname in FACTOR_NAMES:
                    weights[fname] = FACTOR_FLOOR[fname] + remain * (raw_weights[fname] / raw_total if raw_total > 0 else 1/6)

                # 正規化して合計1.0に
                w_sum = sum(weights.values())
                for fname in FACTOR_NAMES:
                    weights[fname] = round(weights[fname] / w_sum, 3)

                # 丸め誤差補正
                diff = round(1.0 - sum(weights.values()), 3)
                weights["ability"] = round(weights["ability"] + diff, 3)

            results[vc] = {**weights, "n_samples": n}

            coef_str = " ".join(f"{FACTOR_NAMES[i][:3]}={raw_coefs[i]:+.3f}" for i in range(6))
            weight_str = " ".join(f"{fname[:3]}={weights[fname]:.3f}" for fname in FACTOR_NAMES)
            print(f"  {name:10s} ({vc}): {weight_str}  n={n:>6,}  coef=[{coef_str}]")

        except Exception as exc:
            print(f"  {name} ({vc}): 回帰エラー → {exc}")

    return results


# ==================================================================
# 結果保存
# ==================================================================
def save_results(calibrated: dict, update_settings: bool = False) -> None:
    """較正結果をJSONに保存。オプションでsettings.pyも更新。"""
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)

    # JSON 保存
    output_data = {}
    for vc, d in calibrated.items():
        output_data[vc] = {fname: d[fname] for fname in FACTOR_NAMES}
        output_data[vc]["n_samples"] = d["n_samples"]

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    print(f"\n較正結果を保存: {OUTPUT_JSON}")

    # settings.py 推奨更新内容を表示
    print("\n=== settings.py VENUE_COMPOSITE_WEIGHTS 推奨更新内容 ===")
    for vc in sorted(calibrated.keys()):
        d = calibrated[vc]
        name = VENUE_CODE_TO_SETTINGS_NAME.get(vc, VENUE_CODE_TO_NAME.get(vc, vc))
        w_str = ", ".join(f'"{f}": {d[f]}' for f in FACTOR_NAMES)
        print(f'    "{name}": {{{w_str}}},  # n={d["n_samples"]:,}')
    print("=" * 70)

    if update_settings:
        _patch_settings_py(calibrated)
    else:
        print("\n(--update-settings なしのため settings.py は更新しません)")


def _patch_settings_py(calibrated: dict) -> None:
    """config/settings.py の VENUE_COMPOSITE_WEIGHTS を較正値で更新。"""
    import re

    with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # ブロック検出
    in_block = False
    block_start = block_end = -1
    for i, line in enumerate(lines):
        if "VENUE_COMPOSITE_WEIGHTS" in line and ":" in line and "dict" in line.split("=")[0]:
            in_block = True
            block_start = i
        if in_block and line.strip() == "}":
            block_end = i
            break

    if block_start == -1 or block_end == -1:
        print("  [警告] VENUE_COMPOSITE_WEIGHTS ブロックが見つかりません")
        return

    pattern = re.compile(r'^(\s*)"([^"]+)":\s*\{[^}]+\}')
    new_lines = list(lines)
    updated = []

    for i in range(block_start + 1, block_end):
        m = pattern.match(lines[i])
        if not m:
            continue
        indent = m.group(1)
        venue_name = m.group(2)
        vc = next((k for k, v in VENUE_CODE_TO_SETTINGS_NAME.items() if v == venue_name), None)
        if vc is None or vc not in calibrated:
            continue
        d = calibrated[vc]
        w_str = ", ".join(f'"{f}": {d[f]}' for f in FACTOR_NAMES)
        new_lines[i] = f'{indent}"{venue_name}": {{{w_str}}},  # 自動較正 n={d["n_samples"]:,}\n'
        updated.append(venue_name)

    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        f.writelines(new_lines)
    print(f"\n  settings.py を更新: {len(updated)} 競馬場")
    for vn in updated:
        print(f"    - {vn}")


# ==================================================================
# エントリポイント
# ==================================================================
def main():
    parser = argparse.ArgumentParser(description="競馬場別6因子重み自動較正")
    parser.add_argument("--min-samples", type=int, default=100)
    parser.add_argument("--update-settings", action="store_true",
                        help="settings.py を自動更新する")
    args = parser.parse_args()

    print("=" * 60)
    print("  VENUE_COMPOSITE_WEIGHTS 6因子自動較正")
    print("=" * 60)
    print(f"  最小サンプル数: {args.min_samples}")
    print(f"  settings.py更新: {'yes' if args.update_settings else 'no'}")
    print()

    if not os.path.exists(DB_PATH):
        print(f"[エラー] DB が見つかりません: {DB_PATH}")
        sys.exit(1)

    # データ読み込み
    print("--- データ読み込み ---")
    data_by_venue = load_race_log_6factor(args.min_samples)
    total = sum(len(v) for v in data_by_venue.values())
    print(f"\n  合計: {len(data_by_venue)} 競馬場 / {total:,} レコード")

    if total == 0:
        print("[警告] データがありません")
        sys.exit(0)

    # 較正
    print("\n--- 競馬場別6因子較正 (ロジスティック回帰) ---")
    calibrated = calibrate_6factor(data_by_venue, min_samples=args.min_samples)

    if not calibrated:
        print("[警告] 較正結果が 0 件")
        sys.exit(0)

    print(f"\n  較正完了: {len(calibrated)} 競馬場")

    # 保存
    save_results(calibrated, update_settings=args.update_settings)

    # ログ保存
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    # （ログはstdout経由でリダイレクトされる想定）

    print("\n=== 完了 ===")
    print(f"  結果ファイル: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
