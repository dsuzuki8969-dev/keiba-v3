"""
Plan-γ Phase 2 動作確認スクリプト
pred.json の race_relative_dev フィールドが正しく出力されているか検証する。

確認内容:
1. race_relative_dev フィールドが全馬に NOT NULL で存在するか
2. 各レース内の race_relative_dev の分布が μ≈50 / σ≈10 に近いか
3. 全レースの min/max/avg を表示
"""

import json
import sys
import statistics
from pathlib import Path

# プロジェクトルートを sys.path に追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config.settings import PREDICTIONS_DIR


def verify_race_relative_dev(pred_file: Path) -> None:
    """pred.json を読み込み、race_relative_dev の分布を確認する"""
    print(f"\n== 検証対象: {pred_file.name} ==")

    with open(pred_file, encoding="utf-8") as f:
        data = json.load(f)

    races = data.get("races", [])
    if not races:
        print("[WARN] races キーが空です")
        return

    total_horses = 0
    null_count = 0
    all_values: list[float] = []
    race_summaries = []

    for race in races:
        horses = race.get("horses", [])
        race_id = race.get("race_id", "?")
        race_name = race.get("race_name", "?")

        race_values = []
        race_null = 0
        for h in horses:
            rrd = h.get("race_relative_dev")
            total_horses += 1
            if rrd is None:
                null_count += 1
                race_null += 1
            else:
                race_values.append(float(rrd))
                all_values.append(float(rrd))

        if race_values:
            mu = statistics.mean(race_values)
            sigma = statistics.stdev(race_values) if len(race_values) >= 2 else 0.0
            race_summaries.append({
                "race_id": race_id,
                "race_name": race_name,
                "n": len(race_values),
                "null": race_null,
                "min": min(race_values),
                "max": max(race_values),
                "mu": mu,
                "sigma": sigma,
            })
        else:
            race_summaries.append({
                "race_id": race_id,
                "race_name": race_name,
                "n": 0,
                "null": race_null,
                "min": None,
                "max": None,
                "mu": None,
                "sigma": None,
            })

    # ---- 全体サマリ ----
    print(f"\n[全体] 総馬数={total_horses}, NULL={null_count}")
    if all_values:
        g_mu = statistics.mean(all_values)
        g_sigma = statistics.stdev(all_values) if len(all_values) >= 2 else 0.0
        print(f"[全体] μ={g_mu:.2f}, σ={g_sigma:.2f}, min={min(all_values):.2f}, max={max(all_values):.2f}")
        if 45 <= g_mu <= 55 and 8 <= g_sigma <= 12:
            print("[OK] 全体 μ≈50 / σ≈10 — 正常範囲")
        else:
            print("[WARN] 全体分布が期待値（μ≈50, σ≈10）から外れています")

    # ---- レース別詳細 ----
    print(f"\n{'race_id':>20} {'race_name':>20} {'n':>3} {'null':>4} {'min':>6} {'max':>6} {'μ':>6} {'σ':>6}")
    print("-" * 80)
    for r in race_summaries:
        mu_s = f"{r['mu']:.1f}" if r["mu"] is not None else "  N/A"
        sg_s = f"{r['sigma']:.1f}" if r["sigma"] is not None else "  N/A"
        mi_s = f"{r['min']:.1f}" if r["min"] is not None else "  N/A"
        ma_s = f"{r['max']:.1f}" if r["max"] is not None else "  N/A"
        name_short = r["race_name"][:18] if r["race_name"] else ""
        print(f"{r['race_id']:>20} {name_short:>20} {r['n']:>3} {r['null']:>4} {mi_s:>6} {ma_s:>6} {mu_s:>6} {sg_s:>6}")

    # ---- NULL チェック ----
    if null_count == 0:
        print("\n[OK] 全馬に race_relative_dev フィールドが存在します（NULL なし）")
    else:
        print(f"\n[WARN] {null_count} 馬で race_relative_dev が NULL です（field_count 不足等の可能性）")


def main() -> None:
    pred_dir = Path(PREDICTIONS_DIR)
    if not pred_dir.exists():
        print(f"[ERROR] PREDICTIONS_DIR が存在しません: {pred_dir}")
        sys.exit(1)

    # 引数でファイル指定 or 最新ファイルを自動選択
    if len(sys.argv) >= 2:
        target = Path(sys.argv[1])
        if not target.exists():
            target = pred_dir / sys.argv[1]
        if not target.exists():
            print(f"[ERROR] ファイルが見つかりません: {sys.argv[1]}")
            sys.exit(1)
        files = [target]
    else:
        # 最新の pred.json を 1 件選択（_prev.json は除外）
        files = sorted(
            [p for p in pred_dir.glob("*_pred.json") if "_prev" not in p.name],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:1]

    if not files:
        print(f"[ERROR] {pred_dir} に *_pred.json が見つかりません")
        sys.exit(1)

    for f in files:
        verify_race_relative_dev(f)


if __name__ == "__main__":
    main()
