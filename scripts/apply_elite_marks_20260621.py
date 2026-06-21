"""
apply_elite_marks_20260621.py — 印体系刷新 Phase 2+3 即時適用スクリプト

処理フロー:
1. pred.json 読み込み
2. apply_daily_elite_marks で ◉/穴 印を付与
3. 影響レース(◉/穴 付与済み)について買い目を再生成
   - compute_danso_columns が発火 → formation_columns 更新
   - 発火しない かつ ◉/穴あり → build_force_buy_columns でフォールバック
4. --dry-run: 対象を表示して保存しない
5. .bak_elite バックアップ付き保存

使用方法:
    python scripts/apply_elite_marks_20260621.py --dry-run
    python scripts/apply_elite_marks_20260621.py
    python scripts/apply_elite_marks_20260621.py --date 20260621

2026-06-21 マスター承認済仕様
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# プロジェクトルートをパスに追加
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from src.calculator.elite_marks import apply_daily_elite_marks
from src.calculator.betting import build_force_buy_columns, compute_danso_columns


# ============================================================
# 定数
# ============================================================

PRED_DIR = _ROOT / "data" / "predictions"
STAKE_PER_POINT = 100  # 1点あたり賭け額（固定モード基準）


# ============================================================
# 内部ヘルパー
# ============================================================

def _load_pred(date_str: str) -> Tuple[Path, Dict]:
    """pred.json を読み込む。"""
    path = PRED_DIR / f"{date_str}_pred.json"
    if not path.exists():
        print(f"[ERROR] pred.json が見つかりません: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return path, json.load(f)


def _backup(path: Path) -> Path:
    """バックアップを作成して返す。"""
    bak = path.with_suffix(path.suffix + ".bak_elite")
    shutil.copy2(str(path), str(bak))
    return bak


def _build_races_flat(races: List[Dict]) -> List[Dict]:
    """pred.json の races list を elite_marks 用フラット形式に変換する。"""
    flat = []
    for r in races:
        flat.append({
            "race_id":  r.get("race_id", ""),
            "race":     {"name": r.get("race_name", ""), "venue": r.get("venue", "")},
            "horses":   r.get("horses", []),
            "is_jra":   bool(r.get("is_jra", False)),
        })
    return flat


def _gen_sanrenpuku_tickets(
    col1: List[int],
    col2: List[int],
    col3: List[int],
    formation: str,
    stake: int = STAKE_PER_POINT,
) -> List[Dict]:
    """col1×col2×col3 の三連複チケットを生成する（odds は未設定）。"""
    seen: Set[Tuple[int, ...]] = set()
    tickets = []
    for a in col1:
        for b in col2:
            for c in col3:
                key = tuple(sorted({a, b, c}))
                if len(key) < 3 or key in seen:
                    continue
                seen.add(key)
                tickets.append({
                    "type": "三連複",
                    "combo": list(key),
                    "stake": stake,
                    "formation": formation,
                    "odds": None,         # 即時適用時は未算出
                    "odds_source": "elite_apply",
                })
    return tickets


def _update_race(
    race: Dict,
    result: Dict,
    affected_race_ids: Set[str],
) -> bool:
    """1レースの formation_columns / tickets_by_mode / bet_decision を更新する。

    Returns: True=更新あり、False=変更なし
    """
    race_id = race.get("race_id", "")
    fc = result
    col1 = fc["col1"]
    col2 = fc["col2"]
    col3 = fc["col3"]
    formation = fc["formation"]

    # formation_columns 更新
    race["formation_columns"] = {"col1": col1, "col2": col2, "col3": col3}

    # チケット生成
    new_tickets = _gen_sanrenpuku_tickets(col1, col2, col3, formation)
    n_tickets = len(new_tickets)

    # 0枚チケット = 三連複不成立 → このレースの更新をスキップ（skip=Falseにしない）
    if not new_tickets:
        return False

    total_stake = n_tickets * STAKE_PER_POINT

    # tickets_by_mode 各モードを更新（チケットリストを差し替え）
    tbm = race.get("tickets_by_mode", {})
    for mode in ("fixed", "accuracy", "balanced", "recovery"):
        tbm[mode] = new_tickets
    # _meta 更新
    meta = tbm.get("_meta", {})
    meta["skipped"] = False
    meta["skip_reason"] = None
    meta["ticket_count"] = n_tickets
    meta["stake_total"] = total_stake
    meta["sanrenpuku_count"] = n_tickets
    meta["danso_formation"] = formation
    meta["formation_columns"] = {"col1": col1, "col2": col2, "col3": col3}
    tbm["_meta"] = meta
    race["tickets_by_mode"] = tbm

    # bet_decision 更新（skip=False に）
    bet = race.get("bet_decision", {})
    bet["skip"] = False
    bet["total_stake"] = total_stake
    bet["ticket_count"] = n_tickets
    bet["sanren_count"] = n_tickets
    bet["tansho_count"] = 0
    race["bet_decision"] = bet

    affected_race_ids.add(race_id)
    return True


# ============================================================
# メイン処理
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="印体系刷新 Phase 2+3 即時適用")
    parser.add_argument("--date", default="20260621", help="pred.json の日付 (デフォルト: 20260621)")
    parser.add_argument("--dry-run", action="store_true", help="対象を表示するのみ・保存しない")
    args = parser.parse_args()

    date_str = args.date
    dry_run = args.dry_run

    print(f"[起動] {'DRY-RUN' if dry_run else '本番適用'} 対象日: {date_str}")

    # ── pred.json 読み込み ──
    path, data = _load_pred(date_str)
    races: List[Dict] = data.get("races", [])
    print(f"[読込] {path.name}: {len(races)} レース")

    # ── races_flat に変換 ──
    races_flat = _build_races_flat(races)

    # ── apply_daily_elite_marks で ◉/穴 印を付与 ──
    result = apply_daily_elite_marks(races_flat)
    pivot_list: List[Tuple[str, int]] = result["pivot"]
    ana_list:   List[Tuple[str, int]] = result["ana"]

    # ── 結果サマリ表示 ──
    print()
    print("=" * 60)
    print(f"◉ 鉄板付与: {len(pivot_list)} 頭")
    for rid, no in pivot_list:
        # 対応する race から馬名を取得
        for r in races:
            if r.get("race_id") == rid:
                for h in r.get("horses", []):
                    if h.get("horse_no") == no:
                        print(f"  {r['venue']} {r['race_no']}R  #{no} {h.get('horse_name','')}  win_prob={h.get('win_prob',0):.3f}")
                break

    print()
    print(f"穴 付与: {len(ana_list)} 頭")
    for rid, no in ana_list:
        for r in races:
            if r.get("race_id") == rid:
                for h in r.get("horses", []):
                    if h.get("horse_no") == no:
                        print(f"  {r['venue']} {r['race_no']}R  #{no} {h.get('horse_name','')}  odds={h.get('odds',0):.1f}倍  popularity={h.get('popularity',0)}人気")
                break

    print("=" * 60)
    print()

    # ── 影響レースの買い目再生成 ──
    pivot_race_ids: Set[str] = {rid for rid, _ in pivot_list}
    ana_race_ids:   Set[str] = {rid for rid, _ in ana_list}
    affected_race_ids: Set[str] = pivot_race_ids | ana_race_ids

    force_buy_races: List[str] = []  # 強制購入(fallback)を使ったレース
    updated_count = 0

    print(f"[買い目再生成] 影響レース: {len(affected_race_ids)} レース")
    for r in races:
        rid = r.get("race_id", "")
        if rid not in affected_race_ids:
            continue

        # horse entries を compute_danso_columns 形式に変換
        horses = r.get("horses", [])
        entries = [
            {
                "mark":         h.get("mark", ""),
                "composite":    float(h.get("composite") or 50.0),
                "horse_no":     int(h.get("horse_no") or 0),
                "odds":         float(h.get("odds") or 10.0),
                "is_scratched": bool(h.get("is_scratched", False)),
            }
            for h in horses
        ]

        # 断層判定
        fc_result = compute_danso_columns(entries)

        if fc_result is None:
            # fallback: ◉/穴 があれば強制購入
            fc_result = build_force_buy_columns(entries)
            if fc_result is not None:
                force_buy_races.append(rid)

        if fc_result is None:
            print(f"  SKIP  {r['venue']} {r['race_no']}R  (danso非発火・◉/穴なし)")
            continue

        col1 = fc_result["col1"]
        col2 = fc_result["col2"]
        col3 = fc_result["col3"]

        # マスター指示(2026-06-22): ◉レースは1列目を◉単独に強制する。
        # danso B型等で col1 が複数頭(◉+○等)でも◉のみに絞り、押し出された
        # 馬番は col2 へ移す(◉は col2/col3 から除外=col1専用)。
        tekipan_no = next(
            (h.get("horse_no") for h in horses
             if h.get("mark") == "◉" and not h.get("is_scratched", False)),
            None,
        )
        if tekipan_no is not None:
            displaced = [n for n in col1 if n != tekipan_no]
            col1 = [tekipan_no]
            col2 = list(dict.fromkeys(
                [n for n in (list(col2) + displaced) if n != tekipan_no]
            ))
            col3 = list(dict.fromkeys([n for n in col3 if n != tekipan_no]))
            fc_result["col1"], fc_result["col2"], fc_result["col3"] = col1, col2, col3

        action = "FORCE-BUY" if fc_result["formation"] == "force_buy" else fc_result["formation"]
        print(f"  OK  {r['venue']} {r['race_no']}R  [{action}]  col1={col1} col2={col2} col3={col3}")

        if not dry_run:
            _update_race(r, fc_result, affected_race_ids)
        updated_count += 1

    print()
    print(f"[サマリ] ◉付与={len(pivot_list)}頭 / 穴付与={len(ana_list)}頭 / 強制購入={len(force_buy_races)}レース / 更新レース={updated_count}")
    print()

    if dry_run:
        print("[DRY-RUN] 保存なし。--dry-run を外して実行すると pred.json を更新します。")
        return

    # ── 保存 ──
    bak = _backup(path)
    print(f"[バックアップ] {bak.name}")

    with open(str(path), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[保存完了] {path.name}")


if __name__ == "__main__":
    main()
