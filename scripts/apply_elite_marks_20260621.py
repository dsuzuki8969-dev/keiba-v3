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

importable 関数: apply_elite_and_formation(date_key, backup=True) -> dict

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
from src.calculator.betting import (
    build_force_buy_columns,
    compute_danso_columns,
    is_no_bet_race_type,
)


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


def _is_excluded_race(race: Dict) -> bool:
    """ばんえい / メイクデビュー / 障害 の除外レース判定。

    - ばんえい: venue == "帯広"
    - メイクデビュー / 障害: race_name に含む (is_no_bet_race_type 共通ヘルパー)
    """
    if race.get("venue", "") == "帯広":
        return True
    race_name = race.get("race_name", "") or ""
    return is_no_bet_race_type(race_name)


def _clear_race_formation(race: Dict) -> None:
    """formation 関連フィールドを明示クリアする (stale 防止)。

    見送り / 除外レースに対して呼ぶ。formation_columns・tickets・bet_decision.skip
    をリセットして旧パターンのゴミデータを残さない。
    """
    race["formation_columns"] = {}

    tbm = race.get("tickets_by_mode", {})
    for mode in ("fixed", "accuracy", "balanced", "recovery"):
        tbm[mode] = []
    meta = tbm.get("_meta", {})
    meta["skipped"] = True
    meta["skip_reason"] = "danso_no_fire"
    meta["ticket_count"] = 0
    meta["stake_total"] = 0
    meta["sanrenpuku_count"] = 0
    meta["danso_formation"] = None
    meta["format"] = "danso:印断層三連複"  # 見送りも danso カードで表示(skip パネル)
    meta["formation_columns"] = {}
    tbm["_meta"] = meta
    race["tickets_by_mode"] = tbm

    bet = race.get("bet_decision", {})
    bet["skip"] = True
    bet["total_stake"] = 0
    bet["ticket_count"] = 0
    bet["sanren_count"] = 0
    bet["tansho_count"] = 0
    race["bet_decision"] = bet

    # 旧キー tickets も空にする
    race["tickets"] = []


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
    # 印断層フォーマット明示: frontend の isDansoFormat 判定(format.startsWith("danso:"))用。
    # 未設定だと旧 M' format が残り MPrimeFormation で誤表示される(2026-06-22)。
    meta["format"] = "danso:印断層三連複"
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
# Public importable 関数
# ============================================================

def apply_elite_and_formation(
    date_key: str,
    *,
    backup: bool = True,
    dry_run: bool = False,
) -> Dict:
    """elite(◉/穴) 付与 → 4パターンformation → force-buy → bet_decision 再生成。

    col1=◉単独・force-buy fallback・bet_decision.skip 制御の既存挙動は厳守。

    Parameters
    ----------
    date_key : str
        YYYYMMDD 形式の日付文字列
    backup : bool
        True の場合 .bak_elite バックアップを作成
    dry_run : bool
        True の場合、保存せず結果 dict のみ返す

    Returns
    -------
    dict
        {
            "pivot_count": int,
            "ana_count": int,
            "force_buy_count": int,
            "updated_count": int,
            "pivot_list": [(race_id, horse_no), ...],
            "ana_list": [(race_id, horse_no), ...],
        }
    """
    path = PRED_DIR / f"{date_key}_pred.json"
    if not path.exists():
        raise FileNotFoundError(f"pred.json が見つかりません: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    races: List[Dict] = data.get("races", [])
    print(f"[elite] 開始: {date_key}  レース数={len(races)}")

    # ── 既存の◉/穴印をリセット (冪等性確保) ──
    # 2回目実行時に前回付与済みの◉/穴が残っていると select_dark_horses の
    # 除外条件で候補集合が変わり、印数が変化してしまう。
    # ◉ → ◎ (本命に戻す), 穴 → "" (無印に戻す) でクリーンな状態から再選定する。
    reset_tekipan = 0
    reset_ana = 0
    for r in races:
        for h in r.get("horses", []):
            mk = h.get("mark", "")
            if mk == "◉":
                h["mark"] = "◎"
                reset_tekipan += 1
            elif mk == "穴":
                h["mark"] = ""
                reset_ana += 1
    if reset_tekipan or reset_ana:
        print(f"[elite] 既存印リセット: ◉→◎ {reset_tekipan}頭 / 穴→無印 {reset_ana}頭")

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

    force_buy_races: List[str] = []
    updated_count = 0
    cleared_count = 0   # 見送りクリア件数
    excluded_count = 0  # 除外レース件数

    print(f"[買い目再生成] 全レース処理 (affected={len(affected_race_ids)} / total={len(races)})")
    for r in races:
        rid = r.get("race_id", "")

        # ── 除外レース (ばんえい / メイクデビュー / 障害) ──
        if _is_excluded_race(r):
            print(f"  EXCLUDED  {r.get('venue','')} {r.get('race_no','')}R  (ばんえい/新馬/障害)")
            if not dry_run:
                _clear_race_formation(r)
            excluded_count += 1
            continue

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
            # 見送り: stale formation を明示クリア
            print(f"  CLEAR  {r.get('venue','')} {r.get('race_no','')}R  (danso非発火・◉/穴なし → formation クリア)")
            if not dry_run:
                _clear_race_formation(r)
            cleared_count += 1
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
        print(f"  OK  {r.get('venue','')} {r.get('race_no','')}R  [{action}]  col1={col1} col2={col2} col3={col3}")

        if not dry_run:
            _update_race(r, fc_result, affected_race_ids)
        updated_count += 1

    print()
    print(f"[サマリ] ◉付与={len(pivot_list)}頭 / 穴付与={len(ana_list)}頭 / "
          f"強制購入={len(force_buy_races)}レース / 更新レース={updated_count} / "
          f"見送りクリア={cleared_count} / 除外={excluded_count}")
    print()

    if dry_run:
        print("[elite] dry-run: 保存なし")
        return {
            "pivot_count": len(pivot_list),
            "ana_count": len(ana_list),
            "force_buy_count": len(force_buy_races),
            "updated_count": updated_count,
            "cleared_count": cleared_count,
            "excluded_count": excluded_count,
            "pivot_list": pivot_list,
            "ana_list": ana_list,
            "dry_run": True,
        }

    # ── 保存 ──
    if backup:
        bak = _backup(path)
        print(f"[elite] バックアップ: {bak.name}")

    with open(str(path), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[elite] 保存完了: {path.name}")

    return {
        "pivot_count": len(pivot_list),
        "ana_count": len(ana_list),
        "force_buy_count": len(force_buy_races),
        "updated_count": updated_count,
        "cleared_count": cleared_count,
        "excluded_count": excluded_count,
        "pivot_list": pivot_list,
        "ana_list": ana_list,
        "dry_run": False,
    }


# ============================================================
# メイン処理 (CLI)
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="印体系刷新 Phase 2+3 即時適用")
    parser.add_argument("--date", default="20260621", help="pred.json の日付 (デフォルト: 20260621)")
    parser.add_argument("--dry-run", action="store_true", help="対象を表示するのみ・保存しない")
    args = parser.parse_args()

    print(f"[起動] {'DRY-RUN' if args.dry_run else '本番適用'} 対象日: {args.date}")

    apply_elite_and_formation(args.date, backup=True, dry_run=args.dry_run)

    if args.dry_run:
        print("[DRY-RUN] 保存なし。--dry-run を外して実行すると pred.json を更新します。")


if __name__ == "__main__":
    main()
