"""過去 pred.json の旧三連単チケットを T-050 戦略 (三連複動的+単勝) で再生成する軽量スクリプト。

マスター指示 2026-05-01: 過去日の予想ページに旧三連単フォーメーションが表示されている問題への対応。
ML モデル再計算は不要 (各馬の印・確率は既存値を流用)、チケット生成のみ T-050 ロジックで実行。

処理:
1. data/predictions/*_pred.json を全件走査 (T-050 commit be24379 以前のみ)
2. 各レースに対して hybrid_summary._layer1_sanrenpuku + _build_*_tickets で再生成
3. tickets_by_mode.fixed / tickets_by_mode._meta.format を上書き
4. tickets フィールド (フラット) も三連複+単勝のみに更新
5. _prev.json バックアップは作成しない (大量・空間圧迫回避)
"""
from __future__ import annotations

import io
import json
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# プロジェクトルートを sys.path に追加
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# hybrid_summary のロジック流用
from src.analytics.hybrid_summary import (
    _layer1_sanrenpuku, _layer1_tansho,
    _build_sanrenpuku_tickets, _build_tansho_tickets,
    _filter_active,
)

PRED_DIR = Path("data/predictions")
RES_DIR = Path("data/results")


def _empty_payouts() -> dict:
    """payouts 取得失敗時の空 dict (チケット生成のみ目的なので payout=0 で進める)"""
    return {"三連複": {"combo": "", "payout": 0}, "単勝": {"combo": "", "payout": 0}}


def regenerate_for_race(race: dict, payouts: dict) -> tuple[list, list, str]:
    """1 レース分のチケット再生成。

    Returns:
        (sanrenpuku_tickets, tansho_tickets, format_str)
        各 ticket: {"type": "三連複"/"単勝", "combo":[..], "horse_no":int(単勝のみ),
                    "mark":str, "odds":float, "ev":float, "stake":100, "payout":0}
    """
    horses = _filter_active(race.get("horses", []))
    if not horses:
        return [], [], "T-050: NoActive"

    # horse_no → horse dict マップ (mark/odds/ev 取得用)
    h_by_no = {h.get("horse_no"): h for h in horses}

    def _h_mark(no: int) -> str:
        h = h_by_no.get(no)
        return (h.get("mark") if h else "") or ""

    def _h_odds(no: int) -> float:
        h = h_by_no.get(no)
        if not h:
            return 0.0
        v = h.get("odds") or h.get("predicted_tansho_odds") or 0
        return float(v)

    def _h_ev(no: int) -> float:
        h = h_by_no.get(no)
        if not h:
            return 0.0
        ev = h.get("ev") or 0
        if ev:
            return float(ev)
        wp = h.get("win_prob") or 0
        o = _h_odds(no)
        return round(float(wp) * o, 4) if (wp and o) else 0.0

    sp_tickets: list[dict] = []
    case = _layer1_sanrenpuku(horses, payouts)
    if case is not None:
        # case == "中" or "広"
        raw = _build_sanrenpuku_tickets(horses, payouts, case)
        for combo, _pb in raw:
            combo_list = list(combo)
            sp_tickets.append({
                "type": "三連複",
                "combo": combo_list,
                "mark_a": _h_mark(combo_list[0]),
                "mark_b": _h_mark(combo_list[1]),
                "mark_c": _h_mark(combo_list[2]),
                "stake": 100,
                "payout": 0,
            })

    tn_tickets: list[dict] = []
    if _layer1_tansho(horses):
        raw = _build_tansho_tickets(horses, payouts)
        for combo, _pb in raw:
            no = combo[0]
            tn_tickets.append({
                "type": "単勝",
                "combo": [no],
                "horse_no": no,
                "mark": _h_mark(no),
                "odds": _h_odds(no),
                "ev": _h_ev(no),
                "stake": 100,
                "payout": 0,
            })

    fmt = f"T-050: 三連複動的 ({case or 'skip'}) + 単勝T-4 ({len(tn_tickets)}点)"
    return sp_tickets, tn_tickets, fmt


def process_pred_file(fp: Path, results_data: dict | None) -> tuple[int, int]:
    """1 pred.json を処理。
    Returns: (modified_races, total_races)
    """
    try:
        data = json.loads(fp.read_text(encoding="utf-8"))
    except Exception:
        return 0, 0

    races = data.get("races", [])
    modified = 0

    for r in races:
        race_id = str(r.get("race_id", ""))

        # payouts 取得 (results.json があれば)
        payouts = _empty_payouts()
        if results_data is not None:
            rdata = results_data.get(race_id)
            if rdata is not None:
                p = rdata.get("payouts", {})
                if "三連複" in p or "sanrenpuku" in p:
                    payouts = p

        sp_tickets, tn_tickets, fmt = regenerate_for_race(r, payouts)
        all_t050 = sp_tickets + tn_tickets

        # tickets_by_mode.fixed と _meta を上書き
        tbm = r.setdefault("tickets_by_mode", {})
        tbm["fixed"] = all_t050
        meta = tbm.setdefault("_meta", {})
        meta["format"] = fmt
        meta["sanrenpuku_count"] = len(sp_tickets)
        meta["tansho_count"] = len(tn_tickets)

        # フラット tickets フィールドも上書き (results_tracker L206 互換)
        r["tickets"] = all_t050

        # bet_decision: T-050 で買い目ありなら skip=False に更新
        # 買い目なしなら T-050 用 skip 表示に書き換え (旧三連単 skip 判定を上書き)
        if all_t050:
            r["bet_decision"] = {"skip": False, "reason": "", "message": ""}
        else:
            r["bet_decision"] = {
                "skip": True,
                "reason": "T-050 発動条件未充足",
                "message": "三連複動的 + 単勝T-4 共に条件未満のため見送り",
            }

        modified += 1

    if modified > 0:
        try:
            fp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            print(f"  保存失敗: {fp.name}: {e}", flush=True)

    return modified, len(races)


def main():
    pred_files = sorted(PRED_DIR.glob("*_pred.json"))
    pred_files = [f for f in pred_files if "_prev" not in f.name]
    print(f"pred ファイル数: {len(pred_files)}")

    started = time.time()
    total_files = 0
    total_races = 0
    modified_races = 0

    for fi, fp in enumerate(pred_files):
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue

        # results.json (payouts 補完用・無くても再生成可能)
        res_fp = RES_DIR / f"{date_str}_results.json"
        results_data = None
        if res_fp.exists():
            try:
                results_data = json.loads(res_fp.read_text(encoding="utf-8"))
            except Exception:
                results_data = None

        m, t = process_pred_file(fp, results_data)
        modified_races += m
        total_races += t
        total_files += 1

        if (fi + 1) % 50 == 0 or (fi + 1) == len(pred_files):
            elapsed = time.time() - started
            print(f"  {fi+1}/{len(pred_files)} ({date_str}) "
                  f"races={total_races} modified={modified_races} "
                  f"elapsed={elapsed:.1f}s", flush=True)

    print(f"\n完了: files={total_files}, total_races={total_races}, "
          f"modified_races={modified_races}, elapsed={time.time()-started:.1f}s")


if __name__ == "__main__":
    main()
