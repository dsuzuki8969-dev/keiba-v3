#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
P0-γ: engine印 vs prob印 三連複 ROI 比較スクリプト（交絡除去版）

【旧ロジック】
- engine印: data/predictions/ の YYYYMMDD_pred.json に保存済みの tickets を使用
- prob印:   data/_diag/p0a_backup/ の YYYYMMDD_pred.json に保存済みの tickets を使用
- 問題: 2つの pred は生成日が違い（engine=6/25, prob=6/23）、その間に
  買い目フォーメーション変更が2回入ったため、ROI差が「印体系の差」と
  「買い目ロジックの差」で交絡している。

【新ロジック（同一買い目ルール）】
- 両印に同一の compute_danso_columns + force_buy を適用して交絡除去
- engine印には engine の composite/mark を渡す
- prob印には prob の composite/mark を渡す
- 同一の決定木・閾値で col1/col2/col3 を生成 → 三連複組合せを展開
- 旧との差分が「買い目ロジック変更による交絡量」を定量化する

実行: PYTHONIOENCODING=utf-8 python scripts/compare_engine_prob_roi.py
"""
import json
import os
import sys
import glob
import itertools
from typing import List, Dict, Optional, Tuple, Any

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
ENGINE_PRED_DIR = os.path.join(BASE_DIR, "data", "predictions")
PROB_PRED_DIR = os.path.join(BASE_DIR, "data", "_diag", "p0a_backup")
RESULT_DIR = os.path.join(BASE_DIR, "data", "results")

# src を import パスに追加
sys.path.insert(0, BASE_DIR)

TARGET_MONTH = "202601"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# pred / result ロード
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def load_preds(pred_dir: str, date_pattern: str) -> Dict[str, Dict]:
    """pred.json を読み込んで race_id -> pred_race dict を返す"""
    races: Dict[str, Dict] = {}
    files = sorted(glob.glob(os.path.join(pred_dir, f"{date_pattern}*_pred.json")))
    print(f"  pred files: {len(files)} ({pred_dir})")
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            for race in data.get("races", []):
                rid = race.get("race_id", "")
                if rid:
                    races[rid] = race
        except Exception as e:
            print(f"  WARN: {fp}: {e}")
    return races


def load_results(date_pattern: str) -> Dict[str, Dict]:
    """results.json を読み込んで race_id -> order+payouts dict を返す"""
    results: Dict[str, Dict] = {}
    files = sorted(glob.glob(os.path.join(RESULT_DIR, f"{date_pattern}*_results.json")))
    print(f"  result files: {len(files)}")
    for fp in files:
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            for rid, rv in data.items():
                results[rid] = rv
        except Exception as e:
            print(f"  WARN: {fp}: {e}")
    return results


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 結果照合ユーティリティ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def normalize_combo(combo_list) -> str:
    return "-".join(str(x) for x in sorted(combo_list))


def get_trio_payouts(payouts: Dict) -> Dict[str, int]:
    """三連複払戻 dict { combo_str: payout } を返す"""
    trio = payouts.get("三連複", payouts.get("trifecta_combo"))
    if trio is None:
        return {}
    if isinstance(trio, dict):
        return {trio["combo"]: trio["payout"]}
    elif isinstance(trio, list):
        return {p["combo"]: p["payout"] for p in trio if isinstance(p, dict)}
    return {}


def get_order_top3(order_list: List[Dict]) -> List:
    """着順リストから1-3着馬番セットを返す"""
    top3 = []
    for entry in sorted(order_list, key=lambda x: x.get("finish", 99)):
        f = entry.get("finish", 99)
        if f and f <= 3:
            top3.append(entry.get("horse_no"))
    return [x for x in top3 if x is not None][:3]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 旧ロジック: 保存済み tickets を使う ROI 計算
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def calc_roi_saved_tickets(pred_races: Dict, result_data: Dict, label: str = "") -> Dict:
    """三連複チケット（保存済み tickets フィールド）で hit% / ROI を計算"""
    total_invest = 0
    total_return = 0
    total_tickets = 0
    hit_races = 0
    total_bet_races = 0

    for race_id, pred_race in pred_races.items():
        res = result_data.get(race_id)
        if not res:
            continue
        order = res.get("order", [])
        payouts = res.get("payouts", {})
        if not order:
            continue
        top3 = get_order_top3(order)
        if len(top3) < 3:
            continue
        win_combo = normalize_combo(top3)
        trio_payouts = get_trio_payouts(payouts)

        tickets = pred_race.get("tickets", [])
        trio_tickets = [t for t in tickets if t.get("type") == "三連複"]
        if not trio_tickets:
            continue

        race_return = 0
        for t in trio_tickets:
            stake = t.get("stake", 100)
            combo = t.get("combo", [])
            combo_str = normalize_combo(combo)
            total_invest += stake
            total_tickets += 1
            if combo_str == win_combo:
                payout = trio_payouts.get(win_combo, 0)
                if payout:
                    units = stake // 100
                    ret = int(payout) * units
                    total_return += ret
                    race_return += ret

        total_bet_races += 1
        if race_return > 0:
            hit_races += 1

    roi = total_return / total_invest * 100 if total_invest > 0 else 0.0
    hit_pct = hit_races / total_bet_races * 100 if total_bet_races > 0 else 0.0
    return {
        "label": label,
        "bet_races": total_bet_races,
        "tickets": total_tickets,
        "invest": total_invest,
        "ret": total_return,
        "roi": roi,
        "hit_pct": hit_pct,
        "hit_races": hit_races,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 新ロジック: 同一買い目ルール適用（交絡除去）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_entries_from_pred_race(pred_race: Dict) -> List[Dict]:
    """pred.json の race dict から compute_danso_columns 用 entries リストを構築する。

    pred.json の horses キー名は:
        horse_no, mark, composite, odds, is_scratched(存在しない場合あり)
    """
    entries = []
    for h in pred_race.get("horses", []):
        mark = h.get("mark", "") or ""
        # '-' / '－'（全角ダッシュ）は無印扱いに統一
        if mark in ("-", "－", "—"):
            mark = "-"
        entries.append({
            "horse_no":    int(h.get("horse_no") or 0),
            "mark":        mark,
            "composite":   float(h.get("composite") or 50.0),
            "odds":        float(h.get("odds") or 10.0),
            "is_scratched": bool(h.get("is_scratched", False)),
        })
    return entries


def _expand_trio_combos_from_columns(
    col1: List[int],
    col2: List[int],
    col3: List[int],
    stake: int = 100,
) -> List[Dict]:
    """col1×col2×col3 の三連複組合せを展開して ticket list を返す。
    generate_danso_tickets の展開ロジックと同等。
    3頭 distinct・昇順tuple でdedup。
    """
    seen: set = set()
    tickets = []
    for a_no in col1:
        for b_no in col2:
            for c_no in col3:
                horse_nos_set = {a_no, b_no, c_no}
                if len(horse_nos_set) < 3:
                    continue
                seen_key = tuple(sorted(horse_nos_set))
                if seen_key in seen:
                    continue
                seen.add(seen_key)
                combo = list(sorted(horse_nos_set))
                tickets.append({
                    "type":  "三連複",
                    "combo": combo,
                    "stake": stake,
                })
    return tickets


def _try_compute_formation(
    entries: List[Dict],
) -> Optional[Dict]:
    """compute_danso_columns を試行し、None ならば build_force_buy_columns を試行する。
    両方 None ならば None を返す（見送り）。

    Returns: {"col1": [...], "col2": [...], "col3": [...], "formation": str} or None
    """
    from src.calculator.betting import compute_danso_columns, build_force_buy_columns

    result = compute_danso_columns(entries)
    if result is not None:
        return result

    # force_buy フォールバック
    force = build_force_buy_columns(entries)
    if force is not None:
        force["formation"] = "force_buy"
        return force

    return None


def calc_roi_unified_formation(
    pred_races: Dict,
    result_data: Dict,
    label: str = "",
    stake_per_point: int = 100,
    sample_race_id: Optional[str] = None,
    race_filter: Optional[set] = None,
) -> Tuple[Dict, Optional[Dict]]:
    """同一買い目ルール（compute_danso_columns + force_buy）を適用して ROI を計算する。

    Parameters
    ----------
    pred_races : race_id -> pred_race dict
    result_data : race_id -> result dict
    label : 表示用ラベル
    stake_per_point : 1点あたり賭け金（円）
    sample_race_id : col1/col2/col3 サンプル出力用のレースID（None=最初のhit）

    Returns
    -------
    (stats_dict, sample_dict_or_None)
    """
    total_invest = 0
    total_return = 0
    total_tickets_count = 0
    hit_races = 0
    total_bet_races = 0
    sample_info: Optional[Dict] = None
    fired_race_ids: List[str] = []

    for race_id, pred_race in pred_races.items():
        # race_filter 指定時は対象レースのみ集計（共通発火レース比較用）
        if race_filter is not None and race_id not in race_filter:
            continue
        # ばんえい除外（venue=65: 帯広）
        venue = pred_race.get("venue", "")
        if venue in ("帯広", "帯広ばんえい", "65"):
            continue

        res = result_data.get(race_id)
        if not res:
            continue
        order = res.get("order", [])
        payouts = res.get("payouts", {})
        if not order:
            continue
        top3 = get_order_top3(order)
        if len(top3) < 3:
            continue
        win_combo = normalize_combo(top3)
        trio_payouts = get_trio_payouts(payouts)

        # entries 構築
        entries = _build_entries_from_pred_race(pred_race)
        if not entries:
            continue

        # 同一買い目ルール適用
        result = _try_compute_formation(entries)
        if result is None:
            continue  # 見送り

        col1 = result["col1"]
        col2 = result["col2"]
        col3 = result["col3"]
        formation = result.get("formation", "danso_gap")

        tickets = _expand_trio_combos_from_columns(col1, col2, col3, stake=stake_per_point)
        if not tickets:
            continue

        # サンプル取得
        if sample_info is None and (sample_race_id is None or race_id == sample_race_id):
            sample_info = {
                "race_id":   race_id,
                "label":     label,
                "formation": formation,
                "col1":      col1,
                "col2":      col2,
                "col3":      col3,
                "tickets":   len(tickets),
            }

        race_return = 0
        for t in tickets:
            combo_str = normalize_combo(t["combo"])
            total_invest += t["stake"]
            total_tickets_count += 1
            if combo_str == win_combo:
                payout = trio_payouts.get(win_combo, 0)
                if payout:
                    units = t["stake"] // 100
                    ret = int(payout) * units
                    total_return += ret
                    race_return += ret

        total_bet_races += 1
        fired_race_ids.append(race_id)
        if race_return > 0:
            hit_races += 1

    roi = total_return / total_invest * 100 if total_invest > 0 else 0.0
    hit_pct = hit_races / total_bet_races * 100 if total_bet_races > 0 else 0.0
    stats = {
        "label":     label,
        "bet_races": total_bet_races,
        "tickets":   total_tickets_count,
        "invest":    total_invest,
        "ret":       total_return,
        "roi":       roi,
        "hit_pct":   hit_pct,
        "hit_races": hit_races,
        "fired_races": set(fired_race_ids),
    }
    return stats, sample_info


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# サンプルレースの両印 col1/col2/col3 比較
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def show_sample_race_columns(
    race_id: str,
    engine_races: Dict,
    prob_races: Dict,
) -> None:
    """指定 race_id で engine印/prob印の col1/col2/col3 を表示する。"""
    print(f"\n=== サンプルレース: {race_id} ===")

    def _show(label: str, pred_race: Dict) -> None:
        entries = _build_entries_from_pred_race(pred_race)
        result = _try_compute_formation(entries)
        if result is None:
            print(f"  [{label}] 見送り (compute_danso_columns → None)")
            return
        # markとcompositeも表示
        entry_map = {e["horse_no"]: e for e in entries}
        col1 = result["col1"]
        col2 = result["col2"]
        col3 = result["col3"]
        formation = result.get("formation", "?")

        def fmt_horses(nos):
            parts = []
            for no in nos:
                e = entry_map.get(no, {})
                parts.append(f"#{no}{e.get('mark','?')}({e.get('composite','?'):.1f})")
            return " / ".join(parts)

        print(f"  [{label}] formation={formation}")
        print(f"    col1: {fmt_horses(col1)}")
        print(f"    col2: {fmt_horses(col2)}")
        print(f"    col3: {fmt_horses(col3)}")
        # 全印サマリ
        print(f"    全頭 marks:")
        for e in sorted(entries, key=lambda x: x["horse_no"]):
            if e["mark"] not in ("-", ""):
                print(f"      #{e['horse_no']:2d} {e['mark']:3s} composite={e['composite']:.1f}")

    er = engine_races.get(race_id)
    pr = prob_races.get(race_id)
    if er:
        _show("engine印", er)
    else:
        print(f"  [engine印] race_id={race_id} 見つからず")
    if pr:
        _show("prob印", pr)
    else:
        print(f"  [prob印] race_id={race_id} 見つからず")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 比較表出力ユーティリティ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def print_stats(stats: Dict) -> None:
    print(f"\n--- {stats['label']} ---")
    print(f"  購入レース数: {stats['bet_races']}")
    print(f"  チケット数:   {stats['tickets']}")
    print(f"  投資額:       {stats['invest']:,}円")
    print(f"  回収額:       {stats['ret']:,}円")
    print(f"  ROI:          {stats['roi']:.1f}%")
    print(f"  hit%:         {stats['hit_pct']:.1f}% ({stats['hit_races']}/{stats['bet_races']}R)")


def print_comparison_table(engine: Dict, prob: Dict, title: str = "") -> None:
    if title:
        print(f"\n{'='*60}")
        print(f"=== {title} ===")
        print(f"{'='*60}")
    print(f"  {'指標':<20} {'engine印':>12} {'prob印':>12} {'差(engine-prob)':>16}")
    print(f"  {'-'*62}")
    print(f"  {'購入レース数':<20} {engine['bet_races']:>12} {prob['bet_races']:>12} {engine['bet_races']-prob['bet_races']:>+16}")
    print(f"  {'チケット数':<20} {engine['tickets']:>12} {prob['tickets']:>12} {engine['tickets']-prob['tickets']:>+16}")
    print(f"  {'ROI%':<20} {engine['roi']:>11.1f}% {prob['roi']:>11.1f}% {engine['roi']-prob['roi']:>+15.1f}%")
    print(f"  {'hit%':<20} {engine['hit_pct']:>11.1f}% {prob['hit_pct']:>11.1f}% {engine['hit_pct']-prob['hit_pct']:>+15.1f}%")
    print(f"  {'投資額':<20} {engine['invest']:>12,} {prob['invest']:>12,}")
    print(f"  {'回収額':<20} {engine['ret']:>12,} {prob['ret']:>12,}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main() -> None:
    print(f"=== P0-γ engine印 vs prob印 ROI比較（交絡除去版）({TARGET_MONTH}) ===")

    # ── データロード ──
    print(f"\n[1] engine印 pred ロード ({ENGINE_PRED_DIR})")
    engine_races = load_preds(ENGINE_PRED_DIR, TARGET_MONTH)
    print(f"  engine印 races: {len(engine_races)}")

    print(f"\n[2] prob印 pred ロード ({PROB_PRED_DIR})")
    prob_races_all = load_preds(PROB_PRED_DIR, TARGET_MONTH)
    print(f"  prob印 races: {len(prob_races_all)}")

    print(f"\n[3] results ロード")
    results = load_results(TARGET_MONTH)
    print(f"  result races: {len(results)}")

    # ── 母数統一: engine印採用レースのみ ──
    engine_race_ids = set(engine_races.keys())
    prob_races_filtered = {rid: p for rid, p in prob_races_all.items() if rid in engine_race_ids}
    print(f"\n[4] 母数統一: engine印採用レース {len(engine_race_ids)}R")
    print(f"    prob印版 同一race_id: {len(prob_races_filtered)}R")

    # ────────────────────────────────────────
    # [旧ロジック] 保存済み tickets を使った ROI
    # ────────────────────────────────────────
    print(f"\n\n{'#'*60}")
    print(f"# [旧ロジック] 保存済み tickets を使った ROI（参考値）")
    print(f"# ※ 買い目ロジックが異なる期間のファイルを混在比較しているため交絡あり")
    print(f"{'#'*60}")

    old_engine = calc_roi_saved_tickets(
        engine_races, results, label="engine印 [旧: 保存済みtickets]"
    )
    old_prob = calc_roi_saved_tickets(
        prob_races_filtered, results, label="prob印 [旧: 保存済みtickets]"
    )
    print_stats(old_engine)
    print_stats(old_prob)
    print_comparison_table(old_engine, old_prob, title="旧ロジック比較表（交絡あり）")

    # ────────────────────────────────────────
    # [新ロジック] 同一買い目ルール適用（交絡除去）
    # ────────────────────────────────────────
    print(f"\n\n{'#'*60}")
    print(f"# [新ロジック] 同一 compute_danso_columns を両印に適用（交絡除去）")
    print(f"# ※ 印体系の差のみを純粋に測定")
    print(f"{'#'*60}")

    # サンプルレース: 202601最初のengine印レース
    first_engine_race_id = next(iter(engine_races), None)

    new_engine, sample_engine = calc_roi_unified_formation(
        engine_races, results,
        label="engine印 [新: 同一買い目ルール]",
        sample_race_id=first_engine_race_id,
    )
    new_prob, sample_prob = calc_roi_unified_formation(
        prob_races_filtered, results,
        label="prob印 [新: 同一買い目ルール]",
        sample_race_id=first_engine_race_id,
    )
    print_stats(new_engine)
    print_stats(new_prob)
    print_comparison_table(new_engine, new_prob, title="新ロジック比較表（交絡除去・純粋な印効果）")

    # ────────────────────────────────────────
    # [共通発火レース] 母数を揃えた最も厳密な印比較
    # ※ engine印・prob印 が両方発火（買い目生成）したレースのみで再計算
    # ────────────────────────────────────────
    engine_fired = new_engine.get("fired_races", set())
    prob_fired = new_prob.get("fired_races", set())
    common = engine_fired & prob_fired
    print(f"\n\n{'#'*60}")
    print(f"# [共通発火レース] engine印・prob印 両方が発火したレースのみ（母数統一）")
    print(f"{'#'*60}")
    print(f"  engine印発火: {len(engine_fired)}R / prob印発火: {len(prob_fired)}R / 共通: {len(common)}R")
    common_engine, _ = calc_roi_unified_formation(
        engine_races, results, label="engine印 [共通発火レース]", race_filter=common
    )
    common_prob, _ = calc_roi_unified_formation(
        prob_races_filtered, results, label="prob印 [共通発火レース]", race_filter=common
    )
    print_stats(common_engine)
    print_stats(common_prob)
    print_comparison_table(common_engine, common_prob, title="共通発火レース比較表（母数統一・最も厳密な印比較）")

    # ────────────────────────────────────────
    # 交絡量の定量化（旧 vs 新の差）
    # ────────────────────────────────────────
    print(f"\n\n{'#'*60}")
    print(f"# 交絡量 = 旧ロジック差 - 新ロジック差")
    print(f"# （買い目ロジック変更が比較結果に与えた影響）")
    print(f"{'#'*60}")
    old_roi_diff = old_engine["roi"] - old_prob["roi"]
    new_roi_diff = new_engine["roi"] - new_prob["roi"]
    confound_roi = old_roi_diff - new_roi_diff
    old_hit_diff = old_engine["hit_pct"] - old_prob["hit_pct"]
    new_hit_diff = new_engine["hit_pct"] - new_prob["hit_pct"]
    confound_hit = old_hit_diff - new_hit_diff

    print(f"\n  {'指標':<25} {'旧の差':>12} {'新の差':>12} {'交絡量(旧-新)':>14}")
    print(f"  {'-'*65}")
    print(f"  {'ROI差(engine-prob)%':<25} {old_roi_diff:>+11.1f}% {new_roi_diff:>+11.1f}% {confound_roi:>+13.1f}%")
    print(f"  {'hit%差(engine-prob)':<25} {old_hit_diff:>+11.1f}% {new_hit_diff:>+11.1f}% {confound_hit:>+13.1f}%")

    if abs(confound_roi) < 2.0:
        print(f"\n  [判定] 交絡量が小さい(|{confound_roi:.1f}%| < 2%) → 旧比較も概ね信頼できる")
    else:
        print(f"\n  [判定] 交絡量が大きい(|{confound_roi:.1f}%| >= 2%) → 新ロジック結果を優先すべき")

    # ────────────────────────────────────────
    # サンプルレースの col1/col2/col3 表示
    # ────────────────────────────────────────
    sample_race_id = first_engine_race_id
    if sample_race_id:
        show_sample_race_columns(sample_race_id, engine_races, prob_races_filtered)

    # ────────────────────────────────────────
    # ファイルパス表示
    # ────────────────────────────────────────
    print(f"\n  比較ファイル:")
    print(f"    engine印: {ENGINE_PRED_DIR}/202601XXXX_pred.json")
    print(f"    prob印:   {PROB_PRED_DIR}/202601XXXX_pred.json")


if __name__ == "__main__":
    main()
