"""新戦略ハイブリッド成績集計 — 三連複動的フォーメーション + 単勝 T-4 (A-NONE 2券種)。

本番採用確定: A-NONE 馬単なし 2 券種 (三連複動的 + 単勝 T-4)
dispatch_backtest.py の Layer 1 ロジックを移植して過去 pred.json + results.json を突合。

マスター指示 2026-04-30:
  - 旧戦略 (三連単 F) は削除せず注釈付きで残す
  - 新戦略 (三連複動的 + 単勝 T-4) を過去成績ページに追加
"""
from __future__ import annotations

import json
import time
from collections import defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, Optional

# ────────────────────────────────────────────────────────────────
# 定数 (dispatch_backtest.py と同値)
# ────────────────────────────────────────────────────────────────
STAKE_PER_TICKET = 100

HONMEI_MARKS = {"◉", "◎"}
TAIKOU_MARKS = {"○", "〇"}
RENKA_MARKS  = {"▲"}
WIDE_MARKS   = {"△", "★"}
OANA_MARKS   = {"☆"}

MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
}

HOSHI_DYNAMIC_MIN_ODDS = 10.0

# ────────────────────────────────────────────────────────────────
# キャッシュ (30 分 TTL)
# ────────────────────────────────────────────────────────────────
_CACHE: Dict[str, tuple] = {}
_CACHE_TTL = 1800


# ────────────────────────────────────────────────────────────────
# ユーティリティ (dispatch_backtest.py 移植)
# ────────────────────────────────────────────────────────────────

def _year_match(date_str: str, year_filter: str) -> bool:
    """year_filter が 'all'/'2024'/'2025'/'2026' 等を受け取り該当するか返す。"""
    if year_filter in ("all", "", None):
        return True
    y = year_filter.replace("年", "").strip()
    return date_str.startswith(y)


def _get_ev_with_fallback(horse: dict) -> float:
    """ev フィールド取得 + フォールバック計算。

    - ev > 0 → 既存値
    - ev <= 0 → win_prob × odds で近似
    - 両方無効 → 0.0 (見送り扱い)
    """
    ev = horse.get("ev", 0) or 0
    if ev > 0:
        return float(ev)
    win_prob = horse.get("win_prob", 0) or 0
    odds = horse.get("odds") or horse.get("predicted_tansho_odds") or 0
    if win_prob > 0 and odds > 0:
        return float(win_prob) * float(odds)
    return 0.0


def _get_odds(h: dict) -> float:
    v = h.get("odds") or h.get("predicted_tansho_odds") or 0.0
    return float(v)


def _filter_active(horses: list) -> list:
    """出走取消 / 除外馬を除く。"""
    return [h for h in horses
            if not h.get("is_tokusen_kiken") and not h.get("is_scratched")]


def _get_mark_horses(horses: list, mark_set: set) -> list:
    result = [h for h in horses if h.get("mark", "") in mark_set]
    result.sort(key=lambda h: (
        MARK_PRIORITY.get(h.get("mark", ""), 9),
        -(h.get("composite") or 0)
    ))
    return result


def _get_pivot(horses: list) -> Optional[dict]:
    """軸馬 (◉◎ 最優先 1 頭)"""
    cands = _get_mark_horses(horses, HONMEI_MARKS)
    return cands[0] if cands else None


def _resolve_hoshi(horses: list) -> Optional[dict]:
    """☆ 印馬 / 動的補完 (オッズ 10 倍以上の無印馬から win_prob 最大を選ぶ)"""
    hoshi_list = _get_mark_horses(horses, OANA_MARKS)
    if hoshi_list:
        return hoshi_list[0]
    unmarked = [
        h for h in horses
        if h.get("mark", "") in ("", None, "－", "-")
        and _get_odds(h) >= HOSHI_DYNAMIC_MIN_ODDS
    ]
    if not unmarked:
        return None
    unmarked.sort(key=lambda h: -(h.get("win_prob") or 0.0))
    return unmarked[0]


def _lookup_payout(payouts: dict, ticket_type: str, combo_nos: list) -> int:
    """払戻額を返す。combo_nos: 単勝=[馬番], 三連複=[昇順3頭]"""
    bucket = payouts.get(ticket_type)
    if bucket is None:
        return 0
    nos_str = "-".join(str(x) for x in combo_nos)

    def _match(item) -> int:
        if not isinstance(item, dict):
            return 0
        if str(item.get("combo", "")) == nos_str:
            return int(item.get("payout", 0) or 0)
        return 0

    if isinstance(bucket, dict):
        return _match(bucket)
    if isinstance(bucket, list):
        for it in bucket:
            v = _match(it)
            if v:
                return v
    return 0


# ────────────────────────────────────────────────────────────────
# Layer 1: EV 判定 (A-NONE 方式 / 馬単なし)
# ────────────────────────────────────────────────────────────────

def _layer1_sanrenpuku(horses: list, payouts: dict) -> Optional[str]:
    """三連複動的フォーメーション発動ケース判定。
    Returns: "絞り" / "中" / "広" / None (見送り)

    三連複払戻データが存在しないレース (NAR R2 等) は None を返す。
    """
    # 三連複払戻データ存在確認
    if payouts.get("三連複") is None and payouts.get("sanrenpuku") is None:
        return None

    pivot = _get_pivot(horses)
    if pivot is None:
        return None

    p_ev          = _get_ev_with_fallback(pivot)
    p_place3_prob = float(pivot.get("place3_prob") or 0.0)

    if p_ev < 1.0:
        return None

    taikou_list = _get_mark_horses(horses, TAIKOU_MARKS)
    renka_list  = _get_mark_horses(horses, RENKA_MARKS)
    taikou = taikou_list[0] if taikou_list else None
    renka  = renka_list[0]  if renka_list  else None

    o_place3 = float(taikou.get("place3_prob") or 0.0) if taikou else 0.0
    r_place3 = float(renka.get("place3_prob")  or 0.0) if renka  else 0.0

    if (p_ev >= 1.8 and p_place3_prob >= 0.65
            and o_place3 >= 0.50 and taikou is not None):
        return "絞り"

    if (p_ev >= 1.3 and p_place3_prob >= 0.55
            and (o_place3 >= 0.40 or r_place3 >= 0.40)):
        return "中"

    return "広"


def _layer1_tansho(horses: list) -> bool:
    """単勝 T-4 発動判定。◉◎ EV≥1.0 または ○ EV≥1.0"""
    honmei = _get_mark_horses(horses, HONMEI_MARKS)
    if honmei and _get_ev_with_fallback(honmei[0]) >= 1.0:
        return True
    taikou = _get_mark_horses(horses, TAIKOU_MARKS)
    if taikou and _get_ev_with_fallback(taikou[0]) >= 1.0:
        return True
    return False


# ────────────────────────────────────────────────────────────────
# チケット生成
# ────────────────────────────────────────────────────────────────

def _build_sanrenpuku_tickets(horses: list, payouts: dict, case: str) -> list:
    """三連複チケット生成 (case: 絞り/中/広)。
    Returns: [(combo_tuple, payout_yen), ...]
    """
    pivot = _get_pivot(horses)
    if pivot is None:
        return []

    hoshi = _resolve_hoshi(horses)
    pivot_no = pivot["horse_no"]

    if case == "絞り":
        taikou_list = _get_mark_horses(horses, TAIKOU_MARKS)
        if not taikou_list:
            return []
        taikou_no = taikou_list[0]["horse_no"]
        third_marks = {"▲", "△", "★"}
        thirds = _get_mark_horses(horses, third_marks)
        if hoshi:
            existing = {h["horse_no"] for h in thirds}
            if hoshi["horse_no"] not in existing:
                thirds.append(hoshi)
        tickets = []
        for t in thirds:
            t_no = t["horse_no"]
            if t_no in (pivot_no, taikou_no):
                continue
            combo = tuple(sorted([pivot_no, taikou_no, t_no]))
            pb = _lookup_payout(payouts, "三連複", list(combo))
            tickets.append((combo, pb))
        return tickets

    elif case == "中":
        second_marks = {"○", "〇", "▲"}
        second_horses = _get_mark_horses(horses, second_marks)
        third_marks = {"○", "〇", "▲", "△", "★"}
        third_horses = _get_mark_horses(horses, third_marks)
        if hoshi:
            existing = {h["horse_no"] for h in third_horses}
            if hoshi["horse_no"] not in existing:
                third_horses.append(hoshi)
        second_nos   = [h["horse_no"] for h in second_horses]
        all_third_nos = [h["horse_no"] for h in third_horses]
        seen: set = set()
        tickets = []
        for s_no in second_nos:
            if s_no == pivot_no:
                continue
            for t_no in all_third_nos:
                if t_no == pivot_no or t_no == s_no:
                    continue
                combo = tuple(sorted([pivot_no, s_no, t_no]))
                if combo in seen:
                    continue
                seen.add(combo)
                pb = _lookup_payout(payouts, "三連複", list(combo))
                tickets.append((combo, pb))
        return tickets

    else:  # 広
        sub_all_marks = {"○", "〇", "▲", "△", "★"}
        sub_all = _get_mark_horses(horses, sub_all_marks)
        if hoshi:
            existing = {h["horse_no"] for h in sub_all}
            if hoshi["horse_no"] not in existing:
                sub_all.append(hoshi)
        sub_nos = [h["horse_no"] for h in sub_all]
        partners = [p for p in sub_nos if p != pivot_no]
        tickets = []
        for p1, p2 in combinations(partners, 2):
            combo = tuple(sorted([pivot_no, p1, p2]))
            pb = _lookup_payout(payouts, "三連複", list(combo))
            tickets.append((combo, pb))
        return tickets


def _build_tansho_tickets(horses: list, payouts: dict) -> list:
    """単勝 T-4 チケット生成 (◉◎+○ 2点)。
    Returns: [(combo_tuple, payout_yen), ...]
    """
    tickets = []
    honmei = _get_mark_horses(horses, HONMEI_MARKS)
    if honmei:
        no = honmei[0]["horse_no"]
        pb = _lookup_payout(payouts, "単勝", [no])
        tickets.append(((no,), pb))
    taikou = _get_mark_horses(horses, TAIKOU_MARKS)
    if taikou:
        no = taikou[0]["horse_no"]
        pb = _lookup_payout(payouts, "単勝", [no])
        tickets.append(((no,), pb))
    return tickets


# ────────────────────────────────────────────────────────────────
# 集計関数
# ────────────────────────────────────────────────────────────────

def _compute_tansho_t4(year_filter: str) -> dict:
    """単勝 T-4 戦略の過去成績を集計する。

    各レースで ◉◎+○ の単勝チケット (2点) をシミュレーション。
    集計: races_played, races_hit, total_stake, total_payback, roi_pct, monthly[]
    """
    pred_dir = Path("data/predictions")
    res_dir  = Path("data/results")

    stats: dict = {
        "races_played":  0,
        "races_hit":     0,
        "total_stake":   0,
        "total_payback": 0,
        "date_from":     "",
        "date_to":       "",
    }
    by_month: dict = {}

    for fp in sorted(pred_dir.glob("*_pred.json")):
        if "_prev" in fp.name:
            continue
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        if not _year_match(date_str, year_filter):
            continue

        res_fp = res_dir / f"{date_str}_results.json"
        if not res_fp.exists():
            continue

        try:
            with fp.open(encoding="utf-8") as f:
                pred = json.load(f)
            with res_fp.open(encoding="utf-8") as f:
                results = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        if not stats["date_from"] or date_str < stats["date_from"]:
            stats["date_from"] = date_str
        if not stats["date_to"] or date_str > stats["date_to"]:
            stats["date_to"] = date_str

        for r in pred.get("races", []):
            race_id = str(r.get("race_id", ""))
            horses = _filter_active(r.get("horses", []))
            if not horses:
                continue

            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue

            # 単勝 T-4 発動判定
            if not _layer1_tansho(horses):
                continue

            tickets = _build_tansho_tickets(horses, payouts)
            if not tickets:
                continue

            stake    = len(tickets) * STAKE_PER_TICKET
            payback  = sum(pb for _, pb in tickets)
            race_hit = any(pb > 0 for _, pb in tickets)

            stats["races_played"]  += 1
            stats["total_stake"]   += stake
            stats["total_payback"] += payback
            if race_hit:
                stats["races_hit"] += 1

            month_key = f"{date_str[:4]}-{date_str[4:6]}"
            bm = by_month.setdefault(month_key, {
                "played": 0, "hit": 0, "stake": 0, "payback": 0
            })
            bm["played"]  += 1
            bm["stake"]   += stake
            bm["payback"] += payback
            if race_hit:
                bm["hit"] += 1

    # 派生指標
    ts = stats["total_stake"]
    tpb = stats["total_payback"]
    rp = stats["races_played"]
    rh = stats["races_hit"]
    stats["balance"]       = tpb - ts
    stats["roi_pct"]       = round(tpb / ts * 100, 1) if ts > 0 else 0.0
    stats["hit_rate_pct"]  = round(rh / rp * 100, 1) if rp > 0 else 0.0

    # 月別 (累積 ROI 付き)
    cum_stake, cum_payback = 0, 0
    monthly = []
    for m in sorted(by_month.keys()):
        v = by_month[m]
        cum_stake   += v["stake"]
        cum_payback += v["payback"]
        monthly.append({
            "month":       m,
            "played":      v["played"],
            "hit":         v["hit"],
            "stake":       v["stake"],
            "payback":     v["payback"],
            "balance":     v["payback"] - v["stake"],
            "roi_pct":     round(v["payback"] / v["stake"] * 100, 1) if v["stake"] > 0 else 0.0,
            "cum_roi_pct": round(cum_payback / cum_stake * 100, 1) if cum_stake > 0 else 0.0,
        })
    stats["monthly"] = monthly

    return stats


def _compute_sanrenpuku_dynamic(year_filter: str) -> dict:
    """三連複動的フォーメーション戦略の過去成績を集計する。

    A-NONE 方式 (信頼度ガードなし / 馬単なし)。
    dispatch_backtest.py の layer1_sanrenpuku + build_sanrenpuku_tickets を移植。
    集計: races_played, races_hit, total_stake, total_payback, roi_pct
    内訳: by_variant = {絞り, 中, 広}
    月別: monthly[]
    """
    pred_dir = Path("data/predictions")
    res_dir  = Path("data/results")

    stats: dict = {
        "races_played":  0,
        "races_hit":     0,
        "total_stake":   0,
        "total_payback": 0,
        "date_from":     "",
        "date_to":       "",
    }
    by_variant: dict = {
        "絞り": {"races": 0, "hit": 0, "stake": 0, "payback": 0},
        "中":   {"races": 0, "hit": 0, "stake": 0, "payback": 0},
        "広":   {"races": 0, "hit": 0, "stake": 0, "payback": 0},
    }
    by_month: dict = {}

    for fp in sorted(pred_dir.glob("*_pred.json")):
        if "_prev" in fp.name:
            continue
        date_str = fp.name.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        if not _year_match(date_str, year_filter):
            continue

        res_fp = res_dir / f"{date_str}_results.json"
        if not res_fp.exists():
            continue

        try:
            with fp.open(encoding="utf-8") as f:
                pred = json.load(f)
            with res_fp.open(encoding="utf-8") as f:
                results = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        if not stats["date_from"] or date_str < stats["date_from"]:
            stats["date_from"] = date_str
        if not stats["date_to"] or date_str > stats["date_to"]:
            stats["date_to"] = date_str

        for r in pred.get("races", []):
            race_id = str(r.get("race_id", ""))
            horses = _filter_active(r.get("horses", []))
            if not horses:
                continue

            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue

            # 三連複動的 Layer 1 判定 (三連複払戻データ存在確認込み)
            case = _layer1_sanrenpuku(horses, payouts)
            if case is None:
                continue

            tickets = _build_sanrenpuku_tickets(horses, payouts, case)
            if not tickets:
                continue

            stake    = len(tickets) * STAKE_PER_TICKET
            payback  = sum(pb for _, pb in tickets)
            race_hit = any(pb > 0 for _, pb in tickets)

            stats["races_played"]  += 1
            stats["total_stake"]   += stake
            stats["total_payback"] += payback
            if race_hit:
                stats["races_hit"] += 1

            bv = by_variant[case]
            bv["races"]   += 1
            bv["stake"]   += stake
            bv["payback"] += payback
            if race_hit:
                bv["hit"] += 1

            month_key = f"{date_str[:4]}-{date_str[4:6]}"
            bm = by_month.setdefault(month_key, {
                "played": 0, "hit": 0, "stake": 0, "payback": 0
            })
            bm["played"]  += 1
            bm["stake"]   += stake
            bm["payback"] += payback
            if race_hit:
                bm["hit"] += 1

    # 派生指標
    ts = stats["total_stake"]
    tpb = stats["total_payback"]
    rp = stats["races_played"]
    rh = stats["races_hit"]
    stats["balance"]      = tpb - ts
    stats["roi_pct"]      = round(tpb / ts * 100, 1) if ts > 0 else 0.0
    stats["hit_rate_pct"] = round(rh / rp * 100, 1) if rp > 0 else 0.0

    # 内訳 ROI
    for v_name, v_data in by_variant.items():
        v_stake = v_data["stake"]
        v_data["roi_pct"]      = round(v_data["payback"] / v_stake * 100, 1) if v_stake > 0 else 0.0
        v_data["hit_rate_pct"] = round(v_data["hit"] / v_data["races"] * 100, 1) if v_data["races"] > 0 else 0.0
    stats["by_variant"] = by_variant

    # 月別 (累積 ROI 付き)
    cum_stake, cum_payback = 0, 0
    monthly = []
    for m in sorted(by_month.keys()):
        v = by_month[m]
        cum_stake   += v["stake"]
        cum_payback += v["payback"]
        monthly.append({
            "month":       m,
            "played":      v["played"],
            "hit":         v["hit"],
            "stake":       v["stake"],
            "payback":     v["payback"],
            "balance":     v["payback"] - v["stake"],
            "roi_pct":     round(v["payback"] / v["stake"] * 100, 1) if v["stake"] > 0 else 0.0,
            "cum_roi_pct": round(cum_payback / cum_stake * 100, 1) if cum_stake > 0 else 0.0,
        })
    stats["monthly"] = monthly

    return stats


# ────────────────────────────────────────────────────────────────
# 公開 API
# ────────────────────────────────────────────────────────────────

def get_hybrid_summary(year_filter: str = "all", force_refresh: bool = False) -> dict:
    """新戦略ハイブリッド成績集計 (三連複動的 + 単勝 T-4) を返す。

    30 分 TTL キャッシュ。force_refresh=True でキャッシュ無視。

    Returns:
        {
            "tansho_t4":       {...}   # 単勝 T-4 集計
            "sanrenpuku_dynamic": {...}  # 三連複動的集計
        }
    """
    cache_key = year_filter or "all"
    if not force_refresh:
        cached = _CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _CACHE_TTL:
            return cached[1]

    tansho_result     = _compute_tansho_t4(cache_key)
    sanrenpuku_result = _compute_sanrenpuku_dynamic(cache_key)

    result = {
        "tansho_t4":          tansho_result,
        "sanrenpuku_dynamic": sanrenpuku_result,
    }
    _CACHE[cache_key] = (time.time(), result)
    return result


def invalidate_cache() -> None:
    """結果取得 / pred 更新後に外部から呼んでキャッシュを破棄する。"""
    _CACHE.clear()
