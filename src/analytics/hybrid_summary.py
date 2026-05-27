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

from data.masters.venue_master import BANEI_VENUE_CODES

# ────────────────────────────────────────────────────────────────
# 定数 (dispatch_backtest.py と同値)
# ────────────────────────────────────────────────────────────────
STAKE_PER_TICKET = 100

HONMEI_MARKS = {"◉", "◎"}
TAIKOU_MARKS = {"○", "〇"}
RENKA_MARKS  = {"▲"}
WIDE_MARKS   = {"△", "★", "☆"}  # v5: ☆は通常6印目としてWIDEに含む
OANA_MARKS   = set()              # v5: ☆はWIDE_MARKSに移動、動的追加不要

MARK_PRIORITY: dict[str, int] = {
    "◉": 0, "◎": 1, "○": 2, "〇": 2,
    "▲": 3, "△": 4, "★": 5, "☆": 6,
    # v5: ×は廃止のため PRIORITY に含まない
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


_TICKET_KEY_ALIASES = {
    "三連複": ("三連複", "3連複", "sanrenpuku"),
    "三連単": ("三連単", "sanrentan"),
    "単勝":   ("単勝", "tansho"),
}

def _lookup_payout(payouts: dict, ticket_type: str, combo_nos: list) -> int:
    """払戻額を返す。combo_nos: 単勝=[馬番], 三連複=[昇順3頭]"""
    bucket = None
    for key in _TICKET_KEY_ALIASES.get(ticket_type, (ticket_type,)):
        bucket = payouts.get(key)
        if bucket is not None:
            break
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
    Returns: "中" / "広" / None (見送り)

    マスター指示 2026-05-01: 「絞り」は採用しない (中 + 広 のみ運用)。
    かつて「絞り」になっていた高確信ケースは「中」として買う。

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

    # 「絞り」発動条件は廃止 — 該当ケースは「中」として処理
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
    # 自信度別内訳 (M' と同じ構造)
    _CONF_LEVELS = ("SS", "S", "A", "B", "C", "D", "E")
    by_confidence: dict = {lv: {"races": 0, "hit": 0, "stake": 0, "payback": 0}
                           for lv in _CONF_LEVELS}
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
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue
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

            # 自信度別集計
            tbm = r.get("tickets_by_mode", {}) or {}
            meta = tbm.get("_meta", {}) or {}
            confidence = meta.get("confidence", "") or r.get("overall_confidence", "") or ""
            if confidence in by_confidence:
                bc = by_confidence[confidence]
                bc["races"]   += 1
                bc["stake"]   += stake
                bc["payback"] += payback
                if race_hit:
                    bc["hit"] += 1

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

    # 自信度別 ROI / hit_rate 計算
    for lv, lv_data in by_confidence.items():
        lv_s = lv_data["stake"]
        lv_r = lv_data["races"]
        lv_data["roi_pct"] = round(lv_data["payback"] / lv_s * 100, 1) if lv_s > 0 else 0.0
        lv_data["hit_rate_pct"] = round(lv_data["hit"] / lv_r * 100, 1) if lv_r > 0 else 0.0
    stats["by_confidence"] = {
        lv: v for lv, v in by_confidence.items() if v["races"] > 0
    }

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
    内訳: by_variant = {中, 広} (絞りは 2026-05-01 マスター指示で廃止)
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
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue
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
# M' 戦略集計 (Phase 6 対応)
# ────────────────────────────────────────────────────────────────

def _is_m_prime_pred(races: list) -> bool:
    """pred.json のレースリストが M' 戦略フォーマットかを判定する。

    判定優先順位 (G-8 2026-05-26 追加: 過去 pred.json の _meta.format 欠落対応):
      1. tickets_by_mode._meta.format が "M'" で始まる → M' 確定
      2. _meta.format が他文字列で始まる → 非 M'
      3. _meta.format 未設定 → tickets に三連複が含まれていれば M' とみなす
         (regen_strategy が _meta.format を書かなかったレガシー pred.json 対応)
    """
    legacy_has_sanrenpuku = False
    for r in races:
        tbm = r.get("tickets_by_mode", {}) or {}
        meta = tbm.get("_meta", {}) or {}
        fmt = meta.get("format", "") or ""
        if fmt:
            return fmt.startswith("M'")
        # _meta.format 空: tickets に三連複があれば候補
        if not legacy_has_sanrenpuku:
            for t in (r.get("tickets", []) or []):
                if t.get("type") == "三連複":
                    legacy_has_sanrenpuku = True
                    break
    return legacy_has_sanrenpuku


def _layer1_m_prime_sanrenpuku(race: dict) -> bool:
    """M' 戦略での三連複チケット発動判定。

    tickets_by_mode._meta.skipped が False かつ
    tickets リストに type=="三連複" が 1 件以上あれば発動とみなす。
    旧 T-050 の EV 判定ロジックとは独立して、pred.json の結果をそのまま使う。

    Returns:
        True  : M' 戦略がこのレースでチケットを出力している
        False : skip 扱い（自信度不足・NAR 除外等）
    """
    tbm = race.get("tickets_by_mode", {}) or {}
    meta = tbm.get("_meta", {}) or {}
    # skip フラグで明示的に見送られたレースは除外
    if meta.get("skipped", False):
        return False
    # tickets に三連複が 1 件以上あれば発動
    tix = race.get("tickets", []) or []
    return any(t.get("type") == "三連複" for t in tix)


def _compute_m_prime_sanrenpuku(year_filter: str) -> dict:
    """M' 戦略（自信度別三連複）の過去成績を集計する。

    pred.json の tickets（type=="三連複"）と results.json の払戻を突合。
    自信度別内訳 by_confidence (SS/S/A/B/C/D/E) も集計する。

    集計: races_played, races_hit, total_stake, total_payback, roi_pct
    内訳: by_confidence = {SS: {...}, S: {...}, ...}
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
    # M' 自信度レベル（_meta.confidence または overall_confidence）
    _CONFIDENCE_LEVELS = ("SS", "S", "A", "B", "C", "D", "E")
    by_confidence: dict = {lv: {"races": 0, "hit": 0, "stake": 0, "payback": 0}
                           for lv in _CONFIDENCE_LEVELS}
    by_month: dict = {}
    # 三連複高配当 TOP10 候補リスト（後で payback 降順ソート → TOP10）
    top_payouts_list: list = []

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

        races = pred.get("races", [])
        # M' フォーマットでない pred.json はスキップ
        if not _is_m_prime_pred(races):
            continue

        if not stats["date_from"] or date_str < stats["date_from"]:
            stats["date_from"] = date_str
        if not stats["date_to"] or date_str > stats["date_to"]:
            stats["date_to"] = date_str

        for r in races:
            race_id = str(r.get("race_id", ""))
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue

            # M' 発動判定（skip レースは除外）
            if not _layer1_m_prime_sanrenpuku(r):
                continue

            # 結果データ取得
            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue

            # 三連複払戻データが存在しないレースはスキップ（NAR R2 等）
            if payouts.get("三連複") is None and payouts.get("sanrenpuku") is None:
                continue

            # 自信度取得（_meta.confidence → overall_confidence → sanrenpuku_confidence の順でフォールバック）
            # G-8 (2026-05-26): 過去 pred.json の _meta.confidence/overall_confidence 欠落対応で
            # sanrenpuku_confidence (backfill_confidence_split.py で埋まる) を最終フォールバックに追加
            tbm = r.get("tickets_by_mode", {}) or {}
            meta = tbm.get("_meta", {}) or {}
            confidence = (
                meta.get("confidence", "")
                or r.get("overall_confidence", "")
                or r.get("sanrenpuku_confidence", "")
                or ""
            )
            # G-8 (2026-05-26 マスター承認): シミュ完全性のため E race も含めて集計する。
            # 過去 pred.json には E でも tickets が残っているため「もし買っていたら」型シミュとして保持。
            # 現運用 (E=skip) との乖離はフロント側で確認する。

            # tickets から三連複チケットを取得
            tix = [t for t in (r.get("tickets", []) or []) if t.get("type") == "三連複"]
            if not tix:
                continue

            # 1-2-3 着の馬番セットを取得（払戻突合用）
            top3_set: set = set()
            if payouts.get("三連複"):
                bucket = payouts["三連複"]
                # bucket が list の場合は最初の combo から top3 を推定
                # 実際の着順は results の order フィールドを優先
            order = rdata.get("order") or []
            if len(order) >= 3:
                finish_map = {int(o["horse_no"]): int(o["finish"]) for o in order}
                top3_set = {h for h, f in finish_map.items() if f <= 3}

            stake    = sum(int(t.get("stake", 0) or 0) for t in tix)
            if stake <= 0:
                stake = len(tix) * STAKE_PER_TICKET

            # 的中チェック: 三連複 combo が top3_set と完全一致するか
            race_hit = False
            payback  = 0
            if len(top3_set) == 3:
                for t in tix:
                    combo_ints = [int(x) for x in (t.get("combo") or [])]
                    if len(combo_ints) != 3:
                        continue
                    if set(combo_ints) == top3_set:
                        race_hit = True
                        pb = _lookup_payout(payouts, "三連複", sorted(combo_ints))
                        t_stake = int(t.get("stake", 0) or 0) or STAKE_PER_TICKET
                        payback += pb * (t_stake // 100)

            stats["races_played"]  += 1
            stats["total_stake"]   += stake
            stats["total_payback"] += payback
            if race_hit:
                stats["races_hit"] += 1
                # 三連複高配当 TOP10 候補に追加（払戻 > 0 のみ）
                if payback > 0:
                    top_payouts_list.append({
                        "payback":   int(payback),
                        "date":      f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}",
                        "venue":     r.get("venue", "") or "",
                        "race_no":   int(r.get("race_no", 0) or 0),
                        "race_name": r.get("race_name", "") or "",
                        "combo":     "-".join(str(x) for x in sorted(top3_set)),
                        "confidence": confidence,
                    })

            # 自信度別内訳
            if confidence in by_confidence:
                bc = by_confidence[confidence]
                bc["races"]   += 1
                bc["stake"]   += stake
                bc["payback"] += payback
                if race_hit:
                    bc["hit"] += 1

            # 月別集計
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
    ts  = stats["total_stake"]
    tpb = stats["total_payback"]
    rp  = stats["races_played"]
    rh  = stats["races_hit"]
    stats["balance"]      = tpb - ts
    stats["roi_pct"]      = round(tpb / ts * 100, 1) if ts > 0 else 0.0
    stats["hit_rate_pct"] = round(rh / rp * 100, 1) if rp > 0 else 0.0

    # 自信度別 ROI
    for lv, lv_data in by_confidence.items():
        lv_s = lv_data["stake"]
        lv_r = lv_data["races"]
        lv_data["roi_pct"] = round(lv_data["payback"] / lv_s * 100, 1) if lv_s > 0 else 0.0
        lv_data["hit_rate_pct"] = round(lv_data["hit"] / lv_r * 100, 1) if lv_r > 0 else 0.0
    stats["by_confidence"] = {
        lv: v for lv, v in by_confidence.items() if v["races"] > 0
    }

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

    # 三連複高配当 TOP10
    stats["top_payouts"] = sorted(
        top_payouts_list, key=lambda x: -x["payback"]
    )[:10]

    return stats


# ────────────────────────────────────────────────────────────────
# 公開 API
# ────────────────────────────────────────────────────────────────

def get_hybrid_summary(year_filter: str = "all", force_refresh: bool = False) -> dict:
    """新戦略ハイブリッド成績集計 (三連複動的 + 単勝 T-4 + M' 戦略 + 馬連/三連複 post-hoc) を返す。

    30 分 TTL キャッシュ。force_refresh=True でキャッシュ無視。

    M' 戦略 (Phase 6 対応):
        pred.json が M' フォーマットの日付は m_prime_sanrenpuku に集計。
        旧 T-050 フォーマットの日付は tansho_t4 / sanrenpuku_dynamic に集計。
        両者は format 判定で自動振り分けされるため、混在期間でも正しく集計される。

    WF post-hoc 集計:
        umaren_5tickets          → WF pred.json アーカイブ (全期間)
        sanrenpuku_7tickets      → WF pred.json アーカイブ (全期間)
        umaren_5tickets_by_year  → 年度別内訳
        sanrenpuku_7tickets_by_year → 年度別内訳

    Returns:
        {
            "tansho_t4":                    {...}  # 単勝 T-4 集計（旧 T-050 / 残置）
            "sanrenpuku_dynamic":           {...}  # 三連複動的集計（旧 T-050 / 残置）
            "m_prime_sanrenpuku":           {...}  # M' 戦略三連複集計（自信度別内訳付き）
            "umaren_5tickets":              {...}  # 馬連 5 馬券 全期間
            "sanrenpuku_7tickets":          {...}  # 三連複 7 馬券 全期間
            "umaren_5tickets_by_year":      {...}  # 年度別内訳
            "sanrenpuku_7tickets_by_year":  {...}  # 年度別内訳
        }
    """
    cache_key = year_filter or "all"
    if not force_refresh:
        cached = _CACHE.get(cache_key)
        if cached and (time.time() - cached[0]) < _CACHE_TTL:
            return cached[1]

    tansho_result       = _compute_tansho_t4(cache_key)
    sanrenpuku_result   = _compute_sanrenpuku_dynamic(cache_key)
    m_prime_result      = _compute_m_prime_sanrenpuku(cache_key)

    # WF post-hoc 集計 (年度フィルターに関わらず全期間 + 年度別を返す)
    # year_filter が指定されている場合は該当年のみ計算し、全期間はキャッシュを参照
    umaren_all       = compute_umaren_5tickets_summary("all")
    sanrenpuku_all   = compute_sanrenpuku_7tickets_summary("all")

    umaren_by_year      = {
        "all":  umaren_all,
        "2024": compute_umaren_5tickets_summary("2024"),
        "2025": compute_umaren_5tickets_summary("2025"),
        "2026": compute_umaren_5tickets_summary("2026"),
    }
    sanrenpuku_by_year  = {
        "all":  sanrenpuku_all,
        "2024": compute_sanrenpuku_7tickets_summary("2024"),
        "2025": compute_sanrenpuku_7tickets_summary("2025"),
        "2026": compute_sanrenpuku_7tickets_summary("2026"),
    }

    result = {
        "tansho_t4":                   tansho_result,
        "sanrenpuku_dynamic":          sanrenpuku_result,
        "m_prime_sanrenpuku":          m_prime_result,
        "umaren_5tickets":             umaren_all,
        "sanrenpuku_7tickets":         sanrenpuku_all,
        "umaren_5tickets_by_year":     umaren_by_year,
        "sanrenpuku_7tickets_by_year": sanrenpuku_by_year,
    }
    _CACHE[cache_key] = (time.time(), result)
    return result


def invalidate_cache() -> None:
    """結果取得 / pred 更新後に外部から呼んでキャッシュを破棄する。"""
    _CACHE.clear()


# ────────────────────────────────────────────────────────────────
# WF pred.json ロード (tar.gz アーカイブ専用)
# ────────────────────────────────────────────────────────────────

import io
import os
import tarfile as _tarfile

# WF pred.json のプロセス内キャッシュ (tar.gz は重いので 1 回だけ読み込む)
_WF_PREDS_CACHE: Optional[dict] = None

_WF_TAR_PATH = str(Path(__file__).resolve().parents[2] / "data" / "_archive" / "predictions_pre_markint_20260527.tar.gz")


def _load_wf_preds() -> dict:
    """WF pred.json を tar.gz からロードしてプロセス内にキャッシュする。

    戻り値: {date_key (YYYYMMDD): pred_dict}
    """
    global _WF_PREDS_CACHE
    if _WF_PREDS_CACHE is not None:
        return _WF_PREDS_CACHE

    out: dict = {}
    if not os.path.exists(_WF_TAR_PATH):
        _WF_PREDS_CACHE = out
        return out

    with _tarfile.open(_WF_TAR_PATH, "r:gz") as tar:
        for member in tar.getmembers():
            name = os.path.basename(member.name)
            if not name.endswith("_pred.json"):
                continue
            date_key = name[:8]
            if not date_key.isdigit() or len(date_key) != 8:
                continue
            f = tar.extractfile(member)
            if f is None:
                continue
            try:
                out[date_key] = json.load(io.TextIOWrapper(f, encoding="utf-8"))
            except Exception:
                pass

    _WF_PREDS_CACHE = out
    return out


# ────────────────────────────────────────────────────────────────
# 馬連・三連複 post-hoc 集計用 ヘルパー
# ────────────────────────────────────────────────────────────────

# 各印グループの定義
_MARK_GROUPS: dict = {
    "honmei": {"◉", "◎"},
    "taikou": {"○", "〇"},
    "renka":  {"▲"},
    "wide1":  {"△"},
    "wide2":  {"★"},
    "wide3":  {"☆"},
}

# 馬連 5 馬券定義: (ticket_id, ticket_label, 相手グループ名)
_UMAREN_TICKETS = [
    ("umaren_honmei_taikou", "◉◎-〇",  "taikou"),
    ("umaren_honmei_renka",  "◉◎-▲",  "renka"),
    ("umaren_honmei_wide1",  "◉◎-△",  "wide1"),
    ("umaren_honmei_wide2",  "◉◎-★",  "wide2"),
    ("umaren_honmei_wide3",  "◉◎-☆",  "wide3"),
]

# 三連複 7 馬券定義: (ticket_id, ticket_label, 相手グループ名リスト)
_TRIO_TICKETS = [
    ("trio_honmei_taikou_renka", "◉◎-〇-▲", ["taikou", "renka"]),
    ("trio_honmei_taikou_wide1", "◉◎-〇-△", ["taikou", "wide1"]),
    ("trio_honmei_taikou_wide2", "◉◎-〇-★", ["taikou", "wide2"]),
    ("trio_honmei_taikou_wide3", "◉◎-〇-☆", ["taikou", "wide3"]),
    ("trio_honmei_renka_wide1",  "◉◎-▲-△", ["renka",  "wide1"]),
    ("trio_honmei_renka_wide2",  "◉◎-▲-★", ["renka",  "wide2"]),
    ("trio_honmei_renka_wide3",  "◉◎-▲-☆", ["renka",  "wide3"]),
]

_CONF_LEVELS_WF = ("SS", "S", "A", "B", "C", "D", "E")


def _empty_conf_stats() -> dict:
    """confidence 別空集計 dict を生成する。"""
    return {lv: {"played": 0, "hit": 0, "stake": 0, "payback": 0}
            for lv in _CONF_LEVELS_WF}


def _finalize_ticket_stats(total: dict, by_conf: dict) -> dict:
    """集計 dict に roi_pct / hit_rate_pct / balance を追加して返す。"""
    def _derive(d: dict) -> dict:
        played = d["played"]
        stake  = d["stake"]
        pb     = d["payback"]
        return {
            "played":       played,
            "hit":          d["hit"],
            "hit_rate_pct": round(d["hit"] / played * 100, 1) if played else 0.0,
            "roi_pct":      round(pb / stake * 100, 1)        if stake  else 0.0,
            "stake":        stake,
            "payback":      pb,
            "balance":      pb - stake,
        }

    result_total = _derive(total)
    result_conf  = {}
    for lv in _CONF_LEVELS_WF:
        result_conf[lv] = _derive(by_conf[lv]) if by_conf[lv]["played"] > 0 else None
    return {"total": result_total, "by_confidence": result_conf}


def _get_first_horse_no_in_group(horses: list, group: str) -> Optional[int]:
    """印グループ内の馬番を 1 頭返す (mark priority 優先)。"""
    marks = _MARK_GROUPS.get(group, set())
    cands = [h for h in horses if h.get("mark", "") in marks]
    if not cands:
        return None
    cands.sort(key=lambda h: MARK_PRIORITY.get(h.get("mark", ""), 9))
    return cands[0].get("horse_no")


def _lookup_umaren_payout(payouts: dict, nos: set) -> int:
    """馬連払戻額を返す。nos: {horse_no1, horse_no2}"""
    bucket = payouts.get("馬連")
    if bucket is None:
        return 0
    nos_str = "-".join(str(x) for x in sorted(nos))

    def _match(item: dict) -> int:
        if not isinstance(item, dict):
            return 0
        # 馬連 combo は "小-大" 形式 (数字のみ)
        combo = item.get("combo", "")
        try:
            combo_set = set(int(x) for x in combo.split("-")) if combo else set()
        except (ValueError, AttributeError):
            return 0
        if combo_set == nos:
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


def _lookup_trio_payout_by_set(payouts: dict, nos: set) -> int:
    """三連複払戻額を返す。nos: {horse_no1, horse_no2, horse_no3}"""
    bucket = payouts.get("三連複")
    if bucket is None:
        return 0

    def _match(item: dict) -> int:
        if not isinstance(item, dict):
            return 0
        combo = item.get("combo", "")
        try:
            combo_set = set(int(x) for x in combo.split("-")) if combo else set()
        except (ValueError, AttributeError):
            return 0
        if combo_set == nos:
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


def _get_wf_results(date_key: str) -> Optional[dict]:
    """results.json を読み込む。"""
    res_dir = Path(__file__).resolve().parents[2] / "data" / "results"
    fp = res_dir / f"{date_key}_results.json"
    if not fp.exists():
        return None
    try:
        with fp.open(encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────
# 馬連 5 馬券集計
# ────────────────────────────────────────────────────────────────

def compute_umaren_5tickets_summary(
    year_filter: str = "all",
) -> dict:
    """馬連 5 馬券 × confidence 7 段階 集計 (WF pred.json アーカイブ使用)。

    データソース: data/_archive/predictions_pre_markint_20260527.tar.gz
    集計: 全 race で ◉◎-相手の馬連購入仮定 (post-hoc シミュレーション)。

    Returns:
        multi_ticket_summary dict (スキーマ仕様通り)
    """
    preds = _load_wf_preds()

    # ticket_id → {total: {...}, by_conf: {...}} の集計バッファ
    agg: dict = {}
    for tid, label, partner_group in _UMAREN_TICKETS:
        agg[tid] = {
            "label":   label,
            "total":   {"played": 0, "hit": 0, "stake": 0, "payback": 0},
            "by_conf": _empty_conf_stats(),
        }

    date_from = ""
    date_to   = ""

    for date_key in sorted(preds.keys()):
        if not _year_match(date_key, year_filter):
            continue

        results = _get_wf_results(date_key)
        if results is None:
            continue

        if not date_from or date_key < date_from:
            date_from = date_key
        if not date_to or date_key > date_to:
            date_to = date_key

        pred_data = preds[date_key]
        for race in pred_data.get("races", []):
            race_id = str(race.get("race_id", ""))
            # ばんえい除外
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue

            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue
            # 馬連データが存在しないレースはスキップ
            if payouts.get("馬連") is None:
                continue

            # 着順 map (horse_no → finish) を取得 — 真の的中判定に必要
            order = rdata.get("order", [])
            if not order:
                continue
            finish_map: dict = {}
            for r in order:
                hn = r.get("horse_no")
                fn = r.get("finish")
                if hn is not None and fn is not None:
                    finish_map[hn] = fn

            horses = race.get("horses", [])

            # confidence 取得 (空文字・未知は集計対象外)
            confidence = race.get("confidence", "") or ""
            if confidence not in _CONF_LEVELS_WF:
                continue

            # 本命馬番 (◉◎ 優先)
            honmei_no = _get_first_horse_no_in_group(horses, "honmei")
            if honmei_no is None:
                continue

            # 各馬券 (相手グループ別)
            for tid, label, partner_group in _UMAREN_TICKETS:
                partner_no = _get_first_horse_no_in_group(horses, partner_group)
                if partner_no is None or partner_no == honmei_no:
                    continue

                # 真の的中判定: 両馬とも 1-2 着 (着順ベース / payout 欠損に依存しない)
                hf = finish_map.get(honmei_no)
                pf = finish_map.get(partner_no)
                hit = (hf in (1, 2) and pf in (1, 2) and hf != pf)

                nos = {honmei_no, partner_no}
                payout_val = _lookup_umaren_payout(payouts, nos) if hit else 0

                buf = agg[tid]
                buf["total"]["played"]  += 1
                buf["total"]["stake"]   += STAKE_PER_TICKET
                buf["total"]["payback"] += payout_val
                if hit:
                    buf["total"]["hit"] += 1

                bc = buf["by_conf"][confidence]
                bc["played"]  += 1
                bc["stake"]   += STAKE_PER_TICKET
                bc["payback"] += payout_val
                if hit:
                    bc["hit"] += 1

    # スキーマ形式に整形
    tickets_out = []
    for tid, label, partner_group in _UMAREN_TICKETS:
        buf = agg[tid]
        stats = _finalize_ticket_stats(buf["total"], buf["by_conf"])
        tickets_out.append({
            "ticket_id":      tid,
            "ticket_label":   label,
            "points":         1,
            "total":          stats["total"],
            "by_confidence":  stats["by_confidence"],
        })

    # date_from / date_to を YYYY-MM-DD 形式に変換
    def _fmt_date(d: str) -> str:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d

    return {
        "ticket_type": "umaren",
        "tickets":     tickets_out,
        "date_from":   _fmt_date(date_from),
        "date_to":     _fmt_date(date_to),
    }


# ────────────────────────────────────────────────────────────────
# 三連複 7 馬券集計 (拡張)
# ────────────────────────────────────────────────────────────────

def compute_sanrenpuku_7tickets_summary(
    year_filter: str = "all",
) -> dict:
    """三連複 7 馬券 × confidence 7 段階 集計 (WF pred.json アーカイブ使用)。

    データソース: data/_archive/predictions_pre_markint_20260527.tar.gz
    集計: 全 race で ◉◎-相手2頭の三連複購入仮定 (post-hoc シミュレーション)。

    Returns:
        multi_ticket_summary dict (スキーマ仕様通り)
    """
    preds = _load_wf_preds()

    # ticket_id → 集計バッファ
    agg: dict = {}
    for tid, label, partner_groups in _TRIO_TICKETS:
        agg[tid] = {
            "label":   label,
            "total":   {"played": 0, "hit": 0, "stake": 0, "payback": 0},
            "by_conf": _empty_conf_stats(),
        }

    date_from = ""
    date_to   = ""

    for date_key in sorted(preds.keys()):
        if not _year_match(date_key, year_filter):
            continue

        results = _get_wf_results(date_key)
        if results is None:
            continue

        if not date_from or date_key < date_from:
            date_from = date_key
        if not date_to or date_key > date_to:
            date_to = date_key

        pred_data = preds[date_key]
        for race in pred_data.get("races", []):
            race_id = str(race.get("race_id", ""))
            # ばんえい除外
            if len(race_id) >= 6 and race_id[4:6] in BANEI_VENUE_CODES:
                continue

            rdata = results.get(race_id)
            if rdata is None:
                continue
            payouts = rdata.get("payouts", {})
            if not payouts:
                continue
            # 三連複データが存在しないレースはスキップ
            if payouts.get("三連複") is None:
                continue

            # 着順 map (horse_no → finish) を取得 — 真の的中判定に必要
            order = rdata.get("order", [])
            if not order:
                continue
            finish_map: dict = {}
            for r in order:
                hn = r.get("horse_no")
                fn = r.get("finish")
                if hn is not None and fn is not None:
                    finish_map[hn] = fn

            horses = race.get("horses", [])

            # confidence 取得 (空文字・未知は集計対象外)
            confidence = race.get("confidence", "") or ""
            if confidence not in _CONF_LEVELS_WF:
                continue

            # 本命馬番 (◉◎ 優先)
            honmei_no = _get_first_horse_no_in_group(horses, "honmei")
            if honmei_no is None:
                continue

            # 各馬券 (相手2グループ別)
            for tid, label, partner_groups in _TRIO_TICKETS:
                partner_nos = []
                valid = True
                for pg in partner_groups:
                    pno = _get_first_horse_no_in_group(horses, pg)
                    if pno is None or pno == honmei_no:
                        valid = False
                        break
                    if pno in partner_nos:
                        # 同一馬番が別グループに存在 → スキップ
                        valid = False
                        break
                    partner_nos.append(pno)
                if not valid:
                    continue

                nos = {honmei_no} | set(partner_nos)
                if len(nos) != 3:
                    continue

                # 真の的中判定: 3 頭全員 1-3 着 (着順ベース / payout 欠損に依存しない)
                hit = all(finish_map.get(h) in (1, 2, 3) for h in nos)
                payout_val = _lookup_trio_payout_by_set(payouts, nos) if hit else 0

                buf = agg[tid]
                buf["total"]["played"]  += 1
                buf["total"]["stake"]   += STAKE_PER_TICKET
                buf["total"]["payback"] += payout_val
                if hit:
                    buf["total"]["hit"] += 1

                bc = buf["by_conf"][confidence]
                bc["played"]  += 1
                bc["stake"]   += STAKE_PER_TICKET
                bc["payback"] += payout_val
                if hit:
                    bc["hit"] += 1

    # スキーマ形式に整形
    tickets_out = []
    for tid, label, partner_groups in _TRIO_TICKETS:
        buf = agg[tid]
        stats = _finalize_ticket_stats(buf["total"], buf["by_conf"])
        tickets_out.append({
            "ticket_id":      tid,
            "ticket_label":   label,
            "points":         1,
            "total":          stats["total"],
            "by_confidence":  stats["by_confidence"],
        })

    def _fmt_date(d: str) -> str:
        return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d

    return {
        "ticket_type": "sanrenpuku_extended",
        "tickets":     tickets_out,
        "date_from":   _fmt_date(date_from),
        "date_to":     _fmt_date(date_to),
    }
